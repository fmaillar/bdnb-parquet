"""Stream one approved Parquet compaction batch into a separate output tree.

The source is immutable. Outputs become visible only after a full row-level
comparison with the source sequence; restart verifies completed outputs.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterator

import pyarrow as pa
import pyarrow.parquet as pq

DEFAULT_BATCH_ROWS = 16384
DEFAULT_ROW_GROUP_ROWS = 100000
DEFAULT_MAX_BUFFER_MIB = 128
MIB = 1024**2
BATCH_ID_PATTERN = re.compile(r"[0-9a-f]{64}-[0-9]{4}\Z")


@dataclass(frozen=True, slots=True)
class Source:
    relative: str
    path: Path
    bytes: int
    rows: int
    schema_hash: str


@dataclass(frozen=True, slots=True)
class Selection:
    batch_id: str
    directory: str
    schema_hash: str
    sources: tuple[Source, ...]
    planned_files: int
    pilot: bool


def relative_path(raw: str) -> Path:
    posix = PurePosixPath(raw)
    if (
        not raw
        or posix.is_absolute()
        or not posix.parts
        or any(part in {".", ".."} for part in posix.parts)
        or "\\" in raw
    ):
        raise ValueError(f"Unsafe relative path in plan: {raw!r}")
    return Path(*posix.parts)


def relative_directory_path(raw: str) -> Path:
    """A dataset directory may legitimately be the corpus root ('.')."""
    if raw == ".":
        return Path(".")
    return relative_path(raw)


def reject_overlapping_trees(source: Path, output: Path) -> None:
    if source == output or source in output.parents or output in source.parents:
        raise ValueError("Source and output trees must not overlap")


def read_selection(
    plan_dir: Path, source_root: Path, batch_id: str, max_files: int | None
) -> Selection:
    if not BATCH_ID_PATTERN.fullmatch(batch_id):
        raise ValueError("Invalid batch ID")
    if max_files is not None and max_files < 2:
        raise ValueError("--max-files must be at least 2")

    summary = json.loads((plan_dir / "summary.json").read_text(encoding="utf-8"))
    if Path(summary["source_root"]).resolve() != source_root:
        raise ValueError("Plan was generated for a different source root")

    with (plan_dir / "batches.tsv").open(newline="", encoding="utf-8") as fh:
        selected = [
            item for item in csv.DictReader(fh, delimiter="\t")
            if item["batch_id"] == batch_id
        ]
    if len(selected) != 1 or selected[0]["status"] != "review-required":
        raise ValueError("Batch must exist uniquely with review-required status")
    row = selected[0]
    directory = row["directory"]
    relative_directory_path(directory)
    schema_hash = row["schema_hash"]

    with (plan_dir / "files.tsv").open(newline="", encoding="utf-8") as fh:
        members = [
            item for item in csv.DictReader(fh, delimiter="\t")
            if item["batch_id"] == batch_id
        ]
    if len(members) != int(row["source_files"]) or len(members) < 2:
        raise ValueError("Inconsistent file count between batch and file manifests")
    if sum(int(item["bytes"]) for item in members) != int(row["source_bytes"]):
        raise ValueError("Inconsistent byte counts in plan")
    if sum(int(item["rows"]) for item in members) != int(row["source_rows"]):
        raise ValueError("Inconsistent row counts in plan")

    sources: list[Source] = []
    seen: set[str] = set()
    for item in members:
        relative = item["path"]
        relative_parsed = relative_path(relative)
        if relative in seen:
            raise ValueError(f"Duplicate source file in plan: {relative}")
        seen.add(relative)
        if (
            item["directory"] != directory
            or relative_parsed.parent.as_posix() != directory
            or item["schema_hash"] != schema_hash
            or item["status"] != "review-required"
            or item["partial"] != "0"
        ):
            raise ValueError(f"Inconsistent planned source: {relative}")
        path = source_root / relative_parsed
        if path.is_symlink() or not path.resolve().is_relative_to(source_root):
            raise ValueError(f"Unsafe source path: {path}")
        sources.append(Source(
            relative=relative,
            path=path,
            bytes=int(item["bytes"]),
            rows=int(item["rows"]),
            schema_hash=schema_hash,
        ))

    if max_files is not None:
        sources = sources[:max_files]
    if len(sources) < 2:
        raise ValueError("Need at least two files for a compaction")
    return Selection(
        batch_id=batch_id,
        directory=directory,
        schema_hash=schema_hash,
        sources=tuple(sources),
        planned_files=len(members),
        pilot=len(sources) < len(members),
    )


def inspect_sources(selection: Selection) -> pa.Schema:
    reference: pa.Schema | None = None
    for item in selection.sources:
        if not item.path.is_file() or item.path.stat().st_size != item.bytes:
            raise ValueError(f"Source missing or its size changed: {item.path}")
        parquet = pq.ParquetFile(item.path)
        try:
            schema = parquet.schema_arrow
            signature = hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()
            if signature != item.schema_hash or parquet.metadata.num_rows != item.rows:
                raise ValueError(f"Source metadata differs from plan: {item.path}")
            if reference is None:
                reference = schema
            elif not reference.equals(schema, check_metadata=True):
                raise ValueError(f"Incompatible schema: {item.path}")
        finally:
            parquet.close()
    assert reference is not None
    return reference


@contextmanager
def arrow_cpu_threads(threads: int) -> Iterator[None]:
    """Configure Arrow's process-global CPU pool during a single CLI run."""
    if threads < 1:
        raise ValueError("--threads must be positive")
    previous = pa.cpu_count()
    try:
        if threads != previous:
            pa.set_cpu_count(threads)
        yield
    finally:
        if threads != previous:
            pa.set_cpu_count(previous)


def batches(
    paths: tuple[Path, ...], batch_rows: int, threads: int = 1,
    progress: bool = False,
) -> Iterator[pa.RecordBatch]:
    for index, path in enumerate(paths, start=1):
        parquet = pq.ParquetFile(path)
        try:
            yield from parquet.iter_batches(
                batch_size=batch_rows, use_threads=threads > 1
            )
        finally:
            parquet.close()
        if progress and (index % 10 == 0 or index == len(paths)):
            print(
                f"VERIFY [{index}/{len(paths)}] source files compared",
                file=sys.stderr, flush=True,
            )


def compare_rows(
    original: tuple[Path, ...], output: Path, batch_rows: int,
    threads: int = 1,
) -> int:
    """Compare all logical Arrow values in order, regardless of batch boundaries."""
    left = iter(batches(original, batch_rows, threads, progress=True))
    right = iter(batches((output,), batch_rows, threads))
    first = next(left, None)
    second = next(right, None)
    left_offset = right_offset = rows = 0
    while True:
        # Normalize iteration before testing exhaustion: adjacent Parquet
        # files and row groups can yield different RecordBatch boundaries.
        if first is not None and left_offset >= first.num_rows:
            first = next(left, None)
            left_offset = 0
        if second is not None and right_offset >= second.num_rows:
            second = next(right, None)
            right_offset = 0
        if first is None or second is None:
            if first is not None or second is not None:
                raise ValueError(f"Row count differs at row {rows}")
            return rows
        n = min(first.num_rows - left_offset, second.num_rows - right_offset)
        if not first.slice(left_offset, n).equals(second.slice(right_offset, n)):
            raise ValueError(f"Parquet values differ starting near row {rows}")
        rows += n
        left_offset += n
        right_offset += n


def verify(
    selection: Selection, output: Path, batch_rows: int,
    threads: int = 1,
) -> int:
    schema = inspect_sources(selection)
    parquet = pq.ParquetFile(output)
    try:
        if not schema.equals(parquet.schema_arrow, check_metadata=True):
            raise ValueError("Output Arrow schema or metadata differs")
        expected = sum(item.rows for item in selection.sources)
        if parquet.metadata.num_rows != expected:
            raise ValueError("Output footer row count differs")
    finally:
        parquet.close()

    verified = compare_rows(
        tuple(item.path for item in selection.sources), output, batch_rows, threads
    )
    if verified != expected:
        raise ValueError("Verified row count differs")
    return verified


def write_stream(
    selection: Selection,
    output: Path,
    schema: pa.Schema,
    batch_rows: int,
    zstd_level: int,
    row_group_rows: int,
    max_buffer_mib: int,
    threads: int = 1,
) -> int:
    """Buffer small read batches into larger Parquet row groups.

    The buffer is limited by both row count and approximate uncompressed
    Arrow bytes. A batch can be sliced when necessary to respect the cap.
    This bounds the *pending* buffer, not PyArrow's total process memory.
    """
    if row_group_rows <= 0 or max_buffer_mib <= 0:
        raise ValueError("Row group size and buffer budget must be positive")
    budget_bytes = max_buffer_mib * MIB
    total = 0
    pending: list[pa.RecordBatch] = []
    pending_rows = 0
    pending_bytes = 0

    with pq.ParquetWriter(
        output, schema=schema, compression="zstd",
        compression_level=zstd_level,
    ) as writer:
        def flush() -> None:
            nonlocal pending, pending_rows, pending_bytes
            if not pending:
                return
            table = pa.Table.from_batches(pending, schema=schema)
            # Exactly one row group per flush, independent of input batches.
            writer.write_table(table, row_group_size=table.num_rows)
            pending = []
            pending_rows = 0
            pending_bytes = 0

        for index, item in enumerate(selection.sources, start=1):
            parquet = pq.ParquetFile(item.path)
            try:
                for batch in parquet.iter_batches(
                    batch_size=batch_rows, use_threads=threads > 1
                ):
                    total += batch.num_rows
                    offset = 0
                    while offset < batch.num_rows:
                        take = min(
                            row_group_rows - pending_rows,
                            batch.num_rows - offset,
                        )
                        piece = batch.slice(offset, take)
                        if pending and pending_bytes + piece.nbytes > budget_bytes:
                            flush()
                            continue
                        # A single input slice may itself exceed the budget.
                        # Halve it until it fits, or report that even a single
                        # very large row cannot be buffered within the cap.
                        while piece.nbytes > budget_bytes and take > 1:
                            take = max(1, take // 2)
                            piece = batch.slice(offset, take)
                        if piece.nbytes > budget_bytes:
                            raise ValueError(
                                "A single Arrow row exceeds --max-buffer-mib"
                            )
                        pending.append(piece)
                        pending_rows += piece.num_rows
                        pending_bytes += piece.nbytes
                        offset += take
                        if pending_rows == row_group_rows:
                            flush()
            finally:
                parquet.close()
            print(
                f"WRITE [{index}/{len(selection.sources)}] "
                f"{total:,} rows, {item.relative}",
                file=sys.stderr,
                flush=True,
            )
        flush()

    if total != sum(item.rows for item in selection.sources):
        raise ValueError("Input row count changed during writing")
    return total


def output_paths(output_root: Path, selection: Selection) -> tuple[Path, Path]:
    suffix = (
        f"-pilot-{len(selection.sources)}" if selection.pilot else ""
    )
    name = f"compact-{selection.batch_id}{suffix}.parquet"
    target = output_root / relative_directory_path(selection.directory) / name
    if not target.resolve().is_relative_to(output_root):
        raise ValueError("Output directory escapes output root")
    return target, target.with_suffix(".json")


def atomic_report(path: Path, content: dict[str, object]) -> None:
    staging = path.with_suffix(path.suffix + ".tmp")
    try:
        staging.write_text(
            json.dumps(content, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        os.replace(staging, path)
    finally:
        staging.unlink(missing_ok=True)


def execute(
    selection: Selection, output_root: Path, batch_rows: int,
    zstd_level: int, verify_only: bool = False,
    row_group_rows: int = DEFAULT_ROW_GROUP_ROWS,
    max_buffer_mib: int = DEFAULT_MAX_BUFFER_MIB,
    threads: int = 1,
) -> dict[str, object]:
    if row_group_rows <= 0 or max_buffer_mib <= 0 or batch_rows <= 0:
        raise ValueError("Row group size, batch size and buffer budget must be positive")
    if threads < 1:
        raise ValueError("--threads must be positive")

    started = time.perf_counter()
    with arrow_cpu_threads(threads):
        schema = inspect_sources(selection)
        target, report = output_paths(output_root, selection)
        if not target.exists() and verify_only:
            raise FileNotFoundError(f"No compacted output to verify: {target}")
        if report.exists() and not target.exists():
            raise RuntimeError(f"Report exists without compacted output: {report}")

        write_seconds = 0.0
        verification_seconds = 0.0
        if target.exists():
            mode = "existing-output-verification"
            if not target.is_file() or target.is_symlink():
                raise ValueError(f"Output is not a regular file: {target}")
            if report.exists():
                saved = json.loads(report.read_text(encoding="utf-8"))
                if (
                    saved.get("batch_id") != selection.batch_id
                    or saved.get("source_paths")
                    != [x.relative for x in selection.sources]
                ):
                    raise ValueError(
                        "Existing report does not match current source selection"
                    )
            # Resume/--verify-only MUST rescan all logical values.
            print("VERIFY: checking existing output", file=sys.stderr, flush=True)
            verifying = time.perf_counter()
            verified = verify(selection, target, batch_rows, threads)
            verification_seconds = time.perf_counter() - verifying
        else:
            mode = "new-output"
            target.parent.mkdir(parents=True, exist_ok=True)
            if not target.parent.resolve().is_relative_to(output_root):
                raise ValueError("Output parent directory escapes output tree")
            stage = target.with_suffix(".partial")
            if stage.is_symlink():
                raise ValueError("Staging file must not be a symbolic link")
            try:
                writing = time.perf_counter()
                write_stream(
                    selection, stage, schema, batch_rows, zstd_level,
                    row_group_rows, max_buffer_mib, threads,
                )
                write_seconds = time.perf_counter() - writing

                # One complete verification is sufficient: os.replace moves
                # the same verified file without re-encoding any data.
                print(
                    "VERIFY: comparing all source values with staged output",
                    file=sys.stderr, flush=True,
                )
                verifying = time.perf_counter()
                verified = verify(selection, stage, batch_rows, threads)
                verification_seconds = time.perf_counter() - verifying
                os.replace(stage, target)
            finally:
                stage.unlink(missing_ok=True)

        output_metadata = pq.read_metadata(target)
        group_lengths = [
            output_metadata.row_group(i).num_rows
            for i in range(output_metadata.num_row_groups)
        ]
        source_bytes = sum(item.bytes for item in selection.sources)
        elapsed = time.perf_counter() - started
        result: dict[str, object] = {
            "batch_id": selection.batch_id,
            "pilot": selection.pilot,
            "planned_files": selection.planned_files,
            "source_files": len(selection.sources),
            "source_paths": [x.relative for x in selection.sources],
            "source_rows": sum(item.rows for item in selection.sources),
            "verified_rows": verified,
            "source_bytes": source_bytes,
            "output_bytes": target.stat().st_size,
            "output_row_groups": len(group_lengths),
            "output_min_row_group_rows": min(group_lengths, default=0),
            "output_max_row_group_rows": max(group_lengths, default=0),
            "output": str(target),
            "codec": "zstd",
            "threads": threads,
            "execution_mode": mode,
            "write_seconds": round(write_seconds, 3),
            "verify_seconds": round(verification_seconds, 3),
            "total_seconds": round(elapsed, 3),
            # Logical source-byte throughput, not physical HDD throughput.
            "write_source_mib_per_sec": round(
                source_bytes / MIB / write_seconds, 3
            ) if write_seconds > 0 else None,
            "verify_source_mib_per_sec": round(
                source_bytes / MIB / verification_seconds, 3
            ) if verification_seconds > 0 else None,
            "validation": "full-ordered-Arrow-record-batch-equality",
        }
        atomic_report(report, result)
        return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Stream and verify a single approved Parquet compaction batch."
    )
    parser.add_argument("--plan-dir", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--max-files", type=int, help="pilot: only first N files")
    parser.add_argument(
        "--batch-rows", type=int, default=DEFAULT_BATCH_ROWS,
        help="source read and verification batch size (default: 16384 rows)",
    )
    parser.add_argument(
        "--row-group-rows", type=int, default=DEFAULT_ROW_GROUP_ROWS,
        help="target output Parquet row group size (default: 100000 rows)",
    )
    parser.add_argument(
        "--max-buffer-mib", type=int, default=DEFAULT_MAX_BUFFER_MIB,
        help="approximate limit on pending uncompressed Arrow bytes (default: 128 MiB)",
    )
    parser.add_argument(
        "--threads", type=int, default=4,
        help="Arrow column decoding threads (default: 4)",
    )
    parser.add_argument("--zstd-level", type=int, default=3)
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args(argv)

    if args.batch_rows <= 0:
        parser.error("--batch-rows must be positive")
    if args.row_group_rows <= 0:
        parser.error("--row-group-rows must be positive")
    if args.max_buffer_mib <= 0:
        parser.error("--max-buffer-mib must be positive")
    if not 1 <= args.zstd_level <= 19:
        parser.error("--zstd-level must be in [1, 19]")
    if args.threads < 1:
        parser.error("--threads must be positive")
    source = args.source_root.expanduser().resolve()
    plan_dir = args.plan_dir.expanduser().resolve()
    output = args.output_root.expanduser().resolve()
    try:
        reject_overlapping_trees(source, output)
        reject_overlapping_trees(plan_dir, output)
        selection = read_selection(plan_dir, source, args.batch_id, args.max_files)
        result = execute(
            selection, output, args.batch_rows, args.zstd_level,
            verify_only=args.verify_only,
            row_group_rows=args.row_group_rows,
            max_buffer_mib=args.max_buffer_mib,
            threads=args.threads,
        )
    except (OSError, RuntimeError, ValueError, KeyError, TypeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
