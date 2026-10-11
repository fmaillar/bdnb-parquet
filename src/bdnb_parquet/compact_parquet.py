"""Read-only planner for conservative Parquet file compaction.

Candidates share an exact Arrow schema and parent directory. This is NOT
sufficient proof of semantic compatibility: every merge requires review.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pyarrow.parquet as pq

MIB = 1024**2
DEFAULT_ROOT = Path("/mnt/data/datasets/parquet")


@dataclass(frozen=True, slots=True)
class FileEntry:
    path: str
    directory: str
    bytes: int
    rows: int
    row_groups: int
    schema_hash: str
    partial: bool


@dataclass(frozen=True, slots=True)
class PlanBatch:
    group_id: str
    batch_id: str
    directory: str
    schema_hash: str
    status: str
    files: tuple[FileEntry, ...]

    @property
    def bytes(self) -> int:
        return sum(item.bytes for item in self.files)

    @property
    def rows(self) -> int:
        return sum(item.rows for item in self.files)


def inspect_file(path: Path, root: Path) -> FileEntry:
    """Read only a Parquet footer and Arrow schema; never load column values."""
    if path.is_symlink():
        raise ValueError(f"Refusing symlinked Parquet file: {path}")
    parquet = pq.ParquetFile(path)
    try:
        # Preserve all Arrow schema/field metadata, including pandas and geo.
        # This deliberately errs on the side of splitting compatible files.
        payload = parquet.schema_arrow.serialize().to_pybytes()
        metadata = parquet.metadata
        stat = path.stat()
        relative = path.relative_to(root).as_posix()
        return FileEntry(
            path=relative,
            directory=Path(relative).parent.as_posix(),
            bytes=stat.st_size,
            rows=int(metadata.num_rows),
            row_groups=int(metadata.num_row_groups),
            schema_hash=hashlib.sha256(payload).hexdigest(),
            partial=path.name.endswith(".partial.parquet"),
        )
    finally:
        parquet.close()


def scan(root: Path) -> list[FileEntry]:
    if not root.is_dir():
        raise NotADirectoryError(root)
    entries: list[FileEntry] = []
    for path in sorted(root.rglob("*.parquet")):
        if not path.is_file():
            raise ValueError(f"Not a regular Parquet file: {path}")
        try:
            entries.append(inspect_file(path, root))
        except Exception as exc:
            raise RuntimeError(f"Cannot inspect {path}: {exc}") from exc
    return entries


def plan(entries: Iterable[FileEntry], target_bytes: int) -> list[PlanBatch]:
    """Estimate output counts using original compressed sizes.

    Source size is only a packing heuristic; recompressed sizes are unknown.
    Never join entries from different directories or different Arrow schemas.
    """
    if target_bytes < MIB:
        raise ValueError("target_bytes must be at least 1 MiB")

    grouped: dict[tuple[str, str], list[FileEntry]] = defaultdict(list)
    for entry in entries:
        grouped[(entry.directory, entry.schema_hash)].append(entry)

    batches: list[PlanBatch] = []
    for (directory, schema_hash), members in sorted(grouped.items()):
        group_id = hashlib.sha256(
            (directory + "\0" + schema_hash).encode("utf-8")
        ).hexdigest()
        pending: list[FileEntry] = []
        pending_bytes = 0
        index = 0

        def emit(files: list[FileEntry]) -> None:
            nonlocal index
            index += 1
            if len(files) > 1:
                status = "review-required"
            elif files[0].partial:
                status = "skip-partial"
            elif files[0].bytes > target_bytes:
                status = "keep-oversize"
            else:
                status = "keep"
            batches.append(
                PlanBatch(
                    group_id=group_id,
                    batch_id=f"{group_id}-{index:04d}",
                    directory=directory,
                    schema_hash=schema_hash,
                    status=status,
                    files=tuple(files),
                )
            )

        for entry in sorted(members, key=lambda item: item.path):
            # Incomplete outputs never enter a merge candidate.
            if entry.partial or entry.bytes > target_bytes:
                if pending:
                    emit(pending)
                    pending = []
                    pending_bytes = 0
                emit([entry])
                continue

            if pending and pending_bytes + entry.bytes > target_bytes:
                emit(pending)
                pending = []
                pending_bytes = 0
            pending.append(entry)
            pending_bytes += entry.bytes

        if pending:
            emit(pending)
    return batches


def write_tsv(path: Path, fieldnames: list[str], rows: Iterable[dict[str, object]]) -> None:
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(
            stream, fieldnames=fieldnames, delimiter="\t", lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def write_report(
    root: Path, output_dir: Path, target_bytes: int, batches: list[PlanBatch]
) -> dict[str, object]:
    """Write plans to a new directory outside the source tree."""
    if output_dir == root or root in output_dir.parents:
        raise ValueError("Output directory must be outside the source tree")
    if output_dir.exists() and (not output_dir.is_dir() or any(output_dir.iterdir())):
        raise FileExistsError(f"Output directory is not empty: {output_dir}")

    source_files = sum(len(batch.files) for batch in batches)
    candidate_batches = [b for b in batches if b.status == "review-required"]
    total_bytes = sum(batch.bytes for batch in batches)
    summary: dict[str, object] = {
        "source_root": str(root),
        "target_mib": target_bytes // MIB,
        "method": "parent-directory + exact serialized Arrow schema",
        "caveat": (
            "Candidates require semantic review. Output file counts use input "
            "compressed sizes, not predicted recompressed sizes."
        ),
        "source_files": source_files,
        "source_bytes": total_bytes,
        "source_gib": round(total_bytes / 1024**3, 6),
        "source_rows": sum(batch.rows for batch in batches),
        "source_directories": len({batch.directory for batch in batches}),
        "source_schemas": len({batch.schema_hash for batch in batches}),
        "estimated_output_files": len(batches),
        "potential_file_reduction": source_files - len(batches),
        "candidate_batches": len(candidate_batches),
        "candidate_input_files": sum(len(batch.files) for batch in candidate_batches),
        "skipped_partial_files": sum(
            batch.status == "skip-partial" for batch in batches
        ),
        "oversize_files": sum(batch.status == "keep-oversize" for batch in batches),
    }

    # No output files are created until scanning/planning has succeeded.
    output_dir.mkdir(parents=True, exist_ok=True)
    write_tsv(
        output_dir / "batches.tsv",
        [
            "batch_id", "group_id", "directory", "schema_hash", "status",
            "source_files", "source_bytes", "source_rows",
        ],
        (
            {
                "batch_id": batch.batch_id,
                "group_id": batch.group_id,
                "directory": batch.directory,
                "schema_hash": batch.schema_hash,
                "status": batch.status,
                "source_files": len(batch.files),
                "source_bytes": batch.bytes,
                "source_rows": batch.rows,
            }
            for batch in batches
        ),
    )
    write_tsv(
        output_dir / "files.tsv",
        [
            "batch_id", "status", "path", "directory", "schema_hash",
            "bytes", "rows", "row_groups", "partial",
        ],
        (
            {
                "batch_id": batch.batch_id,
                "status": batch.status,
                "path": item.path,
                "directory": item.directory,
                "schema_hash": item.schema_hash,
                "bytes": item.bytes,
                "rows": item.rows,
                "row_groups": item.row_groups,
                "partial": int(item.partial),
            }
            for batch in batches
            for item in batch.files
        ),
    )
    temporary = output_dir / "summary.json.tmp"
    temporary.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, output_dir / "summary.json")
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Plan (never execute) conservative Parquet compaction."
    )
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--target-mib", type=int, default=256,
        help="target estimated compressed size per output (default: 256 MiB)",
    )
    args = parser.parse_args(argv)
    if args.target_mib < 1:
        parser.error("--target-mib must be >= 1")

    root = args.root.expanduser().resolve()
    output = args.output_dir.expanduser().resolve()
    if output == root or root in output.parents:
        parser.error("--output-dir must be outside --root")

    try:
        entries = scan(root)
        batches = plan(entries, args.target_mib * MIB)
        summary = write_report(root, output, args.target_mib * MIB, batches)
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    for key, value in summary.items():
        if key not in {"caveat", "method"}:
            print(f"{key.upper()}: {value}")
    print(f"PLAN: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
