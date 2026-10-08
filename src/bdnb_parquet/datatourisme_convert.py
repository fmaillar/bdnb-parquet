from __future__ import annotations

import argparse
import concurrent.futures
import io
import json
import os
import re
import shutil
import sys
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TextIO

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

DEFAULT_SOURCE = Path(
    "/mnt/data/datasets/raw/tourisme/offre-nationale/datatourisme"
)
DEFAULT_OUTPUT = Path(
    "/mnt/data/datasets/parquet/tourisme/offre-nationale/datatourisme"
)

NT_SCHEMA = pa.schema(
    [
        pa.field("subject", pa.string(), nullable=False),
        pa.field("predicate", pa.string(), nullable=False),
        pa.field("object", pa.string(), nullable=False),
        pa.field("object_kind", pa.string(), nullable=False),
    ]
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n")
    os.replace(tmp, path)


def slug(text: str) -> str:
    value = text.lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-") or "dataset"


def parse_ntriple_line(line: str) -> tuple[str, str, str, str]:
    line = line.rstrip("\r\n")
    if not line:
        raise ValueError("empty N-Triples line")
    if not line.endswith(" ."):
        raise ValueError(f"invalid N-Triples terminator: {line[:120]!r}")

    body = line[:-2]
    first = body.find(" ")
    if first <= 0:
        raise ValueError(f"invalid N-Triples subject: {line[:120]!r}")
    second = body.find(" ", first + 1)
    if second <= first + 1:
        raise ValueError(f"invalid N-Triples predicate: {line[:120]!r}")

    subject = body[:first]
    predicate = body[first + 1 : second]
    obj = body[second + 1 :]
    if not obj:
        raise ValueError(f"empty N-Triples object: {line[:120]!r}")

    if obj.startswith("<"):
        kind = "iri"
    elif obj.startswith("_:"):
        kind = "bnode"
    elif obj.startswith('"'):
        kind = "literal"
    else:
        kind = "other"

    return subject, predicate, obj, kind


def parquet_is_valid(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        pf = pq.ParquetFile(path)
        return {
            "rows": pf.metadata.num_rows,
            "row_groups": pf.metadata.num_row_groups,
            "bytes": path.stat().st_size,
        }
    except Exception:
        path.unlink(missing_ok=True)
        return None


def nt_snapshot_name(path: Path) -> str:
    name = path.name
    if name.endswith(".nt.zip"):
        name = name[:-7]
    return slug(name)


def convert_nt_zip(
    source: Path,
    output_root: Path,
    *,
    batch_rows: int,
    row_group_rows: int,
) -> dict[str, Any]:
    snapshot = nt_snapshot_name(source)
    target_dir = output_root / "ntriples" / snapshot
    success_path = target_dir / "_SUCCESS.json"

    if success_path.exists():
        try:
            success = json.loads(success_path.read_text())
            parts = sorted(target_dir.glob("part-*.parquet"))
            if (
                success.get("status") == "ok"
                and len(parts) == success.get("parts")
                and all(parquet_is_valid(p) is not None for p in parts)
            ):
                return {**success, "resumed": True}
        except Exception:
            pass

    staging = output_root / ".staging" / snapshot
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(source) as zf:
        members = [
            info for info in zf.infolist()
            if not info.is_dir() and info.filename.lower().endswith(".nt")
        ]
        if len(members) != 1:
            raise RuntimeError(
                f"{source.name}: expected exactly one .nt member, got {len(members)}"
            )
        member = members[0]

        rows = 0
        part_no = 0
        part_rows: list[tuple[str, str, str, str]] = []
        part_files: list[Path] = []

        def flush() -> None:
            nonlocal part_no, part_rows
            if not part_rows:
                return
            cols = list(zip(*part_rows, strict=True))
            table = pa.Table.from_arrays(
                [
                    pa.array(cols[0], type=pa.string()),
                    pa.array(cols[1], type=pa.string()),
                    pa.array(cols[2], type=pa.string()),
                    pa.array(cols[3], type=pa.string()),
                ],
                schema=NT_SCHEMA,
            )
            target = staging / f"part-{part_no:05d}.parquet"
            pq.write_table(
                table,
                target,
                compression="zstd",
                compression_level=3,
                use_dictionary=True,
                write_statistics=True,
                row_group_size=row_group_rows,
            )
            part_files.append(target)
            part_no += 1
            part_rows = []

        with zf.open(member) as raw:
            text = io.TextIOWrapper(
                raw,
                encoding="utf-8",
                errors="strict",
                newline="",
            )
            for line_no, line in enumerate(text, start=1):
                if not line.strip():
                    continue
                try:
                    part_rows.append(parse_ntriple_line(line))
                except Exception as exc:
                    raise RuntimeError(
                        f"{source.name}:{line_no}: {exc}"
                    ) from exc
                rows += 1
                if len(part_rows) >= batch_rows:
                    flush()
                    if rows % 5_000_000 == 0:
                        print(
                            f"{source.name}: {rows:,} triples",
                            flush=True,
                        )
            flush()

    footer_rows = sum(
        pq.ParquetFile(path).metadata.num_rows
        for path in part_files
    )
    if footer_rows != rows:
        raise RuntimeError(
            f"{source.name}: footer rows {footer_rows} != parsed rows {rows}"
        )

    result = {
        "status": "ok",
        "created_at": utc_now(),
        "source": str(source),
        "source_member": member.filename,
        "rows": rows,
        "parts": len(part_files),
        "bytes": sum(path.stat().st_size for path in part_files),
        "schema": ["subject", "predicate", "object", "object_kind"],
    }
    atomic_json(staging / "_SUCCESS.json", result)

    target_dir.parent.mkdir(parents=True, exist_ok=True)
    if target_dir.exists():
        shutil.rmtree(target_dir)
    os.replace(staging, target_dir)
    return result


def convert_nt_worker(
    source: Path,
    output_root: Path,
    batch_rows: int,
    row_group_rows: int,
) -> tuple[str, dict[str, Any]]:
    return (
        source.name,
        convert_nt_zip(
            source,
            output_root,
            batch_rows=batch_rows,
            row_group_rows=row_group_rows,
        ),
    )


def convert_csv_file(source: Path, output_root: Path) -> dict[str, Any]:
    target = output_root / "csv" / f"{slug(source.stem)}.parquet"
    existing = parquet_is_valid(target)
    if existing is not None:
        return {
            "source": source.name,
            "output": target.relative_to(output_root).as_posix(),
            "resumed": True,
            **existing,
        }

    target.parent.mkdir(parents=True, exist_ok=True)
    reader = pacsv.open_csv(
        source,
        read_options=pacsv.ReadOptions(block_size=16 * 1024 * 1024),
        convert_options=pacsv.ConvertOptions(
            strings_can_be_null=True,
            null_values=["", "null", "NULL"],
        ),
    )

    writer: pq.ParquetWriter | None = None
    rows = 0
    row_groups = 0
    try:
        for batch in reader:
            if writer is None:
                writer = pq.ParquetWriter(
                    target,
                    batch.schema,
                    compression="zstd",
                    compression_level=3,
                    use_dictionary=True,
                    write_statistics=True,
                )
            writer.write_batch(batch)
            rows += batch.num_rows
            row_groups += 1
    finally:
        if writer is not None:
            writer.close()

    if writer is None:
        raise RuntimeError(f"No rows read from CSV: {source}")

    pf = pq.ParquetFile(target)
    if pf.metadata.num_rows != rows:
        raise RuntimeError(f"CSV row-count mismatch: {source}")

    return {
        "source": source.name,
        "output": target.relative_to(output_root).as_posix(),
        "rows": rows,
        "row_groups": row_groups,
        "bytes": target.stat().st_size,
        "resumed": False,
    }


def convert(
    source_root: Path,
    output_root: Path,
    *,
    workers: int,
    batch_rows: int,
    row_group_rows: int,
) -> int:
    source_root = source_root.expanduser().resolve()
    output_root = output_root.expanduser().resolve()
    if not source_root.is_dir():
        raise FileNotFoundError(source_root)

    output_root.mkdir(parents=True, exist_ok=True)
    (output_root / ".staging").mkdir(parents=True, exist_ok=True)

    nt_files = sorted(source_root.glob("*.nt.zip"))
    csv_files = sorted(source_root.glob("*.csv"))

    nt_results: list[dict[str, Any]] = []
    if workers == 1:
        for i, source in enumerate(nt_files, start=1):
            result = convert_nt_zip(
                source,
                output_root,
                batch_rows=batch_rows,
                row_group_rows=row_group_rows,
            )
            nt_results.append(result)
            print(
                f"[NT {i}/{len(nt_files)}] {source.name}: "
                f"{'SKIP ' if result.get('resumed') else ''}"
                f"{result['rows']:,} triples, {result['parts']} parts",
                flush=True,
            )
    else:
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=workers
        ) as executor:
            future_to_source = {
                executor.submit(
                    convert_nt_worker,
                    source,
                    output_root,
                    batch_rows,
                    row_group_rows,
                ): source
                for source in nt_files
            }
            done = 0
            for future in concurrent.futures.as_completed(future_to_source):
                source = future_to_source[future]
                try:
                    _name, result = future.result()
                except Exception as exc:
                    raise RuntimeError(
                        f"{source.name}: conversion failed: {exc!r}"
                    ) from exc
                nt_results.append(result)
                done += 1
                print(
                    f"[NT {done}/{len(nt_files)}] {source.name}: "
                    f"{'SKIP ' if result.get('resumed') else ''}"
                    f"{result['rows']:,} triples, {result['parts']} parts",
                    flush=True,
                )

    csv_results: list[dict[str, Any]] = []
    for i, source in enumerate(csv_files, start=1):
        result = convert_csv_file(source, output_root)
        csv_results.append(result)
        print(
            f"[CSV {i}/{len(csv_files)}] {source.name}: "
            f"{'SKIP ' if result.get('resumed') else ''}"
            f"{result['rows']:,} rows",
            flush=True,
        )

    manifest = {
        "dataset": "DataTourisme",
        "status": "ok",
        "created_at": utc_now(),
        "source": str(source_root),
        "ntriples": nt_results,
        "csv": csv_results,
        "ntriples_files": len(nt_results),
        "csv_files": len(csv_results),
        "rows": sum(int(x["rows"]) for x in nt_results)
        + sum(int(x["rows"]) for x in csv_results),
        "bytes": sum(int(x["bytes"]) for x in nt_results)
        + sum(int(x["bytes"]) for x in csv_results),
        "workers": workers,
        "ntriples_policy": (
            "lossless lexical N-Triples columns: subject, predicate, object, object_kind"
        ),
    }
    atomic_json(output_root / "manifest.json", manifest)
    atomic_json(
        output_root / "dataset.json",
        {
            key: value
            for key, value in manifest.items()
            if key not in {"ntriples", "csv"}
        },
    )

    shutil.rmtree(output_root / ".staging", ignore_errors=True)

    print(
        f"DONE: {len(nt_results)} N-Triples snapshots, "
        f"{len(csv_results)} CSV files, "
        f"{manifest['rows']:,} rows, "
        f"{manifest['bytes'] / 2**30:.3f} GiB",
        flush=True,
    )
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert DataTourisme N-Triples ZIP snapshots and CSV files to Parquet."
    )
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--workers",
        type=int,
        default=min(3, os.cpu_count() or 1),
        help="Parallel .nt.zip snapshot workers; default=min(3, CPU count)",
    )
    parser.add_argument(
        "--batch-rows",
        type=int,
        default=500_000,
        help="N-Triples rows per Parquet part",
    )
    parser.add_argument(
        "--row-group-rows",
        type=int,
        default=100_000,
        help="Rows per Parquet row group",
    )
    args = parser.parse_args(argv)
    if args.workers <= 0:
        parser.error("--workers must be positive")
    if args.batch_rows <= 0 or args.row_group_rows <= 0:
        parser.error("row counts must be positive")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        return convert(
            args.source,
            args.output,
            workers=args.workers,
            batch_rows=args.batch_rows,
            row_group_rows=args.row_group_rows,
        )
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
