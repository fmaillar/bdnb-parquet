from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

DEFAULT_ROOT = Path("/mnt/data/datasets/parquet")


@dataclass(frozen=True)
class Entry:
    path: str
    bytes: int
    rows: int
    row_groups: int
    columns: int
    schema_hash: str
    partial: bool
    sha256: str


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def schema_hash(path: Path) -> str:
    pf = pq.ParquetFile(path)
    payload = pf.schema_arrow.to_string(show_field_metadata=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(16 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def inspect(path: Path, root: Path, *, checksum: bool) -> Entry:
    pf = pq.ParquetFile(path)
    meta = pf.metadata
    return Entry(
        path=path.relative_to(root).as_posix(),
        bytes=path.stat().st_size,
        rows=int(meta.num_rows),
        row_groups=int(meta.num_row_groups),
        columns=int(meta.num_columns),
        schema_hash=schema_hash(path),
        partial=path.name.endswith(".partial.parquet"),
        sha256=file_sha256(path) if checksum else "",
    )


def atomic_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def write_tsv(entries: list[Entry], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, delimiter="\t", lineterminator="\n")
        writer.writerow(
            [
                "path",
                "bytes",
                "rows",
                "row_groups",
                "columns",
                "schema_hash",
                "partial",
                "sha256",
            ]
        )
        for e in entries:
            writer.writerow(
                [
                    e.path,
                    e.bytes,
                    e.rows,
                    e.row_groups,
                    e.columns,
                    e.schema_hash,
                    int(e.partial),
                    e.sha256,
                ]
            )
    os.replace(tmp, path)


def build(root: Path, *, checksum: bool) -> tuple[list[Entry], list[dict[str, str]]]:
    entries: list[Entry] = []
    errors: list[dict[str, str]] = []

    for path in sorted(root.rglob("*.parquet")):
        try:
            entries.append(inspect(path, root, checksum=checksum))
        except Exception as exc:
            errors.append(
                {
                    "path": path.relative_to(root).as_posix(),
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
    return entries, errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build an auditable manifest of a Parquet corpus.")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--checksum",
        action="store_true",
        help="compute SHA-256 for every Parquet file; this reads the entire corpus",
    )
    args = parser.parse_args(argv)

    root = args.root.expanduser().resolve()
    entries, errors = build(root, checksum=args.checksum)

    total_bytes = sum(e.bytes for e in entries)
    total_rows = sum(e.rows for e in entries)
    schema_counts = Counter(e.schema_hash for e in entries)

    tsv_path = args.output
    json_path = args.output.with_suffix(".json")
    write_tsv(entries, tsv_path)
    atomic_json(
        json_path,
        {
            "created_at": utc_now(),
            "root": str(root),
            "checksum": args.checksum,
            "files": len(entries),
            "bytes": total_bytes,
            "gib": total_bytes / 2**30,
            "rows": total_rows,
            "partial_files": sum(e.partial for e in entries),
            "unique_schemas": len(schema_counts),
            "errors": errors,
        },
    )

    print(f"FILES: {len(entries)}")
    print(f"SIZE: {total_bytes / 2**30:.3f} GiB")
    print(f"ROWS: {total_rows}")
    print(f"PARTIAL: {sum(e.partial for e in entries)}")
    print(f"SCHEMAS: {len(schema_counts)}")
    print(f"ERRORS: {len(errors)}")
    print(f"TSV: {tsv_path}")
    print(f"JSON: {json_path}")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
