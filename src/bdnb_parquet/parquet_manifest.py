from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
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


def load_manifest(path: Path) -> dict[str, Entry]:
    if not path.is_file():
        raise FileNotFoundError(path)
    result: dict[str, Entry] = {}
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            entry = Entry(
                path=row["path"],
                bytes=int(row["bytes"]),
                rows=int(row["rows"]),
                row_groups=int(row["row_groups"]),
                columns=int(row["columns"]),
                schema_hash=row["schema_hash"],
                partial=row["partial"] in {"1", "true", "True"},
                sha256=row.get("sha256", ""),
            )
            result[entry.path] = entry
    return result


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


def build(
    root: Path,
    *,
    checksum: bool,
    reuse: dict[str, Entry] | None = None,
    workers: int = 1,
) -> tuple[list[Entry], list[dict[str, str]], int, int]:
    entries: list[Entry] = []
    errors: list[dict[str, str]] = []
    reused = 0
    to_inspect: list[Path] = []

    for path in sorted(root.rglob("*.parquet")):
        rel = path.relative_to(root).as_posix()
        try:
            size = path.stat().st_size
            previous = reuse.get(rel) if reuse is not None else None
            can_reuse = (
                previous is not None
                and previous.bytes == size
                and (not checksum or bool(previous.sha256))
            )
            if can_reuse:
                entries.append(previous)
                reused += 1
            else:
                to_inspect.append(path)
        except Exception as exc:
            errors.append(
                {
                    "path": rel,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )

    if workers <= 1:
        for path in to_inspect:
            rel = path.relative_to(root).as_posix()
            try:
                entries.append(inspect(path, root, checksum=checksum))
            except Exception as exc:
                errors.append(
                    {
                        "path": rel,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(inspect, path, root, checksum=checksum): path
                for path in to_inspect
            }
            for future in as_completed(futures):
                path = futures[future]
                rel = path.relative_to(root).as_posix()
                try:
                    entries.append(future.result())
                except Exception as exc:
                    errors.append(
                        {
                            "path": rel,
                            "error": f"{type(exc).__name__}: {exc}",
                        }
                    )

    entries.sort(key=lambda e: e.path)
    return entries, errors, reused, len(to_inspect)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build an auditable manifest of a Parquet corpus.")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--checksum",
        action="store_true",
        help="compute SHA-256 for every Parquet file; this reads the entire corpus",
    )
    parser.add_argument(
        "--reuse",
        type=Path,
        help=(
            "reuse metadata from an existing manifest when relative path and "
            "file size are unchanged"
        ),
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="parallel file inspections; use 3 on the HDD-backed m710s",
    )
    args = parser.parse_args(argv)
    if args.workers <= 0:
        parser.error("--workers must be positive")

    root = args.root.expanduser().resolve()
    reuse = load_manifest(args.reuse.expanduser().resolve()) if args.reuse else None
    entries, errors, reused, inspected = build(
        root,
        checksum=args.checksum,
        reuse=reuse,
        workers=args.workers,
    )

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
    print(f"REUSED: {reused}")
    print(f"INSPECTED: {inspected}")
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
