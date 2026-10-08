from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

DEFAULT_SOURCE = Path(
    "/mnt/data/datasets/numpy/territoire-geospatial/cadastre/batiments/imope"
)
DEFAULT_OUTPUT = Path(
    "/mnt/data/datasets/parquet/territoire-geospatial/cadastre/batiments/imope"
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n")
    os.replace(tmp, path)


def validate_parquet(path: Path) -> dict[str, Any]:
    pf = pq.ParquetFile(path)
    schema = pf.schema_arrow
    metadata = schema.metadata or {}
    return {
        "path": path.name,
        "rows": pf.metadata.num_rows,
        "row_groups": pf.metadata.num_row_groups,
        "columns": len(schema),
        "bytes": path.stat().st_size,
        "geo": b"geo" in metadata,
    }


def promote_department(source: Path, staging: Path) -> dict[str, Any]:
    files = sorted(source.glob("*.parquet"))
    if not files:
        raise RuntimeError(f"No Parquet files in {source}")

    target = staging / source.name
    target.mkdir(parents=True, exist_ok=False)

    summaries: list[dict[str, Any]] = []
    for src in files:
        info = validate_parquet(src)
        dst = target / src.name
        os.link(src, dst)

        # Re-open the destination hard link, so validation covers the canonical path too.
        dst_info = validate_parquet(dst)
        if info != dst_info:
            raise RuntimeError(f"Validation mismatch after linking {src}")

        summaries.append(info)

    result = {
        "department": source.name,
        "status": "ok",
        "created_at": utc_now(),
        "source": str(source),
        "files": len(summaries),
        "rows": sum(int(item["rows"]) for item in summaries),
        "bytes": sum(int(item["bytes"]) for item in summaries),
        "geo_files": sum(bool(item["geo"]) for item in summaries),
        "parquet": summaries,
    }
    atomic_json(target / "_SUCCESS.json", result)
    return result


def promote(source_root: Path, output_root: Path) -> int:
    source_root = source_root.expanduser().resolve()
    output_root = output_root.expanduser().resolve()

    if not source_root.is_dir():
        raise FileNotFoundError(source_root)
    if output_root.exists():
        raise RuntimeError(
            f"Output already exists: {output_root}. "
            "Refusing to merge with an existing canonical dataset."
        )

    staging = output_root.with_name(output_root.name + ".staging")
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)

    departments = sorted(
        p for p in source_root.iterdir()
        if p.is_dir() and not p.name.startswith(".")
    )
    if not departments:
        raise RuntimeError(f"No department directories under {source_root}")

    manifest: list[dict[str, Any]] = []
    try:
        for i, department in enumerate(departments, start=1):
            result = promote_department(department, staging)
            manifest.append(result)
            print(
                f"[{i:02d}/{len(departments):02d}] {department.name}: "
                f"{result['files']} files, {result['rows']:,} rows, "
                f"{result['bytes'] / 2**30:.3f} GiB",
                flush=True,
            )

        dataset = {
            "dataset": "IMOPE",
            "status": "ok",
            "created_at": utc_now(),
            "source": str(source_root),
            "storage": "hard-linked promotion of validated legacy Parquet files",
            "departments": len(manifest),
            "files": sum(int(item["files"]) for item in manifest),
            "rows": sum(int(item["rows"]) for item in manifest),
            "bytes": sum(int(item["bytes"]) for item in manifest),
            "geo_files": sum(int(item["geo_files"]) for item in manifest),
        }
        atomic_json(staging / "manifest.json", manifest)
        atomic_json(staging / "dataset.json", dataset)

        os.replace(staging, output_root)
    except Exception:
        if staging.exists():
            shutil.rmtree(staging)
        raise

    print(
        f"DONE: {dataset['departments']} departments, "
        f"{dataset['files']} Parquet files, "
        f"{dataset['rows']:,} rows, "
        f"{dataset['bytes'] / 2**30:.3f} GiB",
        flush=True,
    )
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Promote legacy IMOPE Parquet files into the canonical parquet tree "
            "using validated hard links, without re-encoding or duplicating data."
        )
    )
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        return promote(args.source, args.output)
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
