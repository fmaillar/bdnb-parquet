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


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n")
    os.replace(tmp, path)


def parquet_info(path: Path, *, relative_to: Path) -> dict[str, Any]:
    pf = pq.ParquetFile(path)
    schema = pf.schema_arrow
    metadata = schema.metadata or {}
    return {
        "path": path.relative_to(relative_to).as_posix(),
        "rows": pf.metadata.num_rows,
        "row_groups": pf.metadata.num_row_groups,
        "columns": len(schema),
        "bytes": path.stat().st_size,
        "geo": b"geo" in metadata,
    }


def promote(source: Path, output: Path) -> int:
    source = source.expanduser().resolve()
    output = output.expanduser().resolve()

    if not source.is_dir():
        raise FileNotFoundError(source)
    if output.exists():
        raise RuntimeError(f"Output already exists: {output}")

    parquet_files = sorted(source.rglob("*.parquet"))
    if not parquet_files:
        raise RuntimeError(f"No Parquet files under {source}")

    staging = output.with_name(output.name + ".staging")
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)

    manifest: list[dict[str, Any]] = []
    try:
        for i, src in enumerate(parquet_files, start=1):
            info = parquet_info(src, relative_to=source)
            rel = src.relative_to(source)
            dst = staging / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            os.link(src, dst)

            dst_info = parquet_info(dst, relative_to=staging)
            if info != dst_info:
                raise RuntimeError(f"Validation mismatch after linking {src}")

            manifest.append(info)
            if i % 10 == 0 or i == len(parquet_files):
                print(
                    f"[{i}/{len(parquet_files)}] "
                    f"{sum(int(x['rows']) for x in manifest):,} rows",
                    flush=True,
                )

        dataset = {
            "status": "ok",
            "created_at": utc_now(),
            "source": str(source),
            "storage": "hard-linked promotion of validated Parquet files",
            "files": len(manifest),
            "rows": sum(int(item["rows"]) for item in manifest),
            "bytes": sum(int(item["bytes"]) for item in manifest),
            "geo_files": sum(bool(item["geo"]) for item in manifest),
        }
        atomic_json(staging / "manifest.json", manifest)
        atomic_json(staging / "dataset.json", dataset)
        os.replace(staging, output)
    except Exception:
        if staging.exists():
            shutil.rmtree(staging)
        raise

    print(
        f"DONE: {dataset['files']} Parquet files, "
        f"{dataset['rows']:,} rows, "
        f"{dataset['bytes'] / 2**30:.3f} GiB",
        flush=True,
    )
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Promote an existing Parquet tree into the canonical parquet layer "
            "with validation and hard links, without re-encoding."
        )
    )
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
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
