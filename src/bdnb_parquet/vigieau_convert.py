from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shutil
import sys
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from pyogrio.raw import open_arrow
from pyproj import CRS

DEFAULT_SOURCE = Path(
    "/mnt/data/datasets/raw/climat-environnement/secheresse/vigieau"
)
DEFAULT_OUTPUT = Path(
    "/mnt/data/datasets/parquet/climat-environnement/secheresse/vigieau"
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


def geo_metadata(meta: dict[str, Any], geometry_name: str) -> bytes:
    crs = meta.get("crs")
    crs_json = CRS.from_user_input(crs).to_json_dict() if crs else None
    geometry_type = meta.get("geometry_type") or "Unknown"
    payload = {
        "version": "1.1.0",
        "primary_column": geometry_name,
        "columns": {
            geometry_name: {
                "encoding": "WKB",
                "geometry_types": [geometry_type],
                "crs": crs_json,
            }
        },
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()


def write_geojson_to_parquet(src: Path, dst: Path) -> dict[str, Any]:
    dst.parent.mkdir(parents=True, exist_ok=True)
    rows = 0
    row_groups = 0

    with open_arrow(src, use_pyarrow=True, batch_size=65_536) as source:
        meta, reader = source
        geometry_name = meta.get("geometry_name") or "wkb_geometry"
        writer: pq.ParquetWriter | None = None
        try:
            for batch in reader:
                metadata = dict(batch.schema.metadata or {})
                metadata[b"geo"] = geo_metadata(meta, geometry_name)
                batch = batch.replace_schema_metadata(metadata)
                if writer is None:
                    writer = pq.ParquetWriter(
                        dst,
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
        raise RuntimeError(f"No rows read from GeoJSON: {src}")

    pf = pq.ParquetFile(dst)
    if pf.metadata.num_rows != rows:
        raise RuntimeError(f"Row-count mismatch for {dst}")

    return {
        "rows": rows,
        "row_groups": row_groups,
        "bytes": dst.stat().st_size,
        "geo": True,
    }


def write_csv_to_parquet(src: Path, dst: Path) -> dict[str, Any]:
    dst.parent.mkdir(parents=True, exist_ok=True)

    read_options = pacsv.ReadOptions(block_size=16 * 1024 * 1024)
    convert_options = pacsv.ConvertOptions(
        strings_can_be_null=True,
        null_values=["", "null", "NULL"],
    )
    reader = pacsv.open_csv(
        src,
        read_options=read_options,
        convert_options=convert_options,
    )

    writer: pq.ParquetWriter | None = None
    rows = 0
    row_groups = 0
    try:
        for batch in reader:
            if writer is None:
                writer = pq.ParquetWriter(
                    dst,
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
        raise RuntimeError(f"No rows read from CSV: {src}")

    pf = pq.ParquetFile(dst)
    if pf.metadata.num_rows != rows:
        raise RuntimeError(f"Row-count mismatch for {dst}")

    return {
        "rows": rows,
        "row_groups": row_groups,
        "bytes": dst.stat().st_size,
        "geo": False,
    }


def convert_geojson_source(
    source: Path,
    output_root: Path,
    temp_root: Path,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    source_dir = output_root / "geojson" / slug(source.name)

    if zipfile.is_zipfile(source):
        with zipfile.ZipFile(source) as zf:
            members = [
                info for info in zf.infolist()
                if not info.is_dir() and info.filename.lower().endswith(".geojson")
            ]
            if not members:
                raise RuntimeError(f"No GeoJSON members in {source}")

            for index, info in enumerate(members, start=1):
                extracted = temp_root / Path(info.filename).name
                with zf.open(info) as src, extracted.open("wb") as dst:
                    shutil.copyfileobj(src, dst, length=16 * 1024 * 1024)

                target = source_dir / (Path(info.filename).stem + ".parquet")
                result = write_geojson_to_parquet(extracted, target)
                extracted.unlink()

                results.append(
                    {
                        "source_file": source.name,
                        "source_member": info.filename,
                        "output": target.relative_to(output_root).as_posix(),
                        **result,
                    }
                )
                print(
                    f"{source.name}: [{index}/{len(members)}] "
                    f"{result['rows']:,} rows",
                    flush=True,
                )
        return results

    target = source_dir / (slug(source.name) + ".parquet")
    result = write_geojson_to_parquet(source, target)
    results.append(
        {
            "source_file": source.name,
            "source_member": "",
            "output": target.relative_to(output_root).as_posix(),
            **result,
        }
    )
    print(f"{source.name}: {result['rows']:,} rows", flush=True)
    return results


def convert_csv_files(source_root: Path, output_root: Path) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    candidates = sorted(
        p for p in source_root.iterdir()
        if p.is_file() and p.name.startswith("Arrêtés")
    )

    for src in candidates:
        target = output_root / "arretes" / (slug(src.name) + ".parquet")
        result = write_csv_to_parquet(src, target)
        results.append(
            {
                "source_file": src.name,
                "output": target.relative_to(output_root).as_posix(),
                **result,
            }
        )
        print(f"{src.name}: {result['rows']:,} rows", flush=True)

    return results


def convert(source_root: Path, output_root: Path) -> int:
    source_root = source_root.expanduser().resolve()
    output_root = output_root.expanduser().resolve()

    if not source_root.is_dir():
        raise FileNotFoundError(source_root)
    if output_root.exists():
        raise RuntimeError(f"Output already exists: {output_root}")

    staging = output_root.with_name(output_root.name + ".staging")
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)

    geo_archives = sorted(
        p for p in source_root.iterdir()
        if p.is_file() and "GEOJSON" in p.name.upper()
    )

    manifest: list[dict[str, Any]] = []
    try:
        with tempfile.TemporaryDirectory(prefix="vigieau-", dir=staging) as temp_dir:
            temp_root = Path(temp_dir)
            for archive in geo_archives:
                manifest.extend(
                    convert_geojson_source(archive, staging, temp_root)
                )

        manifest.extend(convert_csv_files(source_root, staging))

        dataset = {
            "dataset": "Vigieau",
            "status": "ok",
            "created_at": utc_now(),
            "source": str(source_root),
            "geojson_archives": len(geo_archives),
            "parquet_files": len(manifest),
            "rows": sum(int(item["rows"]) for item in manifest),
            "bytes": sum(int(item["bytes"]) for item in manifest),
            "geo_files": sum(bool(item["geo"]) for item in manifest),
            "pmtiles_policy": "kept in raw; not converted to Parquet",
        }
        atomic_json(staging / "manifest.json", manifest)
        atomic_json(staging / "dataset.json", dataset)
        os.replace(staging, output_root)
    except Exception:
        if staging.exists():
            shutil.rmtree(staging)
        raise

    print(
        f"DONE: {dataset['parquet_files']} Parquet files, "
        f"{dataset['rows']:,} rows, "
        f"{dataset['bytes'] / 2**30:.3f} GiB",
        flush=True,
    )
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert Vigieau GeoJSON ZIP archives and CSV files to Parquet/GeoParquet."
    )
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        return convert(args.source, args.output)
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
