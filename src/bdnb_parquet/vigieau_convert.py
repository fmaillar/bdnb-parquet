from __future__ import annotations

import argparse
import concurrent.futures
import io
import csv
import json
import os
import re
import shutil
import sys
import tempfile
import struct
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from pyogrio.raw import open_arrow
from pyproj import CRS
import zstandard as zstd

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
        schema: pa.Schema | None = None

        try:
            for batch in reader:
                metadata = dict(batch.schema.metadata or {})
                metadata[b"geo"] = geo_metadata(meta, geometry_name)
                batch = batch.replace_schema_metadata(metadata)
                schema = batch.schema

                if writer is None:
                    writer = pq.ParquetWriter(
                        dst,
                        schema,
                        compression="zstd",
                        compression_level=3,
                        use_dictionary=True,
                        write_statistics=True,
                    )

                if batch.num_rows:
                    writer.write_batch(batch)
                    rows += batch.num_rows
                    row_groups += 1
        finally:
            if writer is not None:
                writer.close()

        if schema is None:
            # pyogrio may yield no batches for an empty layer. Obtain the Arrow
            # schema directly from the reader and materialize a typed empty table.
            schema = reader.schema
            metadata = dict(schema.metadata or {})
            metadata[b"geo"] = geo_metadata(meta, geometry_name)
            schema = schema.with_metadata(metadata)
            pq.write_table(
                pa.Table.from_batches([], schema=schema),
                dst,
                compression="zstd",
                compression_level=3,
                use_dictionary=True,
                write_statistics=True,
            )
        elif writer is None:
            pq.write_table(
                pa.Table.from_batches([], schema=schema),
                dst,
                compression="zstd",
                compression_level=3,
                use_dictionary=True,
                write_statistics=True,
            )

    pf = pq.ParquetFile(dst)
    if pf.metadata.num_rows != rows:
        raise RuntimeError(f"Row-count mismatch for {dst}")

    return {
        "rows": rows,
        "row_groups": row_groups,
        "bytes": dst.stat().st_size,
        "geo": True,
    }


def existing_parquet_result(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        pf = pq.ParquetFile(path)
        metadata = pf.schema_arrow.metadata or {}
        return {
            "rows": pf.metadata.num_rows,
            "row_groups": pf.metadata.num_row_groups,
            "bytes": path.stat().st_size,
            "geo": b"geo" in metadata,
        }
    except Exception:
        path.unlink(missing_ok=True)
        return None


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


def extract_zip_member(source: Path, member_name: str, target: Path) -> int:
    """Extract one ZIP member, including Zstandard-compressed ZIP entries."""
    with zipfile.ZipFile(source) as zf:
        info = zf.getinfo(member_name)
        try:
            with zf.open(info) as src, target.open("wb") as dst:
                shutil.copyfileobj(src, dst, length=16 * 1024 * 1024)
            return info.compress_type
        except NotImplementedError:
            if info.compress_type not in {20, 93}:
                raise RuntimeError(
                    f"unsupported ZIP compression method {info.compress_type}"
                )

    # Python < 3.14 cannot read ZIP Zstandard entries. Read the raw compressed
    # payload from the local file record and decompress the Zstandard frame.
    with source.open("rb") as fh:
        fh.seek(info.header_offset)
        header = fh.read(30)
        if len(header) != 30:
            raise RuntimeError("truncated ZIP local header")
        (
            signature,
            _version,
            _flags,
            method,
            _mtime,
            _mdate,
            _crc,
            _compressed_size,
            _uncompressed_size,
            filename_len,
            extra_len,
        ) = struct.unpack("<IHHHHHIIIHH", header)
        if signature != 0x04034B50:
            raise RuntimeError("invalid ZIP local header")
        if method not in {20, 93}:
            raise RuntimeError(f"unexpected ZIP compression method {method}")
        fh.seek(filename_len + extra_len, os.SEEK_CUR)

        remaining = info.compress_size
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_reader(
            io.BufferedReader(_LimitedReader(fh, remaining))
        ) as reader, target.open("wb") as dst:
            shutil.copyfileobj(reader, dst, length=16 * 1024 * 1024)

    if target.stat().st_size != info.file_size:
        raise RuntimeError(
            f"ZIP member size mismatch: expected {info.file_size}, "
            f"got {target.stat().st_size}"
        )
    return info.compress_type


class _LimitedReader(io.RawIOBase):
    def __init__(self, raw: io.BufferedReader, remaining: int) -> None:
        self.raw = raw
        self.remaining = remaining

    def readable(self) -> bool:
        return True

    def readinto(self, b: bytearray) -> int:
        if self.remaining <= 0:
            return 0
        size = min(len(b), self.remaining)
        data = self.raw.read(size)
        if not data:
            return 0
        n = len(data)
        b[:n] = data
        self.remaining -= n
        return n


def convert_zip_member_worker(
    source: Path,
    member_name: str,
    output_root: Path,
    temp_parent: Path,
) -> dict[str, Any]:
    source_dir = output_root / "geojson" / slug(source.name)
    target = source_dir / (Path(member_name).stem + ".parquet")
    existing = existing_parquet_result(target)
    if existing is not None:
        return {
            "source_file": source.name,
            "source_member": member_name,
            "output": target.relative_to(output_root).as_posix(),
            "resumed": True,
            **existing,
        }

    with tempfile.TemporaryDirectory(
        prefix="member-",
        dir=temp_parent,
    ) as temp_dir:
        extracted = Path(temp_dir) / Path(member_name).name
        compression_method = extract_zip_member(
            source,
            member_name,
            extracted,
        )
        result = write_geojson_to_parquet(extracted, target)

    return {
        "source_file": source.name,
        "source_member": member_name,
        "output": target.relative_to(output_root).as_posix(),
        "zip_compression_method": compression_method,
        **result,
    }


def convert_geojson_source(
    source: Path,
    output_root: Path,
    temp_root: Path,
    *,
    workers: int,
) -> list[dict[str, Any]]:
    source_dir = output_root / "geojson" / slug(source.name)

    if zipfile.is_zipfile(source):
        with zipfile.ZipFile(source) as zf:
            members = sorted(
                info.filename
                for info in zf.infolist()
                if not info.is_dir() and info.filename.lower().endswith(".geojson")
            )
        if not members:
            raise RuntimeError(f"No GeoJSON members in {source}")

        results: list[dict[str, Any]] = []
        if workers == 1:
            for index, member_name in enumerate(members, start=1):
                result = convert_zip_member_worker(
                    source,
                    member_name,
                    output_root,
                    temp_root,
                )
                results.append(result)
                print(
                    f"{source.name}: [{index}/{len(members)}] "
                    f"{'SKIP ' if result.get('resumed') else ''}"
                    f"{result['rows']:,} rows",
                    flush=True,
                )
        else:
            with concurrent.futures.ProcessPoolExecutor(
                max_workers=workers
            ) as executor:
                future_to_member = {
                    executor.submit(
                        convert_zip_member_worker,
                        source,
                        member_name,
                        output_root,
                        temp_root,
                    ): member_name
                    for member_name in members
                }
                done = 0
                for future in concurrent.futures.as_completed(future_to_member):
                    member_name = future_to_member[future]
                    try:
                        result = future.result()
                    except Exception as exc:
                        raise RuntimeError(
                            f"{source.name}/{member_name}: "
                            f"conversion failed: {exc!r}"
                        ) from exc
                    results.append(result)
                    done += 1
                    print(
                        f"{source.name}: [{done}/{len(members)}] "
                        f"{'SKIP ' if result.get('resumed') else ''}"
                        f"{result['rows']:,} rows",
                        flush=True,
                    )

        results.sort(key=lambda item: str(item["source_member"]))
        return results

    target = source_dir / (slug(source.name) + ".parquet")
    result = existing_parquet_result(target)
    resumed = result is not None
    if result is None:
        result = write_geojson_to_parquet(source, target)
    print(
        f"{source.name}: {'SKIP ' if resumed else ''}{result['rows']:,} rows",
        flush=True,
    )
    return [
        {
            "source_file": source.name,
            "source_member": "",
            "output": target.relative_to(output_root).as_posix(),
            "resumed": resumed,
            **result,
        }
    ]


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


def convert(source_root: Path, output_root: Path, *, workers: int) -> int:
    source_root = source_root.expanduser().resolve()
    output_root = output_root.expanduser().resolve()

    if not source_root.is_dir():
        raise FileNotFoundError(source_root)
    if output_root.exists():
        raise RuntimeError(f"Output already exists: {output_root}")

    staging = output_root.with_name(output_root.name + ".staging")
    staging.mkdir(parents=True, exist_ok=True)

    geo_archives = sorted(
        p for p in source_root.iterdir()
        if p.is_file() and "GEOJSON" in p.name.upper()
    )

    manifest: list[dict[str, Any]] = []
    try:
        with tempfile.TemporaryDirectory(prefix="vigieau-", dir=staging) as temp_dir:
            temp_root = Path(temp_dir)

            for archive in geo_archives:
                results = convert_geojson_source(
                    archive,
                    staging,
                    temp_root,
                    workers=workers,
                )
                manifest.extend(results)
                print(
                    f"DONE SOURCE {archive.name}: "
                    f"{len(results)} parquet files",
                    flush=True,
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
            "workers": workers,
        }
        atomic_json(staging / "manifest.json", manifest)
        atomic_json(staging / "dataset.json", dataset)
        os.replace(staging, output_root)
    except Exception:
        print(
            f"Staging preserved for resume: {staging}",
            file=sys.stderr,
            flush=True,
        )
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
    parser.add_argument(
        "--workers",
        type=int,
        default=min(3, os.cpu_count() or 1),
        help=(
            "Parallel GeoJSON members within each ZIP archive; "
            "default=min(3, CPU count). Use 1 for sequential conversion."
        ),
    )
    args = parser.parse_args(argv)
    if args.workers <= 0:
        parser.error("--workers must be positive")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        return convert(args.source, args.output, workers=args.workers)
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
