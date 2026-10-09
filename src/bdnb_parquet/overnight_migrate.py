from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
import zipfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from pyogrio import list_layers
from pyogrio.raw import open_arrow
from pyproj import CRS

DEFAULT_ROOT = Path("/mnt/data/datasets")
RAW = "raw"
PARQUET = "parquet"
MIN_FREE_BYTES = 20 * 2**30
MAX_SPREADSHEET_BYTES = 128 * 2**20

ALREADY_HANDLED = {
    "territoire-geospatial/cadastre/batiments/base-donnees-nationale",
    "territoire-geospatial/cadastre/batiments/imope",
    "socio-economie/equipements/accessibilite/localisation-acces-population",
    "climat-environnement/secheresse/vigieau",
    "tourisme/offre-nationale/datatourisme",
}

SKIP_SUFFIXES = {
    ".jpg", ".jpeg", ".png", ".gif", ".webp",
    ".pdf", ".doc", ".docx", ".ppt", ".pptx",
    ".txt", ".md", ".rtf",
    ".nc", ".tif", ".tiff", ".asc",
    ".qml", ".sld", ".lyr", ".lyrx",
    ".prj", ".cpg", ".dbf", ".shx", ".sbn", ".sbx",
    ".xml", ".dxf",
}

TABULAR_SUFFIXES = {".csv", ".tsv"}
SPREADSHEET_SUFFIXES = {".xlsx", ".xls", ".ods"}
GEO_SUFFIXES = {".geojson", ".gpkg", ".shp", ".kml"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def slug(text: str) -> str:
    value = text.lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-") or "dataset"


def atomic_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n")
    os.replace(tmp, path)


def append_jsonl(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n")
        fh.flush()
        os.fsync(fh.fileno())


def parquet_valid(path: Path) -> dict[str, int] | None:
    if not path.exists():
        return None
    try:
        pf = pq.ParquetFile(path)
        return {
            "rows": int(pf.metadata.num_rows),
            "row_groups": int(pf.metadata.num_row_groups),
            "bytes": int(path.stat().st_size),
        }
    except Exception:
        return None


def magic_kind(path: Path) -> str:
    try:
        with path.open("rb") as fh:
            head = fh.read(16)
    except OSError:
        return "unknown"

    if head.startswith(b"PAR1"):
        return "parquet"
    if head.startswith(b"PK\x03\x04") or head.startswith(b"PK\x05\x06"):
        return "zip"
    if head.startswith(b"SQLite format 3\x00"):
        return "sqlite"
    # ESRI shapefile: file code 9994 big-endian.
    if len(head) >= 4 and head[:4] == b"\x00\x00\x27\x0a":
        return "shapefile"
    if head.startswith(b"\x1f\x8b"):
        return "gzip"
    stripped = head.lstrip()
    if stripped.startswith(b"<?xml") or stripped.startswith(b"<"):
        return "text/xml"
    if stripped.startswith(b"{") or stripped.startswith(b"["):
        return "json"
    return "unknown"


def suffix_kind(path: Path) -> str:
    name = path.name.lower()
    if name.endswith(".csv.gz"):
        return "csv.gz"
    if name.endswith(".json.gz"):
        return "json.gz"
    if name.endswith(".geojson.gz"):
        return "geojson.gz"
    if name.endswith(".parquet"):
        return "parquet"
    if name.endswith(".geojson"):
        return "geojson"
    if name.endswith(".gpkg"):
        return "gpkg"
    if name.endswith(".shp"):
        return "shapefile"
    if name.endswith(".kml"):
        return "kml"
    if name.endswith(".csv"):
        return "csv"
    if name.endswith(".tsv"):
        return "tsv"
    if name.endswith(".json"):
        return "json"
    if path.suffix.lower() in SPREADSHEET_SUFFIXES:
        return path.suffix.lower()[1:]
    if path.suffix.lower() == ".zip":
        return "zip"
    if path.suffix.lower() == ".7z":
        return "7z"
    if path.suffix.lower() in SKIP_SUFFIXES:
        return "non-tabular"
    return magic_kind(path)


def stable_name(name: str) -> str:
    digest = hashlib.sha256(name.encode("utf-8", errors="surrogatepass")).hexdigest()[:10]
    return f"{slug(name)}-{digest}"


def json_layout(path: Path, *, gzipped: bool = False) -> str:
    opener = gzip.open if gzipped else open
    mode = "rb"
    with opener(path, mode) as fh:
        chunk = fh.read(64 * 1024)
    stripped = chunk.lstrip()
    if not stripped:
        return "empty"
    if stripped.startswith(b"["):
        return "array"
    if stripped.startswith(b"{"):
        return "object-stream"
    return "unknown"


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


def write_geojson_features_lossless(
    src: Path,
    dst: Path,
) -> dict[str, Any]:
    """Stream GeoJSON FeatureCollection features as lossless JSON text.

    If the GeoJSON is truncated, publish only a ``*.partial.parquet`` output.
    """
    import ijson

    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    tmp.unlink(missing_ok=True)
    schema = pa.schema([pa.field("feature_json", pa.string(), nullable=False)])
    writer = pq.ParquetWriter(
        tmp,
        schema,
        compression="zstd",
        compression_level=3,
        use_dictionary=False,
        write_statistics=True,
    )
    rows = 0
    batch: list[str] = []
    parse_error: Exception | None = None

    def flush() -> None:
        nonlocal batch
        if not batch:
            return
        table = pa.Table.from_arrays(
            [pa.array(batch, type=pa.string())],
            schema=schema,
        )
        writer.write_table(table, row_group_size=min(len(batch), 100_000))
        batch = []

    try:
        with src.open("rb") as fh:
            try:
                for feature in ijson.items(fh, "features.item"):
                    batch.append(
                        json.dumps(
                            feature,
                            ensure_ascii=False,
                            separators=(",", ":"),
                            default=str,
                        )
                    )
                    rows += 1
                    if len(batch) >= 25_000:
                        flush()
            except ijson.JSONError as exc:
                parse_error = exc
        flush()
    finally:
        writer.close()

    if rows == 0:
        tmp.unlink(missing_ok=True)
        raise RuntimeError(f"GeoJSON FeatureCollection yielded no features: {src}")
    info = parquet_valid(tmp)
    if info is None or info["rows"] != rows:
        tmp.unlink(missing_ok=True)
        raise RuntimeError(f"invalid GeoJSON salvage Parquet output for {src}")

    if parse_error is None:
        os.replace(tmp, dst)
        return {
            **info,
            "representation": "lossless-geojson-feature-text",
        }

    partial = dst.with_suffix(".partial.parquet")
    sidecar = dst.with_suffix(".partial.json")
    os.replace(tmp, partial)
    atomic_json(
        sidecar,
        {
            "status": "partial",
            "source": str(src),
            "output": str(partial),
            "rows_recovered": rows,
            "error": f"{type(parse_error).__name__}: {parse_error}",
            "created_at": utc_now(),
        },
    )
    return {
        **info,
        "partial": True,
        "partial_output": str(partial),
        "error": f"{type(parse_error).__name__}: {parse_error}",
        "representation": "lossless-geojson-feature-text",
    }

def write_geo(src: Path, dst: Path, *, layer: str | None = None) -> dict[str, Any]:
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    tmp.unlink(missing_ok=True)
    rows = 0
    row_groups = 0
    schema: pa.Schema | None = None
    writer: pq.ParquetWriter | None = None

    kwargs: dict[str, Any] = {"use_pyarrow": True, "batch_size": 65_536}
    if layer is not None:
        kwargs["layer"] = layer

    source_path: str | Path = src
    alias_dir: tempfile.TemporaryDirectory[str] | None = None
    old_restore_shx = os.environ.get("SHAPE_RESTORE_SHX")
    if suffix_kind(src) == "shapefile":
        os.environ["SHAPE_RESTORE_SHX"] = "YES"
        if src.suffix.lower() != ".shp":
            alias_dir = tempfile.TemporaryDirectory(prefix="bdnb-shp-alias-")
            alias = Path(alias_dir.name) / (src.name + ".shp")
            os.symlink(src, alias)
            source_path = alias

    try:
        with open_arrow(source_path, **kwargs) as source:
            meta, reader = source
            geometry_name = meta.get("geometry_name") or "wkb_geometry"
            for batch in reader:
                metadata = dict(batch.schema.metadata or {})
                metadata[b"geo"] = geo_metadata(meta, geometry_name)
                batch = batch.replace_schema_metadata(metadata)
                schema = batch.schema
                if writer is None:
                    writer = pq.ParquetWriter(
                        tmp,
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
            if writer is not None:
                writer.close()
                writer = None
            if schema is None:
                schema = reader.schema
                metadata = dict(schema.metadata or {})
                metadata[b"geo"] = geo_metadata(meta, geometry_name)
                schema = schema.with_metadata(metadata)
                pq.write_table(pa.Table.from_batches([], schema=schema), tmp)
    finally:
        if writer is not None:
            writer.close()
        if alias_dir is not None:
            alias_dir.cleanup()
        if old_restore_shx is None:
            os.environ.pop("SHAPE_RESTORE_SHX", None)
        else:
            os.environ["SHAPE_RESTORE_SHX"] = old_restore_shx

    info = parquet_valid(tmp)
    if info is None or info["rows"] != rows:
        tmp.unlink(missing_ok=True)
        raise RuntimeError(f"invalid GeoParquet output for {src}")
    os.replace(tmp, dst)
    return {**info, "geo": True}


def csv_header_and_delimiter(path: Path, *, gzipped: bool = False) -> tuple[list[str], str]:
    opener = gzip.open if gzipped else open
    with opener(path, "rt", encoding="utf-8-sig", errors="replace", newline="") as fh:
        sample = fh.read(256 * 1024)
    if not sample:
        return [], ","
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        delimiter = dialect.delimiter
    except csv.Error:
        delimiter = "\t" if "\t" in sample.splitlines()[0] else ","
    reader = csv.reader(io.StringIO(sample, newline=""), delimiter=delimiter)
    try:
        header = next(reader)
    except StopIteration:
        header = []
    return header, delimiter


def write_csv_lossless_rows(
    src: Path,
    dst: Path,
    *,
    gzipped: bool = False,
) -> dict[str, Any]:
    """Fallback CSV reader storing each parsed row as a JSON array string.

    This preserves ragged rows without inventing missing-column semantics.
    """
    _, delimiter = csv_header_and_delimiter(src, gzipped=gzipped)
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    tmp.unlink(missing_ok=True)

    opener = gzip.open if gzipped else open
    encoding = "utf-8-sig"
    try:
        with opener(src, "rt", encoding=encoding, errors="strict", newline="") as probe:
            probe.read(1024 * 1024)
    except UnicodeDecodeError:
        encoding = "cp1252"

    schema = pa.schema([pa.field("row_json", pa.string(), nullable=False)])
    writer = pq.ParquetWriter(
        tmp,
        schema,
        compression="zstd",
        compression_level=3,
        use_dictionary=False,
        write_statistics=True,
    )
    rows = 0
    batch: list[str] = []

    def flush() -> None:
        nonlocal batch
        if not batch:
            return
        table = pa.Table.from_arrays(
            [pa.array(batch, type=pa.string())],
            schema=schema,
        )
        writer.write_table(table, row_group_size=min(len(batch), 100_000))
        batch = []

    try:
        with opener(src, "rt", encoding=encoding, errors="replace", newline="") as fh:
            reader = csv.reader(fh, delimiter=delimiter)
            for row in reader:
                batch.append(
                    json.dumps(row, ensure_ascii=False, separators=(",", ":"))
                )
                rows += 1
                if len(batch) >= 50_000:
                    flush()
        flush()
    finally:
        writer.close()

    info = parquet_valid(tmp)
    if info is None or info["rows"] != rows:
        tmp.unlink(missing_ok=True)
        raise RuntimeError(f"invalid lossless CSV Parquet output for {src}")
    os.replace(tmp, dst)
    return {
        **info,
        "representation": "lossless-csv-row-json",
        "encoding": encoding,
        "delimiter": delimiter,
    }

def write_csv_arrow(src: Path, dst: Path, *, gzipped: bool = False) -> dict[str, Any]:
    header, delimiter = csv_header_and_delimiter(src, gzipped=gzipped)
    if not header:
        raise RuntimeError(f"CSV has no header: {src}")

    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    tmp.unlink(missing_ok=True)

    column_types = {name: pa.string() for name in header}
    input_stream: Any = gzip.open(src, "rb") if gzipped else src
    reader = pacsv.open_csv(
        input_stream,
        read_options=pacsv.ReadOptions(block_size=16 * 1024 * 1024, encoding="utf8"),
        parse_options=pacsv.ParseOptions(
            delimiter=delimiter,
            newlines_in_values=True,
        ),
        convert_options=pacsv.ConvertOptions(
            column_types=column_types,
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
                    tmp,
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
        if gzipped and hasattr(input_stream, "close"):
            input_stream.close()

    if writer is None and not tmp.exists():
        schema = pa.schema([pa.field(name, pa.string()) for name in header])
        pq.write_table(pa.Table.from_batches([], schema=schema), tmp)

    info = parquet_valid(tmp)
    if info is None or info["rows"] != rows:
        tmp.unlink(missing_ok=True)
        raise RuntimeError(f"invalid CSV Parquet output for {src}")
    os.replace(tmp, dst)
    return info


def write_csv(src: Path, dst: Path, *, gzipped: bool = False) -> dict[str, Any]:
    try:
        return write_csv_arrow(src, dst, gzipped=gzipped)
    except (pa.ArrowInvalid, UnicodeDecodeError, csv.Error):
        return write_csv_lossless_rows(src, dst, gzipped=gzipped)
    except RuntimeError as exc:
        if "invalid CSV Parquet output" not in str(exc):
            raise
        return write_csv_lossless_rows(src, dst, gzipped=gzipped)

def write_json_array_lossless(
    src: Path,
    dst: Path,
    *,
    gzipped: bool = False,
    batch_items: int = 50_000,
) -> dict[str, Any]:
    """Stream a top-level JSON array losslessly, salvaging complete elements.

    A truncated source is never published as the canonical destination. Complete
    elements parsed before the error are written to ``*.partial.parquet`` and
    accompanied by a JSON sidecar describing the parse failure.
    """
    import ijson

    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    tmp.unlink(missing_ok=True)

    source: Any = gzip.open(src, "rb") if gzipped else src.open("rb")
    schema = pa.schema([pa.field("json", pa.string(), nullable=True)])
    writer = pq.ParquetWriter(
        tmp,
        schema,
        compression="zstd",
        compression_level=3,
        use_dictionary=False,
        write_statistics=True,
    )
    rows = 0
    batch: list[str | None] = []
    parse_error: Exception | None = None

    def flush() -> None:
        nonlocal batch
        if not batch:
            return
        table = pa.Table.from_arrays(
            [pa.array(batch, type=pa.string())],
            schema=schema,
        )
        writer.write_table(
            table,
            row_group_size=min(len(batch), 100_000),
        )
        batch = []

    try:
        try:
            for item in ijson.items(source, "item"):
                if item is None:
                    batch.append(None)
                else:
                    batch.append(
                        json.dumps(
                            item,
                            ensure_ascii=False,
                            separators=(",", ":"),
                            default=str,
                        )
                    )
                rows += 1
                if len(batch) >= batch_items:
                    flush()
        except ijson.JSONError as exc:
            parse_error = exc
        flush()
    finally:
        writer.close()
        source.close()

    info = parquet_valid(tmp)
    if info is None or info["rows"] != rows:
        tmp.unlink(missing_ok=True)
        raise RuntimeError(f"invalid JSON-array Parquet output for {src}")

    if parse_error is None:
        os.replace(tmp, dst)
        return {
            **info,
            "json_layout": "array",
            "representation": "lossless-json-text",
        }

    partial = dst.with_suffix(".partial.parquet")
    sidecar = dst.with_suffix(".partial.json")
    os.replace(tmp, partial)
    atomic_json(
        sidecar,
        {
            "status": "partial",
            "source": str(src),
            "output": str(partial),
            "rows_recovered": rows,
            "error": f"{type(parse_error).__name__}: {parse_error}",
            "created_at": utc_now(),
        },
    )
    return {
        **info,
        "partial": True,
        "partial_output": str(partial),
        "error": f"{type(parse_error).__name__}: {parse_error}",
        "json_layout": "array",
        "representation": "lossless-json-text",
    }

def write_json_lines_lossless(
    src: Path,
    dst: Path,
    *,
    gzipped: bool = False,
) -> dict[str, Any]:
    """Stream JSONL records into one lossless JSON-text column."""
    opener = gzip.open if gzipped else open
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    tmp.unlink(missing_ok=True)
    schema = pa.schema([pa.field("json", pa.string(), nullable=False)])
    writer = pq.ParquetWriter(
        tmp,
        schema,
        compression="zstd",
        compression_level=3,
        use_dictionary=False,
        write_statistics=True,
    )
    rows = 0
    batch: list[str] = []
    try:
        with opener(src, "rt", encoding="utf-8-sig", errors="strict") as fh:
            for lineno, line in enumerate(fh, start=1):
                if not line.strip():
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(
                        f"invalid JSONL record at line {lineno}: {exc}"
                    ) from exc
                batch.append(
                    json.dumps(
                        obj,
                        ensure_ascii=False,
                        separators=(",", ":"),
                        default=str,
                    )
                )
                rows += 1
                if len(batch) >= 50_000:
                    table = pa.Table.from_arrays(
                        [pa.array(batch, type=pa.string())],
                        schema=schema,
                    )
                    writer.write_table(table, row_group_size=len(batch))
                    batch = []
            if batch:
                table = pa.Table.from_arrays(
                    [pa.array(batch, type=pa.string())],
                    schema=schema,
                )
                writer.write_table(table, row_group_size=len(batch))
    finally:
        writer.close()

    info = parquet_valid(tmp)
    if info is None or info["rows"] != rows:
        tmp.unlink(missing_ok=True)
        raise RuntimeError(f"invalid lossless JSONL Parquet output for {src}")
    os.replace(tmp, dst)
    return {**info, "representation": "lossless-json-text"}

def write_json(src: Path, dst: Path, *, gzipped: bool = False) -> dict[str, Any]:
    import pyarrow.json as pajson

    layout = json_layout(src, gzipped=gzipped)
    if layout == "array":
        return write_json_array_lossless(src, dst, gzipped=gzipped)
    if layout in {"empty", "unknown"}:
        raise RuntimeError(f"unsupported JSON layout: {layout}")

    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    tmp.unlink(missing_ok=True)

    source: Any = gzip.open(src, "rb") if gzipped else src
    writer: pq.ParquetWriter | None = None
    rows = 0
    try:
        reader = pajson.open_json(
            source,
            read_options=pajson.ReadOptions(block_size=64 * 1024 * 1024),
        )
        for batch in reader:
            if writer is None:
                writer = pq.ParquetWriter(
                    tmp,
                    batch.schema,
                    compression="zstd",
                    compression_level=3,
                    use_dictionary=True,
                    write_statistics=True,
                )
            writer.write_batch(batch)
            rows += batch.num_rows
    finally:
        if writer is not None:
            writer.close()
        if gzipped and hasattr(source, "close"):
            source.close()

    if writer is None and not tmp.exists():
        raise RuntimeError(
            f"JSON produced no record batches (expected newline-delimited JSON): {src}"
        )

    info = parquet_valid(tmp)
    if info is None or info["rows"] != rows:
        tmp.unlink(missing_ok=True)
        raise RuntimeError(f"invalid JSON Parquet output for {src}")
    os.replace(tmp, dst)
    return info


def copy_parquet(src: Path, dst: Path) -> dict[str, Any]:
    info = parquet_valid(src)
    if info is None:
        raise RuntimeError(f"invalid source parquet: {src}")
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        existing = parquet_valid(dst)
        if existing is not None:
            return existing
        dst.unlink()
    try:
        os.link(src, dst)
    except OSError:
        shutil.copy2(src, dst)
    out = parquet_valid(dst)
    if out is None or out["rows"] != info["rows"]:
        dst.unlink(missing_ok=True)
        raise RuntimeError(f"Parquet validation mismatch: {src}")
    return out


def spreadsheet_engine(src: Path) -> str | None:
    with src.open("rb") as fh:
        head = fh.read(4096)
    if head.startswith(b"PK\\x03\\x04"):
        return "openpyxl"
    if head.startswith(bytes.fromhex("D0CF11E0A1B11AE1")):
        return "xlrd"
    stripped = head.lstrip().lower()
    if stripped.startswith(b"<?xml") or stripped.startswith(b"<html") or stripped.startswith(b"<!doctype html"):
        return None
    return None

def spreadsheet_to_parquet(src: Path, dst_dir: Path) -> list[dict[str, Any]]:
    try:
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError("spreadsheet support requires pandas/openpyxl/xlrd/odfpy") from exc

    engine = spreadsheet_engine(src)
    if engine is None:
        raise RuntimeError(
            f"spreadsheet extension does not match a supported Excel container: {src}"
        )
    sheets = pd.read_excel(src, sheet_name=None, dtype=str, engine=engine)
    results: list[dict[str, Any]] = []
    for sheet_name, frame in sheets.items():
        target = dst_dir / f"{slug(str(sheet_name))}.parquet"
        table = pa.Table.from_pandas(frame, preserve_index=False)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(".parquet.tmp")
        pq.write_table(table, tmp, compression="zstd", compression_level=3)
        info = parquet_valid(tmp)
        if info is None:
            tmp.unlink(missing_ok=True)
            raise RuntimeError(f"invalid spreadsheet output: {src} sheet={sheet_name}")
        os.replace(tmp, target)
        results.append({"sheet": str(sheet_name), "path": str(target), **info})
    return results


def gpkg_to_parquet(src: Path, dst_dir: Path) -> list[dict[str, Any]]:
    layers = list_layers(src)
    results: list[dict[str, Any]] = []
    for row in layers:
        layer_name = str(row[0])
        target = dst_dir / f"{slug(layer_name)}.parquet"
        if parquet_valid(target) is not None:
            info = parquet_valid(target) or {}
            results.append({"layer": layer_name, "path": str(target), **info, "resumed": True})
            continue
        info = write_geo(src, target, layer=layer_name)
        results.append({"layer": layer_name, "path": str(target), **info})
    return results


def sevenzip_executable() -> str | None:
    return shutil.which("7zz") or shutil.which("7z") or shutil.which("7za")


def sevenzip_test(path: Path) -> tuple[bool, str]:
    exe = sevenzip_executable()
    if exe is None:
        return False, "7z executable unavailable"
    proc = subprocess.run(
        [exe, "t", "-bd", "-y", str(path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    output = proc.stdout.decode("utf-8", errors="replace")
    return proc.returncode == 0, output[-2000:]


def extract_with_7zip(src: Path, temp: Path) -> list[Path]:
    exe = sevenzip_executable()
    if exe is None:
        raise RuntimeError("7z executable unavailable")
    temp.mkdir(parents=True, exist_ok=True)
    proc = subprocess.run(
        [exe, "x", "-bd", "-y", f"-o{temp}", str(src)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    if proc.returncode != 0:
        output = proc.stdout.decode("utf-8", errors="replace")
        raise RuntimeError(
            f"7z extraction failed with status {proc.returncode}: {output[-1200:]}"
        )
    return [p for p in temp.rglob("*") if p.is_file()]


def safe_zip_members(zf: zipfile.ZipFile) -> list[zipfile.ZipInfo]:
    result: list[zipfile.ZipInfo] = []
    for info in zf.infolist():
        if info.is_dir():
            continue
        p = Path(info.filename)
        if p.is_absolute() or ".." in p.parts:
            continue
        result.append(info)
    return result


def extract_zip(src: Path, temp: Path) -> list[Path]:
    temp.mkdir(parents=True, exist_ok=True)
    extracted: list[Path] = []
    try:
        with zipfile.ZipFile(src) as zf:
            for info in safe_zip_members(zf):
                target = temp / info.filename
                target.parent.mkdir(parents=True, exist_ok=True)
                with zf.open(info) as inp, target.open("wb") as out:
                    shutil.copyfileobj(inp, out, length=16 * 1024 * 1024)
                extracted.append(target)
        return extracted
    except (zipfile.BadZipFile, NotImplementedError):
        # Some real ZIP archives have central-directory quirks or compression
        # rejects but 7-Zip can still read safely.
        return extract_with_7zip(src, temp)



def retry_sources_from_log(
    log_path: Path,
    raw_root: Path,
    *,
    statuses: set[str],
    kinds: set[str] | None = None,
) -> list[Path]:
    """Return existing top-level raw sources whose latest status/kind is selected."""
    latest: dict[str, tuple[str, str]] = {}
    raw_prefix = str(raw_root.resolve()) + os.sep

    with log_path.open("r", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise RuntimeError(
                    f"invalid JSONL in retry log at line {lineno}: {exc}"
                ) from exc

            source = record.get("source")
            status = record.get("status")
            if not isinstance(source, str) or not isinstance(status, str):
                continue
            # Archive members use "archive!member"; only retry the owning raw
            # source, never ephemeral extraction paths or nested member records.
            if "!" in source or not source.startswith(raw_prefix):
                continue
            latest[source] = (status, str(record.get("kind", "")))

    selected: list[Path] = []
    missing: list[str] = []
    for source, (status, kind) in latest.items():
        if status not in statuses:
            continue
        if kinds is not None and kind not in kinds:
            continue
        path = Path(source)
        if path.is_file():
            selected.append(path)
        else:
            missing.append(source)

    if missing:
        print(
            f"RETRY: {len(missing)} selected sources no longer exist; skipped",
            file=sys.stderr,
            flush=True,
        )

    return sorted(selected, key=lambda p: str(p))


class Migrator:
    def __init__(self, root: Path, *, log_path: Path, max_depth: int = 4) -> None:
        self.root = root.resolve()
        self.raw_root = self.root / RAW
        self.parquet_root = self.root / PARQUET
        self.log_path = log_path
        self.max_depth = max_depth
        self.temp_root = self.root / ".migration-tmp"
        self.worker_log_root = self.root / ".migration-worker-logs"
        self.counts: Counter[str] = Counter()

    def log(self, **record: Any) -> None:
        record.setdefault("time", utc_now())
        status = str(record.get("status", "event"))
        self.counts[status] += 1
        try:
            append_jsonl(self.log_path, record)
        except Exception as exc:
            # Logging must not abort a long migration. Mirror the failure to
            # stderr; data conversion can continue unless the filesystem itself
            # is unusable.
            print(
                f"LOGGING ERROR: {type(exc).__name__}: {exc}; record={record!r}",
                file=sys.stderr,
                flush=True,
            )

    def output_for(self, src: Path) -> Path:
        rel = src.relative_to(self.raw_root)
        parent = self.parquet_root / rel.parent
        # Preserve the complete source filename in the destination name.
        # foo.csv and foo.json must never collide on foo.parquet.
        return parent / f"{src.name}.parquet"

    def ensure_free_space(self, required: int = 0) -> None:
        usage = shutil.disk_usage(self.root)
        needed = max(MIN_FREE_BYTES, required)
        if usage.free < needed:
            raise RuntimeError(
                f"insufficient free space: {usage.free / 2**30:.1f} GiB free, "
                f"{needed / 2**30:.1f} GiB required"
            )

    def reserve_for_source(self, src: Path, kind: str) -> None:
        try:
            size = src.stat().st_size
        except OSError:
            size = 0
        # Compressed streams can expand substantially. This is deliberately
        # conservative; ordinary uncompressed tabular/geospatial sources use
        # source-size + reserve.
        multiplier = 8 if kind in {"csv.gz", "json.gz"} else 1
        self.ensure_free_space(size * multiplier + MIN_FREE_BYTES)

    def migrate_file(self, src: Path, *, depth: int = 0, base_output: Path | None = None) -> None:
        if depth > self.max_depth:
            self.log(status="unsupported", source=str(src), reason="max archive recursion depth")
            return

        kind = suffix_kind(src)
        dst = self.output_for(src) if base_output is None else base_output

        try:
            if kind == "parquet":
                self.reserve_for_source(src, kind)
                target = dst if dst.suffix == ".parquet" else dst.with_suffix(".parquet")
                if parquet_valid(target) is not None:
                    self.log(status="skip", source=str(src), output=str(target), kind=kind)
                    return
                info = copy_parquet(src, target)
                self.log(status="done", source=str(src), output=str(target), kind=kind, **info)
                return

            if kind in {"csv", "tsv", "csv.gz"}:
                self.reserve_for_source(src, kind)
                target = dst if dst.suffix == ".parquet" else dst.with_suffix(".parquet")
                if parquet_valid(target) is not None:
                    self.log(status="skip", source=str(src), output=str(target), kind=kind)
                    return
                info = write_csv(src, target, gzipped=kind == "csv.gz")
                self.log(status="done", source=str(src), output=str(target), kind=kind, **info)
                return

            if kind in {"json", "json.gz"}:
                self.reserve_for_source(src, kind)
                target = dst if dst.suffix == ".parquet" else dst.with_suffix(".parquet")
                if parquet_valid(target) is not None:
                    self.log(status="skip", source=str(src), output=str(target), kind=kind)
                    return
                try:
                    info = write_json(src, target, gzipped=kind == "json.gz")
                except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
                    if json_layout(src, gzipped=kind == "json.gz") != "object-stream":
                        raise
                    try:
                        info = write_json_lines_lossless(
                            src,
                            target,
                            gzipped=kind == "json.gz",
                        )
                    except RuntimeError:
                        if kind == "json" and not src.name.lower().endswith(".json"):
                            info = write_geojson_features_lossless(src, target)
                        else:
                            raise
                if info.pop("partial", False):
                    partial_output = info.pop("partial_output")
                    error = info.pop("error")
                    self.log(
                        status="partial",
                        source=str(src),
                        output=partial_output,
                        kind=kind,
                        error=error,
                        **info,
                    )
                else:
                    self.log(status="done", source=str(src), output=str(target), kind=kind, **info)
                return

            if kind in {"geojson", "kml", "shapefile"}:
                self.reserve_for_source(src, kind)
                target = dst if dst.suffix == ".parquet" else dst.with_suffix(".parquet")
                if parquet_valid(target) is not None:
                    self.log(status="skip", source=str(src), output=str(target), kind=kind)
                    return
                try:
                    info = write_geo(src, target)
                except Exception:
                    if kind != "geojson":
                        raise
                    info = write_geojson_features_lossless(src, target)
                if info.pop("partial", False):
                    partial_output = info.pop("partial_output")
                    error = info.pop("error")
                    self.log(
                        status="partial",
                        source=str(src),
                        output=partial_output,
                        kind=kind,
                        error=error,
                        **info,
                    )
                else:
                    self.log(status="done", source=str(src), output=str(target), kind=kind, **info)
                return

            if kind in {"gpkg", "sqlite"}:
                self.reserve_for_source(src, kind)
                # Only treat SQLite as geospatial if pyogrio can enumerate layers.
                try:
                    layers = list_layers(src)
                except Exception:
                    self.log(status="unsupported", source=str(src), kind=kind, reason="SQLite is not readable as OGR dataset")
                    return
                if len(layers) == 0:
                    self.log(status="unsupported", source=str(src), kind=kind, reason="GeoPackage has no layers")
                    return
                target_dir = dst.with_suffix("")
                results = gpkg_to_parquet(src, target_dir)
                self.log(status="done", source=str(src), output=str(target_dir), kind="gpkg", layers=len(results), rows=sum(int(x.get("rows", 0)) for x in results))
                return

            if kind in {"xlsx", "xls", "ods"}:
                if src.stat().st_size > MAX_SPREADSHEET_BYTES:
                    self.log(
                        status="unsupported",
                        source=str(src),
                        kind=kind,
                        reason=(
                            "spreadsheet deferred for overnight safety: "
                            f"{src.stat().st_size / 2**20:.1f} MiB exceeds "
                            f"{MAX_SPREADSHEET_BYTES / 2**20:.0f} MiB limit"
                        ),
                    )
                    return
                target_dir = dst.with_suffix("")
                results = spreadsheet_to_parquet(src, target_dir)
                self.log(status="done", source=str(src), output=str(target_dir), kind=kind, sheets=len(results), rows=sum(int(x.get("rows", 0)) for x in results))
                return

            if kind == "zip":
                archive_id = stable_name(src.name)
                target_root = (dst.parent / archive_id) if dst.suffix else dst
                marker = target_root / "_SUCCESS.json"
                if marker.exists():
                    self.log(status="skip", source=str(src), output=str(target_root), kind="zip")
                    return
                if target_root.exists():
                    self.log(
                        status="failed",
                        source=str(src),
                        output=str(target_root),
                        kind="zip",
                        error="existing archive output lacks _SUCCESS marker",
                    )
                    return

                try:
                    with zipfile.ZipFile(src) as zf:
                        members = safe_zip_members(zf)
                        unpacked_bytes = sum(int(info.file_size) for info in members)
                except zipfile.BadZipFile:
                    ok, details = sevenzip_test(src)
                    if not ok:
                        raise RuntimeError(
                            f"archive unreadable by zipfile and 7z: {details}"
                        )
                    # 7z test succeeded but exact unpacked size is not required
                    # for correctness; reserve conservatively from compressed size.
                    unpacked_bytes = src.stat().st_size * 8
                # Need room for extraction plus a safety reserve.
                self.ensure_free_space(unpacked_bytes + MIN_FREE_BYTES)

                staging = target_root.with_name(target_root.name + ".staging")
                staging.mkdir(parents=True, exist_ok=True)
                self.temp_root.mkdir(parents=True, exist_ok=True)
                failed_before = self.counts["failed"]
                with tempfile.TemporaryDirectory(
                    prefix="bdnb-zip-",
                    dir=self.temp_root,
                ) as td:
                    extracted = extract_zip(src, Path(td))
                    # Process .shp once; sidecars stay present in temp tree.
                    shapefiles = {p for p in extracted if suffix_kind(p) == "shapefile"}
                    for member in extracted:
                        member_kind = suffix_kind(member)
                        if member_kind in {"unknown", "text/xml"} and member.suffix.lower() in SKIP_SUFFIXES:
                            self.log(status="unsupported", source=f"{src}!{member.relative_to(td)}", kind=member_kind, reason="non-tabular archive member")
                            continue
                        if member.suffix.lower() in {".dbf", ".shx", ".sbn", ".sbx", ".prj", ".cpg", ".qml", ".pdf", ".tif", ".tiff"}:
                            continue
                        rel = member.relative_to(td)
                        # Preserve the complete member name to prevent collisions
                        # such as foo.csv vs foo.json inside one archive.
                        member_output = Path(str(staging / rel) + ".parquet")
                        self.migrate_file(member, depth=depth + 1, base_output=member_output)
                if self.counts["failed"] > failed_before:
                    self.log(
                        status="partial",
                        source=str(src),
                        output=str(staging),
                        kind="zip",
                        reason="one or more archive members failed; staging preserved for resume",
                    )
                    return

                atomic_json(staging / "_SUCCESS.json", {
                    "status": "ok",
                    "created_at": utc_now(),
                    "source": str(src),
                })
                os.replace(staging, target_root)
                self.log(status="done", source=str(src), output=str(target_root), kind="zip")
                return

            if kind == "7z":
                self.log(status="unsupported", source=str(src), kind=kind, reason="7z archive handler not enabled")
                return

            if kind == "non-tabular" or src.suffix.lower() in SKIP_SUFFIXES or kind == "text/xml":
                self.log(status="unsupported", source=str(src), kind=kind, reason="non-tabular or schema-specific format")
                return

            # Last conservative attempt for extensionless UTF-8 delimited text.
            if kind == "unknown" and src.suffix == "":
                try:
                    header, delimiter = csv_header_and_delimiter(src)
                    if len(header) >= 2 and delimiter in {",", ";", "\t", "|"}:
                        target = dst if dst.suffix == ".parquet" else dst.with_suffix(".parquet")
                        info = write_csv(src, target)
                        self.log(status="done", source=str(src), output=str(target), kind="sniffed-delimited-text", **info)
                        return
                except Exception:
                    pass

            self.log(status="unsupported", source=str(src), kind=kind, reason="unrecognized format")
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            self.log(
                status="failed",
                source=str(src),
                kind=kind,
                error=f"{type(exc).__name__}: {exc}",
                traceback=traceback.format_exc(limit=8),
            )

    def preflight(self) -> int:
        if not self.raw_root.exists():
            raise FileNotFoundError(self.raw_root)
        self.parquet_root.mkdir(parents=True, exist_ok=True)
        self.temp_root.mkdir(parents=True, exist_ok=True)
        self.ensure_free_space()

        files: list[Path] = []
        for dirpath, dirnames, filenames in os.walk(self.raw_root, topdown=True):
            base = Path(dirpath)
            rel = base.relative_to(self.raw_root).as_posix()
            if any(rel == handled or rel.startswith(handled + "/") for handled in ALREADY_HANDLED):
                dirnames[:] = []
                continue
            files.extend(base / filename for filename in filenames)

        destinations: dict[str, str] = {}
        collisions: list[tuple[str, str, str]] = []
        kinds: Counter[str] = Counter()
        zip_warnings: list[str] = []
        fatal_errors: list[str] = []

        for src in files:
            kind = suffix_kind(src)
            kinds[kind] += 1
            dst = self.output_for(src)
            if kind == "zip":
                dest = str(dst.parent / stable_name(src.name))
                if src.stat().st_size == 0:
                    zip_warnings.append(f"{src}: empty file")
                else:
                    try:
                        with zipfile.ZipFile(src) as zf:
                            safe_zip_members(zf)
                    except zipfile.BadZipFile as exc:
                        # Distinguish mislabeled files from quirky real archives.
                        mkind = magic_kind(src)
                        if mkind != "zip":
                            zip_warnings.append(
                                f"{src}: not actually ZIP ({mkind}): {exc}"
                            )
                        else:
                            ok, details = sevenzip_test(src)
                            if ok:
                                zip_warnings.append(
                                    f"{src}: Python zipfile rejects archive; 7z fallback validated"
                                )
                            else:
                                zip_warnings.append(
                                    f"{src}: corrupt/unreadable archive; runtime will log failed and continue: "
                                    f"{details}"
                                )
                    except Exception as exc:
                        zip_warnings.append(
                            f"{src}: archive metadata error; runtime will log failed and continue: "
                            f"{type(exc).__name__}: {exc}"
                        )
            else:
                dest = str(dst)

            previous = destinations.get(dest)
            if previous is not None and previous != str(src):
                collisions.append((dest, previous, str(src)))
            else:
                destinations[dest] = str(src)

        # Dependency smoke checks for optional handlers.
        dependency_errors: list[str] = []
        for module in ("pandas", "openpyxl", "xlrd", "odf"):
            try:
                __import__(module)
            except Exception as exc:
                dependency_errors.append(f"{module}: {type(exc).__name__}: {exc}")

        report = {
            "created_at": utc_now(),
            "files": len(files),
            "kinds": dict(sorted(kinds.items())),
            "collisions": collisions,
            "zip_warnings": zip_warnings,
            "fatal_errors": fatal_errors,
            "dependency_errors": dependency_errors,
            "free_gib": shutil.disk_usage(self.root).free / 2**30,
        }
        atomic_json(self.log_path.with_suffix(".preflight.json"), report)

        print(f"PREFLIGHT: files={len(files)} free={report['free_gib']:.1f} GiB", flush=True)
        print("KINDS:", " ".join(f"{k}={v}" for k, v in sorted(kinds.items())), flush=True)
        print(
            f"CHECKS: collisions={len(collisions)} "
            f"warnings={len(zip_warnings)} "
            f"fatal={len(fatal_errors)} "
            f"dependency_errors={len(dependency_errors)}",
            flush=True,
        )
        if zip_warnings:
            print(
                f"WARNINGS: {len(zip_warnings)} non-blocking archive anomalies; "
                f"see {self.log_path.with_suffix('.preflight.json')}",
                flush=True,
            )
        if collisions or fatal_errors or dependency_errors:
            print(
                f"PRECHECK FAILED: see {self.log_path.with_suffix('.preflight.json')}",
                file=sys.stderr,
                flush=True,
            )
            return 2
        print("PRECHECK OK", flush=True)
        return 0

    def parallel_candidate(self, src: Path) -> bool:
        kind = suffix_kind(src)
        if kind in {
            "parquet", "csv", "tsv", "csv.gz", "json", "json.gz",
            "geojson", "kml", "shapefile", "gpkg", "sqlite",
            "xlsx", "xls", "ods", "zip",
        }:
            return True
        return kind == "unknown" and src.suffix == ""

    def merge_worker_log(self, path: Path) -> None:
        if not path.exists():
            return
        try:
            with path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        self.log(
                            status="failed",
                            source=str(path),
                            error="invalid JSONL record from worker",
                        )
                        continue
                    self.log(**record)
        finally:
            path.unlink(missing_ok=True)

    def run_parallel(self, files: list[Path], workers: int) -> None:
        self.worker_log_root.mkdir(parents=True, exist_ok=True)
        pending = list(files)
        active: dict[subprocess.Popen[Any], tuple[Path, Path]] = {}
        completed = 0
        total = len(files)

        while pending or active:
            while pending and len(active) < workers:
                src = pending.pop(0)
                if not self.parallel_candidate(src):
                    self.migrate_file(src)
                    completed += 1
                    if completed == 1 or completed % 25 == 0:
                        print(f"[{completed}/{total}] completed", flush=True)
                    continue

                token = hashlib.sha256(
                    str(src).encode("utf-8", errors="surrogatepass")
                ).hexdigest()[:16]
                worker_log = self.worker_log_root / f"{token}.jsonl"
                worker_log.unlink(missing_ok=True)
                cmd = [
                    sys.executable,
                    "-m",
                    "bdnb_parquet.overnight_migrate",
                    "--root",
                    str(self.root),
                    "--log",
                    str(worker_log),
                    "--max-depth",
                    str(self.max_depth),
                    "--worker-file",
                    str(src),
                ]
                proc = subprocess.Popen(cmd)
                active[proc] = (src, worker_log)

            if not active:
                continue

            finished: list[subprocess.Popen[Any]] = []
            for proc, (src, worker_log) in active.items():
                rc = proc.poll()
                if rc is None:
                    continue
                finished.append(proc)
                self.merge_worker_log(worker_log)
                if rc != 0:
                    self.log(
                        status="failed",
                        source=str(src),
                        error=f"isolated worker exited with status {rc}",
                    )
                completed += 1
                print(
                    f"[{completed}/{total}] {src.relative_to(self.raw_root)} "
                    f"(worker exit {rc})",
                    flush=True,
                )

            for proc in finished:
                active.pop(proc, None)

            if not finished:
                time.sleep(0.25)

    def run(
        self,
        *,
        workers: int = 1,
        files: list[Path] | None = None,
    ) -> int:
        if not self.raw_root.exists():
            raise FileNotFoundError(self.raw_root)
        self.parquet_root.mkdir(parents=True, exist_ok=True)
        self.temp_root.mkdir(parents=True, exist_ok=True)
        self.ensure_free_space()

        if files is None:
            files = []
            for dirpath, dirnames, filenames in os.walk(self.raw_root, topdown=True):
                base = Path(dirpath)
                rel = base.relative_to(self.raw_root).as_posix()
                if any(
                    rel == handled or rel.startswith(handled + "/")
                    for handled in ALREADY_HANDLED
                ):
                    dirnames[:] = []
                    continue
                for filename in filenames:
                    files.append(base / filename)

        total = len(files)
        print(f"QUEUE: {total} raw files; workers={workers}", flush=True)
        if workers > 1:
            self.run_parallel(files, workers)
        else:
            for idx, src in enumerate(files, start=1):
                if idx == 1 or idx % 25 == 0:
                    print(f"[{idx}/{total}] {src.relative_to(self.raw_root)}", flush=True)
                try:
                    self.migrate_file(src)
                except KeyboardInterrupt:
                    raise
                except Exception as exc:
                    self.log(
                        status="failed",
                        source=str(src),
                        error=f"top-level {type(exc).__name__}: {exc}",
                        traceback=traceback.format_exc(limit=8),
                    )

        summary = {
            "created_at": utc_now(),
            "root": str(self.root),
            "files_seen": total,
            "counts": dict(self.counts),
            "log": str(self.log_path),
        }
        atomic_json(self.log_path.with_suffix(".summary.json"), summary)
        print("DONE:", " ".join(f"{k}={v}" for k, v in sorted(self.counts.items())), flush=True)
        return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Best-effort resumable overnight migration of raw datasets to Parquet."
    )
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument(
        "--log",
        type=Path,
        default=Path("/tmp/bdnb-overnight-migrate.jsonl"),
    )
    parser.add_argument("--max-depth", type=int, default=4)
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="isolated concurrent conversion processes; use 3 on HDD-backed m710s",
    )
    parser.add_argument(
        "--retry-from-log",
        type=Path,
        help="build a targeted retry queue from latest source statuses in this JSONL log",
    )
    parser.add_argument(
        "--retry-status",
        action="append",
        choices=("failed", "partial"),
        help=(
            "status to retry from --retry-from-log; may be repeated; "
            "defaults to failed+partial"
        ),
    )
    parser.add_argument(
        "--retry-kind",
        action="append",
        help=(
            "optional kind filter for --retry-from-log (for example: zip); "
            "may be repeated"
        ),
    )
    parser.add_argument(
        "--worker-file",
        type=Path,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--preflight",
        action="store_true",
        help="validate queue, destinations, ZIP metadata and dependencies without converting",
    )
    args = parser.parse_args(argv)
    if args.workers <= 0:
        parser.error("--workers must be positive")
    if args.worker_file is not None and args.retry_from_log is not None:
        parser.error("--worker-file and --retry-from-log are mutually exclusive")
    if args.preflight and args.retry_from_log is not None:
        parser.error("--preflight and --retry-from-log are mutually exclusive")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        migrator = Migrator(args.root, log_path=args.log, max_depth=args.max_depth)
        if args.worker_file is not None:
            migrator.migrate_file(args.worker_file.resolve())
            return 0
        if args.preflight:
            return migrator.preflight()
        retry_files = None
        if args.retry_from_log is not None:
            retry_log = args.retry_from_log.expanduser().resolve()
            if not retry_log.is_file():
                raise FileNotFoundError(retry_log)
            retry_statuses = set(args.retry_status or ("failed", "partial"))
            retry_kinds = set(args.retry_kind) if args.retry_kind else None
            retry_files = retry_sources_from_log(
                retry_log,
                migrator.raw_root,
                statuses=retry_statuses,
                kinds=retry_kinds,
            )
            kind_text = (
                f" kind={','.join(sorted(retry_kinds))}"
                if retry_kinds is not None
                else ""
            )
            print(
                f"RETRY: selected {len(retry_files)} raw sources "
                f"with status={','.join(sorted(retry_statuses))}"
                f"{kind_text} from {retry_log}",
                flush=True,
            )
        return migrator.run(workers=args.workers, files=retry_files)
    except KeyboardInterrupt:
        print("Interrupted safely; rerun the same command to resume.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"FATAL: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
