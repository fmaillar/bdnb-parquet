from __future__ import annotations

import argparse
import concurrent.futures
import io
import json
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import asdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, BinaryIO, TextIO

import pyarrow as pa
import pyarrow.parquet as pq
from pyproj import CRS

from . import __version__
from .parser import (
    GeometrySpec,
    convert_value,
    normalize_pg_type,
    parse_ddl_line,
    pg_copy_unescape,
    trailing_backslash_count,
)

SQL_MEMBER = "pgdump/bdnb.sql"
DEFAULT_SOURCE = Path(
    "/mnt/data/datasets/raw/territoire-geospatial/cadastre/batiments/"
    "base-donnees-nationale/BDNB - Export france - pgdump"
)
DEFAULT_OUTPUT = Path(
    "/mnt/data/datasets/parquet/territoire-geospatial/cadastre/batiments/"
    "base-donnees-nationale/bdnb_2026_02_a_open_data"
)
DEFAULT_REFERENCE = Path(
    "/mnt/data/datasets/numpy/territoire-geospatial/cadastre/batiments/"
    "base-donnees-nationale/bdnb_2026_02_a_open_data"
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n")
    os.replace(tmp, path)


def pg_decimal_type(t: str) -> pa.DataType | None:
    m = re.match(r"^(?:numeric|decimal)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$", t)
    if not m:
        return None
    precision, scale = map(int, m.groups())
    if precision <= 38:
        return pa.decimal128(precision, scale)
    if precision <= 76:
        return pa.decimal256(precision, scale)
    raise ValueError(f"Decimal precision > 76 not supported: {t}")


def arrow_type_for(pg_type: str) -> tuple[pa.DataType, str | None]:
    t = normalize_pg_type(pg_type)
    depth = 0
    while t.endswith("[]"):
        depth += 1
        t = t[:-2].rstrip()
    fallback: str | None = None

    dec = pg_decimal_type(t)
    if dec is not None:
        typ: pa.DataType = dec
    elif t.startswith("geometry") or t == "bytea":
        typ = pa.binary()
    elif t in {"bool", "boolean"}:
        typ = pa.bool_()
    elif t in {"int2", "smallint"}:
        typ = pa.int16()
    elif t in {"int4", "integer", "serial", "serial4"}:
        typ = pa.int32()
    elif t in {"int8", "bigint", "bigserial", "serial8"}:
        typ = pa.int64()
    elif t in {"real", "float4"}:
        typ = pa.float32()
    elif t in {"double precision", "float8", "float"} or t.startswith("float("):
        typ = pa.float64()
    elif t in {"numeric", "decimal"}:
        typ = pa.float64()
    elif t == "date":
        typ = pa.date32()
    elif t.startswith("timestamp with time zone") or t.startswith("timestamptz"):
        typ = pa.timestamp("us", tz="UTC")
    elif t.startswith("timestamp"):
        typ = pa.timestamp("us")
    elif t.startswith("time"):
        typ = pa.time64("us")
    elif (
        t in {"text", "varchar", "character varying", "char", "character", "bpchar", "uuid", "json", "jsonb", "inet", "cidr"}
        or t.startswith("varchar(")
        or t.startswith("character varying(")
        or t.startswith("char(")
        or t.startswith("character(")
        or t.startswith("bit(")
        or t.startswith("varbit(")
    ):
        typ = pa.string()
    else:
        typ = pa.string()
        fallback = f"unmapped PostgreSQL type preserved as string: {pg_type}"

    for _ in range(depth):
        typ = pa.list_(typ)
    return typ, fallback


def geometry_metadata(geometries: dict[str, GeometrySpec], columns: list[str]) -> bytes | None:
    selected = [geometries[c] for c in columns if c in geometries]
    if not selected:
        return None
    geo_cols: dict[str, Any] = {}
    for geom in selected:
        crs = CRS.from_epsg(geom.srid).to_json_dict()
        suffix = " Z" if geom.dimensions == 3 else ""
        geo_cols[geom.name] = {
            "encoding": "WKB",
            "geometry_types": [geom.geometry_type + suffix],
            "crs": crs,
        }
    payload = {
        "version": "1.1.0",
        "primary_column": selected[0].name,
        "columns": geo_cols,
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()


def build_schema(
    columns: list[str],
    pg_types: dict[str, str],
    geometries: dict[str, GeometrySpec],
    *,
    allow_type_fallback: bool,
) -> tuple[pa.Schema, dict[str, str]]:
    fields: list[pa.Field] = []
    fallbacks: dict[str, str] = {}
    effective_types: dict[str, str] = {}
    for col in columns:
        if col in geometries:
            pg_type = f"geometry({geometries[col].geometry_type}, {geometries[col].srid})"
        elif col in pg_types:
            pg_type = pg_types[col]
        else:
            pg_type = "unknown/text"
            fallbacks[col] = "column type absent from parsed DDL; preserved as string"
        effective_types[col] = pg_type
        arrow_type, fallback = arrow_type_for(pg_type)
        fields.append(pa.field(col, arrow_type, nullable=True))
        if fallback:
            fallbacks[col] = fallback
    if fallbacks and not allow_type_fallback:
        details = "; ".join(f"{k}: {v}" for k, v in sorted(fallbacks.items()))
        raise ValueError(f"Uncontrolled type fallback(s): {details}")
    metadata: dict[bytes, bytes] = {
        b"bdnb_postgresql_types": json.dumps(
            effective_types, ensure_ascii=False, separators=(",", ":")
        ).encode(),
        b"bdnb_converter": f"bdnb-parquet/{__version__}".encode(),
    }
    geo = geometry_metadata(geometries, columns)
    if geo is not None:
        metadata[b"geo"] = geo
    return pa.schema(fields, metadata=metadata), fallbacks


def find_rapidgzip(explicit: str | None) -> str:
    if explicit:
        p = Path(explicit).expanduser()
        if p.is_file() and os.access(p, os.X_OK):
            return str(p)
        raise FileNotFoundError(f"rapidgzip not executable: {p}")
    beside_python = Path(sys.executable).with_name("rapidgzip")
    if beside_python.is_file() and os.access(beside_python, os.X_OK):
        return str(beside_python)
    found = shutil.which("rapidgzip")
    if found:
        return found
    raise FileNotFoundError(
        "rapidgzip CLI not found; install rapidgzip in the converter venv or pass --rapidgzip"
    )


def source_identity(source: Path) -> dict[str, Any]:
    st = source.stat()
    return {
        "path": str(source),
        "size": st.st_size,
        "mtime_ns": st.st_mtime_ns,
    }


def load_reference(reference: Path | None) -> dict[str, dict[str, Any]]:
    if reference is None or not reference.exists():
        return {}
    result: dict[str, dict[str, Any]] = {}
    for p in reference.glob("*.done.json"):
        try:
            obj = json.loads(p.read_text())
        except Exception:
            continue
        table = obj.get("table")
        if isinstance(table, str):
            result[table] = obj
    return result


def success_is_valid(path: Path, identity: dict[str, Any]) -> bool:
    try:
        obj = json.loads(path.read_text())
    except Exception:
        return False
    return obj.get("status") == "ok" and obj.get("source") == identity


def write_part_task(
    *,
    path: str,
    part_no: int,
    raw_lines: list[str],
    columns: list[str],
    pg_types: dict[str, str],
    schema_bytes: bytes,
    compression_level: int,
    row_group_rows: int,
) -> tuple[int, int, int]:
    """Convert one COPY chunk and write one Parquet part.

    This function deliberately lives at module scope so ProcessPoolExecutor can
    pickle it.  Each worker owns one output file, therefore no Parquet writer is
    shared between processes and no cross-process locking is required.
    """
    arrow_schema = pa.ipc.read_schema(pa.BufferReader(schema_bytes))
    buffers: list[list[Any]] = [[] for _ in columns]
    for line in raw_lines:
        values = parse_copy_row(line)
        if len(values) != len(columns):
            raise ValueError(
                f"part {part_no}: got {len(values)} fields, expected {len(columns)}"
            )
        for i, (raw, col) in enumerate(zip(values, columns, strict=True)):
            buffers[i].append(convert_value(raw, pg_types[col]))

    arrays = [
        pa.array(values, type=field.type)
        for values, field in zip(buffers, arrow_schema, strict=True)
    ]
    table = pa.Table.from_arrays(arrays, schema=arrow_schema)
    target = Path(path)
    pq.write_table(
        table,
        target,
        compression="zstd",
        compression_level=compression_level,
        use_dictionary=True,
        write_statistics=True,
        data_page_version="2.0",
        row_group_size=row_group_rows,
    )
    return part_no, len(raw_lines), target.stat().st_size


class TableWriter:
    def __init__(
        self,
        *,
        output_root: Path,
        schema_name: str,
        table: str,
        columns: list[str],
        pg_types: dict[str, str],
        geometries: dict[str, GeometrySpec],
        source: dict[str, Any],
        batch_rows: int,
        file_rows: int,
        compression_level: int,
        reference: dict[str, Any] | None,
        allow_type_fallback: bool,
        executor: concurrent.futures.ProcessPoolExecutor | None,
        workers: int,
        max_in_flight: int,
    ) -> None:
        self.output_root = output_root
        self.schema_name = schema_name
        self.table = table
        self.columns = columns
        self.pg_types = {
            c: (
                f"geometry({geometries[c].geometry_type}, {geometries[c].srid})"
                if c in geometries
                else pg_types.get(c, "text")
            )
            for c in columns
        }
        self.geometries = {c: geometries[c] for c in columns if c in geometries}
        self.source = source
        self.batch_rows = batch_rows
        self.file_rows = file_rows
        self.compression_level = compression_level
        self.executor = executor
        self.workers = workers
        self.max_in_flight = max_in_flight
        self.reference = reference
        if self.reference and isinstance(self.reference.get("columns"), list):
            ref_columns = self.reference["columns"]
            if ref_columns != self.columns:
                raise RuntimeError(
                    f"{self.table}: COPY columns differ from legacy reference"
                )
        self.arrow_schema, self.fallbacks = build_schema(
            columns, pg_types, geometries, allow_type_fallback=allow_type_fallback
        )
        self.schema_bytes = self.arrow_schema.serialize().to_pybytes()
        self.raw_lines: list[str] = []
        self.rows = 0
        self.part_no = 0
        self.part_files: list[Path] = []
        self.pending: dict[concurrent.futures.Future[tuple[int, int, int]], int] = {}
        self.completed_part_rows = 0
        self.completed_part_bytes = 0
        self.started = time.monotonic()
        self.staging = output_root / "tables" / ".staging" / table
        self.final_dir = output_root / "tables" / table
        if self.staging.exists():
            shutil.rmtree(self.staging)
        self.staging.mkdir(parents=True, exist_ok=True)

    def _record_result(self, result: tuple[int, int, int]) -> None:
        _part_no, rows, nbytes = result
        self.completed_part_rows += rows
        self.completed_part_bytes += nbytes

    def _drain_completed(self, *, block: bool) -> None:
        if not self.pending:
            return
        futures = set(self.pending)
        done, _ = concurrent.futures.wait(
            futures,
            timeout=None if block else 0,
            return_when=(
                concurrent.futures.FIRST_COMPLETED
                if block
                else concurrent.futures.ALL_COMPLETED
            ),
        )
        for future in done:
            self.pending.pop(future, None)
            self._record_result(future.result())

    def _submit_part(self) -> None:
        if not self.raw_lines:
            return
        part_no = self.part_no
        path = self.staging / f"part-{part_no:05d}.parquet"
        raw_lines = self.raw_lines
        self.raw_lines = []
        self.part_no += 1
        self.part_files.append(path)

        kwargs = dict(
            path=str(path),
            part_no=part_no,
            raw_lines=raw_lines,
            columns=self.columns,
            pg_types=self.pg_types,
            schema_bytes=self.schema_bytes,
            compression_level=self.compression_level,
            row_group_rows=self.batch_rows,
        )
        if self.executor is None:
            self._record_result(write_part_task(**kwargs))
        else:
            future = self.executor.submit(write_part_task, **kwargs)
            self.pending[future] = part_no
            while len(self.pending) >= self.max_in_flight:
                self._drain_completed(block=True)

    def add_line(self, line: str) -> None:
        self.raw_lines.append(line)
        self.rows += 1
        if len(self.raw_lines) >= self.file_rows:
            self._submit_part()
        elif self.pending:
            self._drain_completed(block=False)

    def finish(self) -> dict[str, Any]:
        self._submit_part()
        while self.pending:
            self._drain_completed(block=True)

        if not self.part_files:
            # Preserve a real typed Parquet object for an empty PostgreSQL table.
            path = self.staging / "part-00000.parquet"
            empty = pa.Table.from_arrays(
                [pa.array([], type=f.type) for f in self.arrow_schema],
                schema=self.arrow_schema,
            )
            pq.write_table(
                empty,
                path,
                compression="zstd",
                compression_level=self.compression_level,
            )
            self.part_files.append(path)

        if self.completed_part_rows != self.rows:
            raise RuntimeError(
                f"{self.table}: worker row-count mismatch: "
                f"submitted={self.rows}, completed={self.completed_part_rows}"
            )

        expected_rows = None
        if self.reference:
            expected_rows = self.reference.get("rows")
            if isinstance(expected_rows, int) and expected_rows != self.rows:
                raise RuntimeError(
                    f"{self.table}: row-count mismatch: source={self.rows}, "
                    f"reference={expected_rows}"
                )

        if self.final_dir.exists():
            raise FileExistsError(
                f"Refusing to replace existing incomplete table directory: {self.final_dir}"
            )
        os.replace(self.staging, self.final_dir)
        final_parts = sorted(self.final_dir.glob("part-*.parquet"))
        validated_rows = 0
        for part in final_parts:
            validated_rows += pq.ParquetFile(part).metadata.num_rows
        if validated_rows != self.rows:
            raise RuntimeError(
                f"{self.table}: Parquet footer row-count mismatch: "
                f"written={self.rows}, footers={validated_rows}"
            )
        total_bytes = sum(p.stat().st_size for p in final_parts)
        elapsed = time.monotonic() - self.started
        success = {
            "status": "ok",
            "converter_version": __version__,
            "completed_at": utc_now(),
            "source": self.source,
            "schema": self.schema_name,
            "table": self.table,
            "rows": self.rows,
            "reference_rows": expected_rows,
            "columns": self.columns,
            "postgresql_types": self.pg_types,
            "arrow_schema": str(self.arrow_schema),
            "geometry_columns": [asdict(g) for g in self.geometries.values()],
            "fallback_types": self.fallbacks,
            "parts": len(final_parts),
            "parquet_bytes": total_bytes,
            "compression": "zstd",
            "compression_level": self.compression_level,
            "batch_rows": self.batch_rows,
            "file_rows": self.file_rows,
            "workers": self.workers,
            "elapsed_seconds": round(elapsed, 3),
        }
        atomic_json(self.final_dir / "_SUCCESS.json", success)
        atomic_json(self.output_root / "schemas" / f"{self.table}.json", success)
        return success


def read_logical_copy_line(text: TextIO) -> str:
    line = text.readline()
    if line == "":
        return ""
    logical = line.rstrip("\r\n")
    while trailing_backslash_count(logical) % 2 == 1:
        nxt = text.readline()
        if nxt == "":
            break
        logical = logical[:-1] + "\n" + nxt.rstrip("\r\n")
    return logical


def skip_copy(text: TextIO) -> int:
    rows = 0
    while True:
        line = read_logical_copy_line(text)
        if line == "":
            raise EOFError("Unexpected EOF inside COPY data")
        if line == r"\.":
            return rows
        rows += 1


def parse_copy_row(line: str) -> list[str | None]:
    return [pg_copy_unescape(v) for v in line.split("\t")]


def open_sql_stream(
    source: Path, rapidgzip: str, gzip_threads: int
) -> tuple[subprocess.Popen[bytes], subprocess.Popen[bytes], BinaryIO]:
    """Stream pgdump/bdnb.sql without asking Python tarfile to seek.

    Python 3.13's tarfile stream reader can expose an internal `_Stream` object
    that is not a complete RawIOBase implementation; wrapping an extracted
    member in TextIOWrapper then fails on `seekable()`.  GNU tar is designed
    for exactly this sequential pipe use-case, so let it select the member and
    keep Python responsible only for parsing SQL.
    """
    tar_cli = shutil.which("tar")
    if tar_cli is None:
        raise FileNotFoundError("GNU tar not found in PATH")

    rg_proc = subprocess.Popen(
        [rapidgzip, "-d", "-c", "-P", str(gzip_threads), str(source)],
        stdout=subprocess.PIPE,
        stderr=sys.stderr,
    )
    if rg_proc.stdout is None:
        raise RuntimeError("rapidgzip stdout pipe unavailable")

    try:
        tar_proc = subprocess.Popen(
            [
                tar_cli,
                "--extract",
                "--to-stdout",
                "--file=-",
                "./" + SQL_MEMBER,
            ],
            stdin=rg_proc.stdout,
            stdout=subprocess.PIPE,
            stderr=sys.stderr,
        )
    except Exception:
        rg_proc.terminate()
        rg_proc.wait()
        raise
    finally:
        # The child owns its duplicate of the pipe. Closing the parent's copy
        # is required for clean SIGPIPE/EOF propagation on early termination.
        rg_proc.stdout.close()

    if tar_proc.stdout is None:
        tar_proc.terminate()
        rg_proc.terminate()
        raise RuntimeError("tar stdout pipe unavailable")

    return rg_proc, tar_proc, tar_proc.stdout


def build_manifest(output: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    tables_root = output / "tables"
    if tables_root.exists():
        for success_path in sorted(tables_root.glob("*/_SUCCESS.json")):
            obj = json.loads(success_path.read_text())
            rows.append(
                {
                    "table": obj["table"],
                    "rows": int(obj["rows"]),
                    "parts": int(obj["parts"]),
                    "parquet_bytes": int(obj["parquet_bytes"]),
                    "columns": len(obj["columns"]),
                    "geometry_columns": json.dumps(obj["geometry_columns"], separators=(",", ":")),
                    "fallback_types": json.dumps(obj["fallback_types"], separators=(",", ":")),
                    "completed_at": obj["completed_at"],
                }
            )
    if rows:
        pq.write_table(
            pa.Table.from_pylist(rows),
            output / "manifest.parquet",
            compression="zstd",
            compression_level=3,
        )
    atomic_json(output / "manifest.json", rows)
    return rows


def convert(args: argparse.Namespace) -> int:
    source = args.source.expanduser().resolve()
    output = args.output.expanduser().resolve()
    reference_dir = args.reference.expanduser().resolve() if args.reference else None
    rapidgzip = find_rapidgzip(args.rapidgzip)
    identity = source_identity(source)
    requested = set(args.tables or [])
    references = load_reference(reference_dir)
    workers = args.workers
    cpu_count = os.cpu_count() or 1
    gzip_threads = (
        args.gzip_threads
        if args.gzip_threads is not None
        else max(1, cpu_count - workers)
    )
    max_in_flight = args.max_in_flight or max(1, workers)

    output.mkdir(parents=True, exist_ok=True)
    (output / "tables" / ".staging").mkdir(parents=True, exist_ok=True)
    (output / "schemas").mkdir(parents=True, exist_ok=True)

    dataset_meta = {
        "dataset": "BDNB",
        "schema": "bdnb_2026_02_a_open_data",
        "converter_version": __version__,
        "source": identity,
        "created_at": utc_now(),
        "conversion_policy": {
            "compression": "zstd",
            "compression_level": args.compression_level,
            "batch_rows": args.batch_rows,
            "file_rows": args.file_rows,
            "numeric_without_precision": "float64",
            "numeric_with_precision": "decimal128/decimal256",
            "text_arrays": "list<string>",
            "geometry": "WKB with GeoParquet 1.1 metadata",
            "workers": workers,
            "gzip_threads": gzip_threads,
            "parallelism": "ProcessPoolExecutor for typed Parquet parts",
        },
    }
    atomic_json(output / "dataset.json", dataset_meta)

    print(f"source      : {source}", flush=True)
    print(f"output      : {output}", flush=True)
    print(f"rapidgzip   : {rapidgzip}", flush=True)
    print(f"reference   : {reference_dir}", flush=True)
    print(f"tables      : {', '.join(sorted(requested)) if requested else 'ALL'}", flush=True)
    print(f"workers     : {workers}", flush=True)
    print(f"gzip threads: {gzip_threads}", flush=True)
    print(f"in-flight   : {max_in_flight}", flush=True)

    pg_types: dict[tuple[str, str], dict[str, str]] = {}
    geoms: dict[tuple[str, str], dict[str, GeometrySpec]] = {}
    completed_requested: set[str] = set()
    tables_seen = 0

    rg_proc: subprocess.Popen[bytes] | None = None
    tar_proc: subprocess.Popen[bytes] | None = None
    binary: BinaryIO | None = None
    text: TextIO | None = None
    executor: concurrent.futures.ProcessPoolExecutor | None = None
    try:
        if workers > 1 and not args.dry_run:
            executor = concurrent.futures.ProcessPoolExecutor(max_workers=workers)
        rg_proc, tar_proc, binary = open_sql_stream(source, rapidgzip, gzip_threads)
        text = io.TextIOWrapper(binary, encoding="utf-8", errors="strict", newline="")
        while True:
            line = text.readline()
            if line == "":
                break
            parsed = parse_ddl_line(line)
            if parsed is None:
                continue
            kind, payload = parsed
            if kind == "column":
                schema, table_name, column, pg_type = payload
                pg_types.setdefault((schema, table_name), {})[column] = pg_type
                continue
            if kind == "geometry":
                schema, table_name, geom = payload
                geoms.setdefault((schema, table_name), {})[geom.name] = geom
                continue
            if kind != "copy":
                continue

            schema, table_name, columns = payload
            tables_seen += 1
            selected = not requested or table_name in requested
            success_path = output / "tables" / table_name / "_SUCCESS.json"
            already_done = success_path.exists() and success_is_valid(success_path, identity)

            if not selected:
                skip_copy(text)
                continue
            if already_done:
                skipped_rows = skip_copy(text)
                print(
                    f"SKIP {schema}.{table_name}: already complete; source rows traversed={skipped_rows:,}",
                    flush=True,
                )
                completed_requested.add(table_name)
            elif (output / "tables" / table_name).exists():
                raise RuntimeError(
                    f"Existing table directory is not valid for this source: "
                    f"{output / 'tables' / table_name}. Remove or move it explicitly before rerun."
                )
            elif args.dry_run:
                skipped_rows = skip_copy(text)
                print(
                    f"DRY  {schema}.{table_name}: columns={len(columns)} rows={skipped_rows:,}",
                    flush=True,
                )
                completed_requested.add(table_name)
            else:
                key = (schema, table_name)
                writer = TableWriter(
                    output_root=output,
                    schema_name=schema,
                    table=table_name,
                    columns=columns,
                    pg_types=pg_types.get(key, {}),
                    geometries=geoms.get(key, {}),
                    source=identity,
                    batch_rows=args.batch_rows,
                    file_rows=args.file_rows,
                    compression_level=args.compression_level,
                    reference=references.get(table_name),
                    allow_type_fallback=args.allow_type_fallback,
                    executor=executor,
                    workers=workers,
                    max_in_flight=max_in_flight,
                )
                print(
                    f"START {schema}.{table_name}: {len(columns)} columns",
                    flush=True,
                )
                try:
                    while True:
                        data_line = read_logical_copy_line(text)
                        if data_line == "":
                            raise EOFError(f"Unexpected EOF inside COPY for {table_name}")
                        if data_line == r"\.":
                            break
                        writer.add_line(data_line)
                        if writer.rows % 1_000_000 == 0:
                            elapsed = max(time.monotonic() - writer.started, 1e-6)
                            print(
                                f"  {table_name}: {writer.rows:,} rows "
                                f"({writer.rows / elapsed:,.0f} rows/s)",
                                flush=True,
                            )
                    success = writer.finish()
                except Exception:
                    raise
                print(
                    f"DONE  {table_name}: {success['rows']:,} rows, "
                    f"{success['parts']} parts, {success['parquet_bytes'] / 2**30:.3f} GiB",
                    flush=True,
                )
                completed_requested.add(table_name)

            if requested and requested <= completed_requested:
                print("All requested tables completed; stopping stream early.", flush=True)
                break
    finally:
        if text is not None:
            try:
                text.detach()
            except Exception:
                pass
        if binary is not None:
            try:
                binary.close()
            except Exception:
                pass
        for child in (tar_proc, rg_proc):
            if child is None:
                continue
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=True)

    if requested:
        missing = requested - completed_requested
        if missing:
            raise RuntimeError(f"Requested tables not found/completed: {sorted(missing)}")

    manifest = build_manifest(output)
    print(f"manifest    : {len(manifest)} completed tables", flush=True)
    print(f"COPY blocks : {tables_seen}", flush=True)
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Stream the BDNB pgdump tar.gz into typed Parquet/GeoParquet datasets."
    )
    p.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    p.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    p.add_argument("--reference", type=Path, default=DEFAULT_REFERENCE)
    p.add_argument("--rapidgzip", help="Path to rapidgzip CLI")
    p.add_argument("--tables", nargs="*", help="Convert only these table names")
    p.add_argument("--batch-rows", type=int, default=100_000)
    p.add_argument(
        "--file-rows",
        type=int,
        default=250_000,
        help="Rows per independently converted/written Parquet part",
    )
    p.add_argument("--compression-level", type=int, default=3)
    p.add_argument(
        "--workers",
        type=int,
        default=min(4, os.cpu_count() or 1),
        help="Parallel conversion/writer processes (1 disables process parallelism)",
    )
    p.add_argument(
        "--gzip-threads",
        type=int,
        help="rapidgzip worker threads; default = CPU count minus --workers",
    )
    p.add_argument(
        "--max-in-flight",
        type=int,
        help="Maximum queued/running Parquet parts; default = --workers",
    )
    p.add_argument("--dry-run", action="store_true")
    p.add_argument(
        "--allow-type-fallback",
        action="store_true",
        help="Allow unknown/missing PostgreSQL types to be preserved as Arrow string",
    )
    args = p.parse_args(argv)
    if args.batch_rows <= 0 or args.file_rows <= 0:
        p.error("--batch-rows and --file-rows must be positive")
    if args.file_rows < args.batch_rows:
        p.error("--file-rows must be >= --batch-rows")
    if args.workers <= 0:
        p.error("--workers must be positive")
    if args.gzip_threads is not None and args.gzip_threads <= 0:
        p.error("--gzip-threads must be positive")
    if args.max_in_flight is not None and args.max_in_flight <= 0:
        p.error("--max-in-flight must be positive")
    return args


def main(argv: list[str] | None = None) -> int:
    try:
        return convert(parse_args(argv))
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
