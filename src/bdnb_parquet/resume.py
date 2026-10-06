from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tarfile
from dataclasses import asdict
from pathlib import Path
from typing import Any, BinaryIO

from .parser import GeometrySpec, parse_ddl_line

SQL_MEMBER = "pgdump/bdnb.sql"
CHUNK_SIZE = 8 * 1024 * 1024


class _ChunkScanner:
    def __init__(self, stream: BinaryIO) -> None:
        self.stream = stream
        self.buffer = b""
        self.offset = 0
        self.eof = False

    def _fill(self) -> None:
        if self.eof:
            return
        chunk = self.stream.read(CHUNK_SIZE)
        if chunk:
            self.buffer += chunk
        else:
            self.eof = True

    def readline(self) -> tuple[int, bytes]:
        start = self.offset
        while True:
            pos = self.buffer.find(b"\n")
            if pos >= 0:
                line = self.buffer[: pos + 1]
                self.buffer = self.buffer[pos + 1 :]
                self.offset += len(line)
                return start, line
            if self.eof:
                line = self.buffer
                self.buffer = b""
                self.offset += len(line)
                return start, line
            self._fill()

    def skip_copy_data(self) -> int:
        """Skip COPY rows through the \\. terminator and return end offset."""
        while True:
            if self.buffer.startswith(b"\\.\n"):
                self.buffer = self.buffer[3:]
                self.offset += 3
                return self.offset

            pos = self.buffer.find(b"\n\\.\n")
            if pos >= 0:
                consumed = pos + 4
                self.buffer = self.buffer[consumed:]
                self.offset += consumed
                return self.offset

            if self.eof:
                raise EOFError("Unexpected EOF while indexing COPY data")

            # Keep enough overlap to recognize a terminator split across chunks.
            if len(self.buffer) > 4:
                consumed = len(self.buffer) - 4
                self.buffer = self.buffer[consumed:]
                self.offset += consumed
            self._fill()


def source_identity(source: Path) -> dict[str, Any]:
    st = source.stat()
    return {
        "path": str(source),
        "size": st.st_size,
        "mtime_ns": st.st_mtime_ns,
    }


def _scan_sql_member(
    stream: BinaryIO,
    *,
    tar_data_offset: int,
    sql_size: int,
    identity: dict[str, Any],
) -> dict[str, Any]:
    scanner = _ChunkScanner(stream)
    pg_types: dict[tuple[str, str], dict[str, str]] = {}
    geometries: dict[tuple[str, str], dict[str, GeometrySpec]] = {}
    tables: list[dict[str, Any]] = []

    while scanner.offset < sql_size:
        line_start, raw = scanner.readline()
        if not raw:
            break
        try:
            line = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise UnicodeError(
                f"Invalid UTF-8 in SQL control stream at offset {line_start}"
            ) from exc

        parsed = parse_ddl_line(line)
        if parsed is None:
            continue
        kind, payload = parsed

        if kind == "column":
            schema, table, column, pg_type = payload
            pg_types.setdefault((schema, table), {})[column] = pg_type
            continue

        if kind == "geometry":
            schema, table, geom = payload
            geometries.setdefault((schema, table), {})[geom.name] = geom
            continue

        if kind != "copy":
            continue

        schema, table, columns = payload
        key = (schema, table)
        block_end = scanner.skip_copy_data()
        table_geometries = geometries.get(key, {})
        tables.append(
            {
                "schema": schema,
                "table": table,
                "columns": columns,
                "pg_types": pg_types.get(key, {}),
                "geometries": {
                    name: asdict(spec)
                    for name, spec in table_geometries.items()
                },
                "sql_offset": line_start,
                "tar_offset": tar_data_offset + line_start,
                "range_size": block_end - line_start,
            }
        )

    return {
        "version": 1,
        "source": identity,
        "sql_member": SQL_MEMBER,
        "sql_tar_offset": tar_data_offset,
        "sql_size": sql_size,
        "tables": tables,
    }


def build_resume_index(
    *,
    source: Path,
    rapidgzip: str,
    index_path: Path,
    catalog_path: Path,
    gzip_threads: int,
) -> dict[str, Any]:
    """Build rapidgzip seek index and COPY-block catalog in one streaming pass."""
    identity = source_identity(source)
    index_path.parent.mkdir(parents=True, exist_ok=True)
    catalog_path.parent.mkdir(parents=True, exist_ok=True)

    tmp_index = index_path.with_name(index_path.name + ".tmp")
    tmp_catalog = catalog_path.with_name(catalog_path.name + ".tmp")
    for path in (tmp_index, tmp_catalog):
        if path.exists():
            path.unlink()

    proc = subprocess.Popen(
        [
            rapidgzip,
            "--export-index",
            str(tmp_index),
            "-d",
            "-c",
            "-P",
            str(gzip_threads),
            str(source),
        ],
        stdout=subprocess.PIPE,
        stderr=sys.stderr,
    )
    if proc.stdout is None:
        raise RuntimeError("rapidgzip stdout pipe unavailable while building index")

    catalog: dict[str, Any] | None = None
    try:
        with tarfile.open(fileobj=proc.stdout, mode="r|*") as archive:
            for member in archive:
                normalized = member.name.lstrip("./")
                if normalized != SQL_MEMBER:
                    continue
                extracted = archive.extractfile(member)
                if extracted is None:
                    raise RuntimeError(f"Cannot extract {SQL_MEMBER} while indexing")
                catalog = _scan_sql_member(
                    extracted,
                    tar_data_offset=member.offset_data,
                    sql_size=member.size,
                    identity=identity,
                )
                # Continue iterating so rapidgzip reaches EOF and finalizes the index.
            # Iteration to archive EOF is sufficient to consume skipped members.
        rc = proc.wait()
    except Exception:
        proc.terminate()
        proc.wait()
        tmp_index.unlink(missing_ok=True)
        tmp_catalog.unlink(missing_ok=True)
        raise

    if rc != 0:
        tmp_index.unlink(missing_ok=True)
        raise RuntimeError(f"rapidgzip index build failed with exit status {rc}")
    if catalog is None:
        tmp_index.unlink(missing_ok=True)
        raise RuntimeError(f"{SQL_MEMBER} not found in tar archive")
    if not tmp_index.exists():
        raise RuntimeError("rapidgzip did not create the requested index")

    tmp_catalog.write_text(
        json.dumps(catalog, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(tmp_index, index_path)
    os.replace(tmp_catalog, catalog_path)
    return catalog


def load_resume_catalog(
    catalog_path: Path,
    *,
    identity: dict[str, Any],
) -> dict[str, Any] | None:
    if not catalog_path.exists():
        return None
    try:
        catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if catalog.get("version") != 1 or catalog.get("source") != identity:
        return None
    if not isinstance(catalog.get("tables"), list):
        return None
    return catalog


def open_indexed_range(
    *,
    source: Path,
    rapidgzip: str,
    index_path: Path,
    offset: int,
    size: int,
    gzip_threads: int,
) -> tuple[subprocess.Popen[bytes], BinaryIO]:
    if not index_path.exists():
        raise FileNotFoundError(f"rapidgzip index not found: {index_path}")
    if offset < 0 or size <= 0:
        raise ValueError(f"Invalid indexed range: size={size}, offset={offset}")

    proc = subprocess.Popen(
        [
            rapidgzip,
            "--import-index",
            str(index_path),
            "--ranges",
            f"{size}@{offset}",
            "-d",
            "-c",
            "-P",
            str(gzip_threads),
            str(source),
        ],
        stdout=subprocess.PIPE,
        stderr=sys.stderr,
    )
    if proc.stdout is None:
        proc.terminate()
        proc.wait()
        raise RuntimeError("rapidgzip stdout pipe unavailable for indexed range")
    return proc, proc.stdout
