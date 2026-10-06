from __future__ import annotations

import re
import struct
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any

ALTER_RE = re.compile(
    r'^ALTER TABLE\s+"([^"]+)"\."([^"]+)"\s+ADD COLUMN\s+"([^"]+)"\s+(.+);$',
    re.IGNORECASE,
)
COPY_RE = re.compile(
    r'^COPY\s+"([^"]+)"\."([^"]+)"\s*\((.*)\)\s+FROM STDIN;$',
    re.IGNORECASE,
)
GEOM_RE = re.compile(
    r"^SELECT\s+AddGeometryColumn\('\s*([^']+)\s*','\s*([^']+)\s*','\s*([^']+)\s*',\s*(\d+)\s*,'\s*([^']+)\s*',\s*(\d+)\s*\);$",
    re.IGNORECASE,
)
CONSTRAINT_RE = re.compile(
    r"\s+(?:CONSTRAINT|NOT\s+NULL|NULL|DEFAULT|PRIMARY\s+KEY|REFERENCES|CHECK|UNIQUE)\b",
    re.IGNORECASE,
)


def normalize_iso_datetime(value: str) -> str:
    """Normalize YYYY/MM/DD or YYYY.MM.DD while preserving the time part."""
    if (
        len(value) >= 10
        and value[4] in "./"
        and value[7] in "./"
        and value[:4].isdigit()
        and value[5:7].isdigit()
        and value[8:10].isdigit()
    ):
        value = f"{value[:4]}-{value[5:7]}-{value[8:]}"
    return value.replace(" ", "T", 1)


@dataclass(frozen=True)
class GeometrySpec:
    name: str
    geometry_type: str
    srid: int
    dimensions: int


def split_copy_columns(text: str) -> list[str]:
    cols: list[str] = []
    i = 0
    n = len(text)
    while i < n:
        while i < n and text[i].isspace():
            i += 1
        if i >= n:
            break
        if text[i] == '"':
            i += 1
            buf: list[str] = []
            while i < n:
                if text[i] == '"':
                    if i + 1 < n and text[i + 1] == '"':
                        buf.append('"')
                        i += 2
                        continue
                    i += 1
                    break
                buf.append(text[i])
                i += 1
            cols.append("".join(buf))
        else:
            start = i
            while i < n and text[i] != ',':
                i += 1
            cols.append(text[start:i].strip())
        while i < n and text[i].isspace():
            i += 1
        if i < n:
            if text[i] != ',':
                raise ValueError(f"Invalid COPY column list near {text[i:i+30]!r}")
            i += 1
    return cols


def extract_pg_type(spec: str) -> str:
    """Return the SQL type portion from an ALTER TABLE ADD COLUMN clause."""
    spec = spec.strip()
    match = CONSTRAINT_RE.search(spec)
    if match:
        spec = spec[: match.start()].rstrip()
    return re.sub(r"\s+", " ", spec)


def parse_ddl_line(line: str) -> tuple[str, tuple[Any, ...]] | None:
    line = line.rstrip("\r\n")
    m = ALTER_RE.match(line)
    if m:
        schema, table, column, rest = m.groups()
        return "column", (schema, table, column, extract_pg_type(rest))
    m = GEOM_RE.match(line)
    if m:
        schema, table, column, srid, geom_type, dimensions = m.groups()
        return "geometry", (
            schema,
            table,
            GeometrySpec(column, geom_type, int(srid), int(dimensions)),
        )
    m = COPY_RE.match(line)
    if m:
        schema, table, columns = m.groups()
        return "copy", (schema, table, split_copy_columns(columns))
    return None


def pg_copy_unescape(value: str) -> str | None:
    if value == r"\N":
        return None
    out: list[str] = []
    i = 0
    while i < len(value):
        c = value[i]
        if c != "\\":
            out.append(c)
            i += 1
            continue
        i += 1
        if i >= len(value):
            out.append("\\")
            break
        c = value[i]
        mapping = {
            "b": "\b",
            "f": "\f",
            "n": "\n",
            "r": "\r",
            "t": "\t",
            "v": "\v",
            "\\": "\\",
        }
        if c in mapping:
            out.append(mapping[c])
            i += 1
            continue
        if c in "01234567":
            j = i
            while j < len(value) and j < i + 3 and value[j] in "01234567":
                j += 1
            out.append(chr(int(value[i:j], 8)))
            i = j
            continue
        if c == "x":
            j = i + 1
            while j < len(value) and j < i + 3 and value[j] in "0123456789abcdefABCDEF":
                j += 1
            if j > i + 1:
                out.append(chr(int(value[i + 1 : j], 16)))
                i = j
                continue
        out.append(c)
        i += 1
    return "".join(out)


def trailing_backslash_count(text: str) -> int:
    return len(text) - len(text.rstrip("\\"))


def parse_pg_array(text: str) -> list[str | None]:
    """Parse a one-dimensional PostgreSQL array literal after COPY unescaping."""
    if text == "{}":
        return []
    if not (text.startswith("{") and text.endswith("}")):
        raise ValueError(f"Unsupported PostgreSQL array literal: {text[:100]!r}")
    inner = text[1:-1]
    result: list[str | None] = []
    buf: list[str] = []
    quoted = False
    was_quoted = False
    escaped = False

    def finish() -> None:
        nonlocal buf, was_quoted
        token = "".join(buf)
        if not was_quoted and token.upper() == "NULL":
            result.append(None)
        else:
            result.append(token)
        buf = []
        was_quoted = False

    i = 0
    while i < len(inner):
        c = inner[i]
        if escaped:
            buf.append(c)
            escaped = False
            i += 1
            continue
        if c == "\\":
            escaped = True
            i += 1
            continue
        if quoted:
            if c == '"':
                quoted = False
            else:
                buf.append(c)
            i += 1
            continue
        if c == '"':
            quoted = True
            was_quoted = True
            i += 1
            continue
        if c == ',':
            finish()
            i += 1
            continue
        buf.append(c)
        i += 1
    if quoted or escaped:
        raise ValueError(f"Malformed PostgreSQL array literal: {text[:100]!r}")
    finish()
    return result


def normalize_pg_type(pg_type: str) -> str:
    return re.sub(r"\s+", " ", pg_type.strip().lower())


def split_array_type(pg_type: str) -> tuple[str, int]:
    t = normalize_pg_type(pg_type)
    depth = 0
    while t.endswith("[]"):
        depth += 1
        t = t[:-2].rstrip()
    return t, depth




def ewkb_to_wkb(value: str) -> bytes:
    """Convert the 2D PostGIS EWKB emitted by COPY to standard WKB.

    BDNB geometries carry the EWKB SRID flag and an inline SRID (for example
    0x20000001 + 2154 for a Point). GeoParquet declares encoding=WKB, so the
    SRID flag and 4-byte SRID payload must not be stored in the geometry bytes;
    the CRS is carried by GeoParquet metadata instead.
    """
    text = value[2:] if value.startswith("\\x") else value
    data = bytes.fromhex(text)
    if len(data) < 5:
        raise ValueError("Geometry payload too short for WKB/EWKB")

    endian = data[0]
    if endian == 0:
        order = ">"
    elif endian == 1:
        order = "<"
    else:
        raise ValueError(f"Invalid WKB byte order: {endian}")

    type_word = struct.unpack_from(order + "I", data, 1)[0]
    flag_z = 0x80000000
    flag_m = 0x40000000
    flag_srid = 0x20000000
    flag_bbox = 0x10000000

    if type_word & (flag_z | flag_m | flag_bbox):
        raise ValueError(
            "Unsupported EWKB Z/M/BBOX flags; refusing to emit mislabeled GeoParquet WKB"
        )
    if not (type_word & flag_srid):
        return data
    if len(data) < 9:
        raise ValueError("EWKB SRID flag set but SRID payload is missing")

    clean_type = type_word & ~flag_srid
    return (
        data[:1]
        + struct.pack(order + "I", clean_type)
        + data[9:]
    )

def convert_scalar(value: str | None, pg_type: str) -> Any:
    if value is None:
        return None
    t = normalize_pg_type(pg_type)
    if t.startswith("geometry"):
        return ewkb_to_wkb(value)
    if t == "bytea":
        if value.startswith("\\x"):
            return bytes.fromhex(value[2:])
        return value.encode("latin1")
    if t in {"bool", "boolean"}:
        v = value.strip().lower()
        if v in {"t", "true", "1"}:
            return True
        if v in {"f", "false", "0"}:
            return False
        raise ValueError(f"Invalid PostgreSQL boolean {value!r}")
    if t in {"int2", "smallint"}:
        return int(value)
    if t in {"int4", "integer", "serial", "serial4"}:
        return int(value)
    if t in {"int8", "bigint", "bigserial", "serial8"}:
        return int(value)
    if t in {"real", "float4"}:
        return float(value)
    if t in {"double precision", "float8", "float"} or t.startswith("float("):
        return float(value)
    if t.startswith("numeric(") or t.startswith("decimal("):
        return Decimal(value)
    if t in {"numeric", "decimal"}:
        return float(value)
    if t == "date":
        return date.fromisoformat(normalize_iso_datetime(value))
    if t.startswith("timestamp"):
        normalized = normalize_iso_datetime(value)
        try:
            dt = datetime.fromisoformat(normalized)
        except ValueError as exc:
            # Python datetime rejects leap-second notation (:60), while
            # PostgreSQL dumps can contain it. Arrow timestamps cannot encode
            # second=60 either, so normalize it to the following instant.
            match = re.match(
                r"^(.*T\\d{2}:\\d{2}):60(\\.\\d+)?(Z|[+-]\\d{2}(?::?\\d{2})?)?$",
                normalized,
            )
            if match is None:
                raise ValueError(
                    f"Invalid PostgreSQL timestamp {value!r} "
                    f"(normalized {normalized!r})"
                ) from exc
            prefix, fraction, zone = match.groups()
            base = datetime.fromisoformat(
                prefix + ":59" + (fraction or "") + (zone or "")
            )
            dt = base + timedelta(seconds=1)
        if "with time zone" in t or t.startswith("timestamptz"):
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
        return dt
    if t.startswith("time"):
        return time.fromisoformat(value)
    return value


def convert_value(value: str | None, pg_type: str) -> Any:
    base, depth = split_array_type(pg_type)
    if depth == 0:
        return convert_scalar(value, base)
    if value is None:
        return None
    if depth != 1:
        raise ValueError(f"Only one-dimensional arrays are supported, got {pg_type!r}")
    return [convert_scalar(v, base) for v in parse_pg_array(value)]
