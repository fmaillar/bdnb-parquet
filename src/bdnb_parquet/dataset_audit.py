from __future__ import annotations

import argparse
import csv
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

DEFAULT_ROOT = Path("/mnt/data/datasets")
LAYERS = ("raw", "numpy", "parquet")


@dataclass(frozen=True)
class Usage:
    bytes: int = 0
    files: int = 0


def allocated_size(path: Path) -> int:
    """Return allocated bytes, matching du semantics when st_blocks is available."""
    st = path.stat(follow_symlinks=False)
    blocks = getattr(st, "st_blocks", None)
    if blocks is not None:
        return int(blocks) * 512
    return int(st.st_size)


def add_usage(target: dict[str, Usage], key: str, size: int) -> None:
    current = target.get(key, Usage())
    target[key] = Usage(current.bytes + size, current.files + 1)


def ancestors(parts: tuple[str, ...], max_depth: int) -> Iterable[str]:
    upper = min(len(parts), max_depth)
    for depth in range(1, upper + 1):
        yield "/".join(parts[:depth])


def scan_layer(root: Path, max_depth: int) -> dict[str, Usage]:
    """Scan one storage layer once and aggregate file usage to relative ancestors."""
    result: dict[str, Usage] = {}
    if not root.exists():
        return result

    for dirpath, dirnames, filenames in os.walk(root):
        base = Path(dirpath)

        # Hidden staging/cache directories are implementation details, not datasets.
        dirnames[:] = [
            name for name in dirnames
            if not name.startswith(".")
        ]

        for filename in filenames:
            path = base / filename
            try:
                rel = path.relative_to(root)
                size = allocated_size(path)
            except (FileNotFoundError, PermissionError, OSError):
                continue

            parts = rel.parts[:-1]
            if not parts:
                # A file immediately below the layer root.
                add_usage(result, ".", size)
                continue

            for key in ancestors(parts, max_depth):
                add_usage(result, key, size)

    return result


def layer_presence(raw: Usage, numpy: Usage, parquet: Usage) -> str:
    present = [
        name
        for name, usage in (
            ("raw", raw),
            ("numpy", numpy),
            ("parquet", parquet),
        )
        if usage.files
    ]
    return "+".join(present) if present else "none"


def migration_status(raw: Usage, numpy: Usage, parquet: Usage) -> str:
    has_raw = raw.files > 0
    has_numpy = numpy.files > 0
    has_parquet = parquet.files > 0

    if has_raw and has_parquet:
        return "parquet-present"
    if has_raw and has_numpy:
        return "legacy-derived-only"
    if has_raw:
        return "raw-only"
    if has_parquet and has_numpy:
        return "derived-without-raw"
    if has_parquet:
        return "parquet-without-raw"
    if has_numpy:
        return "numpy-without-raw"
    return "empty"


def gib(value: int) -> str:
    return f"{value / 2**30:.3f}"


def build_rows(
    scans: dict[str, dict[str, Usage]],
    *,
    depth: int,
) -> list[dict[str, str | int]]:
    keys = set()
    for layer in LAYERS:
        keys.update(scans[layer])

    rows: list[dict[str, str | int]] = []
    for key in sorted(keys):
        if key != "." and len(Path(key).parts) != depth:
            continue

        raw = scans["raw"].get(key, Usage())
        numpy = scans["numpy"].get(key, Usage())
        parquet = scans["parquet"].get(key, Usage())

        rows.append(
            {
                "relative_path": key,
                "presence": layer_presence(raw, numpy, parquet),
                "status": migration_status(raw, numpy, parquet),
                "raw_gib": gib(raw.bytes),
                "numpy_gib": gib(numpy.bytes),
                "parquet_gib": gib(parquet.bytes),
                "raw_files": raw.files,
                "numpy_files": numpy.files,
                "parquet_files": parquet.files,
            }
        )
    return rows


def write_tsv(rows: list[dict[str, str | int]], output: Path | None) -> None:
    fieldnames = [
        "relative_path",
        "presence",
        "status",
        "raw_gib",
        "numpy_gib",
        "parquet_gib",
        "raw_files",
        "numpy_files",
        "parquet_files",
    ]

    stream = sys.stdout
    close = False
    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        stream = output.open("w", encoding="utf-8", newline="")
        close = True

    try:
        writer = csv.DictWriter(
            stream,
            fieldnames=fieldnames,
            delimiter="\t",
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)
    finally:
        if close:
            stream.close()


def print_summary(rows: list[dict[str, str | int]]) -> None:
    counts: dict[str, int] = {}
    for row in rows:
        status = str(row["status"])
        counts[status] = counts.get(status, 0) + 1

    print(f"rows: {len(rows)}", file=sys.stderr)
    for status in sorted(counts):
        print(f"{status}: {counts[status]}", file=sys.stderr)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Cross-audit raw, numpy and parquet dataset layers by relative path. "
            "The scan is read-only."
        )
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=DEFAULT_ROOT,
        help="Dataset root containing raw/, numpy/ and parquet/",
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=3,
        help="Relative directory depth to report (default: 3)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Write TSV here instead of stdout",
    )
    args = parser.parse_args(argv)
    if args.depth <= 0:
        parser.error("--depth must be positive")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    root = args.root.expanduser().resolve()

    scans = {
        layer: scan_layer(root / layer, args.depth)
        for layer in LAYERS
    }
    rows = build_rows(scans, depth=args.depth)
    write_tsv(rows, args.output)
    print_summary(rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
