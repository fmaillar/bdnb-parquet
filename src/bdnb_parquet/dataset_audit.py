from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import unicodedata
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

DEFAULT_ROOT = Path("/mnt/data/datasets")
LAYERS = ("raw", "numpy", "parquet")


@dataclass(frozen=True)
class Dataset:
    layer: str
    relative_path: str
    bytes: int
    files: int
    formats: str


def allocated_size(path: Path) -> int:
    st = path.stat(follow_symlinks=False)
    blocks = getattr(st, "st_blocks", None)
    return int(blocks) * 512 if blocks is not None else int(st.st_size)


def normalized_name(text: str) -> str:
    value = unicodedata.normalize("NFKD", text)
    value = "".join(c for c in value if not unicodedata.combining(c))
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", "-", value).strip("-")
    return value


def format_name(path: Path) -> str:
    name = path.name.lower()
    for suffix in (
        ".tar.gz",
        ".tar.zst",
        ".tar.xz",
        ".geojson.gz",
        ".csv.gz",
        ".json.gz",
    ):
        if name.endswith(suffix):
            return suffix
    return path.suffix.lower() or "<no-ext>"


def scan_leaf_datasets(layer_root: Path, layer: str) -> list[Dataset]:
    """Return directories containing files and no descendant directory with files."""
    if not layer_root.exists():
        return []

    direct: dict[Path, tuple[int, int, Counter[str]]] = {}
    children_with_data: set[Path] = set()

    for dirpath, dirnames, filenames in os.walk(layer_root):
        base = Path(dirpath)
        dirnames[:] = [name for name in dirnames if not name.startswith(".")]

        total = 0
        count = 0
        formats: Counter[str] = Counter()
        for filename in filenames:
            path = base / filename
            try:
                total += allocated_size(path)
            except (FileNotFoundError, PermissionError, OSError):
                continue
            count += 1
            formats[format_name(path)] += 1

        if count:
            direct[base] = (total, count, formats)
            parent = base.parent
            while parent != layer_root.parent and parent != layer_root:
                children_with_data.add(parent)
                parent = parent.parent

    datasets: list[Dataset] = []
    for base, (size, count, formats) in direct.items():
        if base in children_with_data:
            continue
        rel = base.relative_to(layer_root).as_posix()
        fmt = ",".join(
            f"{ext}:{n}"
            for ext, n in sorted(formats.items(), key=lambda item: (-item[1], item[0]))
        )
        datasets.append(Dataset(layer, rel, size, count, fmt))

    return sorted(datasets, key=lambda d: d.relative_path)


def parent_key(path: str) -> str:
    p = Path(path)
    if len(p.parts) <= 1:
        return "."
    return normalized_name("/".join(p.parts[:-1]))


def candidate_score(source: Dataset, target: Dataset) -> tuple[int, str]:
    if source.relative_path == target.relative_path:
        return 100, "exact-path"

    src = Path(source.relative_path)
    dst = Path(target.relative_path)
    src_leaf = normalized_name(src.name)
    dst_leaf = normalized_name(dst.name)

    if src_leaf == dst_leaf and parent_key(source.relative_path) == parent_key(target.relative_path):
        return 90, "same-parent+name"
    if src_leaf == dst_leaf:
        return 70, "same-name"

    # Conservative token overlap for renamed derived datasets.
    src_tokens = {t for t in src_leaf.split("-") if len(t) >= 4}
    dst_tokens = {t for t in dst_leaf.split("-") if len(t) >= 4}
    if src_tokens and dst_tokens:
        overlap = len(src_tokens & dst_tokens) / len(src_tokens | dst_tokens)
        if overlap >= 0.75 and parent_key(source.relative_path) == parent_key(target.relative_path):
            return 60, "same-parent+similar-name"

    return 0, ""


def best_candidate(source: Dataset, targets: list[Dataset]) -> tuple[Dataset | None, int, str]:
    ranked: list[tuple[int, str, Dataset]] = []
    for target in targets:
        score, reason = candidate_score(source, target)
        if score:
            ranked.append((score, reason, target))
    if not ranked:
        return None, 0, ""
    ranked.sort(key=lambda item: (-item[0], item[2].relative_path))
    best_score, best_reason, best = ranked[0]

    # Avoid asserting ambiguous name-only matches.
    if len(ranked) > 1 and ranked[1][0] == best_score and best_score < 100:
        return None, 0, "ambiguous"
    return best, best_score, best_reason


def gib(value: int) -> str:
    return f"{value / 2**30:.3f}"


def build_rows(datasets: dict[str, list[Dataset]]) -> list[dict[str, str | int]]:
    rows: list[dict[str, str | int]] = []
    numpy = datasets["numpy"]
    parquet = datasets["parquet"]

    matched_numpy: set[str] = set()
    matched_parquet: set[str] = set()

    for raw in datasets["raw"]:
        np_match, np_score, np_reason = best_candidate(raw, numpy)
        pq_match, pq_score, pq_reason = best_candidate(raw, parquet)

        if np_match:
            matched_numpy.add(np_match.relative_path)
        if pq_match:
            matched_parquet.add(pq_match.relative_path)

        if pq_match and pq_score >= 60:
            status = "parquet-candidate-present"
            action = "validate-parquet"
        elif np_match and np_score >= 60:
            status = "legacy-only"
            action = "convert-raw-to-parquet"
        else:
            status = "raw-only"
            action = "convert-raw-to-parquet"

        rows.append(
            {
                "raw_path": raw.relative_path,
                "raw_gib": gib(raw.bytes),
                "raw_files": raw.files,
                "raw_formats": raw.formats,
                "numpy_path": np_match.relative_path if np_match else "",
                "numpy_gib": gib(np_match.bytes) if np_match else "0.000",
                "numpy_match": np_reason,
                "numpy_score": np_score,
                "parquet_path": pq_match.relative_path if pq_match else "",
                "parquet_gib": gib(pq_match.bytes) if pq_match else "0.000",
                "parquet_match": pq_reason,
                "parquet_score": pq_score,
                "status": status,
                "action": action,
            }
        )

    for item in numpy:
        if item.relative_path not in matched_numpy:
            rows.append(
                {
                    "raw_path": "",
                    "raw_gib": "0.000",
                    "raw_files": 0,
                    "raw_formats": "",
                    "numpy_path": item.relative_path,
                    "numpy_gib": gib(item.bytes),
                    "numpy_match": "unmatched",
                    "numpy_score": 0,
                    "parquet_path": "",
                    "parquet_gib": "0.000",
                    "parquet_match": "",
                    "parquet_score": 0,
                    "status": "orphan-numpy",
                    "action": "manual-review",
                }
            )

    for item in parquet:
        if item.relative_path not in matched_parquet:
            rows.append(
                {
                    "raw_path": "",
                    "raw_gib": "0.000",
                    "raw_files": 0,
                    "raw_formats": "",
                    "numpy_path": "",
                    "numpy_gib": "0.000",
                    "numpy_match": "",
                    "numpy_score": 0,
                    "parquet_path": item.relative_path,
                    "parquet_gib": gib(item.bytes),
                    "parquet_match": "unmatched",
                    "parquet_score": 0,
                    "status": "orphan-parquet",
                    "action": "manual-review",
                }
            )

    return rows


FIELDNAMES = [
    "raw_path",
    "raw_gib",
    "raw_files",
    "raw_formats",
    "numpy_path",
    "numpy_gib",
    "numpy_match",
    "numpy_score",
    "parquet_path",
    "parquet_gib",
    "parquet_match",
    "parquet_score",
    "status",
    "action",
]


def write_tsv(rows: list[dict[str, str | int]], output: Path | None) -> None:
    stream = sys.stdout
    close = False
    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        stream = output.open("w", encoding="utf-8", newline="")
        close = True
    try:
        writer = csv.DictWriter(
            stream,
            fieldnames=FIELDNAMES,
            delimiter="\t",
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)
    finally:
        if close:
            stream.close()


def print_summary(rows: list[dict[str, str | int]]) -> None:
    counts: Counter[str] = Counter(str(row["status"]) for row in rows)
    raw_to_convert_gib = sum(
        float(row["raw_gib"])
        for row in rows
        if row["action"] == "convert-raw-to-parquet"
    )
    legacy_gib = sum(
        float(row["numpy_gib"])
        for row in rows
        if row["numpy_path"]
    )

    print(f"rows: {len(rows)}", file=sys.stderr)
    for status, count in sorted(counts.items()):
        print(f"{status}: {count}", file=sys.stderr)
    print(f"raw GiB queued for conversion: {raw_to_convert_gib:.3f}", file=sys.stderr)
    print(f"matched/orphan numpy GiB inventoried: {legacy_gib:.3f}", file=sys.stderr)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only migration audit of raw, legacy numpy and canonical parquet "
            "dataset leaves, including conservative cross-layer matching."
        )
    )
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--output", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    root = args.root.expanduser().resolve()
    datasets = {
        layer: scan_leaf_datasets(root / layer, layer)
        for layer in LAYERS
    }
    rows = build_rows(datasets)
    write_tsv(rows, args.output)
    print_summary(rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
