from __future__ import annotations

import argparse
import csv
import json
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


def is_dataset_boundary(base: Path, filenames: list[str], layer: str) -> bool:
    names = set(filenames)
    if layer == "parquet":
        return bool(
            {"dataset.json", "manifest.json", "manifest.parquet"} & names
        )
    if layer == "numpy":
        return any(name.endswith(".done.json") for name in filenames)
    return False


def boundary_metadata(base: Path) -> tuple[int, int, str] | None:
    """Read cheap aggregate metadata from canonical dataset markers.

    Returns None when the marker does not contain enough aggregate information,
    in which case the caller may fall back to a recursive filesystem scan.
    """
    candidates = [base / "dataset.json", base / "manifest.json"]
    for marker in candidates:
        if not marker.is_file():
            continue
        try:
            data = json.loads(marker.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            continue
        if not isinstance(data, dict):
            continue

        raw_bytes = data.get("bytes")
        raw_files = data.get("files")
        if raw_files is None:
            ntriples_files = data.get("ntriples_files")
            csv_files = data.get("csv_files")
            if isinstance(ntriples_files, int) and isinstance(csv_files, int):
                raw_files = ntriples_files + csv_files

        if isinstance(raw_bytes, int) and isinstance(raw_files, int):
            return raw_bytes, raw_files, ".parquet:manifest"

    return None


def scan_leaf_datasets(
    layer_root: Path,
    layer: str,
    *,
    fast: bool = False,
) -> list[Dataset]:
    """Discover dataset roots without descending into known dataset internals.

    Canonical parquet datasets may contain many nested table directories, so
    marker files such as dataset.json/manifest.json define the dataset boundary.
    Legacy numpy datasets may use *.done.json markers. Otherwise the fallback is
    a directory containing files and no descendant directory containing files.
    """
    if not layer_root.exists():
        return []

    datasets: list[Dataset] = []
    fallback_direct: dict[Path, tuple[int, int, Counter[str]]] = {}
    fallback_children_with_data: set[Path] = set()

    for dirpath, dirnames, filenames in os.walk(layer_root, topdown=True):
        base = Path(dirpath)
        dirnames[:] = [name for name in dirnames if not name.startswith(".")]

        if is_dataset_boundary(base, filenames, layer):
            if fast and layer == "parquet":
                meta = boundary_metadata(base)
                if meta is not None:
                    total, count, fmt = meta
                    rel = base.relative_to(layer_root).as_posix()
                    datasets.append(Dataset(layer, rel, total, count, fmt))
                    dirnames[:] = []
                    continue

            total = 0
            count = 0
            formats: Counter[str] = Counter()
            for nested_dirpath, nested_dirnames, nested_filenames in os.walk(base):
                nested_dirnames[:] = [
                    name for name in nested_dirnames if not name.startswith(".")
                ]
                nested_base = Path(nested_dirpath)
                for filename in nested_filenames:
                    path = nested_base / filename
                    try:
                        total += allocated_size(path)
                    except (FileNotFoundError, PermissionError, OSError):
                        continue
                    count += 1
                    formats[format_name(path)] += 1

            rel = base.relative_to(layer_root).as_posix()
            fmt = ",".join(
                f"{ext}:{n}"
                for ext, n in sorted(
                    formats.items(), key=lambda item: (-item[1], item[0])
                )
            )
            datasets.append(Dataset(layer, rel, total, count, fmt))
            dirnames[:] = []
            continue

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
            fallback_direct[base] = (total, count, formats)
            parent = base.parent
            while parent != layer_root.parent and parent != layer_root:
                fallback_children_with_data.add(parent)
                parent = parent.parent

    boundary_paths = {Path(d.relative_path) for d in datasets}
    for base, (size, count, formats) in fallback_direct.items():
        rel_path = base.relative_to(layer_root)
        if any(boundary in rel_path.parents or boundary == rel_path for boundary in boundary_paths):
            continue
        if base in fallback_children_with_data:
            continue

        rel = rel_path.as_posix()
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


def is_path_ancestor(parent: str, child: str) -> bool:
    p = Path(parent).parts
    c = Path(child).parts
    return len(p) < len(c) and c[: len(p)] == p


def same_parent_tree(a: str, b: str) -> bool:
    pa = Path(a)
    pb = Path(b)
    return pa.parent == pb.parent


def candidate_score(source: Dataset, target: Dataset) -> tuple[int, str]:
    if source.relative_path == target.relative_path:
        return 100, "exact-path"

    # Canonical derived datasets are often nested below one raw dataset root
    # (for example BDNB), or legacy outputs may fan out below the same root
    # (for example IMOPE departments).
    if is_path_ancestor(source.relative_path, target.relative_path):
        return 95, "raw-parent-of-derived"
    if is_path_ancestor(target.relative_path, source.relative_path):
        return 85, "derived-parent-of-raw"

    src = Path(source.relative_path)
    dst = Path(target.relative_path)
    src_leaf = normalized_name(src.name)
    dst_leaf = normalized_name(dst.name)

    if src_leaf == dst_leaf and parent_key(source.relative_path) == parent_key(target.relative_path):
        return 90, "same-parent+name"
    if src_leaf == dst_leaf:
        return 70, "same-name"

    src_tokens = {t for t in src_leaf.split("-") if len(t) >= 4}
    dst_tokens = {t for t in dst_leaf.split("-") if len(t) >= 4}
    if src_tokens and dst_tokens:
        common = src_tokens & dst_tokens
        overlap = len(common) / len(src_tokens | dst_tokens)
        if (
            len(common) >= 2
            and overlap >= 0.75
            and parent_key(source.relative_path) == parent_key(target.relative_path)
        ):
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

    ranked.sort(
        key=lambda item: (
            -item[0],
            len(Path(item[2].relative_path).parts),
            item[2].relative_path,
        )
    )
    best_score, best_reason, best = ranked[0]

    # For exact/ancestor relationships, multiple children are legitimate.
    if best_reason in {"exact-path", "raw-parent-of-derived", "derived-parent-of-raw"}:
        return best, best_score, best_reason

    if len(ranked) > 1 and ranked[1][0] == best_score and best_score < 100:
        return None, 0, "ambiguous"
    return best, best_score, best_reason


def gib(value: int) -> str:
    return f"{value / 2**30:.3f}"


def all_structural_matches(source: Dataset, targets: list[Dataset]) -> list[Dataset]:
    matches: list[Dataset] = []
    for target in targets:
        score, reason = candidate_score(source, target)
        if score >= 85 and reason in {
            "exact-path",
            "raw-parent-of-derived",
            "derived-parent-of-raw",
            "same-parent+name",
        }:
            matches.append(target)
    return matches


def build_rows(datasets: dict[str, list[Dataset]]) -> list[dict[str, str | int]]:
    rows: list[dict[str, str | int]] = []
    numpy = datasets["numpy"]
    parquet = datasets["parquet"]

    matched_numpy: set[str] = set()
    matched_parquet: set[str] = set()

    for raw in datasets["raw"]:
        np_struct = all_structural_matches(raw, numpy)
        pq_struct = all_structural_matches(raw, parquet)

        np_match, np_score, np_reason = best_candidate(raw, numpy)
        pq_match, pq_score, pq_reason = best_candidate(raw, parquet)

        for item in np_struct:
            matched_numpy.add(item.relative_path)
        for item in pq_struct:
            matched_parquet.add(item.relative_path)

        if np_match:
            matched_numpy.add(np_match.relative_path)
        if pq_match:
            matched_parquet.add(pq_match.relative_path)

        if pq_struct or (pq_match and pq_score >= 60):
            status = "parquet-candidate-present"
            action = "validate-parquet"
        elif np_struct or (np_match and np_score >= 60):
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
                "numpy_path": (
                    ";".join(item.relative_path for item in np_struct)
                    if len(np_struct) > 1
                    else (np_match.relative_path if np_match else "")
                ),
                "numpy_gib": gib(sum(item.bytes for item in np_struct)) if np_struct else (
                    gib(np_match.bytes) if np_match else "0.000"
                ),
                "numpy_match": (
                    "one-to-many-structural"
                    if len(np_struct) > 1
                    else np_reason
                ),
                "numpy_score": np_score,
                "parquet_path": (
                    ";".join(item.relative_path for item in pq_struct)
                    if len(pq_struct) > 1
                    else (pq_match.relative_path if pq_match else "")
                ),
                "parquet_gib": gib(sum(item.bytes for item in pq_struct)) if pq_struct else (
                    gib(pq_match.bytes) if pq_match else "0.000"
                ),
                "parquet_match": (
                    "one-to-many-structural"
                    if len(pq_struct) > 1
                    else pq_reason
                ),
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
    parser.add_argument(
        "--fast",
        action="store_true",
        help=(
            "use aggregate dataset.json/manifest.json metadata for canonical "
            "Parquet boundaries instead of recursively rescanning their files"
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    root = args.root.expanduser().resolve()
    datasets = {
        layer: scan_leaf_datasets(root / layer, layer, fast=args.fast)
        for layer in LAYERS
    }
    rows = build_rows(datasets)
    write_tsv(rows, args.output)
    print_summary(rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
