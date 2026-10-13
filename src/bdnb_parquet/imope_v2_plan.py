"""Read-only, department-aware planning for a second IMoPE Parquet compaction.

The original IMoPE publication is immutable. This module inventories only the
currently physical, un-compacted Parquet files and proposes *cross-department*
cohorts that agree on every Arrow field and every schema metadata field except
GeoParquet bbox declarations. It does not rewrite data or authorize publishing.

Optional --output-dir writes an external audit plan, never into IMoPE itself.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

MIB = 2**20
DEFAULT_TARGET_MIB = 256


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")


def _checked_bbox(value: Any, source: Path, key: str) -> tuple[float, ...]:
    if (not isinstance(value, list) or len(value) not in (4, 6)
        or any(type(x) not in (int, float) or not math.isfinite(x)
               for x in value)):
        raise ValueError(f"Malformed GeoParquet bbox {key}: {source}")
    dims = len(value) // 2
    if any(value[i] > value[i + dims] for i in range(dims)):
        raise ValueError(f"Inverted GeoParquet bbox {key}: {source}")
    return tuple(float(x) for x in value)


def _geo_profile(raw: bytes, source: Path) -> tuple[bytes, dict[str, tuple[float, ...]]]:
    """Strip *only* GeoParquet bbox keys; extract audited original extents."""
    try:
        geo = json.loads(raw)
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValueError(f"Invalid GeoParquet JSON: {source}") from exc
    if not isinstance(geo, dict) or not isinstance(geo.get("columns"), dict):
        raise ValueError(f"Invalid GeoParquet columns: {source}")
    bbox: dict[str, tuple[float, ...]] = {}
    if "bbox" in geo:
        bbox["root"] = _checked_bbox(geo.pop("bbox"), source, "root")
    for name, column in geo["columns"].items():
        if not isinstance(name, str) or not isinstance(column, dict):
            raise ValueError(f"Invalid GeoParquet column: {source}")
        if "bbox" in column:
            bbox[f"columns/{name}"] = _checked_bbox(
                column.pop("bbox"), source, f"columns/{name}",
            )
    return _canonical_json(geo), bbox


@dataclass(frozen=True)
class SourceInfo:
    path: str
    department: str
    table: str
    bytes: int
    rows: int
    structure_sha256: str
    metadata_sha256: str
    group_sha256: str
    geo: bool
    bboxes: dict[str, tuple[float, ...]]


def _inspect(path: Path, root: Path) -> SourceInfo:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"Unsafe source file: {path}")
    rel = path.relative_to(root)
    if len(rel.parts) < 2:
        raise ValueError(f"IMoPE table without department: {rel}")
    parquet = pq.ParquetFile(path)
    try:
        arrow_schema = parquet.schema_arrow
        rows = parquet.metadata.num_rows
    finally:
        parquet.close()

    structure = _sha(
        arrow_schema.remove_metadata().serialize().to_pybytes(),
    )
    metadata: list[tuple[str, str]] = []
    bbox: dict[str, tuple[float, ...]] = {}
    geo = False
    for key, raw in sorted((arrow_schema.metadata or {}).items()):
        if key == b"geo":
            geo = True
            raw, bbox = _geo_profile(raw, path)
        metadata.append((key.hex(), _sha(raw)))
    metadata_sha = _sha(_canonical_json(metadata))
    group_id = _sha(_canonical_json({
        "table": path.name,
        "structure": structure,
        "metadata": metadata_sha,
    }))
    return SourceInfo(
        path=rel.as_posix(),
        department=rel.parts[0],
        table=path.name,
        bytes=path.stat().st_size,
        rows=rows,
        structure_sha256=structure,
        metadata_sha256=metadata_sha,
        group_sha256=group_id,
        geo=geo,
        bboxes=bbox,
    )


def _union_bboxes(
    sources: list[SourceInfo],
) -> tuple[dict[str, list[float]], list[str]]:
    """Only propose extent unions; do not read/reproject any geometry."""
    if not sources[0].geo:
        return {}, []
    keys = set().union(*(item.bboxes.keys() for item in sources))
    warnings: list[str] = []
    if not keys and any(item.rows for item in sources):
        warnings.append("missing-all-geo-bboxes: geometry scan needed")
    union: dict[str, list[float]] = {}
    for key in sorted(keys):
        members = [item.bboxes[key] for item in sources if key in item.bboxes]
        if not members:
            continue
        if any(item.rows and key not in item.bboxes for item in sources):
            warnings.append(f"missing-nonempty-bbox:{key}")
            continue
        dimensions = {len(box) for box in members}
        if len(dimensions) != 1:
            warnings.append(f"incompatible-bbox-dimensions:{key}")
            continue
        dims = len(members[0]) // 2
        union[key] = [
            *[min(box[i] for box in members) for i in range(dims)],
            *[max(box[i + dims] for box in members) for i in range(dims)],
        ]
    return union, warnings


def _audit_prior_aliases(
    root: Path, planned_sources: list[dict[str, Any]],
) -> tuple[dict[str, int | bool], list[dict[str, Any]]]:
    """Identify prior IMoPE aliases affected by the proposed regrouping.

    This is provenance *planning*, not a metadata rewrite. In particular,
    SHA-256 authenticated aliases external to IMoPE are never assumed to
    be safely remappable to a new physical file.
    """
    index_file = root / "_COMPACTION_SOURCES.json"
    if index_file.is_symlink():
        raise ValueError(f"Unsafe existing IMoPE source index: {index_file}")
    if not index_file.exists():
        return {
            "existing_source_index_present": False,
            "existing_logical_aliases": 0,
            "existing_aliases_requiring_retarget": 0,
            "existing_release_scoped_aliases": 0,
        }, []
    index = json.loads(index_file.read_text(encoding="utf-8"))
    if not isinstance(index, dict) or index.get("dataset") != "imope":
        raise ValueError("Unexpected existing IMoPE compaction source index")
    aliases = index.get("deduplicated_aliases")
    if not isinstance(aliases, list):
        raise ValueError("Existing IMoPE alias inventory missing")
    mapping = {source["original"]: source for source in planned_sources}
    rewritten: list[dict[str, Any]] = []
    seen: set[str] = set()
    external = 0

    for alias in sorted(aliases, key=lambda x: x["original"]):
        original = alias.get("original")
        if not isinstance(original, str) or original in seen:
            raise ValueError("Duplicate or invalid existing IMoPE alias name")
        seen.add(original)
        scope = alias.get("published_scope")
        raw = alias.get("published")
        if not isinstance(raw, str):
            raise ValueError(f"Invalid IMoPE alias physical path: {original}")
        physical = Path(raw)
        if physical.is_absolute() or ".." in physical.parts or not physical.parts:
            raise ValueError(f"Unsafe IMoPE alias physical path: {original}")
        start = alias.get("first_row")
        length = alias.get("rows")
        if (type(start) is not int or type(length) is not int
            or start < 0 or length < 0):
            raise ValueError(f"Unsafe IMoPE alias row range: {original}")

        if scope == "release":
            if physical.parts[0] != "parquet":
                raise ValueError(f"Unsafe external IMoPE alias path: {original}")
            external += 1
            continue
        if scope != "imope":
            raise ValueError(f"Unknown IMoPE alias scope: {scope}")
        file = root / physical
        if file.is_symlink() or not file.is_file():
            raise ValueError(f"Existing IMoPE canonical file missing: {physical}")
        if physical.as_posix() not in mapping:
            # Canonical file is not selected for a second regrouping.
            continue
        target = mapping[physical.as_posix()]
        if start + length > target["rows"]:
            raise ValueError(
                f"IMoPE existing alias row range exceeds original: {original}"
            )
        rewritten.append({
            "original_alias": original,
            "previous_canonical": physical.as_posix(),
            "previous_first_row": start,
            "rows": length,
            "proposed_output": target["output"],
            "proposed_first_row": target["first_row"] + start,
            "original_sha256": alias.get("sha256"),
            "action": "future-index-migration-required-NOT-executed",
        })

    return {
        "existing_source_index_present": True,
        "existing_logical_aliases": len(aliases),
        "existing_aliases_requiring_retarget": len(rewritten),
        "existing_release_scoped_aliases": external,
    }, rewritten


def build_plan(root: Path, *, target_mib: int = DEFAULT_TARGET_MIB) -> dict[str, Any]:
    """Inspect source footers only, then bin-pack each exact-safe cohort.

    No input arrays or geometry payloads are loaded. A proposed row range is
    deterministic but is not an approval to remove/overwrite source files.
    """
    if target_mib < 1:
        raise ValueError("--target-mib must be >= 1")
    root = root.expanduser().resolve()
    if not root.is_dir():
        raise FileNotFoundError(f"IMoPE root missing: {root}")
    candidates = sorted(root.rglob("*.parquet"))
    if not candidates:
        raise ValueError(f"IMoPE contains no physical Parquet files: {root}")

    original = [p for p in candidates if not p.name.startswith("compact-")]
    previous = len(candidates) - len(original)
    info = [_inspect(p, root) for p in original]
    cohorts: dict[tuple[str, str], list[SourceInfo]] = defaultdict(list)
    for item in info:
        cohorts[(item.table, item.group_sha256)].append(item)

    target_bytes = target_mib * MIB
    batches: list[dict[str, Any]] = []
    proposed_sources: list[dict[str, Any]] = []
    untouched: list[dict[str, Any]] = []

    def flush(items: list[SourceInfo], table: str, group_id: str, ordinal: int) -> None:
        if len(items) < 2:
            for item in items:
                untouched.append({
                    "path": item.path, "department": item.department,
                    "table": item.table, "bytes": item.bytes,
                    "rows": item.rows,
                    "reason": "oversize-or-singleton-compatible-cohort",
                })
            return

        batch_id = f"{group_id[:16]}-{ordinal:04d}"
        destination = f"tables/{Path(table).stem}/compact-v2-{batch_id}.parquet"
        bbox, warnings = _union_bboxes(items)
        count = 0
        for item in items:
            proposed_sources.append({
                "batch_id": batch_id,
                "output": destination,
                "original": item.path,
                "department": item.department,
                "table": item.table,
                "first_row": count,
                "rows": item.rows,
                "bytes": item.bytes,
                "original_bboxes": {
                    key: list(value) for key, value in sorted(item.bboxes.items())
                },
            })
            count += item.rows
        batches.append({
            "batch_id": batch_id,
            "output": destination,
            "table": table,
            "group_sha256": group_id,
            "structure_sha256": items[0].structure_sha256,
            "metadata_ignoring_bbox_sha256": items[0].metadata_sha256,
            "sources": len(items),
            "departments": [item.department for item in items],
            "rows": count,
            "source_bytes": sum(item.bytes for item in items),
            "bbox_union": bbox,
            "metadata_review": warnings,
        })

    for (table, group_id), sources in sorted(cohorts.items()):
        # Sort source paths independently of filesystem traversal order.
        pending: list[SourceInfo] = []
        pending_bytes = 0
        ordinal = 0
        for source in sorted(sources, key=lambda x: x.path):
            if pending and pending_bytes + source.bytes > target_bytes:
                flush(pending, table, group_id, ordinal)
                ordinal += 1
                pending = []
                pending_bytes = 0
            pending.append(source)
            pending_bytes += source.bytes
        if pending:
            flush(pending, table, group_id, ordinal)

    batch_count = len(batches)
    source_count = len(proposed_sources)
    file_reduction = source_count - batch_count
    proposed_rows = sum(x["rows"] for x in proposed_sources)
    untouched_rows = sum(x["rows"] for x in untouched)
    prior_alias_stats, pending_alias_rewrites = _audit_prior_aliases(
        root, proposed_sources,
    )
    summary = {
        **prior_alias_stats,
        "format": "imope-v2-physical-planning-v1",
        "mode": "read-only",
        "root": str(root),
        "target_mib": target_mib,
        "physical_files": len(candidates),
        "already_compacted_files": previous,
        "uncompacted_files": len(original),
        "uncompacted_bytes": sum(x.bytes for x in info),
        "uncompacted_rows": sum(x.rows for x in info),
        "table_names": len({x.table for x in info}),
        "compatibility_cohorts": len(cohorts),
        "candidate_source_files": source_count,
        "proposed_batches": batch_count,
        "estimated_file_reduction": file_reduction,
        "projected_physical_files": len(candidates) - file_reduction,
        "uncompacted_files_not_selected": len(untouched),
        "candidate_rows": proposed_rows,
        "candidate_bytes": sum(x["bytes"] for x in proposed_sources),
        "not_selected_rows": untouched_rows,
        "metadata_review_batches": sum(bool(x["metadata_review"]) for x in batches),
        "geo_batches": sum(bool(x["bbox_union"]) for x in batches),
        "warnings": [
            "Planner only; no compaction or publication implementation is enabled.",
            "Filenames, departmental manifests, previously published source "
            "indices and the 187 IMoPE aliases require explicit migration audit.",
            "Source-byte bin packing is an estimate, not a bound on output Parquet bytes.",
            "BBox unions are only proposals; geometry-level validation is required.",
        ],
    }
    if source_count + len(untouched) != len(original):
        raise AssertionError("IMoPE plan lost physical sources")
    if proposed_rows + untouched_rows != summary["uncompacted_rows"]:
        raise AssertionError("IMoPE plan changed row totals")
    if len({x["original"] for x in proposed_sources}) != len(proposed_sources):
        raise AssertionError("IMoPE plan repeats source paths")
    if len({b["output"] for b in batches}) != batch_count:
        raise AssertionError("IMoPE proposed output names collide")
    return {
        "summary": summary,
        "batches": batches,
        "sources": proposed_sources,
        "untouched": untouched,
        "pending_alias_rewrites": pending_alias_rewrites,
    }


def write_plan(plan: dict[str, Any], directory: Path, root: Path) -> None:
    """Create a new outside-corpus plan directory, never overwrite one."""
    root = root.expanduser().resolve()
    directory = directory.expanduser().resolve()
    if directory == root or directory.is_relative_to(root) or root.is_relative_to(directory):
        raise ValueError("Plan output must not overlap the original IMoPE root")
    if directory.exists() or directory.is_symlink():
        raise FileExistsError(f"Refusing to overwrite existing plan: {directory}")
    # Keep parent creation outside this function: avoids silent writes into
    # unexpected hierarchy and requires caller to choose an existing parent.
    if not directory.parent.is_dir() or directory.parent.is_symlink():
        raise ValueError(f"Plan parent missing or unsafe: {directory.parent}")
    directory.mkdir(mode=0o700)
    try:
        for key in ("summary", "batches", "untouched", "pending_alias_rewrites"):
            data = plan[key]
            (directory / f"{key}.json").write_text(
                json.dumps(data, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
        with (directory / "sources.tsv").open(
            "w", encoding="utf-8", newline="",
        ) as stream:
            fields = (
                "batch_id", "output", "original", "department",
                "table", "first_row", "rows", "bytes", "original_bboxes",
            )
            writer = csv.DictWriter(
                stream, fieldnames=fields,
                delimiter="\t", lineterminator="\n",
            )
            writer.writeheader()
            for source in plan["sources"]:
                writer.writerow({
                    **source,
                    "original_bboxes": _canonical_json(
                        source["original_bboxes"],
                    ).decode("utf-8"),
                })
    except BaseException:
        # Only cleanup the newly created plan directory, never any source.
        for name in ("summary.json", "batches.json", "untouched.json", "pending_alias_rewrites.json", "sources.tsv"):
            (directory / name).unlink(missing_ok=True)
        directory.rmdir()
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="IMoPE v2 read-only metadata-aware cross-department planner",
    )
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument(
        "--target-mib", type=int, default=DEFAULT_TARGET_MIB,
        help="target input size in MiB (default: 256)",
    )
    parser.add_argument(
        "--output-dir", type=Path,
        help="optional NEW plan directory outside the dataset; never overwrites",
    )
    args = parser.parse_args(argv)
    try:
        planned = build_plan(args.root, target_mib=args.target_mib)
        print(json.dumps(planned["summary"], ensure_ascii=False, indent=2))
        if args.output_dir is not None:
            write_plan(planned, args.output_dir, args.root)
            print(f"PLAN OUTPUT: {args.output_dir}")
        else:
            print("NO FILES WRITTEN: omit --output-dir for strictly read-only mode")
        return 0
    except (OSError, ValueError, RuntimeError, KeyError, TypeError) as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
