"""Fail-closed, non-publishing 2 GiB IMoPE v2 GeoParquet compaction pilot.

Never swaps source roots or touches the existing IMoPE publication. Requires an
external plan from imope_v2_plan, a new external output root, and --execute.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .compact_run import (
    arrow_cpu_threads, atomic_report, compare_rows, relative_path,
    reject_overlapping_trees,
)
from .imope_v2_plan import (
    _canonical_json, _geo_profile, _inspect, _union_bboxes,
)

MIB = 2**20
PILOT_TARGET_MIB = 2048
BATCH_PATTERN = re.compile(r"[a-f0-9]{16}-[0-9]{4}\Z")


def read_pilot(
    plan_dir: Path, source_root: Path, *,
    batch_id: str | None = None, pick: str = "smallest-geo",
) -> tuple[dict[str, Any], list[dict[str, Any]], list[Path]]:
    """Load and revalidate a single group from an immutable external plan."""
    plan_dir = plan_dir.expanduser().resolve()
    source_root = source_root.expanduser().resolve()
    reject_overlapping_trees(source_root, plan_dir)
    summary = json.loads((plan_dir / "summary.json").read_text(encoding="utf-8"))
    if (summary.get("format") != "imope-v2-physical-planning-v1"
        or summary.get("mode") != "read-only"
        or summary.get("target_mib") != PILOT_TARGET_MIB
        or Path(summary["root"]).resolve() != source_root):
        raise ValueError("Requires an IMoPE v2 plan for this root at exactly 2048 MiB")

    groups = json.loads((plan_dir / "batches.json").read_text(encoding="utf-8"))
    if not isinstance(groups, list):
        raise ValueError("Invalid batches.json")
    known_ids = [batch["batch_id"] for batch in groups]
    if len(known_ids) != len(set(known_ids)):
        raise ValueError("Duplicated batch IDs in plan")
    if batch_id is not None:
        match = [batch for batch in groups if batch["batch_id"] == batch_id]
    else:
        if pick not in {"smallest-geo", "largest-geo"}:
            raise ValueError("Invalid pilot selection")
        geo = [
            batch for batch in groups
            if batch.get("bbox_union") and not batch.get("metadata_review")
        ]
        if not geo:
            raise ValueError("No eligible bbox-complete GeoParquet pilot batch")
        reverse = pick == "largest-geo"
        ordered = sorted(
            geo, key=lambda x: (int(x["source_bytes"]), x["batch_id"]),
            reverse=reverse,
        )
        match = ordered[:1]
    if len(match) != 1:
        raise ValueError("Requested pilot batch not uniquely present")
    selected = match[0]
    if not BATCH_PATTERN.fullmatch(selected["batch_id"]):
        raise ValueError("Invalid IMoPE v2 pilot batch ID")
    if selected.get("metadata_review"):
        raise ValueError("Pilot refused: unresolved metadata review")
    output = relative_path(selected["output"])
    if (len(output.parts) != 3 or output.parts[0] != "tables"
        or output.parts[1] != Path(selected["table"]).stem
        or output.parts[2] != f"compact-v2-{selected['batch_id']}.parquet"):
        raise ValueError("Unsafe or unexpected planned output")

    # The planner's original ordered sources have continuous row ranges.
    import csv
    with (plan_dir / "sources.tsv").open(encoding="utf-8", newline="") as handle:
        rows = [
            row for row in csv.DictReader(handle, delimiter="\t")
            if row["batch_id"] == selected["batch_id"]
        ]
    if len(rows) != selected["sources"] or len(rows) < 2:
        raise ValueError("Pilot batch source count differs from plan")
    if sum(int(row["bytes"]) for row in rows) != selected["source_bytes"]:
        raise ValueError("Pilot batch byte count differs from plan")
    if sum(int(row["rows"]) for row in rows) != selected["rows"]:
        raise ValueError("Pilot batch row count differs from plan")

    paths: list[Path] = []
    position = 0
    reconstructed = []
    for row in rows:
        relative = relative_path(row["original"])
        if (len(relative.parts) < 2
            or relative.parts[0] != row["department"]
            or relative.name != row["table"]
            or relative.name != selected["table"]
            or row["output"] != selected["output"]
            or int(row["first_row"]) != position):
            raise ValueError("Pilot source name or row-range provenance differs")
        path = source_root / relative
        if path.is_symlink() or not path.resolve().is_relative_to(source_root):
            raise ValueError(f"Unsafe IMoPE pilot source: {path}")
        info = _inspect(path, source_root)
        if (info.path != row["original"]
            or info.bytes != int(row["bytes"])
            or info.rows != int(row["rows"])
            or info.group_sha256 != selected["group_sha256"]
            or info.structure_sha256 != selected["structure_sha256"]
            or info.metadata_sha256
            != selected["metadata_ignoring_bbox_sha256"]):
            raise ValueError(f"IMoPE source changed since planning: {path}")
        position += info.rows
        paths.append(path)
        reconstructed.append(info)
    if len(set(paths)) != len(paths):
        raise ValueError("IMoPE pilot contains duplicate source files")
    bbox, warnings = _union_bboxes(reconstructed)
    if warnings or bbox != selected["bbox_union"]:
        raise ValueError("Planned GeoParquet bbox union is stale or incomplete")
    return selected, rows, paths


def merged_schema(paths: list[Path], union: dict[str, list[float]]) -> pa.Schema:
    """Preserve all non-bbox metadata and encode the approved bbox union."""
    ref = pq.read_schema(paths[0])
    metadata = dict(ref.metadata or {})
    raw = metadata.get(b"geo")
    if raw is None:
        if union:
            raise ValueError("GeoParquet bbox proposed without GeoParquet metadata")
        return ref
    stripped, _ = _geo_profile(raw, paths[0])
    geo = json.loads(stripped)
    for key, bbox in union.items():
        if key == "root":
            geo["bbox"] = bbox
        elif key.startswith("columns/"):
            column = key.removeprefix("columns/")
            if column not in geo["columns"]:
                raise ValueError(f"Unrecognized GeoParquet column: {column}")
            geo["columns"][column]["bbox"] = bbox
        else:
            raise ValueError(f"Unrecognized GeoParquet bbox key: {key}")
    metadata[b"geo"] = _canonical_json(geo)
    return ref.with_metadata(metadata)


def write_stream(
    paths: list[Path], stage: Path, schema: pa.Schema, *,
    batch_rows: int, row_group_rows: int, max_buffer_mib: int,
    zstd_level: int, threads: int,
) -> int:
    """Bounded-memory Arrow batches; no geometry or field values are coerced."""
    if min(batch_rows, row_group_rows, max_buffer_mib, threads) < 1:
        raise ValueError("Streaming resource limits must be positive")
    if not 1 <= zstd_level <= 19:
        raise ValueError("Zstandard compression level must be in 1..19")
    max_bytes = max_buffer_mib * MIB
    total = 0
    pending: list[pa.RecordBatch] = []
    held_bytes = held_rows = 0

    with pq.ParquetWriter(
        stage, schema=schema, compression="zstd",
        compression_level=zstd_level,
    ) as writer:
        def flush() -> None:
            nonlocal pending, held_bytes, held_rows
            if not pending:
                return
            table = pa.Table.from_batches(pending, schema=schema)
            writer.write_table(table, row_group_size=table.num_rows)
            pending = []
            held_bytes = held_rows = 0

        for idx, path in enumerate(paths, 1):
            parquet = pq.ParquetFile(path)
            try:
                if not parquet.schema_arrow.equals(schema, check_metadata=False):
                    raise ValueError(f"Source Arrow fields differ: {path}")
                for batch in parquet.iter_batches(
                    batch_size=batch_rows, use_threads=threads > 1,
                ):
                    # The only metadata change is the audited schema bbox.
                    # Rebind the original buffers to that schema; no value cast.
                    offset = 0
                    while offset < batch.num_rows:
                        free = row_group_rows - held_rows
                        count = min(free, batch.num_rows - offset)
                        fragment = batch.slice(offset, count)
                        if pending and held_bytes + fragment.nbytes > max_bytes:
                            flush()
                            continue
                        while fragment.nbytes > max_bytes and count > 1:
                            count = max(1, count // 2)
                            fragment = batch.slice(offset, count)
                        if fragment.nbytes > max_bytes:
                            raise ValueError("A single Arrow row exceeds buffer budget")
                        normalized = pa.RecordBatch.from_arrays(
                            fragment.columns, schema=schema,
                        )
                        pending.append(normalized)
                        held_bytes += normalized.nbytes
                        held_rows += normalized.num_rows
                        total += normalized.num_rows
                        offset += count
                        if held_rows >= row_group_rows:
                            flush()
            finally:
                parquet.close()
            print(
                f"WRITE [{idx}/{len(paths)}] {path.name}: {total} rows",
                file=sys.stderr, flush=True,
            )
        flush()
    return total


def pilot(
    plan_dir: Path, source_root: Path, *, output_root: Path | None,
    batch_id: str | None = None, pick: str = "smallest-geo",
    execute: bool = False, threads: int = 2, batch_rows: int = 16384,
    row_group_rows: int = 100000, max_buffer_mib: int = 128,
    zstd_level: int = 3,
) -> dict[str, Any]:
    """Stage one independent pilot, compare all Arrow values, never publish."""
    selected, sources, paths = read_pilot(
        plan_dir, source_root, batch_id=batch_id, pick=pick,
    )
    preview = {
        "mode": "execute-pilot" if execute else "preview",
        "target_mib": PILOT_TARGET_MIB,
        "batch_id": selected["batch_id"],
        "source_files": len(paths),
        "source_bytes": selected["source_bytes"],
        "rows": selected["rows"],
        "bbox_union": selected["bbox_union"],
        "metadata_review": selected["metadata_review"],
        "first_source": sources[0]["original"],
        "last_source": sources[-1]["original"],
        "originals_untouched": True,
    }
    print(json.dumps(preview, ensure_ascii=False, indent=2), flush=True)
    if not execute:
        return preview
    if output_root is None:
        raise ValueError("--execute requires --output-root")
    out = output_root.expanduser().resolve()
    root = source_root.expanduser().resolve()
    plan = plan_dir.expanduser().resolve()
    reject_overlapping_trees(root, out)
    reject_overlapping_trees(plan, out)
    if out.exists() or out.is_symlink():
        raise FileExistsError(f"Pilot output must be a NEW directory: {out}")
    if not out.parent.is_dir() or out.parent.is_symlink():
        raise ValueError(f"Unsafe or missing pilot output parent: {out.parent}")
    schema = merged_schema(paths, selected["bbox_union"])
    out.mkdir(mode=0o700)
    destination = out / relative_path(selected["output"])
    destination.parent.mkdir(parents=True)
    stage = destination.with_suffix(".partial")
    started = time.perf_counter()
    try:
        with arrow_cpu_threads(threads):
            write_start = time.perf_counter()
            count = write_stream(
                paths, stage, schema,
                batch_rows=batch_rows, row_group_rows=row_group_rows,
                max_buffer_mib=max_buffer_mib, zstd_level=zstd_level,
                threads=threads,
            )
            write_seconds = time.perf_counter() - write_start
            if count != selected["rows"]:
                raise ValueError("Output row count does not match planned sources")
            footer = pq.ParquetFile(stage)
            try:
                if not footer.schema_arrow.equals(schema, check_metadata=True):
                    raise ValueError("Output Arrow schema or GeoParquet bbox changed")
                if footer.metadata.num_rows != selected["rows"]:
                    raise ValueError("Output footer row count changed")
                row_groups = footer.metadata.num_row_groups
            finally:
                footer.close()
            verification_started = time.perf_counter()
            verified = compare_rows(tuple(paths), stage, batch_rows, threads)
            verification_seconds = time.perf_counter() - verification_started
            if verified != selected["rows"]:
                raise ValueError("Full ordered Arrow-value verification failed")
            # Re-inspect all source footers, sizes and normalized schemas before
            # materializing the final pilot. Source files are never mutated.
            read_pilot(
                plan_dir, source_root,
                batch_id=selected["batch_id"], pick=pick,
            )
            os.replace(stage, destination)
        report = {
            **preview,
            "mode": "verified-pilot-only",
            "output": str(destination),
            "output_bytes": destination.stat().st_size,
            "output_row_groups": row_groups,
            "verified_rows": verified,
            "write_seconds": round(write_seconds, 3),
            "verify_seconds": round(verification_seconds, 3),
            "elapsed_seconds": round(time.perf_counter() - started, 3),
            "validation": "full-ordered-Arrow-value-equality-and-metadata-compatibility",
            "geo_bbox_status": (
                "source-metadata-union-only; geometry-level audit pending"
            ),
            "source_provenance": [
                {
                    "original": item["original"],
                    "department": item["department"],
                    "first_row": int(item["first_row"]),
                    "rows": int(item["rows"]),
                } for item in sources
            ],
            "publication_performed": False,
            "source_backup_deleted": False,
        }
        atomic_report(destination.with_suffix(".json"), report)
        print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
        return report
    finally:
        # Never remove any original or backup. Only own incomplete pilot.
        stage.unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Explicit read/write opt-in pilot for IMoPE v2 2048 MiB plan",
    )
    parser.add_argument("--plan-dir", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path)
    selector = parser.add_mutually_exclusive_group()
    selector.add_argument("--batch-id")
    selector.add_argument(
        "--pick", choices=("smallest-geo", "largest-geo"),
        default="smallest-geo",
    )
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--batch-rows", type=int, default=16384)
    parser.add_argument("--row-group-rows", type=int, default=100000)
    parser.add_argument("--max-buffer-mib", type=int, default=128)
    parser.add_argument("--zstd-level", type=int, default=3)
    args = parser.parse_args(argv)
    if (min(
        args.threads, args.batch_rows, args.row_group_rows, args.max_buffer_mib,
    ) < 1 or not 1 <= args.zstd_level <= 19):
        parser.error("Invalid Arrow streaming resource limit")
    if args.execute and args.output_root is None:
        parser.error("--execute requires --output-root")
    try:
        pilot(
            args.plan_dir, args.source_root,
            output_root=args.output_root, batch_id=args.batch_id,
            pick=args.pick, execute=args.execute, threads=args.threads,
            batch_rows=args.batch_rows,
            row_group_rows=args.row_group_rows,
            max_buffer_mib=args.max_buffer_mib,
            zstd_level=args.zstd_level,
        )
        return 0
    except (OSError, RuntimeError, ValueError, KeyError, TypeError) as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
