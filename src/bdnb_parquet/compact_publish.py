"""Publish a fully verified compacted BDNB dataset and prune redundant parts.

This is intentionally BDNB-specific. It builds an independent hardlinked
replacement of the complete dataset tree, updates per-table and top-level
manifests, checks it, then swaps directories. Existing originals are removed
only after the swap succeeds and the new tree is validated.
"""

from __future__ import annotations

import argparse
import errno
import csv
import json
import os
import re
import shutil
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from .compact_inventory import checked_tables_root
from .compact_run import (
    output_paths,
    read_selection,
    reject_overlapping_trees,
    verify,
)
from .convert import atomic_json, build_manifest

PART = re.compile(r"part-(\d{5,})\.parquet\Z")
MIB = 2**20


def link_file(source: Path, destination: Path) -> None:
    if source.is_symlink() or not source.is_file():
        raise ValueError(f"Unsafe input (not a regular file): {source}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    # Same-filesystem links avoid copying tens of GiB or risking partial copies.
    os.link(source, destination)


def copy_tree_hardlinks(source: Path, destination: Path) -> None:
    if source.is_symlink() or not source.is_dir():
        raise ValueError(f"Unsafe input directory: {source}")
    destination.mkdir(parents=True, exist_ok=True)
    for item in source.iterdir():
        target = destination / item.name
        if item.is_symlink():
            raise ValueError(f"Refusing source symlink: {item}")
        if item.is_dir():
            copy_tree_hardlinks(item, target)
        elif item.is_file():
            link_file(item, target)
        else:
            raise ValueError(f"Unexpected dataset filesystem entry: {item}")


def inspect_plan(
    plan_dir: Path, source_root: Path, dataset: Path, tables: Path
) -> tuple[dict[str, dict[str, str]], dict[str, Any]]:
    """Require the plan and physical original table parts to match exactly."""
    summary = json.loads((plan_dir / "summary.json").read_text())
    if Path(summary["source_root"]).resolve() != source_root:
        raise ValueError("The planner source_root does not match")
    if not dataset.is_dir() or not tables.is_dir():
        raise FileNotFoundError("BDNB dataset and tables directory are required")
    with (plan_dir / "files.tsv").open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream, delimiter="\t"))

    indexed: dict[str, dict[str, str]] = {}
    for row in rows:
        relative = row["path"]
        path = source_root / relative
        if not path.is_relative_to(dataset):
            continue
        if path.parent.parent != tables:
            # The original BDNB root also contains manifest.parquet, which
            # the planner inventories as an unrelated singleton and which
            # is regenerated during publication. Other dataset metadata
            # Parquet files must not be interpreted as table fragments.
            if tables in path.parents:
                raise ValueError(f"Unexpected deeper BDNB Parquet path: {relative}")
            continue
        if not PART.fullmatch(path.name):
            raise ValueError(f"Unexpected BDNB filename in plan: {relative}")
        if relative in indexed:
            raise ValueError(f"Duplicate BDNB path in plan: {relative}")
        indexed[relative] = row

    original_tables = {
        path.name: path for path in tables.iterdir() if path.is_dir()
    }
    if not original_tables or any(p.is_symlink() for p in tables.iterdir()):
        raise ValueError("Unexpected or missing BDNB table directories")
    physical: set[str] = set()
    input_parts = input_rows = input_bytes = 0
    for name, directory in sorted(original_tables.items()):
        marker = directory / "_SUCCESS.json"
        if not marker.is_file() or marker.is_symlink():
            raise ValueError(f"Missing original _SUCCESS.json: {directory}")
        success = json.loads(marker.read_text(encoding="utf-8"))
        parts = sorted(directory.glob("*.parquet"))
        numbers = []
        for part in parts:
            found = PART.fullmatch(part.name)
            if not found or part.is_symlink():
                raise ValueError(f"Unrecognized original Parquet part: {part}")
            numbers.append(int(found.group(1)))
            rel = part.relative_to(source_root).as_posix()
            if rel not in indexed:
                raise ValueError(f"Original file omitted by planner: {rel}")
            planned = indexed[rel]
            if int(planned["bytes"]) != part.stat().st_size:
                raise ValueError(f"Source size changed since plan: {part}")
            if planned["status"] == "skip-partial":
                raise ValueError(f"Refusing partial input: {part}")
            physical.add(rel)
            input_rows += int(planned["rows"])
            input_bytes += int(planned["bytes"])
        if numbers != list(range(len(numbers))):
            raise ValueError(f"Original part numbering is not contiguous: {name}")
        if (
            success.get("status") != "ok"
            or success.get("table") != name
            or success.get("parts") != len(parts)
            or int(success.get("rows", -1))
            != sum(int(indexed[p.relative_to(source_root).as_posix()]["rows"])
                   for p in parts)
        ):
            raise ValueError(f"Original _SUCCESS metadata mismatch: {name}")
        input_parts += len(parts)

    if physical != set(indexed):
        raise ValueError("BDNB plan contains files missing from original table tree")
    return indexed, {
        "tables": len(original_tables),
        "input_files": input_parts,
        "input_rows": input_rows,
        "input_bytes": input_bytes,
    }


def selections_for_dataset(
    plan_dir: Path, source_root: Path, indexed: dict[str, dict[str, str]]
) -> dict[str, Any]:
    ids = sorted({
        row["batch_id"] for row in indexed.values()
        if row["status"] == "review-required"
    })
    selections = {
        batch_id: read_selection(plan_dir, source_root, batch_id, None)
        for batch_id in ids
    }
    # Every candidate part must belong to one and only one complete group.
    expected = {
        path for path, row in indexed.items()
        if row["status"] == "review-required"
    }
    actual = [
        src.relative for sel in selections.values() for src in sel.sources
    ]
    if expected != set(actual) or len(actual) != len(expected):
        raise ValueError("Selected compaction batches do not cover the plan exactly")
    return selections


def stage_dataset(
    *,
    source_root: Path,
    plan_dir: Path,
    compact_root: Path,
    tables_relative: str,
    stage: Path,
    reverify: bool = True,
) -> dict[str, Any]:
    base = checked_tables_root(tables_relative)
    tables = source_root / base
    dataset = tables.parent
    if dataset.is_symlink() or tables.is_symlink():
        raise ValueError("Refusing BDNB symlink directory")

    indexed, input_totals = inspect_plan(
        plan_dir, source_root, dataset, tables
    )
    selections = selections_for_dataset(plan_dir, source_root, indexed)
    checked: dict[str, Path] = {}
    for number, (batch_id, selection) in enumerate(selections.items(), start=1):
        output, report = output_paths(compact_root, selection)
        if not output.is_file() or output.is_symlink() or not report.is_file():
            raise FileNotFoundError(f"Unverified or missing compaction: {batch_id}")
        saved = json.loads(report.read_text(encoding="utf-8"))
        if (
            saved.get("batch_id") != batch_id
            or saved.get("source_paths")
            != [src.relative for src in selection.sources]
            or saved.get("verified_rows") != sum(x.rows for x in selection.sources)
            or saved.get("source_bytes") != sum(x.bytes for x in selection.sources)
            or saved.get("output_bytes") != output.stat().st_size
            or saved.get("validation")
            != "full-ordered-Arrow-record-batch-equality"
        ):
            raise ValueError(f"Invalid compaction audit report: {batch_id}")
        if reverify:
            verified = verify(selection, output, batch_rows=16384, threads=4)
            if verified != saved["verified_rows"]:
                raise ValueError(f"Reverification failed: {batch_id}")
        checked[batch_id] = output
        print(
            f"CHECK [{number}/{len(selections)}] {batch_id}",
            file=sys.stderr, flush=True,
        )

    if stage.exists():
        if stage.is_symlink():
            raise ValueError(f"Unsafe pre-existing stage: {stage}")
        shutil.rmtree(stage)
    stage.mkdir()
    for child in dataset.iterdir():
        if child.name == "tables":
            continue
        if child.name in {"manifest.json", "manifest.parquet"}:
            continue
        target = stage / child.name
        if child.is_dir():
            copy_tree_hardlinks(child, target)
        else:
            link_file(child, target)

    stage_tables = stage / "tables"
    stage_tables.mkdir()
    used_batches: set[str] = set()
    final_files = final_bytes = final_rows = 0
    stamp = datetime.now(timezone.utc).isoformat()
    for table_dir in sorted(tables.iterdir()):
        name = table_dir.name
        target_dir = stage_tables / name
        target_dir.mkdir()
        parts = sorted(table_dir.glob("part-*.parquet"))
        for other in table_dir.iterdir():
            if other.name == "_SUCCESS.json" or PART.fullmatch(other.name):
                continue
            if other.suffix == ".parquet":
                raise ValueError(f"Unexpected Parquet source: {other}")
            if other.is_dir():
                copy_tree_hardlinks(other, target_dir / other.name)
            else:
                link_file(other, target_dir / other.name)
        seen: set[str] = set()
        new_sources: list[Path] = []
        for original in parts:
            relative = original.relative_to(source_root).as_posix()
            row = indexed[relative]
            batch_id = row["batch_id"]
            if row["status"] == "review-required":
                if batch_id not in seen:
                    new_sources.append(checked[batch_id])
                    seen.add(batch_id)
                    used_batches.add(batch_id)
            else:
                new_sources.append(original)

        original_success = json.loads(
            (table_dir / "_SUCCESS.json").read_text(encoding="utf-8")
        )
        new_bytes = new_rows = 0
        for index, current in enumerate(new_sources):
            metadata = pq.read_metadata(current)
            new_rows += metadata.num_rows
            new_bytes += current.stat().st_size
            link_file(current, target_dir / f"part-{index:05d}.parquet")
        if new_rows != int(original_success["rows"]):
            raise ValueError(f"Row count changed while staging table: {name}")
        updated = dict(original_success)
        updated.update({
            "parts": len(new_sources),
            "parquet_bytes": new_bytes,
            "batch_rows": None,
            "file_rows": None,
            "compaction": {
                "completed_at": stamp,
                "source_parts": len(parts),
                "source_rows": new_rows,
                "verified_compaction_batches": sorted(seen),
                "preserved_original_parts": len(new_sources) - len(seen),
                "row_verification": "full-ordered-Arrow-record-batch-equality",
            },
        })
        atomic_json(target_dir / "_SUCCESS.json", updated)
        schema_file = stage / "schemas" / f"{name}.json"
        # The converter mirrored _SUCCESS.json under schemas/. Rebuild it.
        if schema_file.exists():
            schema_file.unlink()
        atomic_json(schema_file, updated)
        final_files += len(new_sources)
        final_bytes += new_bytes
        final_rows += new_rows

    if used_batches != set(selections):
        raise ValueError("Some compaction outputs were not materialized")
    if final_rows != input_totals["input_rows"]:
        raise ValueError("Dataset row count mismatch")
    manifest = build_manifest(stage)
    if len(manifest) != input_totals["tables"]:
        raise ValueError("Manifest table count mismatch")
    meta_path = stage / "dataset.json"
    if meta_path.exists():
        obj = json.loads(meta_path.read_text(encoding="utf-8"))
        obj["compaction"] = {
            "completed_at": stamp,
            "verified_batches": len(selections),
            "original_parts": input_totals["input_files"],
            "new_parts": final_files,
            "original_rows": final_rows,
            "source_bytes": input_totals["input_bytes"],
            "compacted_bytes": final_bytes,
        }
        meta_path.unlink()  # do not rewrite the original hardlinked inode
        atomic_json(meta_path, obj)

    result = {
        "tables": input_totals["tables"],
        "verified_batches": len(selections),
        "source_files": input_totals["input_files"],
        "output_files": final_files,
        "redundant_source_files": input_totals["input_files"] - final_files,
        "verified_rows": final_rows,
        "source_bytes": input_totals["input_bytes"],
        "output_bytes": final_bytes,
        "stage": str(stage),
    }
    atomic_json(stage / "_COMPACTION.json", result)
    return result


def validate_published_dataset(dataset: Path, report: dict[str, Any]) -> None:
    """Check every final footer and updated metadata before pruning backup."""
    tables = dataset / "tables"
    count = rows = files = 0
    for directory in sorted(tables.iterdir()):
        if not directory.is_dir() or directory.is_symlink():
            raise ValueError(f"Unsafe published table: {directory}")
        marker = json.loads((directory / "_SUCCESS.json").read_text())
        parts = sorted(directory.glob("part-*.parquet"))
        if marker.get("parts") != len(parts):
            raise ValueError(f"Published part count mismatch: {directory}")
        part_rows = sum(pq.read_metadata(p).num_rows for p in parts)
        if part_rows != marker.get("rows"):
            raise ValueError(f"Published row count mismatch: {directory}")
        count += 1
        files += len(parts)
        rows += part_rows
    if (count, files, rows) != (
        report["tables"], report["output_files"], report["verified_rows"]
    ):
        raise ValueError("Published dataset summary mismatch")
    manifest = json.loads((dataset / "manifest.json").read_text())
    if len(manifest) != count:
        raise ValueError("Published manifest count mismatch")


def require_writable_publication_parent(dataset: Path) -> None:
    """Fail fast if the source dataset or its parent is on a read-only mount.

    The replacement staging directory and backup must be created alongside
    the original BDNB directory; a read-only mount cannot be pruned.
    """
    for path in (dataset.parent, dataset):
        flags = os.statvfs(path).f_flag
        if flags & os.ST_RDONLY:
            raise OSError(
                errno.EROFS,
                "BDNB publication requires a writable mount; "
                "inspect findmnt -T for this path before retrying",
                str(path),
            )


def publish(
    source_root: Path, plan_dir: Path, compact_root: Path,
    tables_relative: str, *, prune_backup: bool,
) -> dict[str, Any]:
    root = source_root.resolve()
    plan_dir = plan_dir.resolve()
    compact_root = compact_root.resolve()
    reject_overlapping_trees(root, compact_root)
    reject_overlapping_trees(root, plan_dir)
    base = checked_tables_root(tables_relative)
    dataset = root / base.parent
    stage = dataset.with_name(dataset.name + ".compaction-stage")
    backup = dataset.with_name(dataset.name + ".precompact-backup")
    if backup.exists() or backup.is_symlink():
        raise RuntimeError(
            f"Existing backup {backup}: publication previously interrupted; "
            "do not delete it or retry until inspected"
        )
    if stage.is_symlink():
        raise ValueError("Staging path may not be a symbolic link")
    require_writable_publication_parent(dataset)
    started = time.perf_counter()
    report = stage_dataset(
        source_root=root, plan_dir=plan_dir,
        compact_root=compact_root, tables_relative=tables_relative,
        stage=stage,
    )
    validate_published_dataset(stage, report)

    # Both renames occur on the source filesystem. If the second fails,
    # restore the original dataset; after unexpected termination the backup
    # remains intact and must be inspected before any further operation.
    os.replace(dataset, backup)
    try:
        os.replace(stage, dataset)
    except BaseException:
        os.replace(backup, dataset)
        raise
    try:
        validate_published_dataset(dataset, report)
    except BaseException:
        raise RuntimeError(
            f"Published validation failed: original preserved at {backup}. "
            "Restore it before attempting another run"
        )

    if prune_backup:
        shutil.rmtree(backup)
    report.update({
        "published_dataset": str(dataset),
        "backup": str(backup) if not prune_backup else None,
        "pruned": prune_backup,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
    })
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Publish fully verified BDNB compaction, replacing obsolete parts."
    )
    parser.add_argument("--plan-dir", required=True, type=Path)
    parser.add_argument("--source-root", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--tables-root", required=True)
    parser.add_argument(
        "--execute", action="store_true",
        help="explicitly opt into replacing BDNB dataset in the original corpus",
    )
    parser.add_argument(
        "--prune-backup", action="store_true",
        help="delete obsolete original BDNB parts after published validation",
    )
    args = parser.parse_args(argv)
    if not args.execute:
        parser.error("Publication requires explicit --execute")
    if args.prune_backup and not args.execute:
        parser.error("--prune-backup requires --execute")
    try:
        publish(
            args.source_root.expanduser(),
            args.plan_dir.expanduser(),
            args.output_root.expanduser(),
            args.tables_root,
            prune_backup=args.prune_backup,
        )
        return 0
    except (OSError, RuntimeError, ValueError, KeyError, TypeError) as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
