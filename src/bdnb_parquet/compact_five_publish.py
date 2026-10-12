"""Transactional publication of five explicitly approved, verified datasets.

Stages replacements, verifies exact Arrow rows, rebuilds source-specific
manifests and per-source row-range provenance, then swaps all five roots.
Rollback is attempted before backup pruning if any swap/validation fails.
No other corpus datasets are touched.
"""

from __future__ import annotations

import argparse
import errno
import json
import os
import re
import shutil
import stat
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from .compact_five import TARGETS, provenance_manifest, read_plan
from .compact_publish import (
    link_file, copy_tree_hardlinks, require_writable_publication_parent,
    prune_validated_backup,
)
from .compact_run import output_paths, reject_overlapping_trees, verify
from .convert import atomic_json
from .datatourisme_convert import nt_snapshot_name
from .imope_promote import validate_parquet
from .normalize_vigieau import YEAR_DIR_RE

PART = re.compile(r"part-(\d{5,})\.parquet\Z")
GIB = 2**30


def replace_json(path: Path, obj: Any) -> None:
    # Stage metadata may initially be a hard link to the original. Never
    # modify the original inode in-place.
    if path.exists():
        if path.is_symlink():
            raise ValueError(f"Unsafe manifest symlink: {path}")
        path.unlink()
    atomic_json(path, obj)


def metadata_paths(root: Path) -> list[Path]:
    return sorted(root.rglob("*.json"))


def _json_strings(value: Any):
    if isinstance(value, str):
        yield value
    elif isinstance(value, list):
        for element in value:
            yield from _json_strings(element)
    elif isinstance(value, dict):
        for element in value.values():
            yield from _json_strings(element)


def require_known_metadata(
    dataset_name: str, original: Path,
    selected: list[tuple[str, Any, str]],
) -> None:
    """Reject unhandled JSON references to soon-to-be-removed file paths.

    Per-source metadata can carry links to unique original filenames.
    We rebuild the known converter manifest formats, but fail closed on
    unknown JSON references instead of silently leaving stale metadata.
    """
    deleted_names = {Path(src.relative).name
                     for _, selection, _ in selected
                     for src in selection.sources}
    deleted_paths = {src.relative
                     for _, selection, _ in selected
                     for src in selection.sources}
    exempt: set[Path] = set()
    if dataset_name in {"imope", "vigieau", "datatourisme"}:
        exempt.update({original / "manifest.json", original / "dataset.json"})
    if dataset_name == "imope":
        exempt.update(original.glob("*/_SUCCESS.json"))
    if dataset_name == "datatourisme":
        exempt.update(original.glob("ntriples/*/_SUCCESS.json"))

    for json_file in metadata_paths(original):
        if json_file in exempt:
            continue
        if json_file.is_symlink():
            raise ValueError(f"Unsafe JSON symlink: {json_file}")
        value = json.loads(json_file.read_text(encoding="utf-8"))
        for entry in _json_strings(value):
            normalized = entry.replace("\\", "/")
            if (normalized in deleted_paths
                or normalized in deleted_names
                or Path(normalized).name in deleted_names):
                raise ValueError(
                    f"Unhandled original filename reference: {json_file}: "
                    f"{entry[:140]}"
                )
    # A physical Parquet manifest would need rewriting, not copying.
    if (original / "manifest.parquet").exists():
        raise ValueError(
            f"Unexpected manifest.parquet needs a dataset-specific rebuild: {original}"
        )


def validate_source_matches_plan(
    original: Path, source_root: Path, inventory: dict[str, int],
    selected: list[tuple[str, Any, str]],
) -> None:
    """Refuse extra/missing Parquet inputs and altered planned candidates."""
    parquet_files = {p.relative_to(source_root).as_posix(): p
                     for p in original.rglob("*.parquet")}
    if len(parquet_files) != inventory["source_files"]:
        raise ValueError(
            f"Source file count changed: {original}: "
            f"planned {inventory['source_files']}, now {len(parquet_files)}"
        )
    for _, selection, _ in selected:
        for source in selection.sources:
            present = parquet_files.get(source.relative)
            if (present is None or present.is_symlink()
                or present.stat().st_size != source.bytes):
                raise ValueError(f"Source missing or size changed: {source.relative}")


def check_output_audit(selection: Any, output: Path, audit: Path) -> None:
    if output.is_symlink() or audit.is_symlink() or not output.is_file():
        raise ValueError(f"Missing or unsafe output: {output}")
    if not audit.is_file():
        raise FileNotFoundError(f"Missing compaction audit: {audit}")
    report = json.loads(audit.read_text(encoding="utf-8"))
    expected = {
        "batch_id": selection.batch_id,
        "pilot": False,
        "source_files": len(selection.sources),
        "source_paths": [x.relative for x in selection.sources],
        "verified_rows": sum(x.rows for x in selection.sources),
        "source_bytes": sum(x.bytes for x in selection.sources),
        "output_bytes": output.stat().st_size,
        "validation": "full-ordered-Arrow-record-batch-equality",
    }
    for k, v in expected.items():
        if report.get(k) != v:
            raise ValueError(f"Invalid output report {k}: {output}")


def verify_outputs(
    selected: list[tuple[str, Any, str]], output_root: Path,
) -> None:
    """Repeat full ordered logical Arrow comparison before destructive swap."""
    for idx, (_key, selection, _category) in enumerate(selected, start=1):
        output, audit = output_paths(output_root, selection)
        check_output_audit(selection, output, audit)
        verified = verify(selection, output, batch_rows=16384, threads=4)
        if verified != sum(s.rows for s in selection.sources):
            raise ValueError(f"Row verification failed: {selection.batch_id}")
        print(f"CHECK [{idx}/{len(selected)}] {selection.batch_id}",
              file=sys.stderr, flush=True)


def _stage_nt_parts(
    stage: Path,
    original: Path,
    old_to_stage: dict[str, tuple[str, int, int]],
) -> None:
    """Keep DataTourisme's numbered NT fragment contract and _SUCCESS."""
    nt_root = original / "ntriples"
    if not nt_root.is_dir():
        raise ValueError("DataTourisme missing ntriples directory")
    for original_snap in sorted(p for p in nt_root.iterdir() if p.is_dir()):
        rel_dir = original_snap.relative_to(original)
        stage_dir = stage / rel_dir
        source_paths = sorted(
            original_snap.glob("part-*.parquet"),
            key=lambda p: int(PART.fullmatch(p.name).group(1))
        )
        if len(source_paths) != len(list(original_snap.glob("*.parquet"))):
            raise ValueError(f"Unexpected NT Parquet filename: {original_snap}")
        source_numbers = [int(PART.fullmatch(p.name).group(1))
                          for p in source_paths]
        if source_numbers != list(range(len(source_numbers))):
            raise ValueError(f"Non-contiguous NT snapshot: {original_snap}")
        physical: list[str] = []
        seen: set[str] = set()
        for src in source_paths:
            rel = src.relative_to(original).as_posix()
            current = old_to_stage.get(rel, (rel, 0, 0))[0]
            if current not in seen:
                seen.add(current)
                physical.append(current)
        # Move everything into non-Parquet temporary names first, then
        # re-number atomically within the staging directory.
        for index, current in enumerate(physical):
            from_path = stage / current
            if not from_path.is_file():
                raise ValueError(f"Missing staged NT fragment: {from_path}")
            os.replace(from_path, stage_dir / f".renumber-{index:05d}")
        mapping = {}
        for index, current in enumerate(physical):
            name = (rel_dir / f"part-{index:05d}.parquet").as_posix()
            os.replace(stage_dir / f".renumber-{index:05d}", stage / name)
            mapping[current] = name
        for rel in list(old_to_stage):
            if rel.startswith(rel_dir.as_posix() + "/"):
                filename, offset, rows = old_to_stage[rel]
                old_to_stage[rel] = (mapping[filename], offset, rows)
        marker = original_snap / "_SUCCESS.json"
        if not marker.is_file():
            raise ValueError(f"Missing original NT _SUCCESS: {marker}")
        info = json.loads(marker.read_text(encoding="utf-8"))
        parts = sorted(stage_dir.glob("part-*.parquet"))
        footer_rows = sum(pq.read_metadata(p).num_rows for p in parts)
        footer_bytes = sum(p.stat().st_size for p in parts)
        if info["rows"] != footer_rows:
            raise ValueError(f"NT rows changed: {original_snap}")
        info.update({"parts": len(parts), "bytes": footer_bytes})
        replace_json(stage_dir / "_SUCCESS.json", info)


def _manifest_datatourisme(
    stage: Path, original: Path,
    old_to_stage: dict[str, tuple[str, int, int]],
) -> None:
    path = original / "manifest.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or not isinstance(manifest.get("ntriples"), list):
        raise ValueError("Unexpected DataTourisme manifest layout")
    ntriples_rows = ntriples_bytes = 0
    for entry in manifest["ntriples"]:
        nt_name = nt_snapshot_name(Path(entry["source"]))
        marker = stage / "ntriples" / nt_name / "_SUCCESS.json"
        info = json.loads(marker.read_text(encoding="utf-8"))
        if int(entry["rows"]) != int(info["rows"]):
            raise ValueError(f"DataTourisme archive rows changed: {nt_name}")
        entry["parts"] = info["parts"]
        entry["bytes"] = info["bytes"]
        ntriples_rows += int(info["rows"])
        ntriples_bytes += int(info["bytes"])
    csv_rows = 0
    unique_csv: set[str] = set()
    csv_bytes = 0
    for entry in manifest["csv"]:
        previous = entry["output"]
        destination, offset, length = old_to_stage.get(
            previous, (previous, 0, int(entry["rows"]))
        )
        parquet = stage / destination
        if (not parquet.is_file() or
            offset + length > pq.read_metadata(parquet).num_rows or
            length != int(entry["rows"])):
            raise ValueError("DataTourisme CSV output changed")
        entry["original_output"] = previous
        entry["output"] = destination
        entry["row_offset"] = offset
        entry["row_count"] = length
        entry["original_bytes"] = entry["bytes"]
        entry["bytes"] = parquet.stat().st_size
        csv_rows += length
        if destination not in unique_csv:
            unique_csv.add(destination)
            csv_bytes += parquet.stat().st_size
    if int(manifest["rows"]) != ntriples_rows + csv_rows:
        raise ValueError("DataTourisme total rows changed")
    manifest["bytes"] = ntriples_bytes + csv_bytes
    manifest["compaction_source_ranges"] = "_COMPACTION_SOURCES.json"
    replace_json(stage / "manifest.json", manifest)
    dataset = json.loads((original / "dataset.json").read_text(encoding="utf-8"))
    dataset.update({"bytes": manifest["bytes"], "rows": manifest["rows"]})
    replace_json(stage / "dataset.json", dataset)


def _manifest_imope(stage: Path, original: Path) -> None:
    listing = json.loads((original / "manifest.json").read_text(encoding="utf-8"))
    if not isinstance(listing, list):
        raise ValueError("Unexpected IMoPE manifest layout")
    updated = []
    for dept in listing:
        name = dept["department"]
        directory = stage / name
        if not directory.is_dir():
            raise ValueError(f"IMoPE department missing: {name}")
        parts = sorted(directory.glob("*.parquet"))
        descriptions = [validate_parquet(p) for p in parts]
        if sum(int(x["rows"]) for x in descriptions) != dept["rows"]:
            raise ValueError(f"IMoPE department rows changed: {name}")
        updated.append({
            **dept, "files": len(parts),
            "bytes": sum(x["bytes"] for x in descriptions),
            "geo_files": sum(x["geo"] for x in descriptions),
            "parquet": descriptions,
            "compacted": True,
        })
        replace_json(directory / "_SUCCESS.json", updated[-1])
    dataset = json.loads((original / "dataset.json").read_text(encoding="utf-8"))
    if int(dataset["rows"]) != sum(x["rows"] for x in updated):
        raise ValueError("IMoPE total rows changed")
    dataset.update({
        "files": sum(x["files"] for x in updated),
        "bytes": sum(x["bytes"] for x in updated),
        "geo_files": sum(x["geo_files"] for x in updated),
        "storage": "verified compacted Parquet with per-source row range index",
    })
    replace_json(stage / "manifest.json", updated)
    replace_json(stage / "dataset.json", dataset)


def _vigieau_current_output(dataset_root: Path, stored: str) -> str:
    """Resolve only the known, deterministic Vigieau year-normalization move.

    normalize_vigieau.py relocated archive-named directories to geojson/YYYY
    without rewriting manifest.json. Never fall back to fuzzy filenames.
    """
    rel = Path(stored)
    if rel.is_absolute() or ".." in rel.parts or not rel.parts:
        raise ValueError(f"Unsafe Vigieau path in manifest: {stored}")
    if (dataset_root / rel).is_file():
        return rel.as_posix()
    if len(rel.parts) >= 3 and rel.parts[0] == "geojson":
        match = YEAR_DIR_RE.fullmatch(rel.parts[1])
        if match:
            normalized = Path("geojson") / match.group(1) / Path(*rel.parts[2:])
            if (dataset_root / normalized).is_file():
                return normalized.as_posix()
    raise ValueError(f"Unresolved Vigieau manifest path: {stored}")


def _manifest_vigieau(
    stage: Path, original: Path,
    old_to_stage: dict[str, tuple[str, int, int]],
) -> None:
    listing = json.loads((original / "manifest.json").read_text(encoding="utf-8"))
    if not isinstance(listing, list):
        raise ValueError("Unexpected Vigieau manifest layout")
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for entry in listing:
        legacy = entry["output"]
        old = _vigieau_current_output(original, legacy)
        rel, offset, num_rows = old_to_stage.get(
            old, (old, 0, int(entry["rows"])),
        )
        if num_rows != int(entry["rows"]):
            raise ValueError(f"Vigieau source rows changed: {old}")
        grouped[rel].append({
            **entry, "original_output": old,
            "legacy_output": legacy,
            "row_offset": offset, "row_count": num_rows,
        })
    updated = []
    for rel, records in sorted(grouped.items()):
        path = stage / rel
        if not path.is_file():
            raise ValueError(f"Missing Vigieau staged Parquet: {rel}")
        metadata = pq.read_metadata(path)
        expected = sum(int(x["row_count"]) for x in records)
        if expected != metadata.num_rows:
            raise ValueError(f"Vigieau manifest rows disagree: {rel}")
        base = dict(records[0])
        base.update({
            "output": rel,
            "rows": expected,
            "bytes": path.stat().st_size,
            "original_sources": records,
            "compacted": len(records) > 1,
        })
        updated.append(base)
    original_total_rows = sum(int(x["rows"]) for x in listing)
    if original_total_rows != sum(int(x["rows"]) for x in updated):
        raise ValueError("Vigieau total rows changed")
    dataset = json.loads((original / "dataset.json").read_text(encoding="utf-8"))
    if int(dataset["rows"]) != original_total_rows:
        raise ValueError("Vigieau source dataset rows changed")
    dataset.update({
        "parquet_files": len(updated),
        "rows": original_total_rows,
        "bytes": sum(x["bytes"] for x in updated),
        "geo_files": sum(bool(x.get("geo")) for x in updated),
    })
    replace_json(stage / "manifest.json", updated)
    replace_json(stage / "dataset.json", dataset)


def _validate_original_metadata(
    dataset_name: str, original: Path,
) -> None:
    """Verify legacy manifests cover physical Parquet files before rewriting."""
    if dataset_name == "imope":
        listing = json.loads((original / "manifest.json").read_text())
        expected = {f"{item['department']}/{p['path']}"
                    for item in listing for p in item["parquet"]}
        physical = {p.relative_to(original).as_posix()
                    for p in original.rglob("*.parquet")}
        if physical != expected:
            raise ValueError("IMoPE manifest misses or duplicates physical inputs")
    elif dataset_name == "vigieau":
        listing = json.loads((original / "manifest.json").read_text())
        paths = [_vigieau_current_output(original, item["output"])
                 for item in listing]
        physical = {p.relative_to(original).as_posix()
                    for p in original.rglob("*.parquet")}
        if len(paths) != len(set(paths)) or physical != set(paths):
            raise ValueError("Vigieau manifest does not match original inputs")
    elif dataset_name == "datatourisme":
        manifest = json.loads((original / "manifest.json").read_text())
        expected_csv = {item["output"] for item in manifest["csv"]}
        physical_csv = {p.relative_to(original).as_posix()
                        for p in (original / "csv").glob("*.parquet")}
        if physical_csv != expected_csv:
            raise ValueError("DataTourisme CSV manifest mismatch")


def _validate_stage(
    stage: Path, dataset_name: str, source_count: int,
    source_rows: int, candidate_files: int, compacted_batches: int,
    mapping: dict[str, tuple[str, int, int]],
) -> dict[str, int]:
    files = sorted(stage.rglob("*.parquet"))
    expected_files = source_count - candidate_files + compacted_batches
    if len(files) != expected_files:
        raise ValueError(
            f"{dataset_name} file count {len(files)} != {expected_files}"
        )
    rows = sum(pq.read_metadata(p).num_rows for p in files)
    if rows != source_rows:
        raise ValueError(
            f"{dataset_name} row count changed: {rows} != {source_rows}"
        )
    for old, (new, offset, length) in mapping.items():
        dest = stage / new
        if not dest.is_file():
            raise ValueError(f"Source mapping points to missing file: {old}")
        if offset < 0 or offset + length > pq.read_metadata(dest).num_rows:
            raise ValueError(f"Invalid row range: {old} -> {new}")
    return {
        "source_files": source_count, "published_files": len(files),
        "removed_files": candidate_files - compacted_batches,
        "rows": rows, "bytes": sum(p.stat().st_size for p in files),
    }


def _build_stage(
    key: str, dataset: Path, stage: Path, source_root: Path,
    publish_root: Path, output_root: Path,
    selected: list[tuple[str, Any, str]], inventory: dict[str, int],
    audit_provenance: dict[str, Any],
) -> dict[str, Any]:
    if stage.exists():
        raise RuntimeError(f"Pre-existing stage needs inspection: {stage}")
    copy_tree_hardlinks(dataset, stage)
    original = source_root / TARGETS[key]
    mapping: dict[str, tuple[str, int, int]] = {}
    # Source root is anchored at the planner's original bind-mount path.
    for _name, selection, _category in selected:
        output, _audit = output_paths(output_root, selection)
        rel_dir = Path(selection.directory).relative_to(Path(TARGETS[key]))
        new_relative = (rel_dir / output.name).as_posix()
        dest = stage / new_relative
        for src in selection.sources:
            relative = Path(src.relative).relative_to(Path(TARGETS[key])).as_posix()
            staged_original = stage / relative
            if not staged_original.is_file():
                raise FileNotFoundError(f"Original missing during stage: {src.relative}")
            staged_original.unlink()
        link_file(output, dest)
        offset = 0
        for src in selection.sources:
            relative = Path(src.relative).relative_to(Path(TARGETS[key])).as_posix()
            mapping[relative] = (new_relative, offset, src.rows)
            offset += src.rows
    if key == "datatourisme":
        _stage_nt_parts(stage, original, mapping)
        _manifest_datatourisme(stage, original, mapping)
    elif key == "imope":
        _manifest_imope(stage, original)
    elif key == "vigieau":
        _manifest_vigieau(stage, original, mapping)

    rows = [
        {"original": old, "published": mapped, "first_row": offset,
         "rows": count}
        for old, (mapped, offset, count) in sorted(mapping.items())
    ]
    index = {
        "format": "source-to-compacted-parquet-row-ranges-v1",
        "dataset": key, "source_root": str(source_root),
        "planner_sha256": audit_provenance["planner_sha256"],
        "candidate_batches": len(selected),
        "sources": rows,
    }
    replace_json(stage / "_COMPACTION_SOURCES.json", index)
    totals = _validate_stage(
        stage, key, inventory["source_files"], inventory["source_rows"],
        inventory["candidate_files"], inventory["candidate_batches"],
        mapping,
    )
    summary = {"dataset": key, **totals, "verified_batches": len(selected)}
    replace_json(stage / "_COMPACTION.json", summary)
    return summary


def _free_space_budget(output_root: Path, publish_root: Path, min_free_gib: int) -> None:
    """Bound staging bytes even if every source hardlink must become a copy."""
    if min_free_gib < 0:
        raise ValueError("--min-free-gib must be >=0")
    candidates = list(output_root.rglob("compact-*.parquet"))
    estimate = sum(p.stat().st_size for p in candidates)
    # Source trees usually stage as hardlinks, but do not assume this.
    originals = [
        p for prefix in TARGETS.values()
        for p in (publish_root / prefix).rglob("*.parquet")
    ]
    estimate += sum(p.stat().st_size for p in originals)
    free = shutil.disk_usage(publish_root).free
    if free < estimate + min_free_gib * GIB:
        raise OSError(
            errno.ENOSPC,
            f"Five-dataset staging may require up to {estimate/GIB:.2f} GiB "
            f"plus {min_free_gib} GiB reserve, available {free/GIB:.2f} GiB",
            str(publish_root),
        )


def publish(
    *, plan_dir: Path, source_root: Path,
    output_root: Path, writable_source_root: Path | None,
    prune_backup: bool, execute: bool, min_free_gib: int = 20,
) -> dict[str, Any]:
    root = source_root.expanduser().resolve()
    publish_root = (
        writable_source_root.expanduser().resolve()
        if writable_source_root is not None else root
    )
    plan_dir = plan_dir.expanduser().resolve()
    output_root = output_root.expanduser().resolve()
    if not root.samefile(publish_root):
        raise ValueError("Source roots must identify the exact same directory")
    for src in (root, publish_root):
        reject_overlapping_trees(src, output_root)
        reject_overlapping_trees(src, plan_dir)
    selected, inventory = read_plan(plan_dir, root)
    expected = provenance_manifest(selected, output_root, plan_dir)
    provenance_file = output_root / "_meta" / "five-datasets-provenance.json"
    if not provenance_file.is_file() or (
        json.loads(provenance_file.read_text(encoding="utf-8")) != expected
    ):
        raise ValueError("Verified compaction provenance changed or missing")
    last = json.loads((output_root / "_meta" / "last-run.json").read_text())
    if (last.get("successful_batches") != len(selected)
        or last.get("failed_batches") != 0):
        raise ValueError("Not every compaction batch completed successfully")
    grouped: dict[str, list[tuple[str, Any, str]]] = {
        key: [] for key in TARGETS
    }
    for item in selected:
        grouped[item[0]].append(item)
    paths = {
        key: publish_root / prefix for key, prefix in TARGETS.items()
    }
    for key, directory in paths.items():
        if not directory.is_dir() or directory.is_symlink():
            raise ValueError(f"Missing or unsafe dataset: {directory}")
        stage = directory.with_name(directory.name + ".compaction-stage")
        backup = directory.with_name(directory.name + ".precompact-backup")
        if stage.exists() or stage.is_symlink():
            raise RuntimeError(f"Existing stage: {stage}; inspect before retry")
        if backup.exists() or backup.is_symlink():
            raise RuntimeError(f"Existing backup: {backup}; inspect before retry")
        validate_source_matches_plan(directory, root, inventory[key], grouped[key])
        _validate_original_metadata(key, root / TARGETS[key])
        require_known_metadata(key, root / TARGETS[key], grouped[key])
        if execute:
            require_writable_publication_parent(directory)
    overview = {
        "datasets": list(TARGETS),
        "candidate_batches": len(selected),
        "source_files": sum(v["source_files"] for v in inventory.values()),
        "candidate_files": sum(v["candidate_files"] for v in inventory.values()),
        "expected_reduction": sum(v["candidate_files"] - v["candidate_batches"]
                                  for v in inventory.values()),
        "mode": "execute" if execute else "dry-run",
        "originals_untouched": True,
    }
    print(json.dumps(overview, indent=2, ensure_ascii=False), flush=True)
    if not execute:
        return overview
    _free_space_budget(output_root, publish_root, min_free_gib)

    started = time.perf_counter()
    staged: dict[str, dict[str, Any]] = {}
    swapped: list[str] = []
    # Strictly stage and validate all five before touching any original root.
    try:
        verify_outputs(selected, output_root)
        for key, directory in paths.items():
            stage = directory.with_name(directory.name + ".compaction-stage")
            staged[key] = _build_stage(
                key, directory, stage, root, publish_root, output_root,
                grouped[key], inventory[key], expected,
            )
            print(f"STAGED {key}: {staged[key]}", flush=True)

        for key, directory in paths.items():
            stage = directory.with_name(directory.name + ".compaction-stage")
            backup = directory.with_name(directory.name + ".precompact-backup")
            os.replace(directory, backup)
            try:
                os.replace(stage, directory)
                swapped.append(key)
            except BaseException:
                os.replace(backup, directory)
                raise
            # Check published data via its final location.
            _validate_stage(
                directory, key, inventory[key]["source_files"],
                inventory[key]["source_rows"],
                inventory[key]["candidate_files"],
                inventory[key]["candidate_batches"],
                {
                    item["original"]: (
                        item["published"], item["first_row"], item["rows"],
                    )
                    for item in json.loads(
                        (directory / "_COMPACTION_SOURCES.json").read_text()
                    )["sources"]
                },
            )
            print(f"PUBLISHED {key}", flush=True)
    except BaseException:
        # If a regular exception occurs during the multi-directory swap,
        # restore every original before returning a failure. SIGKILL/power
        # loss may still require manual recovery from .precompact-backup.
        for key in reversed(swapped):
            directory = paths[key]
            backup = directory.with_name(directory.name + ".precompact-backup")
            displaced = directory.with_name(directory.name + ".failed-published")
            if backup.exists():
                os.replace(directory, displaced)
                os.replace(backup, directory)
                print(f"RESTORED original {key}; inspect {displaced}", file=sys.stderr)
        raise

    # Every published dataset has been validated. Prune only now.
    if prune_backup:
        for key, directory in paths.items():
            backup = directory.with_name(directory.name + ".precompact-backup")
            prune_validated_backup(backup)
            print(f"PRUNED original {key}", flush=True)

    report = {
        "datasets": list(TARGETS),
        "verified_batches": len(selected),
        "source_files": sum(x["source_files"] for x in staged.values()),
        "published_files": sum(x["published_files"] for x in staged.values()),
        "redundant_source_files": sum(x["removed_files"] for x in staged.values()),
        "verified_rows": sum(x["rows"] for x in staged.values()),
        "pruned": prune_backup,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "breakdown": staged,
    }
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return report


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="Safely publish five reviewed, fully verified Parquet datasets."
    )
    p.add_argument("--plan-dir", required=True, type=Path)
    p.add_argument("--source-root", required=True, type=Path)
    p.add_argument("--writable-source-root", type=Path)
    p.add_argument("--output-root", required=True, type=Path)
    p.add_argument("--min-free-gib", type=int, default=20)
    p.add_argument("--execute", action="store_true")
    p.add_argument("--prune-backup", action="store_true")
    args = p.parse_args(argv)
    if args.prune_backup and not args.execute:
        p.error("--prune-backup requires --execute")
    try:
        publish(
            plan_dir=args.plan_dir, source_root=args.source_root,
            output_root=args.output_root,
            writable_source_root=args.writable_source_root,
            prune_backup=args.prune_backup, execute=args.execute,
            min_free_gib=args.min_free_gib,
        )
        return 0
    except (OSError, ValueError, RuntimeError, KeyError, TypeError) as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
