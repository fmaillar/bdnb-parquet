"""Fail-closed IMoPE v2 2048 MiB publisher (single dataset only).

The publisher requires the independent 92-batch WKB/Arrow/alias attestation.
It builds and verifies a complete *separate* replacement tree, migrates
departmental metadata and 187 aliases, then uses same-filesystem directory
renames for the swap. The old published IMoPE is retained in a distinct v2
rollback directory unless --prune-backup is explicitly requested.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import stat
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from .compact_five_publish import replace_json, _json_strings
from .compact_publish import (
    copy_tree_hardlinks, link_file, prune_validated_backup,
    require_writable_publication_parent,
)
from .compact_run import compare_rows, relative_path, reject_overlapping_trees
from .imope_promote import validate_parquet
from .imope_v2_audit import ATTESTATION, sha256
from .imope_v2_pilot import read_pilot
from .imope_v2_schedule import _exclusive_staging_lock, inspect_plan, plan_fingerprint

GIB = 2**30
STAGING_SUFFIX = ".imope-v2-publish-stage"
BACKUP_SUFFIX = ".imope-v2-prepublish-backup"


def _read_json(path: Path) -> Any:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"Missing or unsafe metadata: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _metadata_preflight(root: Path, selected: set[str]) -> tuple[list[dict], dict, dict]:
    """The existing departmental manifest is authoritative for logical rows."""
    manifest = _read_json(root / "manifest.json")
    dataset = _read_json(root / "dataset.json")
    index = _read_json(root / "_COMPACTION_SOURCES.json")
    if (not isinstance(manifest, list) or not isinstance(dataset, dict)
        or not isinstance(index, dict) or index.get("dataset") != "imope"):
        raise ValueError("Unrecognized current IMoPE metadata")
    listed: set[str] = set()
    for dept in manifest:
        name = dept["department"]
        relative_path(name)
        if "/" in name or not (root / name).is_dir():
            raise ValueError(f"Unsafe or missing IMoPE department: {name}")
        descriptions = dept.get("parquet")
        if not isinstance(descriptions, list):
            raise ValueError(f"Missing physical department manifest: {name}")
        row_sum = 0
        for item in descriptions:
            item_name = relative_path(item["path"])
            if len(item_name.parts) != 1 or item_name.suffix != ".parquet":
                raise ValueError(f"Invalid department file: {name}/{item_name}")
            rel = (Path(name) / item_name).as_posix()
            if rel in listed:
                raise ValueError(f"Duplicate physical manifest entry: {rel}")
            listed.add(rel)
            real = root / rel
            if real.is_symlink() or not real.is_file():
                raise ValueError(f"Missing published source file: {rel}")
            metadata = validate_parquet(real)
            for key in ("path", "rows", "row_groups", "columns", "bytes", "geo"):
                if item.get(key) != metadata[key]:
                    raise ValueError(f"Current manifest differs at {key}: {rel}")
            row_sum += item["rows"]
        if row_sum != dept["rows"]:
            raise ValueError(f"Current department row total changed: {name}")
        marker = _read_json(root / name / "_SUCCESS.json")
        if marker != dept:
            raise ValueError(f"Department _SUCCESS disagrees: {name}")
    physical = {
        p.relative_to(root).as_posix() for p in root.rglob("*.parquet")
    }
    if physical != listed:
        raise ValueError(
            "Current physical IMoPE files differ from departmental manifest"
        )
    if sum(x["rows"] for x in manifest) != dataset["rows"]:
        raise ValueError("Current IMoPE dataset physical row count differs")
    if sum(x["files"] for x in manifest) != len(physical):
        raise ValueError("Current IMoPE department physical file counts differ")
    if not selected.issubset(physical):
        raise ValueError("Planned deletion includes unknown physical source files")

    # Only known JSON sidecars can be rewritten. Never leave a forgotten JSON
    # consumer pointing at a moved physical file without explicitly reviewing it.
    allowed = {
        "manifest.json", "dataset.json", "_COMPACTION.json",
        "_COMPACTION_SOURCES.json", "_COMPACTION_V2.json",
    }
    for file in sorted(root.rglob("*.json")):
        rel = file.relative_to(root)
        if (rel.as_posix() in allowed
            or (len(rel.parts) == 2 and rel.name == "_SUCCESS.json")):
            continue
        if file.is_symlink():
            raise ValueError(f"Unsafe extra metadata symlink: {file}")
        item = _read_json(file)
        # Treat the absolute/department-relative old path as a reference;
        # the same table basename may legitimately occur in 96 departments.
        for value in _json_strings(item):
            normalized = value.replace("\\", "/")
            if normalized in selected or any(
                normalized.endswith("/" + path) for path in selected
            ):
                raise ValueError(f"Unhandled old Parquet reference: {file}: {value}")
    if (root / "manifest.parquet").exists():
        raise ValueError("Unexpected manifest.parquet cannot be safely rebuilt")
    return manifest, dataset, index


def _preflight(
    plan: Path, root: Path, outputs: Path, *,
    min_free_gib: int,
) -> dict[str, Any]:
    if min_free_gib < 0:
        raise ValueError("--min-free-gib must be >= 0")
    if not root.is_dir() or root.is_symlink():
        raise ValueError("Unsafe or missing IMoPE publication root")
    if not outputs.is_dir() or outputs.is_symlink():
        raise ValueError("Unsafe or missing IMoPE v2 staging root")
    reject_overlapping_trees(root, plan)
    reject_overlapping_trees(root, outputs)
    reject_overlapping_trees(plan, outputs)
    batches, summary = inspect_plan(plan, root, check_sources=True)
    attested = _read_json(outputs / "_meta" / ATTESTATION)
    fingerprints = plan_fingerprint(plan)
    if (
        attested.get("format") != "imope-v2-validation-v1"
        or attested.get("target_mib") != 2048
        or attested.get("source_root") != str(root)
        or attested.get("staging_root") != str(outputs)
        or attested.get("plan_sha256") != fingerprints
        or attested.get("validated_batches") != len(batches)
        or attested.get("validated_sources") != summary["candidate_source_files"]
        or attested.get("validated_rows") != summary["candidate_rows"]
        or attested.get("geo_batches") != summary["geo_batches"]
    ):
        raise ValueError("Independent IMoPE v2 audit attestation is absent/stale")
    lineage = attested.get("lineage", {})
    if (
        lineage.get("original_physical_files") != summary["physical_files"]
        or lineage.get("existing_aliases") != summary["existing_logical_aliases"]
        or lineage.get("aliases_requiring_retarget")
        != summary["existing_aliases_requiring_retarget"]
        or lineage.get("unchanged_originals")
        != summary["uncompacted_files_not_selected"]
        or lineage.get("first_publication_compacts")
        != summary["already_compacted_files"]
    ):
        raise ValueError("Audited original IMoPE lineage differs from plan")
    inventory = attested.get("outputs")
    if not isinstance(inventory, list) or len(inventory) != len(batches):
        raise ValueError("Attestation does not cover every compacted output")
    by_output = {r["output"]: r for r in inventory}
    if len(by_output) != len(inventory):
        raise ValueError("Duplicate output in independent audit")

    candidate_paths: set[str] = set()
    by_original: dict[str, dict[str, Any]] = {}
    with (plan / "sources.tsv").open(encoding="utf-8", newline="") as stream:
        for source in csv.DictReader(stream, delimiter="\t"):
            old = relative_path(source["original"]).as_posix()
            if old in candidate_paths:
                raise ValueError(f"Duplicate planned original: {old}")
            candidate_paths.add(old)
            by_original[old] = {
                "original": old,
                "published": relative_path(source["output"]).as_posix(),
                "first_row": int(source["first_row"]),
                "rows": int(source["rows"]),
                "department": source["department"],
                "batch_id": source["batch_id"],
            }
    if len(candidate_paths) != summary["candidate_source_files"]:
        raise ValueError("Plan source coverage does not match summary")
    original_manifest, dataset, index = _metadata_preflight(
        root, candidate_paths,
    )
    alias_records = index.get("deduplicated_aliases")
    if (not isinstance(alias_records, list)
        or len(alias_records) != summary["existing_logical_aliases"]):
        raise ValueError("Current SHA-deduplicated alias inventory changed")
    if len(index.get("sources", [])) != lineage["first_publication_sources"]:
        raise ValueError("First-publication provenance inventory changed")
    corrections = _read_json(plan / "pending_alias_rewrites.json")
    if len(corrections) != summary["existing_aliases_requiring_retarget"]:
        raise ValueError("Planned alias changes disagree")
    redirects = {a["original_alias"]: a for a in corrections}
    if len(redirects) != len(corrections):
        raise ValueError("Duplicate alias redirect")
    touched: set[str] = set()
    revised_aliases = []
    for alias in alias_records:
        relative = relative_path(alias["published"]).as_posix()
        updated = dict(alias)
        if relative in by_original:
            item = by_original[relative]
            change = redirects.get(alias["original"])
            if (change is None
                or change["previous_canonical"] != relative
                or change["previous_first_row"] != alias["first_row"]
                or change["rows"] != alias["rows"]
                or change["proposed_output"] != item["published"]
                or change["proposed_first_row"]
                != item["first_row"] + alias["first_row"]):
                raise ValueError(f"Invalid future alias mapping: {alias['original']}")
            touched.add(alias["original"])
            updated["published"] = item["published"]
            updated["first_row"] = change["proposed_first_row"]
            updated["v2_retargeted"] = True
        revised_aliases.append(updated)
    if touched != set(redirects):
        raise ValueError("Some planned IMoPE alias redirects cannot be resolved")

    # Attestation is trusted for the independent geometry scan, but every
    # staged Parquet is re-hashed at publication time to detect later changes.
    verified_outputs = {}
    for group in batches:
        rel = relative_path(group["output"]).as_posix()
        record = by_output.get(rel)
        path = outputs / rel
        if (record is None or path.is_symlink() or not path.is_file()
            or record["batch_id"] != group["batch_id"]
            or record["bytes"] != path.stat().st_size
            or record["rows"] != group["rows"]
            or record["sources"] != group["sources"]
            or sha256(path) != record["sha256"]):
            raise ValueError(f"Verified output differs from attestation: {rel}")
        verified_outputs[rel] = record
    actual = {p.relative_to(outputs).as_posix()
              for p in outputs.rglob("*.parquet")}
    if actual != set(verified_outputs):
        raise ValueError("Stage has missing or unexpected Parquet outputs")
    expected_files = summary["projected_physical_files"]
    if expected_files != summary["physical_files"] - len(candidate_paths) + len(batches):
        raise ValueError("IMoPE projected physical file arithmetic differs")

    # Free disk check: hardlink stage may fall back to full verified copies
    # across mount boundaries, so budget for the entire old tree too.
    staged_bytes = sum(int(x["bytes"]) for x in inventory)
    original_bytes = sum(
        p.stat().st_size for p in root.rglob("*.parquet")
    )
    required = (staged_bytes + original_bytes + min_free_gib * GIB)
    free = shutil.disk_usage(root.parent).free
    if free < required:
        raise OSError(
            f"Insufficient publication reserve: need {required/GIB:.2f} GiB "
            f"(incl {min_free_gib} GiB minimum); free={free/GIB:.2f} GiB"
        )
    return {
        "plan": plan, "root": root, "output_root": outputs,
        "summary": summary,
        "manifest": original_manifest,
        "dataset": dataset,
        "index": index,
        "by_original": by_original,
        "revised_aliases": revised_aliases,
        "verified_outputs": verified_outputs,
        "attestation_sha256": sha256(outputs / "_meta" / ATTESTATION),
        "free_bytes": free,
        "required_bytes": required,
        "backup": root.with_name(root.name + BACKUP_SUFFIX),
        "replacement": root.with_name(root.name + STAGING_SUFFIX),
        "expected_files": expected_files,
        "original_bytes": original_bytes,
    }



def _verify_current_source_values(data: dict[str, Any]) -> None:
    """Refuse to publish if current source values diverged since attestation.

    Footer/schema/size inventory checks cannot detect all in-place data
    mutations. A final complete Arrow comparison runs *before* any swap
    and independently validates every source against the attested output.
    """
    groups, _ = inspect_plan(data["plan"], data["root"], check_sources=False)
    for number, group in enumerate(groups, 1):
        selected, _, sources = read_pilot(
            data["plan"], data["root"], batch_id=group["batch_id"],
        )
        output = data["output_root"] / relative_path(selected["output"])
        verified = compare_rows(tuple(sources), output, 16384, threads=2)
        if verified != selected["rows"]:
            raise ValueError(
                f"Original Arrow data changed since independent audit: "
                f"{group['batch_id']}"
            )
        if number % 10 == 0 or number == len(groups):
            print(
                f"FINAL SOURCE CHECK [{number}/{len(groups)}] "
                "all Arrow values still match",
                file=sys.stderr, flush=True,
            )



def _build_replacement(data: dict[str, Any]) -> dict[str, Any]:
    root = data["root"]
    replacement = data["replacement"]
    copy_tree_hardlinks(root, replacement)
    by_old = data["by_original"]
    for old in sorted(by_old):
        path = replacement / old
        if not path.is_file() or path.is_symlink():
            raise ValueError(f"Missing selected source during staging: {old}")
        path.unlink()
    for name, report in sorted(data["verified_outputs"].items()):
        origin = data["output_root"] / name
        destination = replacement / relative_path(name)
        link_file(origin, destination)
        if (destination.stat().st_size != report["bytes"]
            or sha256(destination) != report["sha256"]):
            raise ValueError(f"Newly staged file changed: {name}")

    by_dept: dict[str, list[dict]] = defaultdict(list)
    for old, mapping in by_old.items():
        by_dept[Path(old).parts[0]].append(mapping)
    dept_alias: dict[str, list[dict]] = defaultdict(list)
    for item in data["revised_aliases"]:
        dept_alias[Path(relative_path(item["original"])).parts[0]].append(item)
    rewritten_depts = []
    for current in data["manifest"]:
        name = current["department"]
        directory = replacement / name
        retained = [
            validate_parquet(p) for p in sorted(directory.glob("*.parquet"))
        ]
        virtual = sorted(by_dept[name], key=lambda x: x["original"])
        if (sum(x["rows"] for x in retained)
            + sum(x["rows"] for x in virtual) != current["rows"]):
            raise ValueError(f"Departmental row total changed: {name}")
        previous_aliases = current.get("deduplicated_aliases", [])
        old_alias_paths = {a["original"] for a in previous_aliases}
        if old_alias_paths != {a["original"] for a in dept_alias[name]}:
            raise ValueError(f"Department dedup alias inventory changed: {name}")
        rewritten = {
            **current,
            "parquet": retained,
            "virtual_parquet": virtual,
            "files": len(retained),
            "physical_rows_within_department": sum(x["rows"] for x in retained),
            "virtual_rows": sum(x["rows"] for x in virtual),
            "bytes": sum(x["bytes"] for x in retained),
            "geo_files": sum(bool(x["geo"]) for x in retained),
            "deduplicated_aliases": sorted(
                dept_alias[name], key=lambda a: a["original"],
            ),
            "layout_version": 2,
            "rows": current["rows"],
        }
        rewritten_depts.append(rewritten)
        replace_json(directory / "_SUCCESS.json", rewritten)

    mapped = [
        {key: item[key] for key in
         ("original", "published", "first_row", "rows", "department", "batch_id")}
        for item in sorted(by_old.values(), key=lambda x: x["original"])
    ]
    lineage = {
        **data["index"],
        "v2_sources": mapped,
        "deduplicated_aliases": data["revised_aliases"],
        "v2_plan_sha256": plan_fingerprint(data["plan"]),
        "v2_validation_attestation_sha256": data["attestation_sha256"],
        "v2_layout_version": 2,
    }
    replace_json(replacement / "_COMPACTION_SOURCES.json", lineage)

    paths = sorted(replacement.rglob("*.parquet"))
    by_path = {p.relative_to(replacement).as_posix(): p for p in paths}
    if len(paths) != data["expected_files"]:
        raise ValueError(
            f"Final IMoPE file count differs: {len(paths)} "
            f"!= {data['expected_files']}"
        )
    expected_paths = (
        set(data["verified_outputs"])
        | {
            p.relative_to(root).as_posix()
            for p in root.rglob("*.parquet") if p.relative_to(root).as_posix() not in by_old
        }
    )
    if set(by_path) != expected_paths:
        raise ValueError("Replacement includes unexpected or missing Parquet")
    physical_rows = sum(pq.read_metadata(p).num_rows for p in paths)
    if physical_rows != data["dataset"]["rows"]:
        raise ValueError(
            f"IMoPE total physical rows changed: {physical_rows} "
            f"!= {data['dataset']['rows']}"
        )
    for item in mapped:
        target = replacement / relative_path(item["published"])
        if not target.is_file() or (
            item["first_row"] + item["rows"] > pq.read_metadata(target).num_rows
        ):
            raise ValueError(f"Invalid relocated source row range: {item['original']}")
    for item in lineage["sources"]:
        target = replacement / relative_path(item["published"])
        if not target.is_file() or (
            item["first_row"] + item["rows"] > pq.read_metadata(target).num_rows
        ):
            raise ValueError(f"Lost v1 historical source range: {item['original']}")
    for item in lineage["deduplicated_aliases"]:
        if item["published_scope"] != "imope":
            raise ValueError("Unreviewed external alias")
        target = replacement / relative_path(item["published"])
        if not target.is_file() or (
            item["first_row"] + item["rows"] > pq.read_metadata(target).num_rows
        ):
            raise ValueError(f"Unresolved updated alias: {item['original']}")
    geo_count = sum(bool((pq.read_schema(p).metadata or {}).get(b"geo"))
                    for p in paths)
    total_bytes = sum(p.stat().st_size for p in paths)
    replace_json(replacement / "manifest.json", rewritten_depts)
    replace_json(replacement / "dataset.json", {
        **data["dataset"],
        "files": len(paths),
        "rows": physical_rows,
        "bytes": total_bytes,
        "geo_files": geo_count,
        "layout_version": 2,
        "v2_compacted_batches": len(data["verified_outputs"]),
        "v2_cross_department_sources": len(mapped),
        "v2_aliased_files": len(lineage["deduplicated_aliases"]),
        "storage": "IMoPE v2 GeoParquet with virtual departmental row ranges",
    })
    report = {
        "format": "imope-v2-publication-v1",
        "input_physical_files": data["summary"]["physical_files"],
        "published_files": len(paths),
        "new_compacts": len(data["verified_outputs"]),
        "replaced_source_files": len(by_old),
        "retained_source_files": len(paths) - len(data["verified_outputs"]),
        "verified_physical_rows": physical_rows,
        "physical_bytes": total_bytes,
        "alias_count": len(lineage["deduplicated_aliases"]),
        "aliases_retargeted": sum(bool(a.get("v2_retargeted"))
                                for a in lineage["deduplicated_aliases"]),
        "plan_sha256": plan_fingerprint(data["plan"]),
        "attestation_sha256": data["attestation_sha256"],
        "publication_phase": "replacement-fully-validated-before-swap",
    }
    replace_json(replacement / "_COMPACTION_V2.json", report)
    # Original directories were immutable for the published snapshot. Restore
    # their exact permission modes only after every stage file is finished.
    for current in [root, *(p for p in root.rglob("*") if p.is_dir())]:
        if current.is_symlink():
            raise ValueError(f"Unsafe original directory symlink: {current}")
        matching = replacement / current.relative_to(root)
        if not matching.is_dir() or matching.is_symlink():
            raise ValueError(f"Missing preserved metadata directory: {matching}")
        os.chmod(matching, stat.S_IMODE(current.stat().st_mode))
    return report


def _validate_published(published: Path, data: dict[str, Any], report: dict) -> None:
    if published.is_symlink() or not published.is_dir():
        raise ValueError("Published dataset not a safe directory")
    physical = sorted(published.rglob("*.parquet"))
    if len(physical) != report["published_files"]:
        raise ValueError("Published physical Parquet count changed")
    if sum(pq.read_metadata(p).num_rows for p in physical) != report["verified_physical_rows"]:
        raise ValueError("Published physical row total changed")
    if sum(p.stat().st_size for p in physical) != report["physical_bytes"]:
        raise ValueError("Published physical byte total changed")
    for name, attested in data["verified_outputs"].items():
        published_file = published / relative_path(name)
        if (not published_file.is_file() or published_file.is_symlink()
            or sha256(published_file) != attested["sha256"]):
            raise ValueError(f"Published compacted SHA256 changed: {name}")
    index = _read_json(published / "_COMPACTION_SOURCES.json")
    if (len(index["v2_sources"]) != report["replaced_source_files"]
        or len(index["deduplicated_aliases"]) != report["alias_count"]
        or sum(bool(x.get("v2_retargeted"))
               for x in index["deduplicated_aliases"]) != report["aliases_retargeted"]):
        raise ValueError("Published provenance or alias count changed")
    for entry in index["v2_sources"] + index["sources"] + index["deduplicated_aliases"]:
        target = published / relative_path(entry["published"])
        if (not target.is_file() or target.is_symlink()
            or entry["first_row"] < 0 or entry["rows"] < 0
            or entry["first_row"] + entry["rows"] > pq.read_metadata(target).num_rows):
            raise ValueError(f"Unresolved published row mapping: {entry['original']}")
    rebuilt = _read_json(published / "manifest.json")
    if len(rebuilt) != len(data["manifest"]):
        raise ValueError("Published department inventory changed")
    for item in rebuilt:
        current = _read_json(published / item["department"] / "_SUCCESS.json")
        if current != item:
            raise ValueError(f"Published departmental marker mismatch: {item['department']}")
        if (sum(x["rows"] for x in item["parquet"])
            + sum(x["rows"] for x in item["virtual_parquet"]) != item["rows"]):
            raise ValueError(f"Published departmental virtual coverage differs: {item['department']}")
    dataset = _read_json(published / "dataset.json")
    if dataset["files"] != len(physical) or dataset["rows"] != report["verified_physical_rows"]:
        raise ValueError("Published dataset.json totals disagree")


def publish(
    plan_dir: Path, root: Path, staged_outputs: Path, *,
    execute: bool = False, prune_backup: bool = False,
    min_free_gib: int = 20,
) -> dict[str, Any]:
    if prune_backup and not execute:
        raise ValueError("--prune-backup requires --execute")
    plan, root, outputs = (p.expanduser().resolve()
                           for p in (plan_dir, root, staged_outputs))
    original_backup = root.with_name(root.name + BACKUP_SUFFIX)
    replacement = root.with_name(root.name + STAGING_SUFFIX)
    for path in (original_backup, replacement):
        if path.exists() or path.is_symlink():
            raise RuntimeError(
                f"Publication stage/backup already exists; inspect before retry: {path}"
            )
    metadata = outputs / "_meta"
    if not metadata.is_dir() or metadata.is_symlink():
        raise ValueError("Missing or unsafe verified v2 staging metadata")
    with _exclusive_staging_lock(metadata):
        data = _preflight(plan, root, outputs, min_free_gib=min_free_gib)
        overview = {
            "mode": "execute" if execute else "dry-run",
            "target_mib": 2048,
            "candidate_files_to_replace": len(data["by_original"]),
            "new_compacted_files": len(data["verified_outputs"]),
            "expected_physical_files": data["expected_files"],
            "aliases": len(data["revised_aliases"]),
            "aliases_to_retarget": sum(
                bool(x.get("v2_retargeted")) for x in data["revised_aliases"]
            ),
            "backup": str(original_backup),
            "prune_backup_after_verification": prune_backup,
            "publication_performed": False,
        }
        print(json.dumps(overview, ensure_ascii=False, indent=2), flush=True)
        if not execute:
            return overview
        require_writable_publication_parent(root)
        start = time.perf_counter()
        _verify_current_source_values(data)
        swapped = False
        try:
            report = _build_replacement(data)
            _validate_published(replacement, data, report)
            os.replace(root, original_backup)
            try:
                os.replace(replacement, root)
            except BaseException:
                os.replace(original_backup, root)
                raise
            swapped = True
            _validate_published(root, data, report)
        except BaseException:
            if swapped and original_backup.exists():
                displaced = root.with_name(root.name + ".imope-v2-failed-published")
                if displaced.exists() or displaced.is_symlink():
                    raise RuntimeError(
                        "Published v2 rollback blocked by existing displaced dataset"
                    )
                os.replace(root, displaced)
                os.replace(original_backup, root)
                print(
                    f"RESTORED old IMoPE dataset; inspect {displaced}",
                    file=sys.stderr, flush=True,
                )
            # Leave any incomplete stages in place for manual inspection,
            # never risk accidentally deleting an original or shared inode.
            raise
        if prune_backup:
            # Explicit irreversible cleanup only after post-swap checks pass.
            prune_validated_backup(original_backup)
        result = {
            **report,
            "publication_performed": True,
            "backup_pruned": prune_backup,
            "backup": str(original_backup),
            "elapsed_seconds": round(time.perf_counter() - start, 3),
        }
        print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
        return result



def prune_published_v2_backup(
    plan_dir: Path, published_root: Path, stage_root: Path,
) -> dict[str, Any]:
    """Separately finalize: recheck the live replacement, then prune v2 backup.

    This is intentionally unavailable before successful publication. It
    doesn't touch the five old first-pass backups or the staging outputs.
    """
    plan, root, stage = (
        p.expanduser().resolve()
        for p in (plan_dir, published_root, stage_root)
    )
    backup = root.with_name(root.name + BACKUP_SUFFIX)
    residual = root.with_name(root.name + STAGING_SUFFIX)
    if residual.exists() or residual.is_symlink():
        raise RuntimeError(f"Unfinished v2 publication stage: {residual}")
    if not backup.is_dir() or backup.is_symlink():
        raise ValueError(f"Verified v2 rollback backup missing: {backup}")
    reject_overlapping_trees(root, plan)
    reject_overlapping_trees(root, stage)
    reject_overlapping_trees(plan, stage)
    attested = _read_json(stage / "_meta" / ATTESTATION)
    fingerprint = plan_fingerprint(plan)
    if (attested.get("format") != "imope-v2-validation-v1"
        or attested.get("source_root") != str(root)
        or attested.get("staging_root") != str(stage)
        or attested.get("plan_sha256") != fingerprint):
        raise ValueError("Original independent audit attestation differs")
    data = _read_json(root / "_COMPACTION_V2.json")
    if (data.get("format") != "imope-v2-publication-v1"
        or data.get("plan_sha256") != fingerprint
        or data.get("attestation_sha256")
        != sha256(stage / "_meta" / ATTESTATION)
        or data.get("input_physical_files") != 3360
        or data.get("published_files") != 213
        or data.get("new_compacts") != attested.get("validated_batches")
        or data.get("aliases_retargeted") !=
        attested["lineage"]["aliases_requiring_retarget"]):
        raise ValueError("Live v2 publication metadata doesn't match attestation")
    manifest = _read_json(root / "manifest.json")
    verified = {
        entry["output"]: entry
        for entry in attested["outputs"]
    }
    if len(verified) != len(attested["outputs"]):
        raise ValueError("Repeated attested output in cleanup")
    with _exclusive_staging_lock(stage / "_meta"):
        _validate_published(root, {
            "manifest": manifest,
            "verified_outputs": verified,
        }, data)
        old_manifest, old_dataset, old_index = _metadata_preflight(backup, set())
        if (sum(x["files"] for x in old_manifest) != data["input_physical_files"]
            or old_dataset["rows"] != data["verified_physical_rows"]
            or len(old_index["deduplicated_aliases"]) != data["alias_count"]):
            raise ValueError("Old v2 rollback backup doesn't match publication")
        prune_validated_backup(backup)
    result = {
        "mode": "finalize-v2-publication",
        "publication_validated": True,
        "physical_files": data["published_files"],
        "physical_rows": data["verified_physical_rows"],
        "backup_pruned": True,
        "backup_path": str(backup),
        "first_pass_backups_untouched": True,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
    return result



def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="Publish independently attested IMoPE v2 after safe staging"
    )
    p.add_argument("--plan-dir", required=True, type=Path)
    p.add_argument("--source-root", required=True, type=Path)
    p.add_argument("--stage-root", required=True, type=Path)
    p.add_argument("--min-free-gib", type=int, default=20)
    p.add_argument("--execute", action="store_true")
    p.add_argument("--prune-backup", action="store_true",
                   help="prune old v2 dataset after checked swap in the same run")
    p.add_argument("--prune-backup-only", action="store_true",
                   help="separate post-publication revalidation then prune v2 backup")
    args = p.parse_args(argv)
    if args.prune_backup_only and (args.execute or args.prune_backup):
        p.error("--prune-backup-only cannot be combined with --execute/--prune-backup")
    try:
        if args.prune_backup_only:
            prune_published_v2_backup(args.plan_dir, args.source_root, args.stage_root)
            return 0
        publish(
            args.plan_dir, args.source_root, args.stage_root,
            execute=args.execute, prune_backup=args.prune_backup,
            min_free_gib=args.min_free_gib,
        )
        return 0
    except (OSError, ValueError, RuntimeError, KeyError, TypeError) as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}",
              file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
