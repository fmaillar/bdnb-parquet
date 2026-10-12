"""Transactional publication of five explicitly approved, verified datasets.

Stages replacements, verifies exact Arrow rows, rebuilds source-specific
manifests and per-source row-range provenance, then swaps all five roots.
Rollback is attempted before backup pruning if any swap/validation fails.
No other corpus datasets are touched.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import errno
import json
import os
import re
import shutil
import stat
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from .compact_five import TARGETS, provenance_manifest, read_plan
from .compact_publish import (
    link_file, copy_tree_hardlinks, require_writable_publication_parent,
    prune_validated_backup,
)
from .compact_run import (
    output_paths, reject_overlapping_trees, relative_path, verify,
)
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
    original: Path, dataset_key: str, inventory: dict[str, int],
    selected: list[tuple[str, Any, str]],
) -> None:
    """Compare physical native files to the original planner-relative names.

    Both mount aliases can reference the same inodes but have distinct path
    prefixes. Compute relative paths within the physical dataset, then
    prepend the plan's fixed logical dataset prefix. Never call relative_to()
    between the bind-mount and native-mount roots.
    """
    prefix = Path(TARGETS[dataset_key])
    parquet_files = {
        (prefix / p.relative_to(original)).as_posix(): p
        for p in original.rglob("*.parquet")
    }
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


def _datatourisme_csv_inventory(
    original: Path, manifest: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Reconcile manifest CSV outputs with files, allowing ONLY absent zero-row
    Parquet entries (with zero row groups) explicitly recorded in metadata.

    The zero-row exception never creates an artificial Parquet file and never
    waives a missing nonempty file, an unlisted physical file, or a duplicate.
    """
    entries = manifest.get("csv")
    if not isinstance(entries, list):
        raise ValueError("DataTourisme CSV manifest has no csv list")
    by_path: dict[str, dict[str, Any]] = {}
    for entry in entries:
        if not isinstance(entry, dict) or not isinstance(entry.get("output"), str):
            raise ValueError("Invalid DataTourisme CSV manifest entry")
        rel = relative_path(entry["output"])
        if len(rel.parts) != 2 or rel.parts[0] != "csv" or rel.suffix != ".parquet":
            raise ValueError(f"Invalid DataTourisme CSV output path: {rel}")
        name = rel.as_posix()
        if name in by_path:
            raise ValueError(
                "DataTourisme CSV manifest has duplicate output references"
            )
        by_path[name] = entry
    folder = original / "csv"
    physical = {
        item.relative_to(original).as_posix()
        for item in folder.rglob("*.parquet")
    } if folder.is_dir() else set()
    extras = sorted(physical - set(by_path))
    missing = sorted(set(by_path) - physical)
    unsafe_missing = [
        name for name in missing
        if (
            type(by_path[name].get("rows")) is not int
            or type(by_path[name].get("row_groups")) is not int
            or by_path[name]["rows"] != 0
            or by_path[name]["row_groups"] != 0
            or (original / name).exists()
            or (original / name).is_symlink()
        )
    ]
    if extras or unsafe_missing:
        raise ValueError(
            "DataTourisme CSV manifest mismatch: "
            f"missing_on_disk={len(missing)} {missing[:8]!r}, "
            f"unlisted_on_disk={len(extras)} {extras[:8]!r}, "
            f"unverified_missing={len(unsafe_missing)} "
            f"{unsafe_missing[:8]!r}"
        )
    kept: list[dict[str, Any]] = []
    omitted: list[dict[str, Any]] = []
    for name, entry in by_path.items():
        path = original / name
        if name in physical:
            if path.is_symlink() or not path.is_file():
                raise ValueError(f"Unsafe CSV output: {path}")
            footer = pq.read_metadata(path)
            if int(entry["rows"]) != footer.num_rows:
                raise ValueError(
                    f"DataTourisme CSV rows differ from manifest: {name}"
                )
            kept.append(entry)
        else:
            # Already validated against the exact zero-row/zero-row-group
            # exception above, with no unlisted physical CSV files.
            omitted.append({
                **entry,
                "reason": "manifest-only CSV output absent from disk; "
                          "declared zero rows and zero row groups",
            })
    return kept, omitted


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
    present_csv, omitted_csv = _datatourisme_csv_inventory(original, manifest)
    # Never modify or prune the original JSON: this manifest is written only
    # into the independent staging tree after all source checks succeed.
    manifest["csv"] = [dict(item) for item in present_csv]
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
    manifest["csv_files"] = len(manifest["csv"])
    manifest["compaction_source_ranges"] = "_COMPACTION_SOURCES.json"
    if omitted_csv:
        manifest["compaction_omitted_empty_csv"] = omitted_csv
    replace_json(stage / "manifest.json", manifest)
    dataset = json.loads((original / "dataset.json").read_text(encoding="utf-8"))
    dataset.update({
        "bytes": manifest["bytes"],
        "rows": manifest["rows"],
        "csv_files": len(manifest["csv"]),
    })
    if omitted_csv:
        dataset["compaction_omitted_empty_csv"] = omitted_csv
    replace_json(stage / "dataset.json", dataset)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _catalog_release_parquet_path(raw: str) -> Path:
    """Normalize the two known build-release catalog root conventions.

    build_release.py emits paths relative to --source-root, which was
    sometimes the Parquet directory itself (without 'parquet/') and sometimes
    the release root (with 'parquet/'). In both cases catalog entries refer
    exclusively to files within the release's parquet/ subtree.
    """
    rel = relative_path(raw)
    if rel.parts[0] == "parquet":
        if len(rel.parts) < 2:
            raise ValueError(f"Empty Parquet catalog path: {raw!r}")
        return rel
    return Path("parquet") / rel


def _imope_deduplicated_aliases(
    original: Path,
    source_root: Path,
    catalog: Path | None,
) -> dict[str, dict[str, Any]]:
    """Authenticate missing IMoPE paths against the release SHA-256 catalog.

    The release builder removed byte-identical Parquet objects globally;
    original IMoPE manifest entries describe the pre-deduplication inventory.
    Never infer aliases from filenames, shapes, or matching byte lengths.
    """
    listing = json.loads((original / "manifest.json").read_text(encoding="utf-8"))
    if not isinstance(listing, list):
        raise ValueError("IMoPE manifest must contain a department list")
    entries: dict[str, dict[str, Any]] = {}
    manifest_paths: list[str] = []
    for dept in listing:
        name = dept["department"]
        if not isinstance(name, str) or not name or "/" in name or name == "..":
            raise ValueError(f"Invalid IMoPE department: {name!r}")
        if not isinstance(dept.get("parquet"), list):
            raise ValueError(f"Missing IMoPE department Parquet inventory: {name}")
        for description in dept["parquet"]:
            fragment = relative_path(description["path"])
            if len(fragment.parts) != 1 or fragment.suffix != ".parquet":
                raise ValueError(f"Unsafe IMoPE filename: {fragment}")
            name_path = (Path(name) / fragment).as_posix()
            manifest_paths.append(name_path)
            entries[name_path] = description
    duplicates = sorted(
        name for name, count in Counter(manifest_paths).items() if count > 1
    )
    physical = {
        p.relative_to(original).as_posix()
        for p in original.rglob("*.parquet")
    }
    missing = sorted(set(entries) - physical)
    unlisted = sorted(physical - set(entries))
    if duplicates or unlisted:
        raise ValueError(
            "IMoPE physical/manifest discrepancy: "
            f"duplicate_references={len(duplicates)} {duplicates[:8]!r}, "
            f"unlisted_physical={len(unlisted)} {unlisted[:8]!r}"
        )
    if not missing:
        return {}
    if catalog is None:
        raise ValueError(
            f"IMoPE has {len(missing)} deduplicated references requiring "
            "--duplicates-catalog <release-parquet-plan/_meta/duplicates.tsv>"
        )
    if not catalog.is_file() or catalog.is_symlink():
        raise ValueError(f"Unsafe or missing SHA-256 duplicates catalog: {catalog}")

    prefix = (Path("parquet") / TARGETS["imope"]).as_posix() + "/"
    found: dict[str, dict[str, Any]] = {}
    release = source_root.parent
    sha_cache: dict[Path, str] = {}
    footer_cache: dict[Path, dict[str, Any]] = {}
    with catalog.open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream, delimiter="\t")
        required = {"duplicate_path", "canonical_path", "bytes", "sha256"}
        if not required.issubset(set(reader.fieldnames or [])):
            raise ValueError("SHA-256 catalog has missing required columns")
        for row in reader:
            duplicate = _catalog_release_parquet_path(
                row["duplicate_path"],
            ).as_posix()
            if not duplicate.startswith(prefix):
                continue
            old = duplicate[len(prefix):]
            if old not in entries:
                raise ValueError(
                    f"IMoPE catalog references unlisted original: {old}"
                )
            if old in physical:
                raise ValueError(
                    f"IMoPE catalog marks an existing file as removed: {old}"
                )
            if old in found:
                raise ValueError(f"IMoPE duplicate catalog has repeated entry: {old}")
            canonical = _catalog_release_parquet_path(
                row["canonical_path"],
            )
            internal = canonical.as_posix().startswith(prefix)
            relative_canon = (
                Path(canonical.as_posix()[len(prefix):])
                if internal else canonical
            )
            path = release / canonical
            if (not path.is_file() or path.is_symlink()
                or path.stat().st_size != int(row["bytes"])):
                raise ValueError(
                    f"Missing/changed canonical file for IMoPE alias: {old} -> {path}"
                )
            if path not in sha_cache:
                sha_cache[path] = _sha256(path)
            if row["sha256"] != sha_cache[path]:
                raise ValueError(f"IMoPE canonical SHA-256 mismatch: {old}")
            if path not in footer_cache:
                footer_cache[path] = validate_parquet(path)
            footer = footer_cache[path]
            original_entry = entries[old]
            for field in ("bytes", "rows", "row_groups", "columns", "geo"):
                if original_entry[field] != footer[field]:
                    raise ValueError(
                        f"IMoPE alias {field} mismatch: {old} -> {canonical}"
                    )
            found[old] = {
                "original": old,
                "canonical": relative_canon.as_posix(),
                "canonical_release_path": canonical.as_posix(),
                "scope": "imope" if internal else "release",
                "sha256": row["sha256"],
                "original_metadata": dict(original_entry),
                "reason": "global-release-sha256-deduplication",
            }
    missing_catalog = sorted(set(missing) - set(found))
    if missing_catalog:
        raise ValueError(
            f"IMoPE {len(missing_catalog)} manifest-only references lack "
            f"verified catalog mappings: {missing_catalog[:12]!r}"
        )
    print(
        f"INFO: IMoPE {len(found)} manifest-only source aliases verified "
        f"against {len(sha_cache)} distinct SHA-256 canonical files; "
        "physical files will not be recreated",
        file=sys.stderr, flush=True,
    )
    return found


def _manifest_imope(
    stage: Path,
    original: Path,
    source_mapping: dict[str, tuple[str, int, int]],
    dedup_aliases: dict[str, dict[str, Any]],
    release_root: Path,
) -> list[dict[str, Any]]:
    """Rebuild *physical* files while preserving exact original dedup aliases.

    Original departmental/global totals include 187 manifest-only files.
    The compacted release must expose physical counts separately from the
    original logical totals; no duplicated Parquet bytes are materialized.
    """
    listing = json.loads((original / "manifest.json").read_text(encoding="utf-8"))
    if not isinstance(listing, list):
        raise ValueError("Unexpected IMoPE manifest layout")
    updated: list[dict[str, Any]] = []
    resolved_aliases: list[dict[str, Any]] = []
    for dept in listing:
        name = dept["department"]
        directory = stage / name
        if not directory.is_dir():
            raise ValueError(f"IMoPE department missing: {name}")
        old_physical_rows = 0
        old_missing_rows = 0
        old_files = 0
        for entry in dept["parquet"]:
            old_files += 1
            original_rel = (Path(name) / relative_path(entry["path"])).as_posix()
            if original_rel in dedup_aliases:
                old_missing_rows += int(entry["rows"])
            else:
                old_physical_rows += int(entry["rows"])
        if old_physical_rows + old_missing_rows != int(dept["rows"]):
            raise ValueError(f"IMoPE source manifest rows disagree: {name}")

        parts = sorted(directory.glob("*.parquet"))
        descriptions = [validate_parquet(p) for p in parts]
        physical_rows = sum(int(x["rows"]) for x in descriptions)
        if physical_rows != old_physical_rows:
            raise ValueError(
                f"IMoPE physical department rows changed: {name}: "
                f"{physical_rows} != {old_physical_rows}"
            )

        department_aliases = []
        for old, alias in sorted(dedup_aliases.items()):
            if Path(old).parent != Path(name):
                continue
            original_canon = alias["canonical"]
            if alias["scope"] == "imope":
                published, offset, rows = source_mapping.get(
                    original_canon,
                    (original_canon, 0, int(alias["original_metadata"]["rows"])),
                )
                target = stage / published
                published_scope = "imope"
            else:
                published = alias["canonical_release_path"]
                offset = 0
                rows = int(alias["original_metadata"]["rows"])
                target = release_root / published
                published_scope = "release"
            if rows != int(alias["original_metadata"]["rows"]):
                raise ValueError(f"IMoPE canonical source rows changed: {old}")
            if (not target.is_file()
                or target.is_symlink()
                or pq.read_metadata(target).num_rows < offset + rows):
                raise ValueError(
                    f"IMoPE canonical mapping cannot be resolved after "
                    f"compaction: {old} -> {published}"
                )
            record = {
                **alias,
                "published": published,
                "published_scope": published_scope,
                "first_row": offset,
                "rows": rows,
            }
            resolved_aliases.append(record)
            department_aliases.append(record)

        updated_entry = {
            **dept,
            "files": len(parts),
            "rows": physical_rows,
            "bytes": sum(x["bytes"] for x in descriptions),
            "geo_files": sum(x["geo"] for x in descriptions),
            "parquet": descriptions,
            "compacted": True,
            "original_manifest_files": old_files,
            "original_logical_rows": int(dept["rows"]),
            "aliased_rows": old_missing_rows,
            "deduplicated_aliases": department_aliases,
        }
        updated.append(updated_entry)
        replace_json(directory / "_SUCCESS.json", updated_entry)

    if len(resolved_aliases) != len(dedup_aliases):
        raise ValueError("Unresolved IMoPE departmental aliases")
    dataset = json.loads((original / "dataset.json").read_text(encoding="utf-8"))
    total_logical_rows = sum(int(x["original_logical_rows"]) for x in updated)
    total_physical_rows = sum(int(x["rows"]) for x in updated)
    if int(dataset["rows"]) != total_logical_rows:
        raise ValueError("IMoPE source dataset logical rows changed")
    if total_physical_rows + sum(int(x["rows"]) for x in resolved_aliases) != total_logical_rows:
        raise ValueError("IMoPE deduplicated alias row totals disagree")
    dataset.update({
        "files": sum(x["files"] for x in updated),
        "rows": total_physical_rows,
        "bytes": sum(x["bytes"] for x in updated),
        "geo_files": sum(x["geo_files"] for x in updated),
        "original_manifest_files": sum(x["original_manifest_files"] for x in updated),
        "original_logical_rows": total_logical_rows,
        "deduplicated_alias_files": len(resolved_aliases),
        "deduplicated_alias_rows": sum(int(x["rows"]) for x in resolved_aliases),
        "storage": "verified compacted Parquet with SHA-256 authenticated logical aliases",
    })
    replace_json(stage / "manifest.json", updated)
    replace_json(stage / "dataset.json", dataset)
    return resolved_aliases


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
        # Caller runs the SHA-256 dedup catalog audit separately so no
        # unverified alias is silently accepted.
        return
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
        _present, missing_empty = _datatourisme_csv_inventory(
            original, manifest,
        )
        if missing_empty:
            print(
                f"INFO: DataTourisme has {len(missing_empty)} absent "
                "zero-row CSV output(s) listed only in its manifest; "
                "publication will omit these physical references and "
                "record every omission in the new manifest",
                file=sys.stderr, flush=True,
            )


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


def _validate_imope_published_aliases(
    dataset: Path, release_root: Path,
) -> None:
    """Check every logical alias resolves after dataset directory swaps."""
    index = json.loads((dataset / "_COMPACTION_SOURCES.json").read_text())
    aliases = index.get("deduplicated_aliases", [])
    if not isinstance(aliases, list):
        raise ValueError("IMoPE published alias index missing")
    for entry in aliases:
        rel = relative_path(entry["published"])
        if entry["published_scope"] == "imope":
            dest = dataset / rel
        elif entry["published_scope"] == "release":
            if rel.parts[0] != "parquet":
                raise ValueError("Unsafe release-wide alias reference")
            dest = release_root / rel
        else:
            raise ValueError("Unknown IMoPE alias scope")
        if (not dest.is_file() or dest.is_symlink()
            or entry["first_row"] < 0 or entry["rows"] < 0
            or pq.read_metadata(dest).num_rows
               < entry["first_row"] + entry["rows"]):
            raise ValueError(
                f"IMoPE published alias unresolved: {entry['original']}"
            )


def _restore_directory_modes(stage: Path, original: Path) -> None:
    """Restore the snapshot's directory protections after stage construction.

    Staging is necessarily writable while building the new tree. Original
    converters often use mode 0555 for immutable dataset directories.
    Reapply all original directory modes just before publication.
    """
    directories = [original, *(
        p for p in original.rglob("*") if p.is_dir()
    )]
    for directory in directories:
        if directory.is_symlink():
            raise ValueError(f"Unsafe directory symlink: {directory}")
        target = stage / directory.relative_to(original)
        if not target.is_dir() or target.is_symlink():
            raise ValueError(f"Missing staging directory: {target}")
        os.chmod(target, stat.S_IMODE(directory.stat().st_mode))


def _build_stage(
    key: str, dataset: Path, stage: Path, source_root: Path,
    publish_root: Path, output_root: Path,
    selected: list[tuple[str, Any, str]], inventory: dict[str, int],
    audit_provenance: dict[str, Any],
    imope_aliases: dict[str, dict[str, Any]],
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
    imope_resolved_aliases: list[dict[str, Any]] = []
    if key == "datatourisme":
        _stage_nt_parts(stage, original, mapping)
        _manifest_datatourisme(stage, original, mapping)
    elif key == "imope":
        imope_resolved_aliases = _manifest_imope(
            stage, original, mapping, imope_aliases, source_root.parent,
        )
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
        "deduplicated_aliases": imope_resolved_aliases,
    }
    replace_json(stage / "_COMPACTION_SOURCES.json", index)
    totals = _validate_stage(
        stage, key, inventory["source_files"], inventory["source_rows"],
        inventory["candidate_files"], inventory["candidate_batches"],
        mapping,
    )
    summary = {
        "dataset": key, **totals, "verified_batches": len(selected),
        "deduplicated_aliases": len(imope_resolved_aliases),
        "deduplicated_alias_rows": sum(int(x["rows"]) for x in imope_resolved_aliases),
    }
    replace_json(stage / "_COMPACTION.json", summary)
    _restore_directory_modes(stage, dataset)
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
    duplicates_catalog: Path | None = None,
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
    nt_root = Path(TARGETS["datatourisme"]) / "ntriples"
    for key, selection, category in selected:
        if (key == "datatourisme"
            and Path(selection.directory).is_relative_to(nt_root)
            and category != "part-sequence"):
            raise ValueError(
                f"Non-contiguous NTriples part group cannot safely be "
                f"published: {selection.batch_id}"
            )
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
    # A plan built against the torrent alias must never be treated as an
    # independent release. Resolve canonical SHA256 objects in the physical
    # release root and require exact catalog records for every missing file.
    imope_aliases = _imope_deduplicated_aliases(
        paths["imope"], root,
        duplicates_catalog.expanduser().resolve()
        if duplicates_catalog is not None else None,
    )
    selected_files = {
        source.relative
        for _key, selection, _category in selected
        for source in selection.sources
    }
    for alias in imope_aliases.values():
        if (alias["scope"] == "release"
            and alias["canonical_release_path"].removeprefix("parquet/")
            in selected_files):
            raise ValueError(
                "IMoPE external canonical is itself scheduled for compaction; "
                f"manual remapping is required: {alias['original']} -> "
                f"{alias['canonical_release_path']}"
            )
    for key, directory in paths.items():
        if not directory.is_dir() or directory.is_symlink():
            raise ValueError(f"Missing or unsafe dataset: {directory}")
        stage = directory.with_name(directory.name + ".compaction-stage")
        backup = directory.with_name(directory.name + ".precompact-backup")
        if stage.exists() or stage.is_symlink():
            raise RuntimeError(f"Existing stage: {stage}; inspect before retry")
        if backup.exists() or backup.is_symlink():
            raise RuntimeError(f"Existing backup: {backup}; inspect before retry")
        validate_source_matches_plan(directory, key, inventory[key], grouped[key])
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
        "imope_deduplicated_aliases_verified": len(imope_aliases),
        "imope_deduplicated_alias_rows": sum(
            int(x["original_metadata"]["rows"]) for x in imope_aliases.values()
        ),
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
                grouped[key], inventory[key], expected, imope_aliases,
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
            if key == "imope":
                _validate_imope_published_aliases(
                    directory, publish_root.parent,
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
        # Stages belong exclusively to this run (their absence was checked
        # before creating any). Clean incomplete work so an ordinary failure
        # is retryable without hand-deleting large staged directories.
        for key, directory in paths.items():
            stage = directory.with_name(directory.name + ".compaction-stage")
            if stage.is_dir() and not stage.is_symlink():
                try:
                    prune_validated_backup(stage)
                except OSError as cleanup_error:
                    print(
                        f"INCOMPLETE stage needs inspection: {stage}: "
                        f"{cleanup_error}",
                        file=sys.stderr,
                    )
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
    p.add_argument(
        "--duplicates-catalog", type=Path,
        help="original SHA-256 release deduplication catalog (mandatory for IMoPE aliases)",
    )
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
            duplicates_catalog=args.duplicates_catalog,
        )
        return 0
    except (OSError, ValueError, RuntimeError, KeyError, TypeError) as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
