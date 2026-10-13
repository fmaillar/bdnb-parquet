"""Read-only independent IMoPE v2 GeoParquet and provenance audit.

Checks actual WKB geometry extents, all original Arrow values, existing
compaction lineage, aliases and the complete 2048 MiB staging inventory.
Only --attest writes a checksum inventory under stage/_meta.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import sys
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from multiprocessing import get_context
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import shapely

from .compact_run import atomic_report, compare_rows, relative_path, reject_overlapping_trees
from .imope_v2_pilot import merged_schema, read_pilot
from .imope_v2_schedule import (
    _check_output_schema, _exclusive_staging_lock, inspect_plan, plan_fingerprint,
)

ATTESTATION = "imope-v2-validation.json"


def sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def geometry_extent(path: Path, schema: pa.Schema, batch_rows: int) -> dict[str, Any]:
    """Validate *actual* WKB envelopes against the complete output geo bbox."""
    raw = (schema.metadata or {}).get(b"geo")
    if raw is None:
        return {}
    geo = json.loads(raw)
    if "bbox" in geo:
        raise ValueError(f"Root-level GeoParquet bbox needs special review: {path}")
    columns = geo.get("columns")
    if not isinstance(columns, dict) or not columns:
        raise ValueError(f"Invalid GeoParquet column descriptors: {path}")
    found = {}
    pf = pq.ParquetFile(path)
    try:
        for name, spec in sorted(columns.items()):
            if not isinstance(spec, dict) or spec.get("encoding") != "WKB":
                raise ValueError(f"Unsupported GeoParquet encoding: {path}: {name}")
            if name not in schema.names or not (
                pa.types.is_binary(schema.field(name).type)
                or pa.types.is_large_binary(schema.field(name).type)
            ):
                raise ValueError(f"Missing/bad WKB column: {path}: {name}")
            expected = spec.get("bbox")
            if not isinstance(expected, list) or len(expected) != 4:
                raise ValueError(f"Unsupported/missing WKB bbox: {path}: {name}")
            bounds: list[float] | None = None
            number = 0
            for batch in pf.iter_batches(
                batch_size=batch_rows, columns=[name], use_threads=True,
            ):
                encoded = [v for v in batch.column(0).to_pylist() if v is not None]
                if not encoded:
                    continue
                try:
                    geometries = shapely.from_wkb(encoded, on_invalid="raise")
                    rectangles = shapely.bounds(geometries)
                except Exception as exc:
                    raise ValueError(f"Invalid WKB geometry: {path}: {name}") from exc
                for rectangle in rectangles:
                    v = [float(x) for x in rectangle]
                    if all(math.isnan(x) for x in v):
                        continue
                    if not all(math.isfinite(x) for x in v):
                        raise ValueError(f"Non-finite WKB extent: {path}: {name}")
                    if bounds is None:
                        bounds = v
                    else:
                        bounds = [
                            min(bounds[0], v[0]), min(bounds[1], v[1]),
                            max(bounds[2], v[2]), max(bounds[3], v[3]),
                        ]
                    number += 1
            if bounds is None or not all(
                math.isclose(a, float(b), rel_tol=1e-11, abs_tol=1e-6)
                for a, b in zip(bounds, expected)
            ):
                raise ValueError(
                    f"GeoParquet WKB bbox mismatch: {path} {name}: "
                    f"computed={bounds}, stored={expected}"
                )
            found[name] = {"bbox": bounds, "nonempty_geometries": number}
    finally:
        pf.close()
    return found


def audit_batch(plan: Path, root: Path, stage: Path, batch_id: str,
                batch_rows: int) -> dict[str, Any]:
    batch, sources, paths = read_pilot(plan, root, batch_id=batch_id)
    output = stage / relative_path(batch["output"])
    if not output.resolve().is_relative_to(stage):
        raise ValueError(f"Unsafe staged output: {output}")
    sidecar = output.with_suffix(".json")
    if sidecar.is_symlink() or not sidecar.is_file():
        raise ValueError(f"Missing staged audit: {output}")
    saved = json.loads(sidecar.read_text(encoding="utf-8"))
    provenance = [
        {"original": r["original"], "department": r["department"],
         "first_row": int(r["first_row"]), "rows": int(r["rows"])}
        for r in sources
    ]
    required = {
        "batch_id": batch_id, "target_mib": 2048,
        "output": str(output), "source_bytes": batch["source_bytes"],
        "source_files": len(paths), "verified_rows": batch["rows"],
        "bbox_union": batch["bbox_union"], "source_provenance": provenance,
        "publication_performed": False, "source_backup_deleted": False,
        "validation": "full-ordered-Arrow-value-equality-and-metadata-compatibility",
    }
    if any(saved.get(k) != v for k, v in required.items()):
        raise ValueError(f"Stage audit disagrees with plan: {output}")
    if saved.get("output_bytes") != output.stat().st_size:
        raise ValueError(f"Staged file size changed: {output}")
    schema = merged_schema(paths, batch["bbox_union"])
    groups = _check_output_schema(output, schema, batch["rows"])
    if saved.get("output_row_groups") != groups:
        raise ValueError(f"Staged row groups changed: {output}")
    verified = compare_rows(tuple(paths), output, batch_rows, threads=2)
    if verified != batch["rows"]:
        raise ValueError(f"Full Arrow comparison failed: {output}")
    return {
        "batch_id": batch_id,
        "output": batch["output"],
        "sha256": sha256(output),
        "bytes": output.stat().st_size,
        "rows": verified,
        "sources": len(paths),
        "wkb_extents": geometry_extent(output, schema, batch_rows),
    }


def lineage(root: Path, plan: Path, stage: Path, proposed: dict[str, Any]) -> dict[str, int]:
    """Check physical coverage, first-publication lineage and 187 aliases."""
    source_index = root / "_COMPACTION_SOURCES.json"
    if source_index.is_symlink() or not source_index.is_file():
        raise ValueError("IMoPE source lineage index missing")
    index = json.loads(source_index.read_text(encoding="utf-8"))
    if index.get("dataset") != "imope":
        raise ValueError("Wrong source lineage dataset")
    aliases = index.get("deduplicated_aliases")
    history = index.get("sources")
    if not isinstance(aliases, list) or not isinstance(history, list):
        raise ValueError("Malformed prior IMoPE source index")

    unchanged = {
        r["path"] for r in json.loads((plan / "untouched.json").read_text())
    }
    present = {
        p.relative_to(root).as_posix()
        for p in root.rglob("*.parquet") if not p.is_symlink()
    }
    if any(p.is_symlink() for p in root.rglob("*.parquet")):
        raise ValueError("Symlink in IMoPE dataset")
    old_compacts = {p for p in present if Path(p).name.startswith("compact-")}
    if (present != set(proposed) | unchanged | old_compacts
        or set(proposed) & unchanged
        or set(proposed) & old_compacts):
        raise ValueError("IMoPE physical files not exhaustively accounted for")

    pending = json.loads(
        (plan / "pending_alias_rewrites.json").read_text(encoding="utf-8")
    )
    changes = {entry["original_alias"]: entry for entry in pending}
    if len(changes) != len(pending):
        raise ValueError("Duplicate alias migration")
    seen: set[str] = set()
    changed: set[str] = set()
    canonical_hashes: dict[Path, str] = {}
    for alias in aliases:
        original = alias["original"]
        if original in seen or alias.get("published_scope") != "imope":
            raise ValueError(f"Duplicated/external IMoPE alias: {original}")
        seen.add(original)
        old = relative_path(alias["published"])
        canonical = root / old
        if not canonical.is_file() or canonical.is_symlink():
            raise ValueError(f"Missing canonical for IMoPE alias {original}")
        first = alias["first_row"]
        rows = alias["rows"]
        if (type(first) is not int or type(rows) is not int
            or first < 0 or rows < 0
            or first + rows > pq.read_metadata(canonical).num_rows):
            raise ValueError(f"Invalid alias range: {original}")
        if first == 0 and rows == pq.read_metadata(canonical).num_rows:
            if canonical not in canonical_hashes:
                canonical_hashes[canonical] = sha256(canonical)
            if canonical_hashes[canonical] != alias.get("sha256"):
                raise ValueError(f"Invalid legacy canonical SHA256: {original}")
        if old.as_posix() in proposed:
            target = proposed[old.as_posix()]
            change = changes.get(original)
            if not change or (
                change["previous_canonical"] != old.as_posix()
                or change["previous_first_row"] != first
                or change["rows"] != rows
                or change["proposed_output"] != target["output"]
                or change["proposed_first_row"] != target["first_row"] + first
            ):
                raise ValueError(f"Unreconciled IMoPE alias: {original}")
            changed.add(original)
        elif original in changes:
            raise ValueError(f"Spurious alias rewrite: {original}")
    if changed != set(changes):
        raise ValueError("Unmatched IMoPE alias migration records")

    seen_sources = set()
    for record in history:
        old = record["original"]
        if old in seen_sources:
            raise ValueError(f"Repeated first-publication source: {old}")
        seen_sources.add(old)
        target = root / relative_path(record["published"])
        if not target.is_file() or target.is_symlink():
            raise ValueError(f"Missing first-publication target: {old}")
        if (record["first_row"] < 0 or
            record["first_row"] + record["rows"] >
            pq.read_metadata(target).num_rows):
            raise ValueError(f"Bad first-publication row range: {old}")
    return {
        "original_physical_files": len(present),
        "first_publication_sources": len(history),
        "existing_aliases": len(aliases),
        "aliases_requiring_retarget": len(changed),
        "unchanged_originals": len(unchanged),
        "first_publication_compacts": len(old_compacts),
    }


def validate(plan: Path, root: Path, stage: Path, *,
             workers: int = 4, batch_rows: int = 16384,
             attest: bool = False) -> dict[str, Any]:
    if not 1 <= workers <= 4 or batch_rows < 1:
        raise ValueError("Invalid audit resources")
    plan, root, stage = (x.expanduser().resolve() for x in (plan, root, stage))
    reject_overlapping_trees(root, stage)
    reject_overlapping_trees(root, plan)
    reject_overlapping_trees(plan, stage)
    groups, summary = inspect_plan(plan, root)
    expected_lock = {
        "format": "imope-v2-stage-lock-v1", "root": str(root),
        "plan_sha256": plan_fingerprint(plan), "target_mib": 2048,
    }
    meta = stage / "_meta"
    if meta.is_symlink() or not meta.is_dir():
        raise ValueError("No safe staging metadata directory")
    with _exclusive_staging_lock(meta):
        lock = meta / "plan-lock.json"
        if not lock.is_file() or lock.is_symlink() or (
            json.loads(lock.read_text()) != expected_lock
        ):
            raise ValueError("Staging plan SHA256 lock mismatch")
        completed = json.loads((meta / "last-run.json").read_text())
        if (completed.get("successful_batches") != len(groups)
            or completed.get("failed_batches") != 0
            or completed.get("verified_rows") != summary["candidate_rows"]):
            raise ValueError("All 2048-MiB batches must be verified first")

        with (plan / "sources.tsv").open(encoding="utf-8", newline="") as f:
            sources = list(csv.DictReader(f, delimiter="\t"))
        mapped = {
            src["original"]: {
                "output": src["output"],
                "first_row": int(src["first_row"]),
                "rows": int(src["rows"]),
            } for src in sources
        }
        if len(mapped) != len(sources):
            raise ValueError("Duplicate staged IMoPE source")
        history = lineage(root, plan, stage, mapped)

        outputs: list[dict[str, Any]] = []
        if workers == 1:
            for batch in groups:
                outputs.append(audit_batch(
                    plan, root, stage, batch["batch_id"], batch_rows,
                ))
                print(f"AUDIT [{len(outputs)}/{len(groups)}] {batch['batch_id']}",
                      file=sys.stderr, flush=True)
        else:
            with ProcessPoolExecutor(
                max_workers=workers, mp_context=get_context("spawn"),
            ) as pool:
                iterator = iter(groups)
                pending = {}
                error = None

                def queue_one() -> bool:
                    try:
                        batch = next(iterator)
                    except StopIteration:
                        return False
                    pending[pool.submit(
                        audit_batch, plan, root, stage, batch["batch_id"],
                        batch_rows,
                    )] = batch
                    return True

                for _ in range(min(workers, len(groups))):
                    queue_one()
                while pending:
                    done, _ = wait(pending, return_when=FIRST_COMPLETED)
                    for future in done:
                        batch = pending.pop(future)
                        try:
                            outputs.append(future.result())
                            print(
                                f"AUDIT [{len(outputs)}/{len(groups)}] "
                                f"{batch['batch_id']}",
                                file=sys.stderr, flush=True,
                            )
                        except Exception as exc:
                            error = error or exc
                            print(f"AUDIT FAILED {batch['batch_id']}: {exc}",
                                  file=sys.stderr, flush=True)
                    if error is None:
                        while len(pending) < workers and queue_one():
                            pass
                if error:
                    raise RuntimeError(f"Independent audit failed: {error}") from error

        outputs.sort(key=lambda x: x["batch_id"])
        expected_outputs = {item["output"] for item in outputs}
        actual_outputs = {
            item.relative_to(stage).as_posix()
            for item in stage.rglob("*.parquet")
        }
        if actual_outputs != expected_outputs:
            raise ValueError(
                "Unexpected/missing physical staged Parquet: "
                f"expected={len(expected_outputs)} actual={len(actual_outputs)} "
                f"extra={sorted(actual_outputs - expected_outputs)[:5]} "
                f"missing={sorted(expected_outputs - actual_outputs)[:5]}"
            )
        if (len(outputs) != len(groups)
            or sum(x["rows"] for x in outputs) != summary["candidate_rows"]
            or sum(x["sources"] for x in outputs) != summary["candidate_source_files"]):
            raise ValueError("Final audited source totals differ")
        report = {
            "format": "imope-v2-validation-v1",
            "mode": "independent-validation",
            "target_mib": 2048,
            "source_root": str(root),
            "staging_root": str(stage),
            "plan_sha256": expected_lock["plan_sha256"],
            "validated_batches": len(outputs),
            "validated_sources": sum(x["sources"] for x in outputs),
            "validated_rows": sum(x["rows"] for x in outputs),
            "validated_bytes": sum(x["bytes"] for x in outputs),
            "geo_batches": sum(bool(x["wkb_extents"]) for x in outputs),
            "lineage": history,
            "outputs": outputs,
            "publication_performed": False,
            "backups_removed": False,
        }
        if attest:
            destination = meta / ATTESTATION
            if destination.exists() or destination.is_symlink():
                raise FileExistsError(f"Existing validation attestation: {destination}")
            atomic_report(destination, report)
        preview = {k: v for k, v in report.items() if k != "outputs"}
        if attest:
            preview["attestation"] = str(meta / ATTESTATION)
        print(json.dumps(preview, indent=2, ensure_ascii=False), flush=True)
        return report


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Independently audit IMoPE v2 output and WKB geometry")
    p.add_argument("--plan-dir", type=Path, required=True)
    p.add_argument("--source-root", type=Path, required=True)
    p.add_argument("--stage-root", type=Path, required=True)
    p.add_argument("--workers", type=int, choices=(1, 2, 3, 4), default=4)
    p.add_argument("--batch-rows", type=int, default=16384)
    p.add_argument("--attest", action="store_true",
                   help="write validated output hashes only on full success")
    args = p.parse_args(argv)
    try:
        validate(args.plan_dir, args.source_root, args.stage_root,
                 workers=args.workers, batch_rows=args.batch_rows,
                 attest=args.attest)
        return 0
    except (ValueError, RuntimeError, OSError, KeyError, TypeError) as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}",
              file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
