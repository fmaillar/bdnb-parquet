"""Independent, resumable IMoPE v2 2048 MiB batch scheduler.

Four spawned workers, each with an isolated PyArrow thread pool. The only
changes are inside an explicitly separate output root. No dataset publication,
no manifest mutation, no alias rewrite, no backup removal, and no torrent
change. Existing outputs are *fully* compared with current originals on resume.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import multiprocessing
import os
import shutil
import sys
import time
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .compact_run import (
    arrow_cpu_threads, atomic_report, compare_rows, relative_path,
    reject_overlapping_trees,
)
from .compact_schedule import append_event
from .imope_v2_pilot import (
    PILOT_TARGET_MIB, merged_schema, read_pilot, write_stream,
)

GIB = 2**30
PLAN_FILES = ("summary.json", "batches.json", "sources.tsv",
              "untouched.json", "pending_alias_rewrites.json")


def plan_fingerprint(plan_dir: Path) -> dict[str, str]:
    """Lock every external plan artifact, not just the 2048 MiB summary."""
    fingerprints: dict[str, str] = {}
    for filename in PLAN_FILES:
        file = plan_dir / filename
        if not file.is_file() or file.is_symlink():
            raise ValueError(f"Unsafe or missing external plan file: {file}")
        fingerprints[filename] = hashlib.sha256(file.read_bytes()).hexdigest()
    return fingerprints


def inspect_plan(
    plan_dir: Path, source_root: Path, *,
    check_sources: bool = True,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Preflight every planned batch and verify complete source coverage."""
    plan_dir = plan_dir.expanduser().resolve()
    root = source_root.expanduser().resolve()
    reject_overlapping_trees(root, plan_dir)
    summary = json.loads((plan_dir / "summary.json").read_text(encoding="utf-8"))
    if (summary.get("format") != "imope-v2-physical-planning-v1"
        or summary.get("target_mib") != PILOT_TARGET_MIB
        or summary.get("mode") != "read-only"
        or Path(summary["root"]).resolve() != root):
        raise ValueError("Not a 2048 MiB IMoPE v2 plan for this exact root")
    batches = json.loads((plan_dir / "batches.json").read_text(encoding="utf-8"))
    if not isinstance(batches, list) or not batches:
        raise ValueError("IMoPE v2 schedule needs a nonempty batch plan")
    if len(batches) != summary["proposed_batches"]:
        raise ValueError("Planned batch count differs from summary")

    ids: set[str] = set()
    outputs: set[str] = set()
    for batch in batches:
        name = batch["batch_id"]
        if name in ids or batch["output"] in outputs:
            raise ValueError("Duplicate batch ID or output in IMoPE v2 plan")
        ids.add(name)
        outputs.add(batch["output"])
        if batch.get("metadata_review"):
            raise ValueError(f"Unreviewed GeoParquet metadata: {name}")

    if check_sources:
        paths: set[Path] = set()
        total_rows = total_bytes = 0
        files = 0
        for index, batch in enumerate(batches, 1):
            verified, entries, sources = read_pilot(
                plan_dir, root, batch_id=batch["batch_id"],
            )
            if verified != batch:
                raise ValueError(f"Plan batch diverged: {batch['batch_id']}")
            for path in sources:
                if path in paths:
                    raise ValueError(f"Duplicate source across batches: {path}")
                paths.add(path)
            total_rows += int(batch["rows"])
            total_bytes += int(batch["source_bytes"])
            files += len(entries)
            if index % 20 == 0 or index == len(batches):
                print(
                    f"PREFLIGHT [{index}/{len(batches)}] batch inventories checked",
                    file=sys.stderr, flush=True,
                )
        if (total_rows != summary["candidate_rows"]
            or total_bytes != summary["candidate_bytes"]
            or files != summary["candidate_source_files"]):
            raise ValueError("Planned source totals diverged from summary")
    return batches, summary


def _check_output_schema(
    output: Path, expected_schema: pa.Schema, expected_rows: int,
) -> int:
    if not output.is_file() or output.is_symlink():
        raise ValueError(f"Missing or unsafe Parquet output: {output}")
    parquet = pq.ParquetFile(output)
    try:
        if not parquet.schema_arrow.equals(expected_schema, check_metadata=True):
            raise ValueError(f"Output schema or GeoParquet bbox mismatch: {output}")
        if parquet.metadata.num_rows != expected_rows:
            raise ValueError(f"Output row count mismatch: {output}")
        return parquet.metadata.num_row_groups
    finally:
        parquet.close()


def _run_one(
    plan_dir: Path, source_root: Path, output_root: Path, batch_id: str,
    threads: int, batch_rows: int, row_group_rows: int,
    max_buffer_mib: int, zstd_level: int,
) -> dict[str, Any]:
    """One worker exclusively owns one compacted output and its sidecar."""
    started = time.perf_counter()
    selected, entries, paths = read_pilot(
        plan_dir, source_root, batch_id=batch_id,
    )
    if selected.get("metadata_review"):
        raise ValueError(f"Unreviewed metadata: {batch_id}")
    destination = output_root / relative_path(selected["output"])
    if not destination.resolve().is_relative_to(output_root):
        raise ValueError(f"Output escapes staging root: {destination}")
    report_path = destination.with_suffix(".json")
    temporary = destination.with_suffix(".partial")
    schema = merged_schema(paths, selected["bbox_union"])
    provenance = [
        {
            "original": row["original"],
            "department": row["department"],
            "first_row": int(row["first_row"]),
            "rows": int(row["rows"]),
        }
        for row in entries
    ]
    if temporary.exists() or temporary.is_symlink():
        raise RuntimeError(
            f"Existing incomplete batch: {temporary}; inspect before restart"
        )
    if destination.is_symlink() or report_path.is_symlink():
        raise ValueError(f"Unsafe output or audit symlink: {destination}")

    write_seconds = 0.0
    with arrow_cpu_threads(threads):
        if destination.exists():
            if not report_path.is_file():
                raise RuntimeError(
                    f"Existing output without validated audit: {destination}; "
                    "manual inspection required before retry"
                )
            previous = json.loads(report_path.read_text(encoding="utf-8"))
            required = {
                "batch_id": batch_id,
                "target_mib": PILOT_TARGET_MIB,
                "output": str(destination),
                "source_files": len(paths),
                "source_bytes": selected["source_bytes"],
                "verified_rows": selected["rows"],
                "bbox_union": selected["bbox_union"],
                "source_provenance": provenance,
                "validation": "full-ordered-Arrow-value-equality-and-metadata-compatibility",
                "publication_performed": False,
                "source_backup_deleted": False,
            }
            for key, value in required.items():
                if previous.get(key) != value:
                    raise ValueError(
                        f"Existing output audit differs at {key}: {destination}"
                    )
            if previous.get("output_bytes") != destination.stat().st_size:
                raise ValueError(f"Existing output size differs: {destination}")
            row_groups = _check_output_schema(
                destination, schema, selected["rows"],
            )
            if previous.get("output_row_groups") != row_groups:
                raise ValueError(f"Existing output row groups differ: {destination}")
            began_verifying = time.perf_counter()
            verified = compare_rows(tuple(paths), destination, batch_rows, threads)
            verify_seconds = time.perf_counter() - began_verifying
            if verified != selected["rows"]:
                raise ValueError(f"Full Arrow verification failed: {destination}")
            mode = "verified-existing-output"
        else:
            if report_path.exists():
                raise RuntimeError(
                    f"Existing batch audit without output: {report_path}"
                )
            destination.parent.mkdir(parents=True, exist_ok=True)
            if not destination.parent.resolve().is_relative_to(output_root):
                raise ValueError(f"Unsafe staged output directory: {destination.parent}")
            try:
                began_writing = time.perf_counter()
                count = write_stream(
                    paths, temporary, schema, batch_rows=batch_rows,
                    row_group_rows=row_group_rows,
                    max_buffer_mib=max_buffer_mib, zstd_level=zstd_level,
                    threads=threads,
                )
                write_seconds = time.perf_counter() - began_writing
                if count != selected["rows"]:
                    raise ValueError(f"Compaction row count differs: {batch_id}")
                row_groups = _check_output_schema(
                    temporary, schema, selected["rows"],
                )
                began_verifying = time.perf_counter()
                verified = compare_rows(
                    tuple(paths), temporary, batch_rows, threads,
                )
                verify_seconds = time.perf_counter() - began_verifying
                if verified != selected["rows"]:
                    raise ValueError(f"Full Arrow verification failed: {batch_id}")
                # Recheck source footers, normalized schema and bbox
                # before claiming the newly written output.
                read_pilot(plan_dir, source_root, batch_id=batch_id)
                os.replace(temporary, destination)
            finally:
                # Only this worker's newly created partial, never source data.
                temporary.unlink(missing_ok=True)
            mode = "new-verified-output"

    result = {
        "batch_id": batch_id,
        "target_mib": PILOT_TARGET_MIB,
        "mode": mode,
        "output": str(destination),
        "source_files": len(paths),
        "source_bytes": selected["source_bytes"],
        "source_provenance": provenance,
        "bbox_union": selected["bbox_union"],
        "metadata_review": [],
        "output_bytes": destination.stat().st_size,
        "output_row_groups": row_groups,
        "verified_rows": verified,
        "write_seconds": round(write_seconds, 3),
        "verify_seconds": round(verify_seconds, 3),
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "validation": "full-ordered-Arrow-value-equality-and-metadata-compatibility",
        "geo_bbox_status": "metadata-union-only; geometry-level audit pending",
        "publication_performed": False,
        "source_backup_deleted": False,
    }
    if mode == "new-verified-output":
        # Reports are committed only *after* complete validation. An
        # interruption between output rename and audit write fails closed.
        atomic_report(report_path, result)
    return result


def _budget(
    output_root: Path, batches: list[dict[str, Any]],
    workers: int, min_free_gib: int,
) -> dict[str, int]:
    if min_free_gib < 0:
        raise ValueError("--min-free-gib must be nonnegative")
    largest = sorted(
        (int(b["source_bytes"]) for b in batches), reverse=True,
    )[:workers]
    usage = shutil.disk_usage(output_root)
    # 2x largest in-flight source bytes as a conservative write reserve
    # for expanded Parquet output plus the requested free-space floor.
    required = 2 * sum(largest) + min_free_gib * GIB
    if usage.free < required:
        raise RuntimeError(
            f"Insufficient staging disk space: free={usage.free / GIB:.2f} GiB, "
            f"required={required / GIB:.2f} GiB "
            "(2x biggest in-flight sources plus reserve)"
        )
    return {
        "free_bytes": usage.free,
        "estimated_inflight_headroom": 2 * sum(largest),
        "minimum_free_bytes": min_free_gib * GIB,
    }


def schedule(
    plan_dir: Path, source_root: Path, output_root: Path, *,
    workers: int = 4, threads: int = 2,
    batch_rows: int = 16384, row_group_rows: int = 100000,
    max_buffer_mib: int = 128, zstd_level: int = 3,
    min_free_gib: int = 20, execute: bool = False,
) -> dict[str, Any]:
    if not 1 <= workers <= 4 or threads < 1:
        raise ValueError("Scheduler requires 1-4 workers and positive Arrow threads")
    if (min(batch_rows, row_group_rows, max_buffer_mib) < 1
        or not 1 <= zstd_level <= 19 or min_free_gib < 0):
        raise ValueError("Invalid resource or compression limit")
    plan = plan_dir.expanduser().resolve()
    root = source_root.expanduser().resolve()
    output = output_root.expanduser().resolve()
    reject_overlapping_trees(root, output)
    reject_overlapping_trees(plan, output)
    if output.is_symlink() or output == root or output == plan:
        raise ValueError("Unsafe schedule output root")
    # A true preview performs all expensive source checks but never creates
    # or modifies directories/files, including audit journals.
    groups, summary = inspect_plan(plan, root, check_sources=True)
    fingerprint = plan_fingerprint(plan)
    preview = {
        "mode": "execute-staging-only" if execute else "read-only",
        "target_mib": PILOT_TARGET_MIB,
        "batches": len(groups),
        "source_files": summary["candidate_source_files"],
        "source_rows": summary["candidate_rows"],
        "source_bytes": summary["candidate_bytes"],
        "projected_physical_files": summary["projected_physical_files"],
        "workers": workers, "threads_per_worker": threads,
        "output_root": str(output),
        "plan_sha256": fingerprint,
        "source_files_untouched": True,
        "publication_performed": False,
        "source_backup_deleted": False,
    }
    print(json.dumps(preview, indent=2, ensure_ascii=False), flush=True)
    if not execute:
        return preview

    lock = {"format": "imope-v2-stage-lock-v1", "root": str(root),
            "plan_sha256": fingerprint, "target_mib": PILOT_TARGET_MIB}
    is_resume = output.exists() or output.is_symlink()
    if is_resume:
        if not output.is_dir() or output.is_symlink():
            raise ValueError("Existing staging root is not a safe directory")
        metadata = output / "_meta"
        lock_path = metadata / "plan-lock.json"
        if metadata.is_symlink() or lock_path.is_symlink() or not lock_path.is_file():
            raise ValueError(
                "Existing output cannot be resumed without original plan lock"
            )
        if json.loads(lock_path.read_text(encoding="utf-8")) != lock:
            raise ValueError("Existing output belongs to a different plan")
    else:
        if not output.parent.is_dir() or output.parent.is_symlink():
            raise ValueError(f"Unsafe or absent output parent: {output.parent}")
        output.mkdir(mode=0o700)
        metadata = output / "_meta"
        metadata.mkdir()
        atomic_report(metadata / "plan-lock.json", lock)
    budget = _budget(output, groups, workers, min_free_gib)
    journal = metadata / "imope-v2-compaction-journal.jsonl"
    append_event(journal, {
        "event": "start", "batch_count": len(groups), "workers": workers,
        "threads": threads, "resume": is_resume, "budget": budget,
    })
    started = time.perf_counter()
    iterator = iter(groups)
    completed = 0
    new_files = 0
    reused = 0
    verified_rows = verified_source_bytes = output_bytes = 0
    failures: list[dict[str, str]] = []
    stopped = False

    def account(batch: dict[str, Any], result: dict[str, Any]) -> None:
        nonlocal completed, new_files, reused, verified_rows
        nonlocal verified_source_bytes, output_bytes
        completed += 1
        if result["mode"] == "verified-existing-output":
            reused += 1
        else:
            new_files += 1
        verified_rows += result["verified_rows"]
        verified_source_bytes += result["source_bytes"]
        output_bytes += result["output_bytes"]
        print(
            f"DONE [{completed}/{len(groups)}] {batch['table']}: "
            f"{batch['sources']} -> 1; "
            f"{result['mode']} ({result['elapsed_seconds']:.2f} s)",
            file=sys.stderr, flush=True,
        )
        append_event(journal, {
            "event": "success", "batch_id": batch["batch_id"],
            "mode": result["mode"], "output": result["output"],
            "verified_rows": result["verified_rows"],
        })

    def fail(batch: dict[str, Any], exc: BaseException) -> None:
        nonlocal stopped
        stopped = True
        message = f"{type(exc).__name__}: {exc}"
        failures.append({"batch_id": batch["batch_id"], "error": message})
        print(f"FAILED {batch['batch_id']}: {message}",
              file=sys.stderr, flush=True)
        append_event(journal, {
            "event": "failed", "batch_id": batch["batch_id"],
            "error": message,
        })

    def start_batch(batch: dict[str, Any]) -> tuple:
        # Recheck free space before starting another worker. Conservative
        # headroom is based on every in-flight batch.
        return (
            plan, root, output, batch["batch_id"], threads,
            batch_rows, row_group_rows, max_buffer_mib, zstd_level,
        )

    def run_serial() -> None:
        for batch in groups:
            try:
                _budget(output, [batch], 1, min_free_gib)
                append_event(journal, {
                    "event": "batch-start", "batch_id": batch["batch_id"],
                })
                result = _run_one(*start_batch(batch))
                account(batch, result)
            except Exception as exc:
                fail(batch, exc)
                break

    def run_parallel() -> None:
        nonlocal stopped
        with ProcessPoolExecutor(
            max_workers=workers,
            mp_context=multiprocessing.get_context("spawn"),
        ) as pool:
            inflight: dict[Any, dict[str, Any]] = {}

            def enqueue() -> bool:
                nonlocal stopped
                try:
                    batch = next(iterator)
                except StopIteration:
                    return False
                try:
                    _budget(
                        output, [*inflight.values(), batch],
                        workers, min_free_gib,
                    )
                    append_event(journal, {
                        "event": "batch-start", "batch_id": batch["batch_id"],
                    })
                    inflight[pool.submit(_run_one, *start_batch(batch))] = batch
                    return True
                except Exception as exc:
                    fail(batch, exc)
                    return False

            for _ in range(min(workers, len(groups))):
                if stopped or not enqueue():
                    break
            while inflight:
                done, _ = wait(inflight, return_when=FIRST_COMPLETED)
                for future in done:
                    batch = inflight.pop(future)
                    try:
                        account(batch, future.result())
                    except Exception as exc:
                        fail(batch, exc)
                if not stopped:
                    while len(inflight) < workers and enqueue():
                        pass

    if workers == 1:
        run_serial()
    else:
        run_parallel()

    result = {
        "mode": "staging-only",
        "target_mib": PILOT_TARGET_MIB,
        "selected_batches": len(groups),
        "successful_batches": completed,
        "failed_batches": len(failures),
        "new_outputs": new_files,
        "existing_outputs_reverified": reused,
        "verified_rows": verified_rows,
        "verified_source_bytes": verified_source_bytes,
        "output_bytes": output_bytes,
        "workers": workers, "threads_per_worker": threads,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "failures": failures,
        "journal": str(journal),
        "publication_performed": False,
        "source_backup_deleted": False,
    }
    append_event(journal, {"event": "run-end", "report": result})
    atomic_report(metadata / "last-run.json", result)
    print(json.dumps(result, indent=2, ensure_ascii=False), flush=True)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Stage all approved 2048 MiB IMoPE v2 batches separately",
    )
    parser.add_argument("--plan-dir", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--workers", type=int, choices=(1, 2, 3, 4), default=4)
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--batch-rows", type=int, default=16384)
    parser.add_argument("--row-group-rows", type=int, default=100000)
    parser.add_argument("--max-buffer-mib", type=int, default=128)
    parser.add_argument("--zstd-level", type=int, default=3)
    parser.add_argument("--min-free-gib", type=int, default=20)
    parser.add_argument("--execute", action="store_true",
                        help="opt in to writing only --output-root")
    args = parser.parse_args(argv)
    try:
        result = schedule(
            args.plan_dir, args.source_root, args.output_root,
            workers=args.workers, threads=args.threads,
            batch_rows=args.batch_rows, row_group_rows=args.row_group_rows,
            max_buffer_mib=args.max_buffer_mib,
            zstd_level=args.zstd_level,
            min_free_gib=args.min_free_gib, execute=args.execute,
        )
        return 1 if result.get("failed_batches", 0) else 0
    except (OSError, RuntimeError, ValueError, KeyError, TypeError) as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}",
              file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
