"""Conservative, resumable scheduler for reviewed BDNB Parquet part batches.

Dry-run by default. Explicit selection is mandatory. Each worker owns a
separate process to prevent contention over Arrow's process-global CPU pool.
"""

from __future__ import annotations

import argparse
import csv
import json
import multiprocessing
import os
import shutil
import sys
import time
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .compact_inventory import (
    PART_PATTERN,
    TABLE_PATTERN,
    checked_tables_root,
    read_approved_tables,
    table_inventory,
    write_inventory,
)
from .compact_run import (
    Selection,
    execute,
    read_selection,
    reject_overlapping_trees,
    relative_directory_path,
)

MIB = 2**20
GIB = 2**30
DEFAULT_THREADS = 4


def load_candidates(
    plan_dir: Path,
    source_root: Path,
    tables_root: str,
    tables: set[str],
    *,
    all_tables: bool,
    limit: int,
) -> list[Selection]:
    """Select only consecutive numbered parts of explicitly allowed tables."""
    if limit < 1:
        raise ValueError("--max-batches must be >= 1")
    if not tables and not all_tables:
        raise ValueError("Specify --table at least once or --all-tables")
    if all_tables and tables:
        raise ValueError("Use either --table or --all-tables, not both")
    if any(not TABLE_PATTERN.fullmatch(name) for name in tables):
        raise ValueError("Invalid table name")
    base = checked_tables_root(tables_root)

    with (plan_dir / "batches.tsv").open(
        newline="", encoding="utf-8"
    ) as stream:
        candidates = [
            row for row in csv.DictReader(stream, delimiter="\t")
            if row["status"] == "review-required"
            and Path(row["directory"]).parent == base
            and (all_tables or Path(row["directory"]).name in tables)
        ]

    selections: list[Selection] = []
    for row in sorted(candidates, key=lambda r: (r["directory"], r["batch_id"])):
        # The runner rechecks the original plan and actual source metadata.
        chosen = read_selection(plan_dir, source_root, row["batch_id"], None)
        names = [Path(src.relative).name for src in chosen.sources]
        numbers = [
            int(match.group(1))
            for name in names
            if (match := PART_PATTERN.fullmatch(name))
        ]
        if len(numbers) != len(names):
            raise ValueError(
                f"Refusing non-part source names in batch {chosen.batch_id}"
            )
        if numbers != list(range(numbers[0], numbers[0] + len(numbers))):
            raise ValueError(
                f"Refusing non-consecutive part numbers in {chosen.batch_id}"
            )
        selections.append(chosen)
        if len(selections) >= limit:
            break
    if not selections:
        raise ValueError("No review-required batches match the selected BDNB tables")
    return selections


def nearest_existing(path: Path) -> Path:
    while not path.exists():
        if path == path.parent:
            raise FileNotFoundError(path)
        path = path.parent
    return path


def check_disk_budget(
    output_root: Path,
    selections: list[Selection],
    workers: int,
    min_free_gib: int,
) -> dict[str, int]:
    if min_free_gib < 0:
        raise ValueError("--min-free-gib must be nonnegative")
    usage = shutil.disk_usage(nearest_existing(output_root))
    # Upper-bound concurrent staged output headroom: compressed sources can
    # expand on rewrite; allow 2x for each of the largest in-flight batches.
    peak_sources = sorted(
        (sum(item.bytes for item in sel.sources) for sel in selections),
        reverse=True,
    )[:workers]
    estimated_peak = 2 * sum(peak_sources)
    required = min_free_gib * GIB + estimated_peak
    if usage.free < required:
        raise RuntimeError(
            f"Insufficient free disk space: {usage.free/GIB:.2f} GiB; "
            f"requires >= {required/GIB:.2f} GiB including staging reserve"
        )
    return {
        "free_bytes": usage.free,
        "reserve_bytes": min_free_gib * GIB,
        "staging_budget_bytes": estimated_peak,
    }


def append_event(path: Path, event: dict[str, Any]) -> None:
    """Append one durable event in the scheduler process (not workers)."""
    if path.is_symlink():
        raise ValueError("Journal cannot be a symbolic link")
    payload = (
        json.dumps(
            {"timestamp": datetime.now(timezone.utc).isoformat(), **event},
            ensure_ascii=False,
            sort_keys=True,
        ) + "\n"
    ).encode("utf-8")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        view = memoryview(payload)
        while view:
            written = os.write(fd, view)
            if written == 0:
                raise OSError("Journal write returned zero bytes")
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)


def run_one(
    selected: Selection,
    output_root: Path,
    threads: int,
    batch_rows: int,
    row_group_rows: int,
    max_buffer_mib: int,
    zstd_level: int,
) -> dict[str, Any]:
    return execute(
        selected,
        output_root,
        batch_rows=batch_rows,
        zstd_level=zstd_level,
        row_group_rows=row_group_rows,
        max_buffer_mib=max_buffer_mib,
        threads=threads,
    )


def run_schedule(
    selections: list[Selection],
    output_root: Path,
    *,
    workers: int,
    threads: int,
    batch_rows: int,
    row_group_rows: int,
    max_buffer_mib: int,
    zstd_level: int,
    min_free_gib: int,
) -> dict[str, Any]:
    """Execute independently verified batches; journal before and after each."""
    if not 1 <= workers <= 4:
        raise ValueError("--workers must be in [1, 4]")
    if threads < 1:
        raise ValueError("--threads must be positive")
    budget = check_disk_budget(
        output_root, selections, workers, min_free_gib
    )
    output_root.mkdir(parents=True, exist_ok=True)
    metadata = output_root / "_meta"
    if metadata.is_symlink():
        raise ValueError("Metadata directory cannot be a symbolic link")
    metadata.mkdir(exist_ok=True)
    journal = metadata / "compaction-journal.jsonl"
    started = time.perf_counter()
    success = 0
    failures: list[dict[str, str]] = []
    files_removed = 0
    source_bytes = output_bytes = rows = 0

    append_event(journal, {
        "event": "run-start", "selected": len(selections),
        "workers": workers, "threads": threads,
        "disk": budget,
    })

    def record_success(sel: Selection, result: dict[str, Any]) -> None:
        nonlocal success, files_removed, source_bytes, output_bytes, rows
        success += 1
        files_removed += len(sel.sources) - 1
        source_bytes += int(result["source_bytes"])
        output_bytes += int(result["output_bytes"])
        rows += int(result["verified_rows"])
        print(
            f"DONE [{success}/{len(selections)}] {sel.directory}: "
            f"{len(sel.sources)} -> 1 files; "
            f"{result['total_seconds']:.2f} s",
            file=sys.stderr,
            flush=True,
        )
        append_event(journal, {
            "event": "success", "batch_id": sel.batch_id,
            "output": result["output"],
            "mode": result["execution_mode"],
            "verified_rows": result["verified_rows"],
            "source_bytes": result["source_bytes"],
            "output_bytes": result["output_bytes"],
            "total_seconds": result["total_seconds"],
        })

    def record_error(sel: Selection, exc: BaseException) -> None:
        message = f"{type(exc).__name__}: {exc}"
        failures.append({"batch_id": sel.batch_id, "error": message})
        print(f"FAILED {sel.batch_id}: {message}", file=sys.stderr, flush=True)
        append_event(journal, {
            "event": "failed", "batch_id": sel.batch_id, "error": message
        })

    if workers == 1:
        for selected in selections:
            try:
                # Recheck available space before every newly staged output.
                check_disk_budget(output_root, [selected], 1, min_free_gib)
                append_event(journal, {
                    "event": "start", "batch_id": selected.batch_id
                })
                result = run_one(
                    selected, output_root, threads, batch_rows,
                    row_group_rows, max_buffer_mib, zstd_level,
                )
                record_success(selected, result)
            except Exception as exc:
                record_error(selected, exc)
                break
    else:
        # Spawn isolates Arrow's process-global thread pool per worker.
        # At most four bounded, independent jobs can be in-flight.
        with ProcessPoolExecutor(
            max_workers=workers,
            mp_context=multiprocessing.get_context("spawn"),
        ) as pool:
            iterator = iter(selections)
            inflight: dict[Any, Selection] = {}
            stop = False

            def enqueue() -> bool:
                try:
                    selected = next(iterator)
                except StopIteration:
                    return False
                check_disk_budget(
                    output_root,
                    [*inflight.values(), selected],
                    workers,
                    min_free_gib,
                )
                append_event(journal, {
                    "event": "start", "batch_id": selected.batch_id
                })
                future = pool.submit(
                    run_one, selected, output_root, threads,
                    batch_rows, row_group_rows, max_buffer_mib, zstd_level,
                )
                inflight[future] = selected
                return True

            for _ in range(min(workers, len(selections))):
                enqueue()
            while inflight:
                done, _ = wait(inflight, return_when=FIRST_COMPLETED)
                for future in done:
                    selected = inflight.pop(future)
                    try:
                        record_success(selected, future.result())
                    except Exception as exc:
                        record_error(selected, exc)
                        stop = True
                if not stop:
                    while len(inflight) < workers and enqueue():
                        pass

    summary: dict[str, Any] = {
        "selected_batches": len(selections),
        "successful_batches": success,
        "failed_batches": len(failures),
        "files_reduced": files_removed,
        "verified_rows": rows,
        "source_bytes": source_bytes,
        "output_bytes": output_bytes,
        "workers": workers,
        "threads_per_worker": threads,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "failures": failures,
        "journal": str(journal),
    }
    append_event(journal, {"event": "run-end", "summary": summary})
    latest = metadata / "last-run.json"
    staging = metadata / "last-run.json.tmp"
    staging.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    os.replace(staging, latest)
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Plan or execute explicitly selected BDNB Parquet compaction batches."
    )
    parser.add_argument("--plan-dir", required=True, type=Path)
    parser.add_argument("--source-root", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument(
        "--tables-root", required=True,
        help="relative path to the BDNB tables/ directory",
    )
    selector = parser.add_mutually_exclusive_group(required=True)
    selector.add_argument("--table", action="append", dest="tables")
    selector.add_argument("--all-tables", action="store_true")
    selector.add_argument(
        "--approved-tables", type=Path,
        help="TSV inventory edited with approved=yes and documented review reason",
    )
    selector.add_argument(
        "--inventory", action="store_true",
        help="print all planned BDNB tables as a reviewable TSV; no output changes",
    )
    parser.add_argument(
        "--max-batches", type=int, default=1,
        help="maximum batches this run may process (default: 1)",
    )
    parser.add_argument("--workers", type=int, choices=(1, 2, 3, 4), default=1)
    parser.add_argument("--threads", type=int, default=DEFAULT_THREADS)
    parser.add_argument("--batch-rows", type=int, default=16384)
    parser.add_argument("--row-group-rows", type=int, default=100000)
    parser.add_argument("--max-buffer-mib", type=int, default=128)
    parser.add_argument("--zstd-level", type=int, default=3)
    parser.add_argument("--min-free-gib", type=int, default=20)
    parser.add_argument(
        "--execute", action="store_true",
        help="opt in to creating compacted output (default: dry-run)",
    )
    args = parser.parse_args(argv)
    if (
        args.threads < 1 or args.max_batches < 1
        or args.batch_rows < 1 or args.row_group_rows < 1
        or args.max_buffer_mib < 1 or args.min_free_gib < 0
        or not (1 <= args.zstd_level <= 19)
    ):
        parser.error("Invalid resource limit or compression level")

    source = args.source_root.expanduser().resolve()
    output = args.output_root.expanduser().resolve()
    plan_dir = args.plan_dir.expanduser().resolve()
    try:
        reject_overlapping_trees(source, output)
        reject_overlapping_trees(plan_dir, output)
        if args.inventory:
            if args.execute:
                raise ValueError("--inventory cannot be combined with --execute")
            write_inventory(sys.stdout, table_inventory(plan_dir, args.tables_root))
            return 0
        if args.all_tables and args.execute:
            raise ValueError(
                "--all-tables is preview-only; use --approved-tables or --table "
                "for explicit execution approval"
            )
        allowed = (
            read_approved_tables(args.approved_tables)
            if args.approved_tables is not None
            else set(args.tables or [])
        )
        selections = load_candidates(
            plan_dir, source, args.tables_root,
            allowed,
            all_tables=args.all_tables,
            limit=args.max_batches,
        )
        print(
            json.dumps({
                "mode": "execute" if args.execute else "dry-run",
                "selected_batches": len(selections),
                "source_files": sum(len(s.sources) for s in selections),
                "source_mib": round(sum(
                    sum(src.bytes for src in s.sources) for s in selections
                ) / MIB, 3),
                "workers": args.workers,
                "threads_per_worker": args.threads,
                "batch_ids": [s.batch_id for s in selections],
                "directories": sorted({s.directory for s in selections}),
            }, indent=2, ensure_ascii=False),
            flush=True,
        )
        if not args.execute:
            return 0

        summary = run_schedule(
            selections, output,
            workers=args.workers,
            threads=args.threads,
            batch_rows=args.batch_rows,
            row_group_rows=args.row_group_rows,
            max_buffer_mib=args.max_buffer_mib,
            zstd_level=args.zstd_level,
            min_free_gib=args.min_free_gib,
        )
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        return 1 if summary["failed_batches"] else 0
    except (OSError, RuntimeError, ValueError, KeyError, TypeError) as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
