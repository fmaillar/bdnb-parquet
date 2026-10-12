"""One command: verify complete BDNB source, compact every candidate, publish.

This is an explicitly destructive operation only with both --execute and
--prune-originals. Uncompacted singletons are never deleted.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .compact_inventory import checked_tables_root
from .compact_publish import (
    inspect_plan,
    publish,
    require_writable_publication_parent,
    require_hardlink_compatible,
    selections_for_dataset,
)
from .compact_run import reject_overlapping_trees
from .compact_schedule import run_schedule


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Compact all source-verified BDNB tables and publish in one run."
    )
    parser.add_argument("--plan-dir", required=True, type=Path)
    parser.add_argument("--source-root", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--tables-root", required=True)
    parser.add_argument("--writable-source-root", type=Path)
    parser.add_argument("--workers", type=int, choices=(1, 2, 3, 4), default=4)
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--batch-rows", type=int, default=16384)
    parser.add_argument("--row-group-rows", type=int, default=100000)
    parser.add_argument("--max-buffer-mib", type=int, default=128)
    parser.add_argument("--zstd-level", type=int, default=3)
    parser.add_argument("--min-free-gib", type=int, default=20)
    parser.add_argument(
        "--execute", action="store_true",
        help="run all BDNB candidates and replace the original BDNB directory",
    )
    parser.add_argument(
        "--prune-originals", action="store_true",
        help="permanently delete old BDNB fragments after successful publication",
    )
    args = parser.parse_args(argv)
    if (
        args.threads < 1 or args.batch_rows < 1
        or args.row_group_rows < 1 or args.max_buffer_mib < 1
        or args.min_free_gib < 0 or not 1 <= args.zstd_level <= 19
    ):
        parser.error("Invalid resource or compression parameter")
    if args.prune_originals and not args.execute:
        parser.error("--prune-originals requires --execute")
    root = args.source_root.expanduser().resolve()
    publish_root = (
        args.writable_source_root.expanduser().resolve()
        if args.writable_source_root is not None
        else root
    )
    plan_dir = args.plan_dir.expanduser().resolve()
    output_root = args.output_root.expanduser().resolve()
    try:
        reject_overlapping_trees(root, output_root)
        reject_overlapping_trees(root, plan_dir)
        reject_overlapping_trees(publish_root, plan_dir)
        reject_overlapping_trees(publish_root, output_root)
        if not root.samefile(publish_root):
            raise ValueError("--writable-source-root must match --source-root")
        tables = root / checked_tables_root(args.tables_root)
        indexed, totals = inspect_plan(
            plan_dir, root, tables.parent, tables
        )
        selections = selections_for_dataset(plan_dir, root, indexed)
        overview = {
            "mode": "execute" if args.execute else "dry-run",
            "tables": totals["tables"],
            "source_files": totals["input_files"],
            "source_rows": totals["input_rows"],
            "candidate_batches": len(selections),
            "candidate_files": sum(len(sel.sources) for sel in selections.values()),
            "workers": args.workers,
            "threads_per_worker": args.threads,
            "prune_originals": args.prune_originals,
        }
        print(json.dumps(overview, indent=2), flush=True)
        if not args.execute:
            return 0
        # Do not spend minutes verifying/scheduling all batches when the
        # final publication itself cannot write into the original snapshot.
        publish_dataset = publish_root / checked_tables_root(args.tables_root).parent
        if not tables.parent.samefile(publish_dataset):
            raise ValueError("BDNB dataset paths do not refer to the same data")
        require_writable_publication_parent(publish_dataset)
        output_root.mkdir(parents=True, exist_ok=True)
        require_hardlink_compatible(output_root, publish_dataset)
        if not selections:
            raise ValueError("No BDNB compaction batches in source plan")
        results = run_schedule(
            list(selections.values()), output_root,
            workers=args.workers,
            threads=args.threads,
            batch_rows=args.batch_rows,
            row_group_rows=args.row_group_rows,
            max_buffer_mib=args.max_buffer_mib,
            zstd_level=args.zstd_level,
            min_free_gib=args.min_free_gib,
        )
        if (
            results["failed_batches"]
            or results["successful_batches"] != len(selections)
        ):
            raise RuntimeError(
                "Compaction incomplete: original BDNB files preserved; "
                "inspect the scheduler journal and rerun"
            )
        report = publish(
            root, plan_dir, output_root, args.tables_root,
            prune_backup=args.prune_originals,
            writable_source_root=publish_root,
        )
        print(
            "COMPLETE: "
            f"{report['source_files']} -> {report['output_files']} BDNB Parquet files; "
            f"{report['verified_rows']} rows; "
            f"originals pruned: {report['pruned']}",
            flush=True,
        )
        return 0
    except (OSError, RuntimeError, ValueError, KeyError, TypeError) as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
