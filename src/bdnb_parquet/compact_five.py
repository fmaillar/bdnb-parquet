"""Stage verified compaction for five explicitly selected non-BDNB datasets.

Compaction outputs are *not* published over the originals: unrelated source
files can share Arrow schemas, and filenames/sidecars can encode independent
source identity. Preserve exact ordered origin ranges in a durable manifest.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from .compact_run import (
    Selection, Source, relative_path, reject_overlapping_trees,
)
from .compact_schedule import run_schedule

TARGETS: dict[str, str] = {
    "waldec": "administration/associations/repertoire-national/waldec",
    "datatourisme": "tourisme/offre-nationale/datatourisme",
    "imope": "territoire-geospatial/cadastre/batiments/imope",
    "import": "administration/associations/repertoire-national/import",
    "vigieau": "climat-environnement/secheresse/vigieau",
}
PART_PATTERN = re.compile(r"part-(\d{5,})\.parquet\Z")
GIB = 2**30


def read_plan(
    plan_dir: Path, source_root: Path
) -> tuple[list[tuple[str, Selection, str]], dict[str, dict[str, int]]]:
    """Read the planner once and select only the five exact source trees.

    The underlying Parquet runner still revalidates actual footers, schemas,
    byte counts and every Arrow row against the original source files.
    """
    summary = json.loads((plan_dir / "summary.json").read_text(encoding="utf-8"))
    if Path(summary["source_root"]).resolve() != source_root:
        raise ValueError("Plan source_root does not match --source-root")

    with (plan_dir / "batches.tsv").open(
        encoding="utf-8", newline=""
    ) as stream:
        batch_rows = list(csv.DictReader(stream, delimiter="\t"))
    with (plan_dir / "files.tsv").open(
        encoding="utf-8", newline=""
    ) as stream:
        file_rows = list(csv.DictReader(stream, delimiter="\t"))

    members: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in file_rows:
        members[row["batch_id"]].append(row)

    selected: list[tuple[str, Selection, str]] = []
    inventory: dict[str, dict[str, int]] = {
        key: {
            "source_files": 0, "source_bytes": 0, "source_rows": 0,
            "candidate_batches": 0, "candidate_files": 0,
            "candidate_bytes": 0, "candidate_rows": 0,
            "safe_part_batches": 0, "independent_file_batches": 0,
            "keep_files": 0,
        }
        for key in TARGETS
    }
    used: set[str] = set()
    seen_paths: set[str] = set()

    for batch in batch_rows:
        directory = relative_path(batch["directory"])
        matching = [
            key for key, prefix in TARGETS.items()
            if directory == Path(prefix) or Path(prefix) in directory.parents
        ]
        if not matching:
            continue
        if len(matching) != 1:
            raise ValueError(f"Ambiguous dataset path: {directory}")
        dataset = matching[0]
        stats = inventory[dataset]
        rows = members.pop(batch["batch_id"], [])
        if not rows or len(rows) != int(batch["source_files"]):
            raise ValueError(f"Incomplete plan batch: {batch['batch_id']}")
        if batch["batch_id"] in used:
            raise ValueError(f"Repeated batch id: {batch['batch_id']}")
        used.add(batch["batch_id"])
        sources: list[Source] = []
        for row in rows:
            rel = relative_path(row["path"])
            if rel.parent != directory:
                raise ValueError(f"Unexpected input parent: {rel}")
            if row["path"] in seen_paths:
                raise ValueError(f"Duplicate file in plan: {row['path']}")
            seen_paths.add(row["path"])
            if (row["schema_hash"] != batch["schema_hash"]
                or row["directory"] != batch["directory"]
                or row["batch_id"] != batch["batch_id"]
                or row["status"] != batch["status"]):
                raise ValueError(f"Plan metadata differs: {rel}")
            path = source_root / rel
            if path.is_symlink() or not path.resolve().is_relative_to(source_root):
                raise ValueError(f"Unsafe input: {path}")
            sources.append(Source(
                relative=row["path"], path=path,
                bytes=int(row["bytes"]), rows=int(row["rows"]),
                schema_hash=row["schema_hash"],
            ))
        if sum(x.bytes for x in sources) != int(batch["source_bytes"]):
            raise ValueError("Plan source bytes inconsistent")
        if sum(x.rows for x in sources) != int(batch["source_rows"]):
            raise ValueError("Plan source rows inconsistent")
        stats["source_files"] += len(sources)
        stats["source_bytes"] += sum(x.bytes for x in sources)
        stats["source_rows"] += sum(x.rows for x in sources)
        if batch["status"] != "review-required":
            stats["keep_files"] += len(sources)
            continue
        if len(sources) < 2 or any(
            row["partial"] != "0" for row in rows
        ):
            raise ValueError(f"Invalid candidate batch: {batch['batch_id']}")
        names = [Path(x.relative).name for x in sources]
        nums = [
            int(m.group(1)) if (m := PART_PATTERN.fullmatch(name)) else -1
            for name in names
        ]
        # Contiguous part-N fragments are known converter output for NT
        # snapshots; *other* files may represent independent sources and
        # must not be automatically published as one logical source.
        part_sequence = bool(
            nums and all(n >= 0 for n in nums)
            and nums == list(range(nums[0], nums[0] + len(nums)))
            and dataset == "datatourisme"
            and directory.is_relative_to(Path(TARGETS["datatourisme"]) / "ntriples")
        )
        category = "part-sequence" if part_sequence else "independent-files"
        if part_sequence:
            stats["safe_part_batches"] += 1
        else:
            stats["independent_file_batches"] += 1
        stats["candidate_batches"] += 1
        stats["candidate_files"] += len(sources)
        stats["candidate_bytes"] += sum(x.bytes for x in sources)
        stats["candidate_rows"] += sum(x.rows for x in sources)
        selected.append((
            dataset,
            Selection(
                batch_id=batch["batch_id"],
                directory=batch["directory"],
                schema_hash=batch["schema_hash"],
                sources=tuple(sources),
                planned_files=len(sources), pilot=False,
            ),
            category,
        ))
    selected.sort(key=lambda x: (list(TARGETS).index(x[0]), x[1].directory, x[1].batch_id))
    if not selected:
        raise ValueError("No candidate compaction batches in the five datasets")
    return selected, inventory


def provenance_manifest(
    selections: list[tuple[str, Selection, str]], output_root: Path,
    plan_dir: Path,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for dataset, selected, category in selections:
        total = 0
        for source in selected.sources:
            rows.append({
                "dataset": dataset,
                "batch_id": selected.batch_id,
                "status": category,
                "directory": selected.directory,
                "schema_hash": selected.schema_hash,
                "output": (
                    Path(selected.directory) /
                    f"compact-{selected.batch_id}.parquet"
                ).as_posix(),
                "original_path": source.relative,
                "first_row": total,
                "rows": source.rows,
                "source_bytes": source.bytes,
            })
            total += source.rows
    return {
        "format": "five-datasets-compaction-provenance-v1",
        "planner_sha256": hashlib.sha256(
            (plan_dir / "batches.tsv").read_bytes()
            + (plan_dir / "files.tsv").read_bytes()
        ).hexdigest(),
        "sources": rows,
        "candidate_batches": len(selections),
        "candidate_source_files": len(rows),
        "note": (
            "Rows are preserved exactly in source sequence. Independent source "
            "files may carry semantic identities in filenames or manifests; "
            "do NOT delete originals or publish without dataset-specific audit."
        ),
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="Inventory and stage verified compaction of five datasets."
    )
    p.add_argument("--plan-dir", required=True, type=Path)
    p.add_argument("--source-root", required=True, type=Path)
    p.add_argument("--output-root", type=Path)
    p.add_argument("--workers", type=int, choices=(1,2,3,4), default=4)
    p.add_argument("--threads", type=int, default=2)
    p.add_argument("--min-free-gib", type=int, default=20)
    p.add_argument("--execute", action="store_true",
                   help="write verified outputs under --output-root only")
    p.add_argument("--include-independent-files", action="store_true",
                   help="stage non-part-file batches; NEVER delete source files")
    args = p.parse_args(argv)
    if args.min_free_gib < 0 or args.threads < 1:
        p.error("invalid resource limit")
    if args.execute and args.output_root is None:
        p.error("--execute requires --output-root")
    try:
        root = args.source_root.expanduser().resolve()
        plan_dir = args.plan_dir.expanduser().resolve()
        output = (
            args.output_root.expanduser().resolve()
            if args.output_root is not None else None
        )
        if output is not None:
            reject_overlapping_trees(root, output)
            reject_overlapping_trees(plan_dir, output)
        selected, inventory = read_plan(plan_dir, root)
        preview = {
            "dataset_roots": TARGETS,
            "inventory": inventory,
            "selected_batches": len(selected),
            "safe_part_batches": sum(k == "part-sequence" for _, _, k in selected),
            "independent_file_batches": sum(k == "independent-files" for _, _, k in selected),
            "potential_file_reduction": sum(
                len(sel.sources) - 1 for _, sel, _ in selected
            ),
            "mode": "execute" if args.execute else "preview",
            "originals_untouched": True,
        }
        print(json.dumps(preview, indent=2, ensure_ascii=False), flush=True)
        if not args.execute:
            return 0
        assert output is not None
        review = [x for x in selected if x[2] == "independent-files"]
        if review and not args.include_independent_files:
            raise ValueError(
                f"{len(review)} batches contain independently named input files. "
                "Inspect inventory, then use --include-independent-files to "
                "stage their compacted outputs without deleting originals."
            )
        selections = [selection for _, selection, _ in selected]
        # Durable selection inventory: write before scheduling, protected
        # from accidental overwrite by a divergent current plan.
        output.mkdir(parents=True, exist_ok=True)
        meta = output / "_meta"
        meta.mkdir(exist_ok=True)
        provenance = provenance_manifest(selected, output, plan_dir)
        path = meta / "five-datasets-provenance.json"
        if path.exists():
            previous = json.loads(path.read_text(encoding="utf-8"))
            if previous != provenance:
                raise ValueError(
                    "Output root already belongs to another source selection"
                )
        else:
            tmp = path.with_suffix(".json.tmp")
            tmp.write_text(
                json.dumps(provenance, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            os.replace(tmp, path)
        results = run_schedule(
            selections, output,
            workers=args.workers, threads=args.threads,
            batch_rows=16384, row_group_rows=100000,
            max_buffer_mib=128, zstd_level=3,
            min_free_gib=args.min_free_gib,
        )
        if results["failed_batches"] or results["successful_batches"] != len(selected):
            raise RuntimeError("Some compaction batches failed; originals preserved")
        print(
            "STAGED: all candidate groups verified; no original files removed. "
            "Publisher needs dataset-specific manifest review.",
            flush=True,
        )
        return 0
    except (OSError, ValueError, RuntimeError, KeyError, TypeError) as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
