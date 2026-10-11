"""Read-only BDNB table inventory and explicit semantic approval manifest.

The inventory is built exclusively from an existing planner's TSV files; it
never scans Parquet footers or certifies that merging is semantically safe.
"""

from __future__ import annotations

import csv
import re
from collections import defaultdict
from pathlib import Path, PurePosixPath
from typing import TextIO

from .compact_run import relative_directory_path

PART_PATTERN = re.compile(r"part-(\d{5,})\.parquet\Z")
TABLE_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9_-]*\Z")

INVENTORY_COLUMNS = (
    "table", "approved", "reason", "candidate_batches",
    "eligible_batches", "blocked_batches", "candidate_files",
    "candidate_mib", "candidate_rows", "schema_variants",
    "other_batches",
)


def checked_tables_root(raw: str) -> Path:
    base = relative_directory_path(raw)
    if base == Path(".") or base.name != "tables":
        raise ValueError("--tables-root must identify a directory ending in /tables")
    return base


def table_inventory(plan_dir: Path, tables_root: str) -> list[dict[str, object]]:
    """Enumerate table candidates, including blocked groups, with no approvals."""
    base = checked_tables_root(tables_root)
    with (plan_dir / "batches.tsv").open(
        newline="", encoding="utf-8"
    ) as stream:
        batches = list(csv.DictReader(stream, delimiter="\t"))
    with (plan_dir / "files.tsv").open(
        newline="", encoding="utf-8"
    ) as stream:
        files = list(csv.DictReader(stream, delimiter="\t"))

    members: dict[str, list[str]] = defaultdict(list)
    for file in files:
        members[file["batch_id"]].append(file["path"])

    groups: dict[str, list[dict[str, str]]] = defaultdict(list)
    for batch in batches:
        directory = PurePosixPath(batch["directory"])
        if directory.parent.as_posix() != base.as_posix():
            continue
        table = directory.name
        if not TABLE_PATTERN.fullmatch(table):
            raise ValueError(f"Invalid table name in plan: {table!r}")
        groups[table].append(batch)

    result: list[dict[str, object]] = []
    for table, table_batches in sorted(groups.items()):
        candidates = [
            batch for batch in table_batches
            if batch["status"] == "review-required"
        ]
        eligible = 0
        blocked = 0
        schema_hashes: set[str] = set()
        for batch in candidates:
            paths = members[batch["batch_id"]]
            numbers: list[int] = []
            for path in paths:
                match = PART_PATTERN.fullmatch(PurePosixPath(path).name)
                if match:
                    numbers.append(int(match.group(1)))
            contiguous = (
                len(paths) == int(batch["source_files"])
                and len(numbers) == len(paths)
                and len(numbers) >= 2
                and numbers == list(range(numbers[0], numbers[0] + len(numbers)))
            )
            if contiguous:
                eligible += 1
                schema_hashes.add(batch["schema_hash"])
            else:
                blocked += 1
        result.append({
            "table": table,
            "approved": "",
            "reason": "",
            "candidate_batches": len(candidates),
            "eligible_batches": eligible,
            "blocked_batches": blocked,
            "candidate_files": sum(int(b["source_files"]) for b in candidates),
            "candidate_mib": round(
                sum(int(b["source_bytes"]) for b in candidates) / 2**20, 3
            ),
            "candidate_rows": sum(int(b["source_rows"]) for b in candidates),
            "schema_variants": len(schema_hashes),
            "other_batches": len(table_batches) - len(candidates),
        })
    return result


def write_inventory(stream: TextIO, rows: list[dict[str, object]]) -> None:
    writer = csv.DictWriter(
        stream, fieldnames=INVENTORY_COLUMNS,
        delimiter="\t", lineterminator="\n",
    )
    writer.writeheader()
    writer.writerows(rows)


def read_approved_tables(path: Path) -> set[str]:
    """Load human-approved tables from an edited inventory TSV, fail closed."""
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"Approval file must be a regular file: {path}")
    with path.open(newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream, delimiter="\t")
        if not {"table", "approved", "reason"} <= set(reader.fieldnames or []):
            raise ValueError(
                "Approval TSV must contain table, approved and reason columns"
            )
        approved: set[str] = set()
        seen: set[str] = set()
        for row in reader:
            table = row["table"]
            flag = row["approved"].strip().lower()
            rationale = row["reason"].strip()
            if not TABLE_PATTERN.fullmatch(table) or table in seen:
                raise ValueError(f"Invalid or duplicated approval table: {table!r}")
            seen.add(table)
            if flag not in {"", "no", "yes"}:
                raise ValueError(f"Use yes or no for {table}, not {flag!r}")
            if flag == "yes":
                if not rationale:
                    raise ValueError(f"Approved table {table} needs a review reason")
                if row.get("blocked_batches") not in (None, "", "0"):
                    raise ValueError(
                        f"Table {table} contains blocked batches; review individually"
                    )
                approved.add(table)
    if not approved:
        raise ValueError("Approval TSV has no explicitly approved tables")
    return approved
