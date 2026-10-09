from __future__ import annotations

import argparse
import csv
import re
from collections import Counter
from pathlib import Path


DATE_PATTERNS = (
    re.compile(r"(?<!\d)(20\d{2})[-_](\d{2})[-_](\d{2})(?!\d)"),
    re.compile(r"(?<!\d)(20\d{2})(\d{2})(\d{2})(?!\d)"),
)


def read_tsv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def extract_date(path: str) -> str:
    for pattern in DATE_PATTERNS:
        match = pattern.search(path)
        if match:
            year, month, day = match.groups()
            return f"{year}-{month}-{day}"
    return ""


def load_renames(path: Path | None) -> dict[str, str]:
    if path is None or not path.exists():
        return {}

    mapping: dict[str, str] = {}
    for row in read_tsv(path):
        source = row["source"]
        destination = row["destination"]
        action = row.get("action", "")
        if action in {"move", "drop-source-duplicate"}:
            mapping[source] = destination
    return mapping


def resolve_path(path: str, renames: dict[str, str]) -> str:
    seen: set[str] = set()
    current = path
    while current in renames and current not in seen:
        seen.add(current)
        current = renames[current]
    return current


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build provenance metadata for historical snapshot paths that "
            "were removed as exact duplicates."
        )
    )
    parser.add_argument("--duplicates", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--renames", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)

    duplicates = read_tsv(args.duplicates.expanduser().resolve())
    manifest_rows = read_tsv(args.manifest.expanduser().resolve())
    renames = load_renames(
        args.renames.expanduser().resolve() if args.renames else None
    )

    current_by_sha: dict[str, list[str]] = {}
    for row in manifest_rows:
        sha = row.get("sha256", "")
        path = row.get("path", "")
        if sha and path:
            current_by_sha.setdefault(sha, []).append(path)

    rows: list[list[object]] = []
    missing_current = 0
    dated = 0
    by_domain: Counter[str] = Counter()

    for row in duplicates:
        if row.get("reason") != "snapshot-identical":
            continue

        sha = row["sha256"]
        old_canonical = row["canonical_path"]
        historical = row["duplicate_path"]
        size = int(row["bytes"])

        resolved = resolve_path(old_canonical, renames)
        candidates = current_by_sha.get(sha, [])

        if resolved in candidates:
            current = resolved
        elif len(candidates) == 1:
            current = candidates[0]
        elif old_canonical in candidates:
            current = old_canonical
        else:
            current = ""
            missing_current += 1

        snapshot_date = extract_date(historical)
        if snapshot_date:
            dated += 1

        domain = historical.split("/", 1)[0]
        by_domain[domain] += 1

        rows.append(
            [
                sha,
                current,
                historical,
                snapshot_date,
                size,
                "identical-snapshot",
            ]
        )

    rows.sort(key=lambda r: (str(r[1]), str(r[3]), str(r[2])))

    output = args.output.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    tmp = output.with_name(output.name + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, delimiter="\t", lineterminator="\n")
        writer.writerow(
            [
                "sha256",
                "current_path",
                "historical_path",
                "snapshot_date",
                "bytes",
                "relation",
            ]
        )
        writer.writerows(rows)
    tmp.replace(output)

    print(f"SNAPSHOT_ALIASES: {len(rows)}")
    print(f"DATED: {dated}")
    print(f"UNDATED: {len(rows) - dated}")
    print(f"MISSING_CURRENT: {missing_current}")
    for domain, count in by_domain.most_common():
        print(f"{domain}: {count}")
    print(f"OUTPUT: {output}")

    return 2 if missing_current else 0


if __name__ == "__main__":
    raise SystemExit(main())
