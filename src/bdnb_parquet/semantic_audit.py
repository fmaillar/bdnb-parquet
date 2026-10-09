from __future__ import annotations

import argparse
import csv
from collections import Counter, defaultdict
from pathlib import Path
from typing import Iterable


def read_duplicates(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def write_tsv(path: Path, header: list[str], rows: Iterable[list[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, delimiter="\t", lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)


def top_domain(path: str) -> str:
    parts = Path(path).parts
    return parts[0] if parts else ""


def parent_key(path: str) -> str:
    return str(Path(path).parent)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Audit semantic redundancy from an exact-duplicate plan."
    )
    parser.add_argument("--duplicates", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)

    rows = read_duplicates(args.duplicates.expanduser().resolve())
    out = args.output_dir.expanduser().resolve()
    out.mkdir(parents=True, exist_ok=True)

    by_reason = Counter()
    bytes_by_reason = Counter()
    by_domain = Counter()
    bytes_by_domain = Counter()

    alias_pairs: dict[tuple[str, str, str], dict[str, int]] = defaultdict(
        lambda: {"files": 0, "bytes": 0}
    )
    snapshot_groups: dict[tuple[str, str], dict[str, object]] = defaultdict(
        lambda: {"copies": 0, "bytes": 0, "paths": []}
    )

    for row in rows:
        reason = row.get("reason", "exact-duplicate")
        size = int(row["bytes"])
        canonical = row["canonical_path"]
        duplicate = row["duplicate_path"]
        sha = row["sha256"]

        by_reason[reason] += 1
        bytes_by_reason[reason] += size

        domain = top_domain(canonical)
        by_domain[domain] += 1
        bytes_by_domain[domain] += size

        if reason in {
            "dataset-alias",
            "archive-vs-standalone",
            "duplicate-export",
            "exact-duplicate",
        }:
            cparent = parent_key(canonical)
            dparent = parent_key(duplicate)
            key = (reason, cparent, dparent)
            alias_pairs[key]["files"] += 1
            alias_pairs[key]["bytes"] += size

        if reason == "snapshot-identical":
            key = (sha, canonical)
            group = snapshot_groups[key]
            group["copies"] = int(group["copies"]) + 1
            group["bytes"] = int(group["bytes"]) + size
            cast_paths = group["paths"]
            assert isinstance(cast_paths, list)
            cast_paths.append(duplicate)

    write_tsv(
        out / "by-reason.tsv",
        ["reason", "duplicate_files", "bytes", "gib"],
        (
            [
                reason,
                by_reason[reason],
                bytes_by_reason[reason],
                f"{bytes_by_reason[reason] / 2**30:.6f}",
            ]
            for reason in sorted(by_reason)
        ),
    )

    write_tsv(
        out / "by-domain.tsv",
        ["domain", "duplicate_files", "bytes", "gib"],
        (
            [
                domain,
                by_domain[domain],
                bytes_by_domain[domain],
                f"{bytes_by_domain[domain] / 2**30:.6f}",
            ]
            for domain in sorted(by_domain, key=lambda d: bytes_by_domain[d], reverse=True)
        ),
    )

    alias_rows = []
    for (reason, canonical_parent, duplicate_parent), stats in alias_pairs.items():
        alias_rows.append(
            [
                reason,
                stats["files"],
                stats["bytes"],
                f"{stats['bytes'] / 2**20:.3f}",
                canonical_parent,
                duplicate_parent,
            ]
        )
    alias_rows.sort(key=lambda r: int(r[2]), reverse=True)
    write_tsv(
        out / "directory-alias-candidates.tsv",
        [
            "reason",
            "files",
            "bytes",
            "mib",
            "canonical_parent",
            "duplicate_parent",
        ],
        alias_rows,
    )

    snapshot_rows = []
    for (sha, canonical), info in snapshot_groups.items():
        paths = info["paths"]
        assert isinstance(paths, list)
        snapshot_rows.append(
            [
                sha,
                canonical,
                int(info["copies"]) + 1,
                info["bytes"],
                f"{int(info['bytes']) / 2**20:.3f}",
                " | ".join(paths),
            ]
        )
    snapshot_rows.sort(key=lambda r: int(r[3]), reverse=True)
    write_tsv(
        out / "snapshot-groups.tsv",
        [
            "sha256",
            "canonical_path",
            "logical_versions",
            "duplicate_bytes",
            "duplicate_mib",
            "duplicate_paths",
        ],
        snapshot_rows,
    )

    total_bytes = sum(bytes_by_reason.values())
    print(f"DUPLICATE_FILES: {len(rows)}")
    print(f"REMOVED: {total_bytes / 2**30:.3f} GiB")
    for reason in sorted(by_reason):
        print(
            f"{reason}: {by_reason[reason]} files, "
            f"{bytes_by_reason[reason] / 2**30:.3f} GiB"
        )
    print(f"ALIAS_PAIRS: {len(alias_rows)}")
    print(f"SNAPSHOT_GROUPS: {len(snapshot_rows)}")
    print(f"OUTPUT: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
