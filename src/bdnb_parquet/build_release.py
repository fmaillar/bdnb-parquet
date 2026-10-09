from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class ManifestEntry:
    path: str
    bytes: int
    sha256: str
    partial: bool


def load_manifest(path: Path) -> list[ManifestEntry]:
    entries: list[ManifestEntry] = []
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        required = {"path", "bytes", "sha256", "partial"}
        missing = required.difference(reader.fieldnames or ())
        if missing:
            raise RuntimeError(
                f"manifest missing required columns: {', '.join(sorted(missing))}"
            )
        for row in reader:
            sha = row["sha256"].strip()
            if not sha:
                raise RuntimeError(
                    f"manifest entry has no SHA-256: {row['path']}"
                )
            entries.append(
                ManifestEntry(
                    path=row["path"],
                    bytes=int(row["bytes"]),
                    sha256=sha,
                    partial=row["partial"] in {"1", "true", "True"},
                )
            )
    return entries


def path_penalty(path: str) -> tuple[int, int, int, str]:
    lowered = path.lower()
    parts = Path(path).parts
    archive_markers = sum(
        (
            "-zip-" in part.lower()
            or part.lower().endswith(".zip")
            or "-7z-" in part.lower()
            or part.lower().endswith(".7z")
        )
        for part in parts
    )
    technical_markers = sum(
        (
            part.endswith(".staging")
            or part.startswith(".")
            or part in {"_tmp", "tmp"}
        )
        for part in parts
    )
    generic_export = int(
        "export au format" in lowered
        or "/fichier csv" in lowered
        or "/fichier json" in lowered
    )
    return (
        technical_markers * 100 + archive_markers * 10 + generic_export,
        len(parts),
        len(path),
        path,
    )


def canonical_entry(group: Iterable[ManifestEntry]) -> ManifestEntry:
    return min(group, key=lambda entry: path_penalty(entry.path))


def classify_duplicate(canonical: str, duplicate: str) -> str:
    c = canonical.lower()
    d = duplicate.lower()

    if "vigieau/" in c and "vigieau/" in d:
        return "snapshot-identical"
    if "repertoire-national/" in c and "repertoire-national/" in d:
        return "snapshot-identical"
    if (
        ("-zip-" in c or ".zip/" in c)
        != ("-zip-" in d or ".zip/" in d)
    ):
        return "archive-vs-standalone"
    if (
        "export au format" in c
        or "export au format" in d
        or "fichier csv" in c
        or "fichier csv" in d
        or "fichier json" in c
        or "fichier json" in d
    ):
        return "duplicate-export"
    if Path(canonical).name == Path(duplicate).name:
        return "dataset-alias"
    return "exact-duplicate"


def write_tsv(path: Path, header: list[str], rows: Iterable[list[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, delimiter="\t", lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)
    os.replace(tmp, path)


def materialize(src: Path, dst: Path, mode: str) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        raise FileExistsError(dst)
    if mode == "hardlink":
        os.link(src, dst)
    elif mode == "copy":
        shutil.copy2(src, dst)
    else:
        raise ValueError(mode)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build a deduplicated Parquet release from a SHA-256 manifest. "
            "The source corpus is never modified."
        )
    )
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument(
        "--mode",
        choices=("hardlink", "copy"),
        default="hardlink",
        help=(
            "materialization mode; hardlink is space-efficient when source and "
            "release are on the same filesystem"
        ),
    )
    parser.add_argument(
        "--plan-only",
        action="store_true",
        help="write metadata and summary without materializing files",
    )
    args = parser.parse_args(argv)

    manifest = args.manifest.expanduser().resolve()
    source_root = args.source_root.expanduser().resolve()
    output_root = args.output_root.expanduser().resolve()

    if output_root.exists() and any(output_root.iterdir()):
        raise RuntimeError(f"output directory is not empty: {output_root}")
    output_root.mkdir(parents=True, exist_ok=True)

    entries = load_manifest(manifest)
    groups: dict[str, list[ManifestEntry]] = defaultdict(list)
    for entry in entries:
        groups[entry.sha256].append(entry)

    canonical: list[ManifestEntry] = []
    duplicate_rows: list[list[object]] = []
    duplicate_bytes = 0

    for sha, group in groups.items():
        keep = canonical_entry(group)
        canonical.append(keep)
        for entry in group:
            if entry.path == keep.path:
                continue
            duplicate_bytes += entry.bytes
            duplicate_rows.append(
                [
                    sha,
                    keep.path,
                    entry.path,
                    entry.bytes,
                    classify_duplicate(keep.path, entry.path),
                ]
            )

    canonical.sort(key=lambda entry: entry.path)
    duplicate_rows.sort(key=lambda row: (str(row[1]), str(row[2])))

    meta = output_root / "_meta"
    write_tsv(
        meta / "duplicates.tsv",
        ["sha256", "canonical_path", "duplicate_path", "bytes", "reason"],
        duplicate_rows,
    )
    write_tsv(
        meta / "manifest.tsv",
        ["path", "bytes", "sha256", "partial"],
        (
            [entry.path, entry.bytes, entry.sha256, int(entry.partial)]
            for entry in canonical
        ),
    )

    if not args.plan_only:
        for index, entry in enumerate(canonical, start=1):
            src = source_root / entry.path
            dst = output_root / entry.path
            if not src.is_file():
                raise FileNotFoundError(src)
            if src.stat().st_size != entry.bytes:
                raise RuntimeError(f"size changed since manifest: {src}")
            materialize(src, dst, args.mode)
            if index == 1 or index % 1000 == 0 or index == len(canonical):
                print(f"[{index}/{len(canonical)}] {entry.path}", flush=True)

    logical_bytes = sum(entry.bytes for entry in entries)
    release_bytes = sum(entry.bytes for entry in canonical)
    summary = {
        "source_manifest": str(manifest),
        "source_root": str(source_root),
        "output_root": str(output_root),
        "mode": args.mode,
        "plan_only": args.plan_only,
        "source_files": len(entries),
        "release_files": len(canonical),
        "duplicate_files_removed": len(entries) - len(canonical),
        "duplicate_groups": sum(len(group) > 1 for group in groups.values()),
        "source_bytes": logical_bytes,
        "release_bytes": release_bytes,
        "bytes_removed": duplicate_bytes,
        "source_gib": logical_bytes / 2**30,
        "release_gib": release_bytes / 2**30,
        "removed_gib": duplicate_bytes / 2**30,
        "partial_files": sum(entry.partial for entry in canonical),
    }
    (meta / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    print(f"SOURCE_FILES: {len(entries)}")
    print(f"RELEASE_FILES: {len(canonical)}")
    print(f"DUPLICATE_FILES_REMOVED: {len(entries) - len(canonical)}")
    print(f"DUPLICATE_GROUPS: {summary['duplicate_groups']}")
    print(f"SOURCE_SIZE: {logical_bytes / 2**30:.3f} GiB")
    print(f"RELEASE_SIZE: {release_bytes / 2**30:.3f} GiB")
    print(f"REMOVED: {duplicate_bytes / 2**30:.3f} GiB")
    print(f"PARTIAL: {summary['partial_files']}")
    print(f"META: {meta}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
