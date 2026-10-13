"""Audit a published snapshot before rebuilding whole-release metadata/torrent.

No mutation of the published snapshot. Default invocation performs only an
inventory; --write-manifest computes fresh SHA-256 for *every* payload file
and writes immutable TSV/JSON in a new external directory.

Legacy compaction backups and leftover staging trees are *reported* and
excluded, never silently added as torrent payload. Torrent creation must
wait until those trees are removed or a clean, independently verified
hardlink release tree is assembled; this tool never deletes anything.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import stat
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq

MIB = 2**20
TORRENT_PIECE_MIB = 32
BACKUP_SUFFIXES = (
    ".precompact-backup",
    ".imope-v2-prepublish-backup",
    ".compaction-stage",
    ".imope-v2-publish-stage",
    ".failed-published",
    ".imope-v2-failed-published",
)
TSV_COLUMNS = ("path", "bytes", "sha256", "kind", "rows", "row_groups")


@dataclass(frozen=True)
class SnapshotFile:
    relative: str
    path: Path
    size: int
    device: int
    inode: int
    mtime_ns: int


def _is_excluded_directory(name: str) -> bool:
    return name.endswith(BACKUP_SUFFIXES)


def _safe_location(root: Path, output: Path | None) -> Path:
    if root.is_symlink() or not root.is_dir():
        raise ValueError(f"Snapshot root must be a real directory: {root}")
    root = root.expanduser().resolve()
    if not (root / "parquet").is_dir():
        raise ValueError(
            "Expected complete snapshot root containing parquet/; "
            "do not point --root at parquet/ alone"
        )
    if (root / "parquet").is_symlink():
        raise ValueError("parquet/ is a symlink; inspect mount first")
    if output is not None:
        if output.is_symlink():
            raise ValueError("Manifest destination must not be symlink")
        output = output.expanduser().resolve()
        if (output == root or output in root.parents
            or root in output.parents):
            raise ValueError(
                "Manifest destination cannot overlap the published snapshot"
            )
        if output.exists():
            raise FileExistsError(f"External manifest directory exists: {output}")
        if not output.parent.is_dir():
            raise ValueError(f"Manifest destination parent missing: {output.parent}")
    return root


def inventory(root: Path) -> tuple[list[SnapshotFile], list[str]]:
    """List every payload file once; reject symlinks and unusual FS entries."""
    records: list[SnapshotFile] = []
    excluded: list[str] = []
    def fail_walk(error: OSError) -> None:
        raise error

    for directory, subdirs, filenames in os.walk(
        root, topdown=True, followlinks=False, onerror=fail_walk,
    ):
        base = Path(directory)
        for name in sorted(subdirs[:]):
            path = base / name
            if path.is_symlink():
                raise ValueError(f"Symlinked directory in snapshot: {path}")
            if _is_excluded_directory(name):
                excluded.append(path.relative_to(root).as_posix())
                subdirs.remove(name)
            elif not path.is_dir():
                raise ValueError(f"Unexpected directory entry: {path}")
        subdirs.sort()
        for name in sorted(filenames):
            file = base / name
            info = file.lstat()
            if not stat.S_ISREG(info.st_mode):
                raise ValueError(f"Unsafe nonregular snapshot file: {file}")
            relative = file.relative_to(root).as_posix()
            if file.suffix == ".torrent":
                # An obsolete torrent in payload would make its own
                # replacement self-referential. Stop and move it out manually.
                raise ValueError(
                    f"Torrent file inside payload must be relocated: {relative}"
                )
            records.append(SnapshotFile(
                relative, file, info.st_size, info.st_dev, info.st_ino,
                info.st_mtime_ns,
            ))
    records.sort(key=lambda r: r.relative.encode("utf-8"))
    if not records:
        raise ValueError("Snapshot payload has no regular files")
    if len({r.relative for r in records}) != len(records):
        raise ValueError("Duplicate snapshot relative paths")
    return records, sorted(excluded)


def _verify_stable(rec: SnapshotFile) -> None:
    info = rec.path.lstat()
    if (not stat.S_ISREG(info.st_mode)
        or (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)
        != (rec.device, rec.inode, rec.size, rec.mtime_ns)):
        raise ValueError(f"Snapshot changed during audit: {rec.relative}")


def _digest(rec: SnapshotFile) -> str:
    _verify_stable(rec)
    hashed = hashlib.sha256()
    # O_NOFOLLOW prevents symlink swap after the first scan.
    fd = os.open(rec.path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode)
            or (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)
            != (rec.device, rec.inode, rec.size, rec.mtime_ns)):
            raise ValueError(f"Snapshot changed before hashing: {rec.relative}")
        with os.fdopen(fd, "rb", closefd=False) as f:
            for block in iter(lambda: f.read(4 * MIB), b""):
                hashed.update(block)
        info = os.fstat(fd)
        if (info.st_size != rec.size or info.st_mtime_ns != rec.mtime_ns):
            raise ValueError(f"Snapshot changed while hashing: {rec.relative}")
    finally:
        os.close(fd)
    _verify_stable(rec)
    return hashed.hexdigest()


def _encoded_size(records: list[SnapshotFile], name: str,
                  piece_mib: int) -> int:
    """Exact size of bare torrent v1 info dict (without trackers or hashes)."""
    length = sum(rec.size for rec in records)
    pieces = math.ceil(length / (piece_mib * MIB))
    # Size estimation includes entire v1 info dictionary, with placeholder
    # 20-byte piece hashes; trackers, announce-lists and extras add overhead.
    def bstr(raw: str | bytes) -> int:
        value = raw if isinstance(raw, bytes) else raw.encode("utf-8")
        return len(str(len(value))) + 1 + len(value)

    files = 0
    for rec in records:
        components = rec.relative.split("/")
        files += 1  # dictionary opener
        files += bstr("length") + len(f"i{rec.size}e")
        files += bstr("path") + 1  # key + list opener
        files += sum(bstr(part) for part in components)
        files += 1 + 1  # list and dictionary terminator
    return (
        1  # info dictionary
        + bstr("files") + 1 + files + 1
        + bstr("name") + bstr(name)
        + bstr("piece length") + len(f"i{piece_mib * MIB}e")
        + bstr("pieces") + len(str(20 * pieces)) + 1 + 20 * pieces
        + 1  # info dictionary terminator
    )


def audit(root: Path, *, output_dir: Path | None = None,
          write_manifest: bool = False,
          piece_mib: int = TORRENT_PIECE_MIB,
          verify_parquet: bool = False) -> dict:
    if piece_mib <= 0 or piece_mib & (piece_mib - 1):
        raise ValueError("--piece-mib must be a positive power of two")
    if write_manifest and output_dir is None:
        raise ValueError("--write-manifest requires --output-dir")
    if not write_manifest and output_dir is not None:
        raise ValueError("--output-dir only applies with --write-manifest")
    root = _safe_location(root, output_dir)
    files, excluded = inventory(root)
    bytes_total = sum(rec.size for rec in files)
    parquet = [rec for rec in files if rec.relative.endswith(".parquet")]
    partial = [
        rec.relative for rec in files
        if rec.relative.endswith((".partial", ".partial.parquet", ".tmp"))
    ]
    summary: dict = {
        "format": "france-open-data-snapshot-inventory-v1",
        "root": str(root),
        "mode": "external-sha256-manifest" if write_manifest else "read-only-inventory",
        "payload_files": len(files),
        "payload_bytes": bytes_total,
        "payload_gib": round(bytes_total / 2**30, 4),
        "parquet_files": len(parquet),
        "other_files": len(files) - len(parquet),
        "suspicious_partial_files": partial[:30],
        "suspicious_partial_file_count": len(partial),
        "parquet_footer_verification": verify_parquet,
        "piece_mib": piece_mib,
        "v1_pieces": math.ceil(bytes_total / (piece_mib * MIB)),
        "v1_info_size_without_trackers": _encoded_size(files, root.name, piece_mib),
        "v1_torrent_under_10mib_estimate":
            _encoded_size(files, root.name, piece_mib) < 10 * MIB,
        "excluded_backup_or_stage_directories": excluded,
        "excluded_directories_count": len(excluded),
        "torrent_payload_root_not_clean": bool(excluded or partial),
        "torrent_created": False,
        "published_files_modified": False,
    }
    if not write_manifest:
        if verify_parquet:
            total_rows = 0
            for index, rec in enumerate(parquet, 1):
                _verify_stable(rec)
                total_rows += pq.read_metadata(rec.path).num_rows
                _verify_stable(rec)
                if index % 1000 == 0 or index == len(parquet):
                    print(f"FOOTER [{index}/{len(parquet)}]", file=sys.stderr)
            summary["parquet_rows"] = total_rows
            summary["parquet_footers_verified"] = len(parquet)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return summary

    out = output_dir.expanduser().resolve()
    out.mkdir(mode=0o700)
    manifest = out / "files.sha256.tsv"
    first = time.perf_counter()
    counts = {"parquet": 0, "other": 0}
    parquet_rows = 0
    # File is opened and hashed exactly once. The sorted stable relative
    # paths also define a reproducible future BitTorrent file-list order.
    with manifest.open("x", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, delimiter="\t", lineterminator="\n")
        writer.writerow(TSV_COLUMNS)
        for index, rec in enumerate(files, 1):
            suffix = "parquet" if rec.relative.endswith(".parquet") else "other"
            digest = _digest(rec)
            rows = row_groups = ""
            if verify_parquet and suffix == "parquet":
                _verify_stable(rec)
                footer = pq.read_metadata(rec.path)
                rows, row_groups = int(footer.num_rows), int(footer.num_row_groups)
                parquet_rows += rows
                _verify_stable(rec)
            writer.writerow([
                rec.relative, rec.size, digest, suffix, rows, row_groups,
            ])
            counts[suffix] += 1
            if index % 250 == 0 or index == len(files):
                print(
                    f"HASH [{index}/{len(files)}] "
                    f"{rec.relative}",
                    file=sys.stderr, flush=True,
                )
        fh.flush()
        os.fsync(fh.fileno())

    # A second inventory proves file count, order, sizes and identities have
    # not changed since processing began, without rereading all content.
    refreshed, refreshed_excluded = inventory(root)
    if files != refreshed or excluded != refreshed_excluded:
        raise RuntimeError(
            "Snapshot inventory changed while building SHA256 manifest; "
            "do not use the generated manifest"
        )
    with manifest.open("rb") as stream:
        manifest_sha = hashlib.file_digest(stream, "sha256").hexdigest()
    summary.update({
        "manifest": str(manifest),
        "manifest_sha256": manifest_sha,
        "parquet_hashed": counts["parquet"],
        "other_hashed": counts["other"],
        "parquet_rows": parquet_rows if verify_parquet else None,
        "parquet_footers_verified": len(parquet) if verify_parquet else 0,
        "elapsed_seconds": round(time.perf_counter() - first, 3),
        "torrent_ready": not bool(excluded or partial),
    })
    with (out / "summary.json").open("x", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, ensure_ascii=False)
        fh.write("\n")
        fh.flush()
        os.fsync(fh.fileno())
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Inventory full snapshot and optionally hash every torrent payload file"
    )
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--write-manifest", action="store_true")
    parser.add_argument("--verify-parquet", action="store_true",
                        help="read and validate all Parquet file footers and row counts")
    parser.add_argument("--piece-mib", type=int, default=32)
    args = parser.parse_args(argv)
    try:
        audit(
            args.root, output_dir=args.output_dir,
            write_manifest=args.write_manifest, piece_mib=args.piece_mib,
            verify_parquet=args.verify_parquet,
        )
        return 0
    except (ValueError, OSError, RuntimeError) as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
