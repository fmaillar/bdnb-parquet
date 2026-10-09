from __future__ import annotations

import argparse
import csv
import hashlib
import os
import re
from dataclasses import dataclass
from pathlib import Path


YEAR_DIR_RE = re.compile(
    r"^cartes?-des-zones-et-arr-t-s-en-vigueur-geojson-(20\d{2})$"
)


@dataclass(frozen=True)
class Move:
    source: str
    destination: str
    year: str


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(16 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def build_plan(root: Path) -> list[Move]:
    geojson = root / "geojson"
    moves: list[Move] = []

    if not geojson.exists():
        return moves

    for src_dir in sorted(p for p in geojson.iterdir() if p.is_dir()):
        match = YEAR_DIR_RE.match(src_dir.name)
        if match is None:
            continue

        year = match.group(1)
        for src in sorted(src_dir.rglob("*.parquet")):
            rel_inside = src.relative_to(src_dir)
            dst = geojson / year / rel_inside
            if src == dst:
                continue
            moves.append(
                Move(
                    source=src.relative_to(root).as_posix(),
                    destination=dst.relative_to(root).as_posix(),
                    year=year,
                )
            )

    return moves


def write_plan(path: Path, rows: list[list[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, delimiter="\t", lineterminator="\n")
        writer.writerow(["source", "destination", "year", "action"])
        writer.writerows(rows)
    os.replace(tmp, path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Normalize Vigieau GeoJSON Parquet layout by year."
    )
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)

    root = args.root.expanduser().resolve()
    plan = build_plan(root)

    actions: list[tuple[Move, str]] = []
    conflicts: list[tuple[Move, str]] = []
    rows: list[list[object]] = []

    for move in plan:
        src = root / move.source
        dst = root / move.destination

        if not dst.exists():
            action = "move"
            actions.append((move, action))
            rows.append([move.source, move.destination, move.year, action])
            continue

        if not dst.is_file():
            action = "conflict-destination-not-file"
            conflicts.append((move, action))
            rows.append([move.source, move.destination, move.year, action])
            continue

        if src.stat().st_size != dst.stat().st_size:
            action = "conflict-size"
            conflicts.append((move, action))
            rows.append([move.source, move.destination, move.year, action])
            continue

        if sha256(src) != sha256(dst):
            action = "conflict-sha256"
            conflicts.append((move, action))
            rows.append([move.source, move.destination, move.year, action])
            continue

        action = "drop-source-duplicate"
        actions.append((move, action))
        rows.append([move.source, move.destination, move.year, action])

    output = args.output.expanduser().resolve()
    write_plan(output, rows)

    print(f"PLAN: {len(plan)} files")
    print(f"ACTIONS: {len(actions)}")
    print(f"CONFLICTS: {len(conflicts)}")
    print(f"OUTPUT: {output}")

    if conflicts:
        for move, reason in conflicts[:20]:
            print(f"CONFLICT {reason}: {move.source} -> {move.destination}")
        return 2

    if not args.apply:
        print("MODE: plan-only")
        return 0

    moved = 0
    dropped = 0
    for move, action in actions:
        src = root / move.source
        dst = root / move.destination

        if action == "move":
            dst.parent.mkdir(parents=True, exist_ok=True)
            os.replace(src, dst)
            moved += 1
        elif action == "drop-source-duplicate":
            src.unlink()
            dropped += 1

    geojson = root / "geojson"
    for path in sorted(
        (p for p in geojson.rglob("*") if p.is_dir()),
        key=lambda p: len(p.parts),
        reverse=True,
    ):
        try:
            path.rmdir()
        except OSError:
            pass

    print(f"MOVED: {moved}")
    print(f"DROPPED_SOURCE_DUPLICATES: {dropped}")
    print("MODE: applied")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
