from __future__ import annotations

import argparse
import csv
import hashlib
import os
import re
from dataclasses import dataclass
from pathlib import Path

RNA_FILE_RE = re.compile(
    r"^rna_(import|waldec)_(20\d{2})(\d{2})(\d{2})_.*\.parquet$"
)


@dataclass(frozen=True)
class Move:
    source: str
    destination: str
    kind: str
    date: str


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(16 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def build_plan(root: Path) -> list[Move]:
    moves: list[Move] = []
    for src in sorted(root.rglob("*.parquet")):
        rel = src.relative_to(root)
        if len(rel.parts) < 2:
            continue

        match = RNA_FILE_RE.match(src.name)
        if match is None:
            continue

        kind, year, month, day = match.groups()
        date = f"{year}-{month}-{day}"
        dst = root / kind / date / src.name

        if src == dst:
            continue

        moves.append(
            Move(
                source=rel.as_posix(),
                destination=dst.relative_to(root).as_posix(),
                kind=kind,
                date=date,
            )
        )
    return moves


def write_plan(path: Path, rows: list[list[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, delimiter="\t", lineterminator="\n")
        writer.writerow(
            ["source", "destination", "kind", "date", "action"]
        )
        writer.writerows(rows)
    os.replace(tmp, path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Normalize RNA Parquet snapshots by kind and date."
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
            rows.append(
                [move.source, move.destination, move.kind, move.date, action]
            )
            continue

        if not dst.is_file():
            action = "conflict-destination-not-file"
            conflicts.append((move, action))
            rows.append(
                [move.source, move.destination, move.kind, move.date, action]
            )
            continue

        if src.stat().st_size != dst.stat().st_size:
            action = "conflict-size"
            conflicts.append((move, action))
            rows.append(
                [move.source, move.destination, move.kind, move.date, action]
            )
            continue

        if sha256(src) != sha256(dst):
            action = "conflict-sha256"
            conflicts.append((move, action))
            rows.append(
                [move.source, move.destination, move.kind, move.date, action]
            )
            continue

        action = "drop-source-duplicate"
        actions.append((move, action))
        rows.append(
            [move.source, move.destination, move.kind, move.date, action]
        )

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

    for path in sorted(
        (p for p in root.rglob("*") if p.is_dir()),
        key=lambda p: len(p.parts),
        reverse=True,
    ):
        if path in {root / "import", root / "waldec"}:
            continue
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
