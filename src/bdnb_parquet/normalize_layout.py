from __future__ import annotations

import argparse
import csv
import hashlib
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class Move:
    source: str
    destination: str
    reason: str


RULES: tuple[tuple[str, str, str], ...] = (
    (
        "energie/consommation-electricite-gaz",
        "energie/consommation",
        "normalize-energy-consumption",
    ),
    (
        "energie/installations/registre",
        "energie/renouvelables/registre-installations",
        "normalize-energy-installations",
    ),
    (
        "transport/baac",
        "transport/accidents/baac",
        "normalize-baac",
    ),
    (
        "transport/infrastructures-ferroviaires/voies-rfn",
        "transport/infrastructures-ferroviaires/rfn/voies",
        "normalize-rfn-voies",
    ),
    (
        "transport/infrastructures-ferroviaires/lignes-rfn",
        "transport/infrastructures-ferroviaires/rfn/lignes",
        "normalize-rfn-lignes",
    ),
)


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(16 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_tsv(path: Path, header: list[str], rows: Iterable[list[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, delimiter="\t", lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)
    os.replace(tmp, path)


def build_plan(root: Path) -> list[Move]:
    moves: list[Move] = []
    for src_prefix, dst_prefix, reason in RULES:
        src_root = root / src_prefix
        if not src_root.exists():
            continue
        for src in sorted(src_root.rglob("*.parquet")):
            rel = src.relative_to(src_root)
            dst = root / dst_prefix / rel
            moves.append(
                Move(
                    source=src.relative_to(root).as_posix(),
                    destination=dst.relative_to(root).as_posix(),
                    reason=reason,
                )
            )
    return moves


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Normalize selected canonical dataset layout aliases."
    )
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="TSV path for the rename plan/audit log",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="apply the plan after validating all collisions",
    )
    args = parser.parse_args(argv)

    root = args.root.expanduser().resolve()
    plan = build_plan(root)

    rows: list[list[object]] = []
    conflicts: list[tuple[Move, str]] = []
    actions: list[tuple[Move, str]] = []

    for move in plan:
        src = root / move.source
        dst = root / move.destination

        if not src.is_file():
            continue

        if not dst.exists():
            actions.append((move, "move"))
            rows.append([move.source, move.destination, move.reason, "move"])
            continue

        if not dst.is_file():
            conflicts.append((move, "destination-exists-not-file"))
            rows.append(
                [
                    move.source,
                    move.destination,
                    move.reason,
                    "conflict-destination-not-file",
                ]
            )
            continue

        if src.stat().st_size != dst.stat().st_size:
            conflicts.append((move, "different-size"))
            rows.append(
                [move.source, move.destination, move.reason, "conflict-size"]
            )
            continue

        src_sha = file_sha256(src)
        dst_sha = file_sha256(dst)
        if src_sha != dst_sha:
            conflicts.append((move, "different-sha256"))
            rows.append(
                [move.source, move.destination, move.reason, "conflict-sha256"]
            )
            continue

        actions.append((move, "drop-source-duplicate"))
        rows.append(
            [
                move.source,
                move.destination,
                move.reason,
                "drop-source-duplicate",
            ]
        )

    write_tsv(
        args.output.expanduser().resolve(),
        ["source", "destination", "reason", "action"],
        rows,
    )

    print(f"PLAN: {len(plan)} files")
    print(f"ACTIONS: {len(actions)}")
    print(f"CONFLICTS: {len(conflicts)}")
    print(f"OUTPUT: {args.output.expanduser().resolve()}")

    if conflicts:
        for move, reason in conflicts[:20]:
            print(
                f"CONFLICT {reason}: {move.source} -> {move.destination}"
            )
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

    for src_prefix, _, _ in RULES:
        src_root = root / src_prefix
        if src_root.exists():
            for path in sorted(
                (p for p in src_root.rglob("*") if p.is_dir()),
                key=lambda p: len(p.parts),
                reverse=True,
            ):
                try:
                    path.rmdir()
                except OSError:
                    pass
            try:
                src_root.rmdir()
            except OSError:
                pass

    print(f"MOVED: {moved}")
    print(f"DROPPED_SOURCE_DUPLICATES: {dropped}")
    print("MODE: applied")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
