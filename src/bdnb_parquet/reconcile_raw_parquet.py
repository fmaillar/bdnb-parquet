from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path

DEFAULT_ROOT = Path("/mnt/data/datasets")
DEFAULT_LOG_DIR = Path("/mnt/data/datasets/logs")


def iter_records(paths: list[Path]):
    for path in paths:
        with path.open("r", encoding="utf-8") as fh:
            for lineno, line in enumerate(fh, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(
                        f"invalid JSONL in {path} line {lineno}: {exc}"
                    ) from exc


def classify_latest(
    logs: list[Path],
    raw_root: Path,
) -> dict[str, tuple[str, str]]:
    latest: dict[str, tuple[str, str]] = {}
    raw_prefix = str(raw_root.resolve()) + "/"

    for rec in iter_records(logs):
        src = rec.get("source")
        status = rec.get("status")
        if not isinstance(src, str) or not isinstance(status, str):
            continue
        if "!" in src or not src.startswith(raw_prefix):
            continue

        reason = (
            rec.get("error")
            or rec.get("reason")
            or rec.get("kind")
            or ""
        )
        latest[src] = (status, str(reason))

    result: dict[str, tuple[str, str]] = {}
    for src, (status, reason) in latest.items():
        if status in {"done", "skip"}:
            cls = "complete"
        elif status == "partial":
            cls = "partial"
        elif status == "failed":
            cls = "missing"
        elif status == "unsupported":
            cls = "ignored"
        else:
            cls = "unknown"
        result[src] = (cls, reason)

    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Reconcile top-level raw sources against migration logs."
    )
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument(
        "--logs",
        type=Path,
        nargs="+",
        required=True,
        help="migration JSONL logs in chronological order",
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)

    root = args.root.expanduser().resolve()
    raw_root = root / "raw"
    logs = [p.expanduser().resolve() for p in args.logs]

    def natural_key(path: Path) -> tuple[object, ...]:
        import re
        return tuple(
            int(part) if part.isdigit() else part
            for part in re.split(r"(\d+)", path.name)
        )

    # Preserve the base log first and order passN logs numerically, so pass10
    # cannot be evaluated before pass2 merely because of lexical glob order.
    if logs:
        base = [p for p in logs if "-pass" not in p.name]
        passes = sorted((p for p in logs if "-pass" in p.name), key=natural_key)
        logs = base + passes

    for p in logs:
        if not p.is_file():
            raise FileNotFoundError(p)

    states = classify_latest(logs, raw_root)

    rows = []
    counts: Counter[str] = Counter()
    bytes_by_class: Counter[str] = Counter()

    for src, (cls, reason) in sorted(states.items()):
        p = Path(src)
        try:
            size = p.stat().st_size
        except OSError:
            size = 0

        rel = p.relative_to(raw_root).as_posix()
        rows.append((rel, cls, size, reason))
        counts[cls] += 1
        bytes_by_class[cls] += size

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, delimiter="\t", lineterminator="\n")
        writer.writerow(["raw_path", "class", "bytes", "gib", "reason"])
        for rel, cls, size, reason in rows:
            writer.writerow([rel, cls, size, f"{size / 2**30:.6f}", reason])

    for cls in ("complete", "partial", "missing", "ignored", "unknown"):
        if counts[cls]:
            print(
                f"{cls.upper():8s} "
                f"{counts[cls]:6d} files "
                f"{bytes_by_class[cls] / 2**30:10.3f} GiB"
            )

    print(f"OUTPUT: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
