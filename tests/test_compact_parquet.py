import csv
import json
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from bdnb_parquet.compact_parquet import (
    MIB,
    FileEntry,
    inspect_file,
    main,
    plan,
    scan,
    write_report,
)


def entry(
    path: str,
    *,
    size: int = 100,
    schema: str = "schema-a",
    partial: bool = False,
) -> FileEntry:
    return FileEntry(
        path=path,
        directory=Path(path).parent.as_posix(),
        bytes=size,
        rows=3,
        row_groups=1,
        schema_hash=schema,
        partial=partial,
    )


class PlanTests(unittest.TestCase):
    def test_same_directory_and_schema_are_review_candidates(self):
        entries = [entry("d/part-2.parquet"), entry("d/part-1.parquet")]
        batches = plan(entries, MIB)
        self.assertEqual(len(batches), 1)
        self.assertEqual(batches[0].status, "review-required")
        self.assertEqual([e.path for e in batches[0].files],
                         ["d/part-1.parquet", "d/part-2.parquet"])
        self.assertEqual(batches[0].rows, 6)

    def test_different_directory_or_schema_not_combined(self):
        batches = plan(
            [entry("a/1.parquet"), entry("a/2.parquet", schema="b"),
             entry("b/3.parquet")],
            MIB,
        )
        self.assertEqual(len(batches), 3)
        self.assertTrue(all(batch.status == "keep" for batch in batches))

    def test_target_size_splits_without_reordering(self):
        batches = plan(
            [entry(f"d/{i}.parquet", size=600_000) for i in range(3)],
            MIB,
        )
        self.assertEqual([len(batch.files) for batch in batches], [1, 1, 1])

        batches = plan(
            [entry(f"d/{i}.parquet", size=400_000) for i in range(3)],
            MIB,
        )
        self.assertEqual([len(batch.files) for batch in batches], [2, 1])

    def test_partial_and_oversize_never_merged(self):
        batches = plan(
            [entry("d/part-1.parquet", size=2 * MIB),
             entry("d/part-2.partial.parquet", partial=True),
             entry("d/part-3.parquet")],
            MIB,
        )
        self.assertEqual([b.status for b in batches],
                         ["keep-oversize", "skip-partial", "keep"])

    def test_non_positive_target_rejected(self):
        with self.assertRaises(ValueError):
            plan([], 0)

    def test_deterministic_group_id(self):
        items = [entry("x/2.parquet"), entry("x/1.parquet")]
        self.assertEqual(plan(items, MIB), plan(reversed(items), MIB))


class ScannerTests(unittest.TestCase):
    def test_real_parquet_scan_and_report(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            source = base / "input"
            folder = source / "domain" / "dataset"
            folder.mkdir(parents=True)
            for name in ("part-001.parquet", "part-002.parquet"):
                pq.write_table(pa.table({"id": pa.array([1, 2, 3])}), folder / name)
            output = base / "plan"

            files = scan(source)
            self.assertEqual(len(files), 2)
            self.assertEqual(sum(item.rows for item in files), 6)
            self.assertEqual(files[0].schema_hash, files[1].schema_hash)

            summary = write_report(source, output, 256 * MIB, plan(files, 256 * MIB))
            self.assertEqual(summary["source_files"], 2)
            self.assertEqual(summary["estimated_output_files"], 1)
            self.assertEqual(summary["potential_file_reduction"], 1)
            self.assertEqual(summary["candidate_batches"], 1)
            self.assertEqual(summary["source_rows"], 6)
            self.assertTrue((output / "summary.json").is_file())
            with (output / "files.tsv").open(newline="") as stream:
                rows = list(csv.DictReader(stream, delimiter="\t"))
            self.assertEqual(len(rows), 2)
            self.assertEqual(len({row["batch_id"] for row in rows}), 1)
            self.assertEqual(json.loads((output / "summary.json").read_text()),
                             summary)
            self.assertTrue(all(file.is_file() for file in folder.iterdir()))

    def test_schema_metadata_distinctions_preserved(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name, tag in [("a.parquet", b"a"), ("b.parquet", b"b")]:
                table = pa.table({"x": [10]}).replace_schema_metadata({b"geo": tag})
                pq.write_table(table, root / name)
            entries = scan(root)
            self.assertNotEqual(entries[0].schema_hash, entries[1].schema_hash)
            self.assertEqual(len(plan(entries, MIB)), 2)

    def test_invalid_parquet_fails_closed(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "data"
            root.mkdir()
            (root / "broken.parquet").write_bytes(b"not parquet")
            with self.assertRaises(RuntimeError):
                scan(root)
            output = Path(temporary) / "plan"
            self.assertEqual(
                main(["--root", str(root), "--output-dir", str(output)]), 1
            )
            self.assertFalse(output.exists())

    def test_refuse_output_within_input(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaises(SystemExit) as error:
                main(["--root", str(root),
                      "--output-dir", str(root / "plan")])
            self.assertEqual(error.exception.code, 2)
            self.assertFalse((root / "plan").exists())

    def test_refuse_overwriting_existing_report(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            out = root / "plan"
            out.mkdir()
            (out / "notes.txt").write_text("preserve")
            source = root / "data"
            source.mkdir()
            with self.assertRaises(FileExistsError):
                write_report(source, out, MIB, [])
            self.assertEqual((out / "notes.txt").read_text(), "preserve")

    def test_file_symlinks_not_scanned(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            actual = root / "real.parquet"
            pq.write_table(pa.table({"x": [1]}), actual)
            link = root / "linked.parquet"
            link.symlink_to(actual)
            with self.assertRaises(RuntimeError):
                scan(root)
            with self.assertRaises(ValueError):
                inspect_file(link, root)


if __name__ == "__main__":
    unittest.main()
