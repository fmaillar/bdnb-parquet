import csv
import io
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from bdnb_parquet.compact_inventory import (
    read_approved_tables,
    table_inventory,
    write_inventory,
)
from bdnb_parquet.compact_parquet import MIB, plan, scan, write_report
from bdnb_parquet.compact_schedule import main


class InventoryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "corpus"
        self.plan_dir = self.root / "plan"
        self.output = self.root / "output"
        self.tables_root = "bdnb/tables"
        for name in ("hthd", "risques"):
            directory = self.source / self.tables_root / name
            directory.mkdir(parents=True)
            for index in range(3):
                pq.write_table(
                    pa.table({"id": pa.array([index, index + 1])}),
                    directory / f"part-{index:05d}.parquet",
                )
        write_report(
            self.source, self.plan_dir, MIB,
            plan(scan(self.source), MIB),
        )

    def args(self):
        return [
            "--plan-dir", str(self.plan_dir),
            "--source-root", str(self.source),
            "--output-root", str(self.output),
            "--tables-root", self.tables_root,
        ]

    def test_inventory_is_read_only_and_technically_informative(self):
        rows = table_inventory(self.plan_dir, self.tables_root)
        self.assertEqual([row["table"] for row in rows], ["hthd", "risques"])
        for row in rows:
            self.assertEqual(row["approved"], "")
            self.assertEqual(row["reason"], "")
            self.assertEqual(row["candidate_batches"], 1)
            self.assertEqual(row["eligible_batches"], 1)
            self.assertEqual(row["blocked_batches"], 0)
            self.assertEqual(row["candidate_files"], 3)
            self.assertEqual(row["candidate_rows"], 6)
            self.assertEqual(row["other_batches"], 0)
        output = io.StringIO()
        write_inventory(output, rows)
        parsed = list(csv.DictReader(
            io.StringIO(output.getvalue()), delimiter="\t"
        ))
        self.assertEqual(len(parsed), 2)
        self.assertEqual(main(self.args() + ["--inventory"]), 0)
        self.assertFalse(self.output.exists())

    def test_approval_file_demands_yes_and_rationale(self):
        rows = table_inventory(self.plan_dir, self.tables_root)
        path = self.root / "review.tsv"
        def save():
            with path.open("w", encoding="utf-8", newline="") as stream:
                write_inventory(stream, rows)
        save()
        with self.assertRaisesRegex(ValueError, "no explicitly approved"):
            read_approved_tables(path)
        rows[0]["approved"] = "yes"
        save()
        with self.assertRaisesRegex(ValueError, "review reason"):
            read_approved_tables(path)
        rows[0]["reason"] = "Reviewed part file naming and provenance"
        save()
        self.assertEqual(read_approved_tables(path), {"hthd"})
        rows[1]["approved"] = "maybe"
        save()
        with self.assertRaisesRegex(ValueError, "Use yes or no"):
            read_approved_tables(path)

    def test_approval_file_rejects_blocked_table(self):
        rows = table_inventory(self.plan_dir, self.tables_root)
        rows[0].update({
            "approved": "yes", "reason": "Reviewed", "blocked_batches": 1,
        })
        path = self.root / "review.tsv"
        with path.open("w", encoding="utf-8", newline="") as stream:
            write_inventory(stream, rows)
        with self.assertRaisesRegex(ValueError, "blocked batches"):
            read_approved_tables(path)

    def test_approved_tsv_executes_only_checked_table(self):
        rows = table_inventory(self.plan_dir, self.tables_root)
        rows[0].update({
            "approved": "yes", "reason": "Confirmed each part represents a shard",
        })
        path = self.root / "review.tsv"
        with path.open("w", encoding="utf-8", newline="") as stream:
            write_inventory(stream, rows)
        self.assertEqual(
            main(self.args() + [
                "--approved-tables", str(path),
                "--max-batches", "2", "--min-free-gib", "0",
                "--execute",
            ]),
            0,
        )
        parquet_outputs = list(self.output.rglob("compact-*.parquet"))
        self.assertEqual(len(parquet_outputs), 1)
        self.assertIn("/hthd/", str(parquet_outputs[0]))

    def test_all_tables_execute_is_refused(self):
        self.assertEqual(
            main(self.args() + ["--all-tables", "--execute"]), 1
        )
        self.assertFalse(self.output.exists())
        self.assertEqual(
            main(self.args() + ["--inventory", "--execute"]), 1
        )
        self.assertFalse(self.output.exists())


if __name__ == "__main__":
    unittest.main()
