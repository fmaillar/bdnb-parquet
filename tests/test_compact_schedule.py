import csv
import json
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from bdnb_parquet.compact_parquet import MIB, plan, scan, write_report
from bdnb_parquet.compact_schedule import (
    check_disk_budget,
    load_candidates,
    main,
    run_schedule,
)


class SchedulerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "source"
        self.plan_dir = self.root / "plan"
        self.output = self.root / "output"
        self.tables_root = "territoire/bdnb/tables"
        for name in ("batiment_groupe_hthd", "batiment_groupe_risques"):
            directory = self.source / self.tables_root / name
            directory.mkdir(parents=True)
            for i in range(3):
                pq.write_table(
                    pa.table({
                        "id": pa.array([i * 10 + k for k in range(20)]),
                        "label": pa.array([name] * 20),
                    }),
                    directory / f"part-{i:05d}.parquet",
                    compression="zstd",
                    row_group_size=10,
                )
        write_report(self.source, self.plan_dir, MIB, plan(scan(self.source), MIB))

    def candidates(self, *, table="batiment_groupe_hthd", limit=1):
        return load_candidates(
            self.plan_dir,
            self.source.resolve(),
            self.tables_root,
            {table},
            all_tables=False,
            limit=limit,
        )

    def args(self, *, table="batiment_groupe_hthd"):
        return [
            "--plan-dir", str(self.plan_dir),
            "--source-root", str(self.source),
            "--output-root", str(self.output),
            "--tables-root", self.tables_root,
            "--table", table,
            "--max-batches", "1",
            "--min-free-gib", "0",
            "--threads", "2",
            "--batch-rows", "7",
            "--row-group-rows", "20",
        ]

    def test_explicit_dry_run_never_creates_output(self):
        self.assertEqual(main(self.args()), 0)
        self.assertFalse(self.output.exists())

    def test_requires_explicit_table_selector(self):
        args = self.args()
        i = args.index("--table")
        del args[i:i + 2]
        with self.assertRaises(SystemExit) as caught:
            main(args)
        self.assertEqual(caught.exception.code, 2)

    def test_selected_table_only(self):
        selected = self.candidates()
        self.assertEqual(len(selected), 1)
        self.assertEqual(len(selected[0].sources), 3)
        self.assertTrue(selected[0].directory.endswith("/batiment_groupe_hthd"))
        self.assertEqual(
            len(load_candidates(
                self.plan_dir, self.source.resolve(), self.tables_root,
                set(), all_tables=True, limit=5,
            )),
            2,
        )

    def test_rejects_wrong_tables_root_and_unknown_table(self):
        with self.assertRaisesRegex(ValueError, "ending in"):
            load_candidates(
                self.plan_dir, self.source.resolve(),
                "territoire/bdnb",
                {"batiment_groupe_hthd"}, all_tables=False, limit=1,
            )
        with self.assertRaisesRegex(ValueError, "No review-required"):
            self.candidates(table="no_such_table")
        with self.assertRaisesRegex(ValueError, "Invalid table name"):
            self.candidates(table="../escape")

    def test_reject_nonconsecutive_parts(self):
        files = self.plan_dir / "files.tsv"
        with files.open(encoding="utf-8", newline="") as stream:
            rows = list(csv.DictReader(stream, delimiter="\t"))
            columns = list(rows[0])
        for row in rows:
            if row["path"].endswith("part-00001.parquet"):
                row["path"] = row["path"].replace(
                    "part-00001.parquet", "part-00004.parquet"
                )
                break
        with files.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(
                stream, fieldnames=columns,
                delimiter="\t", lineterminator="\n",
            )
            writer.writeheader()
            writer.writerows(rows)
        with self.assertRaisesRegex(ValueError, "non-consecutive"):
            self.candidates()

    def test_disk_budget_rejects_unavailable_reserve(self):
        with self.assertRaisesRegex(RuntimeError, "Insufficient free"):
            check_disk_budget(
                self.output, self.candidates(), 1, min_free_gib=10_000_000,
            )

    def test_sequential_execution_journal_and_verified_resume(self):
        args = self.args()
        self.assertEqual(main(args + ["--execute"]), 0)
        metadata = self.output / "_meta"
        summary = json.loads((metadata / "last-run.json").read_text())
        self.assertEqual(summary["successful_batches"], 1)
        self.assertEqual(summary["failed_batches"], 0)
        self.assertEqual(summary["files_reduced"], 2)
        self.assertEqual(summary["verified_rows"], 60)
        self.assertEqual(summary["workers"], 1)
        self.assertEqual(summary["threads_per_worker"], 2)
        self.assertTrue((metadata / "compaction-journal.jsonl").exists())
        path = next(self.output.rglob("compact-*.parquet"))
        mtime = path.stat().st_mtime_ns
        first_events = [
            json.loads(line)["event"]
            for line in (metadata / "compaction-journal.jsonl").read_text().splitlines()
        ]
        self.assertEqual(
            first_events,
            ["run-start", "start", "success", "run-end"],
        )
        self.assertEqual(main(args + ["--execute"]), 0)
        self.assertEqual(path.stat().st_mtime_ns, mtime)
        tail = [
            json.loads(line)
            for line in (metadata / "compaction-journal.jsonl").read_text().splitlines()
        ]
        self.assertEqual(len(tail), 8)
        self.assertEqual(tail[-2]["mode"], "existing-output-verification")
        self.assertEqual(len(list(self.source.rglob("*.parquet"))), 6)

    def test_two_processes_compact_two_different_tables(self):
        args = [
            "--plan-dir", str(self.plan_dir),
            "--source-root", str(self.source),
            "--output-root", str(self.output),
            "--tables-root", self.tables_root,
            "--table", "batiment_groupe_hthd",
            "--table", "batiment_groupe_risques",
            "--max-batches", "2",
            "--workers", "2",
            "--threads", "2",
            "--min-free-gib", "0",
            "--execute",
        ]
        self.assertEqual(main(args), 0)
        summary = json.loads(
            (self.output / "_meta" / "last-run.json").read_text()
        )
        self.assertEqual(summary["successful_batches"], 2)
        self.assertEqual(summary["failed_batches"], 0)
        self.assertEqual(summary["verified_rows"], 120)
        self.assertEqual(summary["files_reduced"], 4)
        self.assertEqual(len(list(self.output.rglob("compact-*.parquet"))), 2)

    def test_four_processes_compact_four_separate_tables(self):
        import shutil
        for name in ("batiment_groupe_local", "batiment_groupe_adresse"):
            directory = self.source / self.tables_root / name
            directory.mkdir(parents=True)
            for index in range(3):
                pq.write_table(
                    pa.table({
                        "id": pa.array([index * 10 + k for k in range(20)]),
                        "label": pa.array([name] * 20),
                    }),
                    directory / f"part-{index:05d}.parquet",
                    row_group_size=10,
                )
        shutil.rmtree(self.plan_dir)
        write_report(
            self.source, self.plan_dir, MIB,
            plan(scan(self.source), MIB),
        )
        args = [
            "--plan-dir", str(self.plan_dir),
            "--source-root", str(self.source),
            "--output-root", str(self.output),
            "--tables-root", self.tables_root,
            "--table", "batiment_groupe_hthd",
            "--table", "batiment_groupe_risques",
            "--table", "batiment_groupe_local",
            "--table", "batiment_groupe_adresse",
            "--max-batches", "4",
            "--workers", "4",
            "--threads", "2",
            "--min-free-gib", "0",
            "--execute",
        ]
        self.assertEqual(main(args), 0)
        summary = json.loads(
            (self.output / "_meta" / "last-run.json").read_text()
        )
        self.assertEqual(summary["successful_batches"], 4)
        self.assertEqual(summary["failed_batches"], 0)
        self.assertEqual(summary["verified_rows"], 240)
        self.assertEqual(summary["files_reduced"], 8)
        self.assertEqual(len(list(self.output.rglob("compact-*.parquet"))), 4)

    def test_cli_no_execute_does_not_create_journal(self):
        self.assertEqual(main(self.args()), 0)
        self.assertFalse((self.output / "_meta").exists())


if __name__ == "__main__":
    unittest.main()
