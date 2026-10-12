import errno
import json
import os
from types import SimpleNamespace
from unittest.mock import patch
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from bdnb_parquet.compact_parquet import MIB, plan, scan, write_report
from bdnb_parquet.compact_publish import main, publish
from bdnb_parquet.compact_all import main as compact_all_main
from bdnb_parquet.compact_run import (
    execute, read_selection,
)


class PublishCompactionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name)
        self.root = self.path / "corpus"
        self.dataset = self.root / "parquet" / "bdnb"
        self.tables_root = "parquet/bdnb/tables"
        self.tables = self.dataset / "tables"
        self.plan_dir = self.path / "plan"
        self.output_root = self.path / "compacted"
        self.dataset.mkdir(parents=True)
        (self.dataset / "schemas").mkdir()
        (self.dataset / "dataset.json").write_text(
            json.dumps({"dataset": "BDNB", "source": "test fixture"})
        )
        for name, parts in (("many", 3), ("singleton", 1)):
            directory = self.tables / name
            directory.mkdir(parents=True)
            total = 0
            for index in range(parts):
                table = pa.table({
                    "id": pa.array([index * 5 + j for j in range(5)]),
                    "name": [name] * 5,
                })
                pq.write_table(
                    table, directory / f"part-{index:05d}.parquet",
                    compression="zstd",
                )
                total += table.num_rows
            metadata = {
                "status": "ok", "table": name, "parts": parts,
                "rows": total, "parquet_bytes": sum(
                    p.stat().st_size for p in directory.glob("*.parquet")
                ),
                "file_rows": 250000, "batch_rows": 100000,
                "compression_level": 3, "source": "test COPY",
                "columns": ["id", "name"],
                "geometry_columns": [], "fallback_types": [],
                "completed_at": "2026-10-11T00:00:00Z",
            }
            (directory / "_SUCCESS.json").write_text(json.dumps(metadata))
            (self.dataset / "schemas" / f"{name}.json").write_text(
                json.dumps(metadata)
            )
        (self.dataset / "manifest.json").write_text("[]")
        pq.write_table(pa.table({"wrong": [0]}), self.dataset / "manifest.parquet")
        write_report(
            self.root.resolve(), self.plan_dir, MIB,
            plan(scan(self.root.resolve()), MIB),
        )
        import csv
        with (self.plan_dir / "batches.tsv").open(
            newline="", encoding="utf-8"
        ) as stream:
            selected_rows = list(csv.DictReader(stream, delimiter="\t"))
        chosen = next(
            row for row in selected_rows
            if row["status"] == "review-required"
        )
        self.selection = read_selection(
            self.plan_dir, self.root.resolve(),
            chosen["batch_id"], None,
        )

    def compact(self):
        execute(
            self.selection, self.output_root,
            batch_rows=3, zstd_level=3,
            row_group_rows=6, threads=2,
        )

    def kwargs(self):
        return {
            "source_root": self.root,
            "plan_dir": self.plan_dir,
            "compact_root": self.output_root,
            "tables_relative": self.tables_root,
        }

    def test_rw_mount_without_parent_write_permission_fails_fast(self):
        args = [
            "--plan-dir", str(self.plan_dir),
            "--source-root", str(self.root),
            "--output-root", str(self.output_root),
            "--tables-root", self.tables_root,
            "--execute", "--prune-originals",
        ]
        with patch(
            "bdnb_parquet.compact_publish.tempfile.mkdtemp",
            side_effect=PermissionError(13, "Permission denied"),
        ):
            self.assertEqual(compact_all_main(args), 1)
            with self.assertRaises(PermissionError):
                publish(**self.kwargs(), prune_backup=True)
        self.assertFalse(self.output_root.exists())
        self.assertFalse(self.dataset.with_name("bdnb.compaction-stage").exists())
        self.assertEqual(len(list(self.tables.rglob("part-*.parquet"))), 4)

    def test_read_only_dataset_fails_before_compaction_or_verification(self):
        args = [
            "--plan-dir", str(self.plan_dir),
            "--source-root", str(self.root),
            "--output-root", str(self.output_root),
            "--tables-root", self.tables_root,
            "--execute", "--prune-originals",
        ]
        with patch(
            "bdnb_parquet.compact_publish.os.statvfs",
            return_value=SimpleNamespace(f_flag=os.ST_RDONLY),
        ):
            self.assertEqual(compact_all_main(args), 1)
            with self.assertRaises(OSError) as caught:
                publish(**self.kwargs(), prune_backup=True)
            self.assertEqual(caught.exception.errno, errno.EROFS)
        self.assertFalse(self.output_root.exists())
        self.assertFalse(self.dataset.with_name("bdnb.compaction-stage").exists())
        self.assertEqual(len(list(self.tables.rglob("part-*.parquet"))), 4)

    def test_missing_compacted_output_does_not_touch_original(self):
        old_files = list(self.tables.rglob("*.parquet"))
        with self.assertRaises(FileNotFoundError):
            publish(**self.kwargs(), prune_backup=True)
        self.assertEqual(len(list(self.tables.rglob("*.parquet"))), 4)
        self.assertTrue(all(path.exists() for path in old_files))
        self.assertFalse(self.dataset.with_name("bdnb.precompact-backup").exists())

    def test_publish_and_prune_only_redundant_parts(self):
        self.compact()
        result = publish(**self.kwargs(), prune_backup=True)
        self.assertEqual(result["tables"], 2)
        self.assertEqual(result["verified_batches"], 1)
        self.assertEqual(result["source_files"], 4)
        self.assertEqual(result["output_files"], 2)
        self.assertEqual(result["redundant_source_files"], 2)
        self.assertEqual(result["verified_rows"], 20)
        self.assertTrue(result["pruned"])
        self.assertFalse(self.dataset.with_name("bdnb.precompact-backup").exists())
        self.assertFalse(self.dataset.with_name("bdnb.compaction-stage").exists())
        self.assertEqual(len(list(self.tables.rglob("*.parquet"))), 2)
        self.assertTrue((self.tables / "singleton" / "part-00000.parquet").exists())
        self.assertEqual(
            pq.read_metadata(self.tables / "many" / "part-00000.parquet").num_rows,
            15,
        )
        original = json.loads(
            (self.tables / "many" / "_SUCCESS.json").read_text()
        )
        self.assertEqual(original["rows"], 15)
        self.assertEqual(original["parts"], 1)
        self.assertEqual(original["compaction"]["source_parts"], 3)
        self.assertEqual(original["file_rows"], None)
        self.assertEqual(
            json.loads((self.dataset / "manifest.json").read_text())[0]["parts"],
            1,
        )
        self.assertEqual(
            pq.read_metadata(self.dataset / "manifest.parquet").num_rows,
            2,
        )
        self.assertEqual(
            json.loads((self.dataset / "schemas" / "many.json").read_text())["parts"],
            1,
        )
        self.assertTrue((self.dataset / "_COMPACTION.json").exists())

    def test_preserve_original_backup_by_default(self):
        self.compact()
        report = publish(**self.kwargs(), prune_backup=False)
        backup = Path(report["backup"])
        self.assertTrue(backup.exists())
        self.assertEqual(len(list(backup.rglob("*.parquet"))), 5)
        self.assertEqual(len(list(self.tables.rglob("*.parquet"))), 2)

    def test_original_success_mismatch_refuses_publish(self):
        self.compact()
        marker = self.tables / "many" / "_SUCCESS.json"
        obj = json.loads(marker.read_text())
        obj["rows"] = -1
        marker.write_text(json.dumps(obj))
        with self.assertRaisesRegex(ValueError, "_SUCCESS metadata mismatch"):
            publish(**self.kwargs(), prune_backup=True)
        self.assertTrue((self.tables / "many" / "part-00001.parquet").exists())
        self.assertFalse(self.dataset.with_name("bdnb.precompact-backup").exists())

    def test_refuses_existing_backup(self):
        self.compact()
        backup = self.dataset.with_name("bdnb.precompact-backup")
        backup.mkdir()
        with self.assertRaisesRegex(RuntimeError, "previously interrupted"):
            publish(**self.kwargs(), prune_backup=True)
        self.assertTrue((self.tables / "many" / "part-00001.parquet").exists())

    def test_all_in_one_run_and_prune(self):
        args = [
            "--plan-dir", str(self.plan_dir),
            "--source-root", str(self.root),
            "--output-root", str(self.output_root),
            "--tables-root", self.tables_root,
            "--workers", "2", "--threads", "2",
            "--min-free-gib", "0",
        ]
        self.assertEqual(compact_all_main(args), 0)
        self.assertEqual(len(list(self.tables.rglob("part-*.parquet"))), 4)
        self.assertFalse(self.output_root.exists())
        self.assertEqual(
            compact_all_main(args + ["--execute", "--prune-originals"]), 0
        )
        self.assertEqual(len(list(self.tables.rglob("part-*.parquet"))), 2)
        self.assertFalse(
            self.dataset.with_name("bdnb.precompact-backup").exists()
        )
        self.assertTrue((self.dataset / "_COMPACTION.json").exists())

    def test_all_in_one_refuses_prune_without_execute(self):
        args = [
            "--plan-dir", str(self.plan_dir),
            "--source-root", str(self.root),
            "--output-root", str(self.output_root),
            "--tables-root", self.tables_root,
            "--prune-originals",
        ]
        with self.assertRaises(SystemExit):
            compact_all_main(args)
        self.assertEqual(len(list(self.tables.rglob("part-*.parquet"))), 4)

    def test_cli_requires_explicit_execute(self):
        args = [
            "--plan-dir", str(self.plan_dir),
            "--source-root", str(self.root),
            "--output-root", str(self.output_root),
            "--tables-root", self.tables_root,
        ]
        with self.assertRaises(SystemExit):
            main(args)
        self.assertTrue((self.tables / "many" / "part-00002.parquet").exists())


if __name__ == "__main__":
    unittest.main()
