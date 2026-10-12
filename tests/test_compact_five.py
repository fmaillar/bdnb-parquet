import json
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from bdnb_parquet.compact_five import TARGETS, main, read_plan, provenance_manifest
from bdnb_parquet.compact_parquet import MIB, plan, scan, write_report


class FiveDatasetCompactionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)
        self.source = self.path / "snapshot"
        self.source.mkdir()
        self.plan = self.path / "plan"
        self.output = self.path / "compacted"
        for key, relative in TARGETS.items():
            directory = self.source / relative
            if key == "datatourisme":
                directory = directory / "ntriples" / "snapshot_2026"
                names = ["part-00000.parquet", "part-00001.parquet"]
            else:
                directory = directory / "test_partition"
                names = ["distinct-source-001.parquet", "distinct-source-002.parquet"]
            directory.mkdir(parents=True)
            for idx, name in enumerate(names):
                pq.write_table(
                    pa.table({
                        "source": [idx, idx],
                        "value": [f"{key}_{idx}", f"{key}_{idx}"]
                    }),
                    directory / name,
                    compression="zstd",
                )
        batches = plan(scan(self.source.resolve()), MIB)
        write_report(self.source.resolve(), self.plan, MIB, batches)

    def cli(self, *flags):
        return main([
            "--plan-dir", str(self.plan),
            "--source-root", str(self.source),
            "--output-root", str(self.output),
            "--workers", "2", "--threads", "2",
            "--min-free-gib", "0",
            *flags,
        ])

    def test_exact_five_roots_and_provenance_offsets(self):
        selected, inventory = read_plan(self.plan, self.source.resolve())
        self.assertEqual(len(selected), 5)
        self.assertEqual(set(inventory), set(TARGETS))
        self.assertEqual(sum(v["candidate_files"] for v in inventory.values()), 10)
        self.assertEqual(sum(v["safe_part_batches"] for v in inventory.values()), 1)
        self.assertEqual(
            sum(v["independent_file_batches"] for v in inventory.values()),
            4,
        )
        manifest = provenance_manifest(selected, self.output, self.plan)
        self.assertEqual(manifest["candidate_source_files"], 10)
        by_batch = {}
        for source in manifest["sources"]:
            by_batch.setdefault(source["batch_id"], []).append(source)
        for sources in by_batch.values():
            self.assertEqual([x["first_row"] for x in sources], [0, 2])

    def test_preview_never_mutates(self):
        self.assertEqual(self.cli(), 0)
        self.assertFalse(self.output.exists())
        self.assertEqual(len(list(self.source.rglob("*.parquet"))), 10)

    def test_execute_blocks_independent_files_without_explicit_flag(self):
        self.assertEqual(self.cli("--execute"), 1)
        self.assertEqual(len(list(self.source.rglob("*.parquet"))), 10)
        self.assertEqual(len(list(self.output.rglob("compact-*.parquet"))), 0)

    def test_execute_all_five_resumable_with_source_preserved(self):
        self.assertEqual(
            self.cli("--execute", "--include-independent-files"), 0,
        )
        provenance = self.output / "_meta" / "five-datasets-provenance.json"
        obj = json.loads(provenance.read_text(encoding="utf-8"))
        self.assertEqual(obj["candidate_batches"], 5)
        self.assertEqual(len(list(self.output.rglob("compact-*.parquet"))), 5)
        self.assertEqual(len(list(self.source.rglob("*.parquet"))), 10)
        self.assertTrue(all(
            pq.read_metadata(p).num_rows == 4
            for p in self.output.rglob("compact-*.parquet")
        ))
        self.assertEqual(
            self.cli("--execute", "--include-independent-files"), 0,
        )
        self.assertEqual(len(list(self.output.rglob("compact-*.parquet"))), 5)

    def test_reject_different_plan_root(self):
        other = self.path / "other"
        other.mkdir()
        with self.assertRaisesRegex(ValueError, "source_root"):
            read_plan(self.plan, other)

    def test_reject_overlapping_output(self):
        self.assertEqual(
            main([
                "--plan-dir", str(self.plan),
                "--source-root", str(self.source),
                "--output-root", str(self.source / "nested"),
                "--execute", "--include-independent-files",
            ]),
            1,
        )
        self.assertFalse((self.source / "nested").exists())


if __name__ == "__main__":
    unittest.main()
