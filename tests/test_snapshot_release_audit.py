"""Snapshot audit excludes backup trees without ever changing the release."""

import csv
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from bdnb_parquet.snapshot_release_audit import audit, inventory


class SnapshotReleaseAuditTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.root = self.base / "france-open-data-parquet-2026-10-10"
        (self.root / "parquet" / "dataset").mkdir(parents=True)
        self.a = self.root / "parquet" / "dataset" / "compact-v2.parquet"
        pq.write_table(pa.table({"id": [1, 2, 3]}), self.a)
        self.b = self.root / "parquet" / "dataset" / "dataset.json"
        self.b.write_text('{"rows": 1}\n')
        backup = self.root / "parquet" / "dataset.precompact-backup"
        backup.mkdir()
        (backup / "obsolete.parquet").write_bytes(b"old-published-bytes")
        self.stage = self.root / "parquet" / "dataset.compaction-stage"
        self.stage.mkdir()
        (self.stage / "unfinished.parquet").write_bytes(b"incomplete")

    def snapshot(self):
        return {
            p.relative_to(self.root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in self.root.rglob("*") if p.is_file()
        }

    def test_read_only_count_with_detected_backup_dirs(self):
        before = self.snapshot()
        result = audit(self.root)
        self.assertEqual(result["payload_files"], 2)
        self.assertEqual(result["parquet_files"], 1)
        self.assertEqual(result["other_files"], 1)
        self.assertEqual(result["excluded_directories_count"], 2)
        self.assertTrue(result["torrent_payload_root_not_clean"])
        self.assertFalse(result["torrent_created"])
        self.assertEqual(before, self.snapshot())

    def test_external_sha256_manifest_has_only_current_payload(self):
        before = self.snapshot()
        output = self.base / "external-inventory"
        result = audit(self.root, output_dir=output, write_manifest=True)
        self.assertFalse(result["torrent_ready"])
        self.assertEqual(result["parquet_hashed"], 1)
        self.assertEqual(result["other_hashed"], 1)
        with (output / "files.sha256.tsv").open(newline="") as fh:
            rows = list(csv.DictReader(fh, delimiter="\t"))
        self.assertEqual(
            [row["path"] for row in rows],
            ["parquet/dataset/compact-v2.parquet", "parquet/dataset/dataset.json"],
        )
        self.assertEqual(rows[0]["sha256"], self.digest(self.a))
        self.assertEqual(rows[1]["sha256"], self.digest(self.b))
        self.assertEqual(before, self.snapshot())
        self.assertEqual(
            json.loads((output / "summary.json").read_text())["payload_files"], 2,
        )
        with self.assertRaises(FileExistsError):
            audit(self.root, output_dir=output, write_manifest=True)

    @staticmethod
    def digest(path):
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def test_symlink_is_rejected_not_traversed(self):
        link = self.root / "parquet" / "dataset" / "alias.parquet"
        link.symlink_to(self.a)
        with self.assertRaisesRegex(ValueError, "nonregular"):
            inventory(self.root)

    def test_manifest_must_live_outside_corpus(self):
        with self.assertRaisesRegex(ValueError, "cannot overlap"):
            audit(
                self.root,
                output_dir=self.root / "manifest-output",
                write_manifest=True,
            )

    def test_torrent_file_inside_payload_is_a_blocker(self):
        (self.root / "old.torrent").write_bytes(b"stale")
        with self.assertRaisesRegex(ValueError, "Torrent file inside"):
            audit(self.root)

    def test_all_parquet_footers_and_row_counts(self):
        output = self.base / "verified-manifest"
        result = audit(
            self.root, output_dir=output,
            write_manifest=True, verify_parquet=True,
        )
        self.assertEqual(result["parquet_footers_verified"], 1)
        self.assertEqual(result["parquet_rows"], 3)
        with (output / "files.sha256.tsv").open(newline="") as fh:
            rows = list(csv.DictReader(fh, delimiter="\t"))
        self.assertEqual(rows[0]["rows"], "3")
        self.assertEqual(rows[0]["row_groups"], "1")

    def test_piece_count_for_simple_files(self):
        result = audit(self.root, piece_mib=1)
        self.assertEqual(result["v1_pieces"], 1)
        self.assertTrue(result["v1_torrent_under_10mib_estimate"])


if __name__ == "__main__":
    unittest.main()
