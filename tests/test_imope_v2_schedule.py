"""End-to-end staging-only and resumability tests for the IMoPE v2 scheduler."""

from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from bdnb_parquet.imope_v2_plan import build_plan, write_plan
from bdnb_parquet.imope_v2_schedule import (
    _exclusive_staging_lock, inspect_plan, main, schedule,
)


class ImopeV2ScheduleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.source = self.base / "imope"
        self.source.mkdir()
        for department in ("Dpt01", "Dpt02", "Dpt03"):
            self.make_file(department, "rpls.parquet", 3, geo=False)
        for department in ("Dpt04", "Dpt05"):
            self.make_file(department, "bati.parquet", 2, geo=True)
        self.plandir = self.base / "plan-2048"
        write_plan(
            build_plan(self.source, target_mib=2048),
            self.plandir, self.source,
        )
        self.out = self.base / "stage-2048"

    def make_file(self, department, name, rows, *, geo):
        location = self.source / department
        location.mkdir(parents=True, exist_ok=True)
        metadata = {b"pandas": b"test-pandas"}
        columns = [pa.field("value", pa.int64())]
        arrays = [pa.array(list(range(rows)), type=pa.int64())]
        if geo:
            bbox = [0, 0, 1, 1] if department == "Dpt04" else [2, 2, 4, 4]
            metadata[b"geo"] = json.dumps({
                "version": "1.1.0",
                "primary_column": "geometry",
                "columns": {
                    "geometry": {
                        "encoding": "WKB",
                        "geometry_types": ["Point"],
                        "crs": "EPSG:2154",
                        "bbox": bbox,
                    },
                },
            }).encode()
            columns.append(pa.field("geometry", pa.binary()))
            arrays.append(pa.array([b"point"] * rows, type=pa.binary()))
        schema = pa.schema(columns, metadata=metadata)
        pq.write_table(pa.Table.from_arrays(arrays, schema=schema),
                       location / name, compression="zstd")

    def source_hashes(self):
        return {
            p.relative_to(self.source).as_posix():
                hashlib.sha256(p.read_bytes()).hexdigest()
            for p in self.source.rglob("*") if p.is_file()
        }

    def run_batches(self, *, workers=2, execute=True):
        return schedule(
            self.plandir, self.source, self.out,
            workers=workers, threads=2, min_free_gib=0,
            batch_rows=2, row_group_rows=3,
            max_buffer_mib=1, execute=execute,
        )

    def test_plan_only_has_two_independent_geo_and_non_geo_batches(self):
        plan, summary = inspect_plan(self.plandir, self.source)
        self.assertEqual(len(plan), 2)
        self.assertEqual(summary["candidate_source_files"], 5)
        self.assertEqual(summary["candidate_rows"], 13)
        self.assertEqual(summary["projected_physical_files"], 2)

    def test_preview_is_read_only(self):
        before = self.source_hashes()
        report = self.run_batches(execute=False)
        self.assertEqual(report["mode"], "read-only")
        self.assertEqual(report["batches"], 2)
        self.assertFalse(self.out.exists())
        self.assertEqual(before, self.source_hashes())

    def test_parallel_stage_then_verify_every_existing_batch_again(self):
        before = self.source_hashes()
        initial = self.run_batches()
        self.assertEqual(initial["failed_batches"], 0)
        self.assertEqual(initial["successful_batches"], 2)
        self.assertEqual(initial["new_outputs"], 2)
        self.assertEqual(initial["existing_outputs_reverified"], 0)
        self.assertEqual(initial["verified_rows"], 13)
        self.assertTrue((self.out / "_meta" / "plan-lock.json").is_file())
        self.assertTrue((self.out / "_meta" / "last-run.json").is_file())
        self.assertTrue((self.out / "_meta" / "imope-v2-compaction-journal.jsonl").is_file())
        staged = list((self.out / "tables").rglob("*.parquet"))
        self.assertEqual(len(staged), 2)
        for result_path in staged:
            sidecar = result_path.with_suffix(".json")
            self.assertTrue(sidecar.is_file())
            record = json.loads(sidecar.read_text())
            self.assertEqual(record["mode"], "new-verified-output")
            self.assertFalse(record["publication_performed"])
            self.assertEqual(record["verified_rows"], pq.read_metadata(result_path).num_rows)
        checksums = {
            str(p): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in staged
        }

        repeat = self.run_batches(workers=1)
        self.assertEqual(repeat["failed_batches"], 0)
        self.assertEqual(repeat["new_outputs"], 0)
        self.assertEqual(repeat["existing_outputs_reverified"], 2)
        self.assertEqual(repeat["verified_rows"], 13)
        self.assertEqual(
            checksums, {
                str(p): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in staged
            },
        )
        self.assertEqual(before, self.source_hashes())

    def test_corrupted_existing_parquet_fails_and_never_overwrites(self):
        self.run_batches(workers=1)
        file = next((self.out / "tables").rglob("*.parquet"))
        file.write_bytes(b"tampered")
        corrupted_hash = hashlib.sha256(file.read_bytes()).hexdigest()
        report = self.run_batches(workers=1)
        self.assertEqual(report["failed_batches"], 1)
        self.assertEqual(
            hashlib.sha256(file.read_bytes()).hexdigest(), corrupted_hash,
        )

    def test_concurrent_stage_root_is_exclusively_locked(self):
        self.run_batches(workers=1)
        metadata = self.out / "_meta"
        with _exclusive_staging_lock(metadata):
            with self.assertRaisesRegex(RuntimeError, "Another IMoPE v2"):
                self.run_batches(workers=1)

    def test_missing_output_report_requires_manual_inspection(self):
        self.run_batches(workers=1)
        file = next((self.out / "tables").rglob("*.parquet"))
        file.with_suffix(".json").unlink()
        result = self.run_batches(workers=1)
        self.assertEqual(result["failed_batches"], 1)
        self.assertTrue(file.exists())

    def test_mismatching_lock_rejected_before_any_staging(self):
        self.run_batches(workers=1)
        path = self.plandir / "untouched.json"
        path.write_text(path.read_text() + "\n")
        with self.assertRaisesRegex(ValueError, "different plan"):
            self.run_batches(workers=1)

    def test_failed_source_preflight_writes_no_staging(self):
        self.make_file("Dpt02", "rpls.parquet", 5, geo=False)
        with self.assertRaisesRegex(ValueError, "changed since planning"):
            self.run_batches()
        self.assertFalse(self.out.exists())

    def test_refuse_source_overlap_and_invalid_resources(self):
        with self.assertRaisesRegex(ValueError, "must not overlap"):
            schedule(self.plandir, self.source, self.source / "stage", execute=False)
        with self.assertRaisesRegex(ValueError, "Scheduler requires"):
            self.run_batches(workers=5, execute=False)

    def test_cli_reports_bad_parameters(self):
        self.assertEqual(main([
            "--plan-dir", str(self.plandir),
            "--source-root", str(self.source),
            "--output-root", str(self.out),
            "--threads", "0",
        ]), 1)
        self.assertFalse(self.out.exists())


if __name__ == "__main__":
    unittest.main()
