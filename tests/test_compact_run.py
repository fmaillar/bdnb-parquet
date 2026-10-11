import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from bdnb_parquet.compact_parquet import MIB, plan, scan, write_report
from bdnb_parquet.compact_run import (
    arrow_cpu_threads,
    compare_rows,
    execute,
    main,
    output_paths,
    read_selection,
    reject_overlapping_trees,
    relative_path,
    verify,
)


class CompactionRunnerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "input"
        self.source.mkdir()
        self.plan_dir = self.root / "plan"
        self.destination = self.root / "output"
        directory = self.source / "bdnb" / "tables" / "hthd"
        directory.mkdir(parents=True)
        self.metadata = {b"geo": b'{"version": "1.1.0"}'}
        self.tables = []
        for i in range(4):
            table = pa.table({
                "id": pa.array([i * 10, None, i * 10 + 2], type=pa.int64()),
                "description": pa.array([f"x{i}", "", None]),
                "value": pa.array([float(i), 1.5, -2.0]),
            }).replace_schema_metadata(self.metadata)
            self.tables.append(table)
            pq.write_table(
                table,
                directory / f"part-{i:05d}.parquet",
                row_group_size=2,
            )
        batches = plan(scan(self.source), MIB)
        self.assertEqual(len(batches), 1)
        write_report(self.source, self.plan_dir, MIB, batches)
        self.batch_id = batches[0].batch_id

    def selection(self, max_files=None):
        return read_selection(
            self.plan_dir, self.source, self.batch_id, max_files
        )

    def test_full_compaction_and_value_comparison(self):
        selected = self.selection()
        result = execute(selected, self.destination, batch_rows=2, zstd_level=3)
        self.assertEqual(result["source_rows"], 12)
        self.assertEqual(result["verified_rows"], 12)
        self.assertFalse(result["pilot"])
        output, report = output_paths(self.destination, selected)
        self.assertTrue(output.exists())
        self.assertTrue(report.exists())
        self.assertFalse(output.with_suffix(".partial").exists())
        self.assertEqual(json.loads(report.read_text())["source_files"], 4)
        self.assertEqual(
            pq.ParquetFile(output).schema_arrow.metadata, self.metadata
        )
        self.assertEqual(
            compare_rows(
                tuple(source.path for source in selected.sources), output, 5
            ),
            12,
        )
        self.assertEqual(
            len(list(self.source.rglob("*.parquet"))), 4
        )

    def test_multi_threaded_decode_and_timing_report(self):
        selected = self.selection()
        result = execute(
            selected, self.destination, batch_rows=2, zstd_level=3,
            threads=4, row_group_rows=5, max_buffer_mib=1,
        )
        output, report = output_paths(self.destination, selected)
        self.assertEqual(result["verified_rows"], 12)
        self.assertEqual(result["threads"], 4)
        self.assertEqual(result["execution_mode"], "new-output")
        self.assertGreaterEqual(result["write_seconds"], 0)
        self.assertGreaterEqual(result["verify_seconds"], 0)
        self.assertGreaterEqual(result["total_seconds"], 0)
        self.assertIsNotNone(result["write_source_mib_per_sec"])
        self.assertIsNotNone(result["verify_source_mib_per_sec"])
        self.assertEqual(result, json.loads(report.read_text()))
        self.assertEqual(
            compare_rows(
                tuple(item.path for item in selected.sources),
                output,
                batch_rows=3,
                threads=4,
            ), 12,
        )

    def test_new_output_verifies_once_but_resume_reverifies(self):
        selected = self.selection(max_files=2)
        from bdnb_parquet import compact_run

        original_verify = compact_run.verify
        with patch.object(
            compact_run, "verify", wraps=original_verify
        ) as checked:
            first = execute(
                selected, self.destination, batch_rows=2,
                zstd_level=3, threads=2,
            )
            self.assertEqual(checked.call_count, 1)
            self.assertEqual(first["execution_mode"], "new-output")
            checked.reset_mock()
            resumed = execute(
                selected, self.destination, batch_rows=2,
                zstd_level=3, threads=2,
            )
            self.assertEqual(checked.call_count, 1)
            self.assertEqual(
                resumed["execution_mode"], "existing-output-verification"
            )
            self.assertEqual(resumed["write_seconds"], 0.0)

    def test_arrow_cpu_thread_limit_is_restored(self):
        previous = pa.cpu_count()
        with arrow_cpu_threads(2):
            self.assertEqual(pa.cpu_count(), 2)
        self.assertEqual(pa.cpu_count(), previous)
        with self.assertRaises(ValueError):
            with arrow_cpu_threads(0):
                pass

    def test_invalid_threads_cli(self):
        args = [
            "--plan-dir", str(self.plan_dir),
            "--source-root", str(self.source),
            "--output-root", str(self.destination),
            "--batch-id", self.batch_id,
            "--threads", "0",
        ]
        with self.assertRaises(SystemExit) as error:
            main(args)
        self.assertEqual(error.exception.code, 2)
        self.assertFalse(self.destination.exists())

    def test_row_groups_span_input_batch_and_file_boundaries(self):
        selected = self.selection()
        result = execute(
            selected, self.destination, batch_rows=2, zstd_level=3,
            row_group_rows=5, max_buffer_mib=1,
        )
        output, _ = output_paths(self.destination, selected)
        metadata = pq.read_metadata(output)
        sizes = [
            metadata.row_group(i).num_rows
            for i in range(metadata.num_row_groups)
        ]
        self.assertEqual(sizes, [5, 5, 2])
        self.assertEqual(result["output_row_groups"], 3)
        self.assertEqual(result["output_min_row_group_rows"], 2)
        self.assertEqual(result["output_max_row_group_rows"], 5)
        self.assertEqual(result["verified_rows"], 12)

    def test_realistic_hundred_thousand_row_groups(self):
        root = self.root / "large-input"
        root.mkdir()
        for i in range(2):
            table = pa.table({
                "id": pa.array(
                    range(i * 100_000, (i + 1) * 100_000),
                    type=pa.int32(),
                )
            })
            pq.write_table(
                table, root / f"part-{i:05d}.parquet",
                compression="zstd", row_group_size=100_000,
            )
        plan_dir = self.root / "large-plan"
        candidate_batches = plan(scan(root), 256 * MIB)
        write_report(root, plan_dir, 256 * MIB, candidate_batches)
        chosen = read_selection(
            plan_dir, root, candidate_batches[0].batch_id, None
        )
        result = execute(
            chosen, self.root / "large-output",
            batch_rows=16_384, zstd_level=3,
            row_group_rows=100_000, max_buffer_mib=16,
        )
        output, _ = output_paths(self.root / "large-output", chosen)
        metadata = pq.read_metadata(output)
        sizes = [
            metadata.row_group(i).num_rows
            for i in range(metadata.num_row_groups)
        ]
        self.assertEqual(sizes, [100_000, 100_000])
        self.assertEqual(result["verified_rows"], 200_000)

    def test_byte_buffer_budget_can_force_early_flush(self):
        root = self.root / "wide-input"
        root.mkdir()
        for i in range(2):
            pq.write_table(
                pa.table({"text": [str(i) + "x" * 600_000] * 3}),
                root / f"part-{i:05d}.parquet",
            )
        plan_dir = self.root / "wide-plan"
        candidate_batches = plan(scan(root), MIB)
        write_report(root, plan_dir, MIB, candidate_batches)
        chosen = read_selection(
            plan_dir, root, candidate_batches[0].batch_id, None
        )
        result = execute(
            chosen, self.root / "wide-output",
            batch_rows=3, zstd_level=3,
            row_group_rows=100, max_buffer_mib=1,
        )
        self.assertEqual(result["verified_rows"], 6)
        # A large Arrow record batch must be sliced rather than
        # allowing the pending buffer to exceed its configured budget.
        self.assertGreater(result["output_row_groups"], 1)

    def test_pilot_only_first_two_files_and_resume(self):
        selected = self.selection(max_files=2)
        result = execute(selected, self.destination, batch_rows=1, zstd_level=3)
        output, report = output_paths(self.destination, selected)
        self.assertTrue(result["pilot"])
        self.assertEqual(result["source_rows"], 6)
        self.assertIn("pilot-2", output.name)
        mtime = output.stat().st_mtime_ns
        resumed = execute(selected, self.destination, batch_rows=4, zstd_level=3)
        self.assertEqual(resumed["verified_rows"], 6)
        self.assertEqual(output.stat().st_mtime_ns, mtime)
        self.assertTrue(report.exists())

    def test_verify_only_refuses_missing_output(self):
        selected = self.selection(max_files=2)
        with self.assertRaises(FileNotFoundError):
            execute(
                selected, self.destination, batch_rows=2,
                zstd_level=3, verify_only=True,
            )

    def test_modified_output_is_detected(self):
        selected = self.selection(max_files=2)
        execute(selected, self.destination, batch_rows=2, zstd_level=3)
        output, _ = output_paths(self.destination, selected)
        # Preserve schema and count, change one logical value.
        data = pa.concat_tables(self.tables[:2])
        wrong = data.set_column(
            0, "id", pa.array([999, None, 2, 10, None, 12], type=pa.int64())
        )
        pq.write_table(wrong, output)
        with self.assertRaisesRegex(ValueError, "values differ"):
            verify(selected, output, 2)

    def test_source_changed_after_plan_is_rejected(self):
        selected = self.selection()
        selected.sources[0].path.write_bytes(b"corrupted")
        with self.assertRaisesRegex(ValueError, "size changed"):
            execute(selected, self.destination, batch_rows=2, zstd_level=3)
        self.assertFalse(self.destination.exists())

    def test_wrong_source_root_and_plan_conflicts_rejected(self):
        with self.assertRaisesRegex(ValueError, "different source root"):
            read_selection(self.plan_dir, self.root, self.batch_id, None)
        with self.assertRaisesRegex(ValueError, "overlap"):
            reject_overlapping_trees(
                self.source, self.source / "compact"
            )
        with self.assertRaisesRegex(ValueError, "overlap"):
            reject_overlapping_trees(self.source, self.root)

    def test_paths_reject_traversal(self):
        for value in ("/absolute", "../outside", "abc/../outside", "x\\y"):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    relative_path(value)

    def test_incorrect_plan_batch_id_is_rejected(self):
        with self.assertRaises(ValueError):
            read_selection(self.plan_dir, self.source, "../unsafe", 10)
        with self.assertRaisesRegex(ValueError, "at least 2"):
            read_selection(self.plan_dir, self.source, self.batch_id, 1)

    def test_cli_pilot_and_verify_only(self):
        args = [
            "--plan-dir", str(self.plan_dir),
            "--source-root", str(self.source),
            "--output-root", str(self.destination),
            "--batch-id", self.batch_id,
            "--max-files", "2",
            "--batch-rows", "2",
        ]
        self.assertEqual(main(args), 0)
        self.assertEqual(main(args + ["--verify-only"]), 0)

    def test_fail_closed_on_existing_unrelated_output(self):
        selected = self.selection(max_files=2)
        output, report = output_paths(self.destination, selected)
        output.parent.mkdir(parents=True)
        # A previously existing file cannot be overwritten, and is verified.
        output.write_bytes(b"unrelated")
        self.assertFalse(report.exists())
        with self.assertRaises(Exception):
            execute(selected, self.destination, batch_rows=2, zstd_level=3)
        self.assertEqual(output.read_bytes(), b"unrelated")


if __name__ == "__main__":
    unittest.main()
