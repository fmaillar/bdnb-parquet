"""Five-dataset publication integration tests using converter-shaped manifests."""

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from bdnb_parquet.compact_five import TARGETS, main as compact_main, read_plan
from bdnb_parquet.compact_five_publish import (
    main as publish_main, publish, validate_source_matches_plan,
)
from bdnb_parquet.compact_parquet import MIB, plan, scan, write_report
from bdnb_parquet.imope_promote import validate_parquet


class FiveDatasetPublicationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.root = self.base / "snapshot"
        self.root.mkdir()
        self.plan = self.base / "plan"
        self.outputs = self.base / "outputs"
        self.dirs = {key: self.root / rel for key, rel in TARGETS.items()}
        self.excluded = self.root / "other-dataset" / "keep.parquet"
        self.write_part(self.excluded, "other", 0)

        for key in ("waldec", "import"):
            for index in range(2):
                self.write_part(
                    self.dirs[key] / "by-source" / f"event-{index}.parquet",
                    key, index,
                )

        imope_dir = self.dirs["imope"] / "67"
        for index in range(2):
            self.write_part(imope_dir / f"im-{index}.parquet", "imope", index)
        descriptions = sorted(
            (validate_parquet(p) for p in imope_dir.glob("*.parquet")),
            key=lambda x: x["path"],
        )
        dept = {
            "department": "67", "status": "ok", "rows": 8,
            "files": len(descriptions), "geo_files": 0,
            "bytes": sum(x["bytes"] for x in descriptions),
            "parquet": descriptions,
        }
        self.put_json(imope_dir / "_SUCCESS.json", dept)
        self.put_json(self.dirs["imope"] / "manifest.json", [dept])
        self.put_json(self.dirs["imope"] / "dataset.json", {
            "dataset": "IMOPE", "rows": 8, "files": 2,
            "bytes": dept["bytes"], "geo_files": 0,
        })

        vigieau_entries = []
        for idx in range(2):
            rel = f"geojson/2025/arretes-2025-12-{idx+1:02d}.parquet"
            target = self.dirs["vigieau"] / rel
            self.write_part(target, "vigieau", idx)
            vigieau_entries.append({
                "source_file": f"archive-{idx}.zip",
                "source_member": f"arretes-{idx}.geojson",
                "output": (
                    rel.replace("geojson/2025/", "geojson/"
                                "cartes-des-zones-et-arr-t-s-en-vigueur-geojson-2025/")
                    if idx == 0 else rel
                ),
                "rows": 4, "bytes": target.stat().st_size,
                "geo": False, "status": "ok",
            })
        self.put_json(self.dirs["vigieau"] / "manifest.json", vigieau_entries)
        self.put_json(self.dirs["vigieau"] / "dataset.json", {
            "dataset": "Vigieau", "rows": 8, "bytes": sum(
                x["bytes"] for x in vigieau_entries
            ), "parquet_files": 2, "geo_files": 0,
        })

        nt_dir = self.dirs["datatourisme"] / "ntriples" / "snap"
        for index in range(3):
            self.write_part(
                nt_dir / f"part-{index:05d}.parquet", "datatourisme", index,
            )
        bytes_nt = sum(p.stat().st_size for p in nt_dir.glob("*.parquet"))
        self.put_json(nt_dir / "_SUCCESS.json", {
            "status": "ok", "source": "/raw/snap.nt.zip",
            "rows": 12, "parts": 3, "bytes": bytes_nt,
        })
        csv = self.dirs["datatourisme"] / "csv" / "dataset.parquet"
        self.write_part(csv, "csv", 0)
        self.put_json(self.dirs["datatourisme"] / "manifest.json", {
            "dataset": "DataTourisme",
            "ntriples": [{"source": "/raw/snap.nt.zip", "rows": 12,
                          "parts": 3, "bytes": bytes_nt, "status": "ok"}],
            "csv": [{"source": "dataset.csv", "output": "csv/dataset.parquet",
                     "rows": 4, "bytes": csv.stat().st_size}],
            "rows": 16, "bytes": bytes_nt + csv.stat().st_size,
        })
        self.put_json(self.dirs["datatourisme"] / "dataset.json", {
            "dataset": "DataTourisme", "rows": 16,
            "bytes": bytes_nt + csv.stat().st_size,
        })

        write_report(
            self.root.resolve(), self.plan, MIB,
            plan(scan(self.root.resolve()), MIB),
        )
        self.assertEqual(
            compact_main([
                "--plan-dir", str(self.plan),
                "--source-root", str(self.root),
                "--output-root", str(self.outputs),
                "--workers", "2", "--threads", "2", "--min-free-gib", "0",
                "--execute", "--include-independent-files",
            ]),
            0,
        )

    def write_part(self, path, category, idx):
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table({
                "id": pa.array(list(range(idx * 4, idx * 4 + 4))),
                "category": [category] * 4,
            }), path, compression="zstd",
        )

    def put_json(self, path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value), encoding="utf-8")

    def run_pub(self, *args):
        return publish_main([
            "--plan-dir", str(self.plan),
            "--source-root", str(self.root),
            "--output-root", str(self.outputs),
            "--min-free-gib", "0", *args,
        ])

    def test_source_plan_alias_and_native_dataset_paths_are_distinct(self):
        # A bind mount may expose the exact same dataset under two textually
        # different parent paths; never call Path.relative_to(bind_root)
        # on a file enumerated through the native dataset prefix.
        native_alias = self.base / "native-alias"
        native_alias.symlink_to(self.root, target_is_directory=True)
        selected, inventory = read_plan(self.plan, self.root.resolve())
        for dataset_key, prefix in TARGETS.items():
            original = native_alias / prefix
            groups = [entry for entry in selected if entry[0] == dataset_key]
            self.assertNotEqual(
                (original / "placeholder.parquet").parent,
                self.root / prefix,
            )
            validate_source_matches_plan(
                original, dataset_key, inventory[dataset_key], groups,
            )
        self.assertEqual(len(list(self.root.rglob("*.parquet"))), 13)

    def test_physical_native_dataset_extra_file_is_rejected(self):
        selected, inventory = read_plan(self.plan, self.root.resolve())
        self.write_part(
            self.dirs["waldec"] / "by-source" / "new-unplanned.parquet",
            "waldec", 99,
        )
        with self.assertRaisesRegex(ValueError, "Source file count changed"):
            validate_source_matches_plan(
                self.dirs["waldec"], "waldec", inventory["waldec"],
                [entry for entry in selected if entry[0] == "waldec"],
            )

    def test_datatourisme_csv_diagnostic_lists_discrepancies(self):
        path = self.dirs["datatourisme"] / "manifest.json"
        metadata = json.loads(path.read_text(encoding="utf-8"))
        metadata["csv"][0]["output"] = "csv/never-created.parquet"
        self.put_json(path, metadata)
        with self.assertRaisesRegex(
            ValueError,
            "missing_on_disk=1.*csv/never-created.parquet.*unlisted_on_disk=1.*csv/dataset.parquet",
        ):
            publish(
                plan_dir=self.plan, source_root=self.root,
                output_root=self.outputs, writable_source_root=None,
                prune_backup=False, execute=False, min_free_gib=0,
            )
        self.assertTrue(
            (self.dirs["datatourisme"] / "csv/dataset.parquet").exists()
        )
        self.assertFalse(
            self.dirs["datatourisme"].with_name(
                "datatourisme.precompact-backup"
            ).exists()
        )

    def test_missing_zero_row_csv_manifest_entry_is_reconciled_and_audited(self):
        manifest_path = self.dirs["datatourisme"] / "manifest.json"
        dataset_path = self.dirs["datatourisme"] / "dataset.json"
        original = json.loads(manifest_path.read_text(encoding="utf-8"))
        missing = {
            "source": "datatourisme-reg-myt.csv",
            "output": "csv/datatourisme-reg-myt.parquet",
            "rows": 0, "row_groups": 0, "bytes": 2886,
            "resumed": False,
        }
        original["csv"].append(missing)
        original["csv_files"] = len(original["csv"])
        original["bytes"] += missing["bytes"]
        self.put_json(manifest_path, original)
        dataset = json.loads(dataset_path.read_text(encoding="utf-8"))
        dataset.update({
            "csv_files": len(original["csv"]), "bytes": original["bytes"],
        })
        self.put_json(dataset_path, dataset)

        # The preflight must be strictly read-only and accept ONLY this
        # zero-row discrepancy. The source JSON must remain unchanged.
        before = manifest_path.read_bytes()
        self.assertEqual(self.run_pub(), 0)
        self.assertEqual(manifest_path.read_bytes(), before)
        self.assertFalse(
            (self.dirs["datatourisme"] / "csv/datatourisme-reg-myt.parquet").exists()
        )

        result = publish(
            plan_dir=self.plan, source_root=self.root,
            output_root=self.outputs, writable_source_root=None,
            prune_backup=True, execute=True, min_free_gib=0,
        )
        self.assertEqual(result["verified_batches"], 5)
        new_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(len(new_manifest["csv"]), 1)
        self.assertEqual(new_manifest["csv_files"], 1)
        self.assertEqual(new_manifest["rows"], 16)
        self.assertEqual(new_manifest["bytes"], original["bytes"] - 2886 - (
            original["csv"][0]["bytes"]
            - (self.dirs["datatourisme"] / "csv/dataset.parquet").stat().st_size
        ) - (
            original["ntriples"][0]["bytes"]
            - sum(
                p.stat().st_size for p in (
                    self.dirs["datatourisme"] / "ntriples/snap"
                ).glob("*.parquet")
            )
        ))
        omitted = new_manifest["compaction_omitted_empty_csv"]
        self.assertEqual(len(omitted), 1)
        self.assertEqual(omitted[0]["output"], missing["output"])
        self.assertEqual(omitted[0]["rows"], 0)
        self.assertEqual(omitted[0]["row_groups"], 0)
        new_dataset = json.loads(dataset_path.read_text(encoding="utf-8"))
        self.assertEqual(new_dataset["csv_files"], 1)
        self.assertEqual(new_dataset["compaction_omitted_empty_csv"], omitted)
        self.assertEqual(len(list(self.root.rglob("*.parquet"))), 7)

    def test_missing_nonempty_csv_manifest_entry_fails_closed(self):
        manifest_path = self.dirs["datatourisme"] / "manifest.json"
        original = json.loads(manifest_path.read_text(encoding="utf-8"))
        original["csv"].append({
            "source": "missing.csv",
            "output": "csv/missing.parquet",
            "rows": 1, "row_groups": 1, "bytes": 2886,
        })
        self.put_json(manifest_path, original)
        self.assertEqual(self.run_pub(), 1)
        self.assertTrue((self.dirs["datatourisme"] / "csv/dataset.parquet").is_file())
        self.assertFalse(
            self.dirs["datatourisme"].with_name(
                "datatourisme.precompact-backup"
            ).exists()
        )

    def test_missing_zero_row_csv_with_nonzero_row_groups_fails_closed(self):
        manifest_path = self.dirs["datatourisme"] / "manifest.json"
        original = json.loads(manifest_path.read_text(encoding="utf-8"))
        original["csv"].append({
            "source": "missing.csv",
            "output": "csv/missing.parquet",
            "rows": 0, "row_groups": 1, "bytes": 2886,
        })
        self.put_json(manifest_path, original)
        self.assertEqual(self.run_pub(), 1)

    def test_preview_does_not_modify_sources(self):
        self.assertEqual(self.run_pub(), 0)
        self.assertEqual(len(list(self.root.rglob("*.parquet"))), 13)
        self.assertTrue((self.dirs["vigieau"] / "geojson/2025/arretes-2025-12-01.parquet").exists())
        self.assertFalse(self.dirs["vigieau"].with_name("vigieau.compaction-stage").exists())

    def test_publish_all_five_and_prune(self):
        before = self.excluded.read_bytes()
        report = publish(
            plan_dir=self.plan, source_root=self.root,
            output_root=self.outputs, writable_source_root=None,
            prune_backup=True, execute=True, min_free_gib=0,
        )
        self.assertEqual(report["verified_batches"], 5)
        self.assertEqual(report["redundant_source_files"], 6)
        self.assertTrue(report["pruned"])
        self.assertEqual(self.excluded.read_bytes(), before)
        self.assertEqual(len(list(self.root.rglob("*.parquet"))), 7)
        self.assertEqual(
            pq.read_metadata(self.dirs["datatourisme"] / "ntriples/snap/part-00000.parquet").num_rows,
            12,
        )
        nt = json.loads(
            (self.dirs["datatourisme"] / "ntriples/snap/_SUCCESS.json").read_text()
        )
        self.assertEqual(nt["parts"], 1)
        self.assertEqual(len(list((self.dirs["imope"] / "67").glob("*.parquet"))), 1)
        imo = json.loads((self.dirs["imope"] / "manifest.json").read_text())
        self.assertEqual(imo[0]["files"], 1)
        self.assertEqual(imo[0]["rows"], 8)
        vigi = json.loads((self.dirs["vigieau"] / "manifest.json").read_text())
        self.assertEqual(len(vigi), 1)
        self.assertEqual(vigi[0]["rows"], 8)
        self.assertEqual(len(vigi[0]["original_sources"]), 2)
        self.assertTrue(any(
            "cartes-des-zones-et-arr-t-s-en-vigueur-geojson-2025"
            in x["legacy_output"] for x in vigi[0]["original_sources"]
        ))
        for key, directory in self.dirs.items():
            idx = json.loads((directory / "_COMPACTION_SOURCES.json").read_text())
            self.assertEqual(idx["dataset"], key)
            for row in idx["sources"]:
                file = directory / row["published"]
                self.assertTrue(file.exists())
                self.assertLessEqual(row["first_row"] + row["rows"],
                                     pq.read_metadata(file).num_rows)
            self.assertFalse(directory.with_name(
                directory.name + ".precompact-backup"
            ).exists())

    def test_unknown_manifest_reference_fails_before_publication(self):
        self.put_json(
            self.dirs["waldec"] / "unknown.json",
            {"current_file": "event-0.parquet"},
        )
        self.assertEqual(self.run_pub("--execute", "--prune-backup"), 1)
        self.assertTrue(
            (self.dirs["waldec"] / "by-source/event-0.parquet").exists()
        )
        self.assertFalse(
            self.dirs["waldec"].with_name("waldec.precompact-backup").exists()
        )

    def test_corrupted_output_report_refuses_swap(self):
        output = next(self.outputs.rglob("compact-*.parquet"))
        output.write_bytes(b"corrupted")
        self.assertEqual(self.run_pub("--execute", "--prune-backup"), 1)
        self.assertEqual(len(list(self.root.rglob("*.parquet"))), 13)
        self.assertFalse(any(
            p.name.endswith(".precompact-backup")
            for p in self.root.rglob("*")
        ))

    def test_publication_rollback_after_second_swap_failure(self):
        real_replace = os.replace
        fail_stage = self.dirs["datatourisme"].with_name(
            "datatourisme.compaction-stage"
        )

        def replace_with_failure(src, dest):
            if Path(src) == fail_stage and Path(dest) == self.dirs["datatourisme"]:
                raise OSError("simulated rename failure")
            return real_replace(src, dest)

        with patch("bdnb_parquet.compact_five_publish.os.replace",
                   side_effect=replace_with_failure):
            self.assertEqual(self.run_pub("--execute", "--prune-backup"), 1)
        self.assertEqual(
            sum(len(list(directory.rglob("*.parquet")))
                for directory in self.dirs.values()) + 1,
            13,
        )
        self.assertTrue(
            (self.dirs["waldec"] / "by-source/event-0.parquet").exists()
        )
        self.assertTrue(
            (self.dirs["datatourisme"] / "ntriples/snap/part-00002.parquet").exists()
        )

    def test_refuse_nonconsecutive_nt_fragment_compaction(self):
        # A changed-schema part between two same-schema parts must not be
        # reordered merely because the planner groups identical schemas.
        middle = self.dirs["datatourisme"] / "ntriples/snap/part-00001.parquet"
        pq.write_table(
            pa.table({"different_schema": [1, 2, 3, 4]}),
            middle,
            compression="zstd",
        )
        new_plan = self.base / "noncontiguous-plan"
        write_report(
            self.root.resolve(), new_plan, MIB,
            plan(scan(self.root.resolve()), MIB),
        )
        with self.assertRaisesRegex(ValueError, "Non-contiguous NTriples"):
            publish(
                plan_dir=new_plan, source_root=self.root,
                output_root=self.outputs, writable_source_root=None,
                prune_backup=False, execute=False, min_free_gib=0,
            )

    def test_cross_mount_verified_copies_publish_all_five(self):
        real_link = os.link

        def cross_mount_exdev(source, destination, *args, **kwargs):
            # Simulate separate VFS mount IDs; the publisher must copy and
            # SHA-256-verify instead of attempting to recompact inputs.
            raise OSError(18, "Invalid cross-device link")

        with patch("bdnb_parquet.compact_publish.os.link",
                   side_effect=cross_mount_exdev):
            self.assertEqual(self.run_pub("--execute", "--prune-backup"), 0)
        self.assertEqual(len(list(self.root.rglob("*.parquet"))), 7)
        self.assertTrue(all(
            (directory / "_COMPACTION_SOURCES.json").is_file()
            for directory in self.dirs.values()
        ))

    def test_datatourisme_csv_grouping_preserves_logical_source_ranges(self):
        csv_dir = self.dirs["datatourisme"] / "csv"
        second = csv_dir / "second.parquet"
        self.write_part(second, "csv", 1)
        manifest_path = self.dirs["datatourisme"] / "manifest.json"
        original = json.loads(manifest_path.read_text())
        original["csv"].append({
            "source": "second.csv", "output": "csv/second.parquet",
            "rows": 4, "bytes": second.stat().st_size,
        })
        original["rows"] += 4
        original["bytes"] += second.stat().st_size
        self.put_json(manifest_path, original)
        dataset_path = self.dirs["datatourisme"] / "dataset.json"
        dataset = json.loads(dataset_path.read_text())
        dataset["rows"] += 4
        dataset["bytes"] += second.stat().st_size
        self.put_json(dataset_path, dataset)
        replan = self.base / "replan"
        outputs = self.base / "reoutputs"
        write_report(
            self.root.resolve(), replan, MIB,
            plan(scan(self.root.resolve()), MIB),
        )
        self.assertEqual(compact_main([
            "--plan-dir", str(replan),
            "--source-root", str(self.root),
            "--output-root", str(outputs),
            "--min-free-gib", "0",
            "--execute", "--include-independent-files",
        ]), 0)
        report = publish(
            plan_dir=replan, source_root=self.root,
            output_root=outputs, writable_source_root=None,
            prune_backup=True, execute=True, min_free_gib=0,
        )
        self.assertEqual(report["verified_batches"], 6)
        metadata = json.loads(manifest_path.read_text())
        records = metadata["csv"]
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["output"], records[1]["output"])
        self.assertEqual(
            sorted(e["row_offset"] for e in records), [0, 4],
        )
        self.assertTrue(
            (self.dirs["datatourisme"] / records[0]["output"]).is_file()
        )

    def test_backup_preserved_when_not_pruned(self):
        report = publish(
            plan_dir=self.plan, source_root=self.root,
            output_root=self.outputs, writable_source_root=None,
            prune_backup=False, execute=True, min_free_gib=0,
        )
        self.assertFalse(report["pruned"])
        self.assertTrue(all(
            directory.with_name(
                directory.name + ".precompact-backup"
            ).exists()
            for directory in self.dirs.values()
        ))


if __name__ == "__main__":
    unittest.main()
