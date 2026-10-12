"""Five-dataset publication integration tests using converter-shaped manifests."""

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from bdnb_parquet.compact_five import TARGETS, main as compact_main
from bdnb_parquet.compact_five_publish import main as publish_main, publish
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
