"""Synthetic transactional IMoPE v2 publication integration tests."""

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from shapely.geometry import Point

from bdnb_parquet.imope_promote import validate_parquet
from bdnb_parquet.imope_v2_plan import build_plan, write_plan
from bdnb_parquet.imope_v2_schedule import schedule
from bdnb_parquet.imope_v2_audit import validate
from bdnb_parquet.imope_v2_publish import BACKUP_SUFFIX, publish


class IMoPEV2PublisherTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self.root = self.base / "imope"
        self.root.mkdir()
        for dept, x in (("Dpt01", 1.0), ("Dpt02", 3.0)):
            folder = self.root / dept
            folder.mkdir()
            geo = {
                "version": "1.1.0", "primary_column": "geometry",
                "columns": {"geometry": {
                    "encoding": "WKB", "crs": "EPSG:2154",
                    "geometry_types": ["Point"],
                    "bbox": [x, x + 1, x, x + 1],
                }},
            }
            schema = pa.schema([
                pa.field("id", pa.int64()), pa.field("geometry", pa.binary()),
            ], metadata={
                b"pandas": b"same",
                b"geo": json.dumps(geo, sort_keys=True).encode(),
            })
            pq.write_table(pa.Table.from_arrays([
                pa.array([int(x)]), pa.array([Point(x, x + 1).wkb]),
            ], schema=schema), folder / "geo.parquet")
            pq.write_table(pa.table({"id": [1, 2]}), folder / "plain.parquet")
        pq.write_table(
            pa.table({"id": [5]}),
            self.root / "Dpt01" / "compact-v1.parquet",
        )
        (self.root / "Dpt99").mkdir()
        alias = {
            "original": "Dpt99/geo.parquet",
            "published_scope": "imope",
            "published": "Dpt01/geo.parquet",
            "first_row": 0, "rows": 1,
            "sha256": self.digest(self.root / "Dpt01" / "geo.parquet"),
        }
        manifest = []
        for dept in ("Dpt01", "Dpt02", "Dpt99"):
            files = [
                validate_parquet(path)
                for path in sorted((self.root / dept).glob("*.parquet"))
            ]
            rows = sum(f["rows"] for f in files)
            entry = {
                "department": dept, "files": len(files), "rows": rows,
                "bytes": sum(f["bytes"] for f in files),
                "geo_files": sum(bool(f["geo"]) for f in files),
                "parquet": files,
                "original_logical_rows": rows + int(dept == "Dpt99"),
                "deduplicated_aliases": [alias] if dept == "Dpt99" else [],
            }
            manifest.append(entry)
            (self.root / dept / "_SUCCESS.json").write_text(json.dumps(entry))
        self.row_count = sum(x["rows"] for x in manifest)
        (self.root / "manifest.json").write_text(json.dumps(manifest))
        (self.root / "dataset.json").write_text(json.dumps({
            "rows": self.row_count, "files": 5,
            "bytes": sum(x["bytes"] for x in manifest),
            "geo_files": 2, "original_logical_rows": self.row_count + 1,
        }))
        (self.root / "_COMPACTION_SOURCES.json").write_text(json.dumps({
            "dataset": "imope",
            "sources": [{
                "original": "legacy.csv",
                "published": "Dpt01/compact-v1.parquet",
                "first_row": 0, "rows": 1,
            }],
            "deduplicated_aliases": [alias],
        }))
        self.plan = self.base / "plan"
        write_plan(build_plan(self.root, target_mib=2048), self.plan, self.root)
        self.outputs = self.base / "outputs"
        compaction = schedule(
            self.plan, self.root, self.outputs, execute=True,
            workers=1, threads=2, batch_rows=2, row_group_rows=2,
            max_buffer_mib=1, min_free_gib=0,
        )
        self.assertEqual(compaction["failed_batches"], 0)
        independent = validate(
            self.plan, self.root, self.outputs,
            workers=1, batch_rows=2, attest=True,
        )
        self.assertEqual(independent["validated_batches"], 2)

    @staticmethod
    def digest(path):
        with path.open("rb") as stream:
            return hashlib.file_digest(stream, "sha256").hexdigest()

    def snapshot(self):
        return {
            p.relative_to(self.root).as_posix(): self.digest(p)
            for p in self.root.rglob("*") if p.is_file()
        }

    def test_preview_never_mutates_published_dataset(self):
        before = self.snapshot()
        result = publish(
            self.plan, self.root, self.outputs,
            min_free_gib=0,
        )
        self.assertEqual(result["expected_physical_files"], 3)
        self.assertEqual(result["aliases_to_retarget"], 1)
        self.assertEqual(before, self.snapshot())

    def test_pub_preserves_previous_history_and_migrates_alias(self):
        before = self.snapshot()
        result = publish(
            self.plan, self.root, self.outputs,
            execute=True, min_free_gib=0,
        )
        self.assertTrue(result["publication_performed"])
        self.assertEqual(result["published_files"], 3)
        self.assertEqual(result["replaced_source_files"], 4)
        backup = self.root.with_name(self.root.name + BACKUP_SUFFIX)
        self.assertTrue(backup.is_dir())
        self.assertEqual({
            p.relative_to(backup).as_posix(): self.digest(p)
            for p in backup.rglob("*") if p.is_file()
        }, before)
        index = json.loads((self.root / "_COMPACTION_SOURCES.json").read_text())
        self.assertEqual(len(index["v2_sources"]), 4)
        self.assertEqual(index["sources"][0]["published"],
                         "Dpt01/compact-v1.parquet")
        alias = index["deduplicated_aliases"][0]
        self.assertTrue(alias["v2_retargeted"])
        self.assertTrue((self.root / alias["published"]).is_file())
        dept = json.loads((self.root / "Dpt01" / "_SUCCESS.json").read_text())
        self.assertEqual(
            sum(x["rows"] for x in dept["parquet"])
            + sum(x["rows"] for x in dept["virtual_parquet"]),
            dept["rows"],
        )
        self.assertEqual(len(list(self.root.rglob("*.parquet"))), 3)
        self.assertEqual(
            json.loads((self.root / "dataset.json").read_text())["rows"],
            self.row_count,
        )

    def test_wrong_stage_bytes_rejected_before_any_swap(self):
        victim = next(self.outputs.rglob("*.parquet"))
        victim.write_bytes(b"tampered")
        before = self.snapshot()
        with self.assertRaises(ValueError):
            publish(
                self.plan, self.root, self.outputs,
                execute=True, min_free_gib=0,
            )
        self.assertEqual(before, self.snapshot())


if __name__ == "__main__":
    unittest.main()
