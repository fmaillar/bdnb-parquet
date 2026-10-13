"""Independent IMoPE v2 audit: exact WKB bbox and logical alias coverage."""

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from shapely.geometry import Point

from bdnb_parquet.imope_v2_plan import build_plan, write_plan
from bdnb_parquet.imope_v2_schedule import schedule
from bdnb_parquet.imope_v2_audit import validate


class ImopeV2AuditTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.root = self.base / "imope"
        self.root.mkdir()
        self.add_geo("Dpt01", [1.0, 2.0], [1.0, 2.0, 1.0, 2.0])
        self.add_geo("Dpt02", [3.0, 4.0], [3.0, 4.0, 3.0, 4.0])
        self.add_plain("Dpt01")
        self.add_plain("Dpt02")
        sha = self.digest(self.root / "Dpt01" / "geo.parquet")
        self.index = {
            "dataset": "imope",
            "sources": [],
            "deduplicated_aliases": [{
                "original": "Dpt99/geo.parquet",
                "published_scope": "imope",
                "published": "Dpt01/geo.parquet",
                "first_row": 0,
                "rows": 1,
                "sha256": sha,
            }],
        }
        (self.root / "_COMPACTION_SOURCES.json").write_text(
            json.dumps(self.index)
        )
        self.plan = self.base / "plan"
        write_plan(build_plan(self.root, target_mib=2048), self.plan, self.root)
        self.stage = self.base / "stage"

    @staticmethod
    def digest(path):
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def add_geo(self, dept, xy, bbox):
        path = self.root / dept
        path.mkdir(parents=True, exist_ok=True)
        geo = {
            "version": "1.1.0",
            "primary_column": "geom",
            "columns": {
                "geom": {
                    "encoding": "WKB",
                    "crs": "EPSG:2154",
                    "geometry_types": ["Point"],
                    "bbox": bbox,
                }
            },
        }
        schema = pa.schema([
            pa.field("x", pa.float64()),
            pa.field("geom", pa.binary()),
        ], metadata={
            b"pandas": b"identical",
            b"geo": json.dumps(geo, sort_keys=True).encode(),
        })
        table = pa.Table.from_arrays([
            pa.array([xy[0]]),
            pa.array([Point(*xy).wkb], type=pa.binary()),
        ], schema=schema)
        pq.write_table(table, path / "geo.parquet", compression="zstd")

    def add_plain(self, dept):
        path = self.root / dept / "plain.parquet"
        pq.write_table(pa.table({"id": pa.array([1, 2])}),
                       path, compression="zstd")

    def compact(self):
        result = schedule(
            self.plan, self.root, self.stage,
            workers=1, threads=2, min_free_gib=0,
            batch_rows=2, row_group_rows=2,
            max_buffer_mib=1, execute=True,
        )
        self.assertEqual(result["successful_batches"], 2)
        self.assertEqual(result["failed_batches"], 0)

    def test_independent_geometry_audit_and_attestation(self):
        before = {
            str(p): self.digest(p)
            for p in self.root.rglob("*") if p.is_file()
        }
        self.compact()
        r = validate(
            self.plan, self.root, self.stage,
            workers=1, batch_rows=2, attest=True,
        )
        self.assertEqual(r["validated_batches"], 2)
        self.assertEqual(r["validated_sources"], 4)
        self.assertEqual(r["validated_rows"], 6)
        self.assertEqual(r["geo_batches"], 1)
        self.assertEqual(r["lineage"]["existing_aliases"], 1)
        self.assertEqual(r["lineage"]["aliases_requiring_retarget"], 1)
        self.assertTrue(
            (self.stage / "_meta" / "imope-v2-validation.json").is_file()
        )
        self.assertEqual(
            before, {
                str(p): self.digest(p)
                for p in self.root.rglob("*") if p.is_file()
            },
        )
        with self.assertRaises(FileExistsError):
            validate(
                self.plan, self.root, self.stage,
                workers=1, batch_rows=2, attest=True,
            )

    def test_bad_bbox_not_allowed_even_when_all_arrow_values_match(self):
        # A source-provided bbox can be self-consistent yet incorrect;
        # the real WKB scan must refuse it.
        self.add_geo("Dpt01", [1.0, 2.0], [0, 0, 9, 9])
        self.add_geo("Dpt02", [3.0, 4.0], [0, 0, 9, 9])
        self.index["deduplicated_aliases"][0]["sha256"] = self.digest(
            self.root / "Dpt01" / "geo.parquet"
        )
        (self.root / "_COMPACTION_SOURCES.json").write_text(json.dumps(self.index))
        self.plan = self.base / "plan-wrong-bbox"
        write_plan(build_plan(self.root, target_mib=2048), self.plan, self.root)
        self.compact()
        with self.assertRaisesRegex(ValueError, "WKB bbox mismatch"):
            validate(self.plan, self.root, self.stage, workers=1, batch_rows=2)
        self.assertFalse(
            (self.stage / "_meta" / "imope-v2-validation.json").exists()
        )

    def test_modified_canonical_sha256_blocks_attestation(self):
        self.compact()
        self.index["deduplicated_aliases"][0]["sha256"] = "0" * 64
        (self.root / "_COMPACTION_SOURCES.json").write_text(json.dumps(self.index))
        with self.assertRaisesRegex(ValueError, "Invalid legacy canonical SHA256"):
            validate(self.plan, self.root, self.stage, workers=1, batch_rows=2)
        self.assertFalse(
            (self.stage / "_meta" / "imope-v2-validation.json").exists()
        )


if __name__ == "__main__":
    unittest.main()
