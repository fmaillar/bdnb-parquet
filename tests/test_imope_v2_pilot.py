"""Non-destructive IMoPE v2 2048 MiB pilot tests."""

import hashlib
import json
import struct
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from bdnb_parquet.imope_v2_plan import build_plan, write_plan
from bdnb_parquet.imope_v2_pilot import main, pilot, read_pilot


def wkb_point(x, y):
    return struct.pack("<BIdd", 1, 1, float(x), float(y))


class TestImopeV2Pilot(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.root = self.base / "imope"
        self.root.mkdir()
        self.add_file("Dpt01", 2, [0, 0, 1, 1])
        self.add_file("Dpt02", 3, [2, 3, 4, 5])
        self.add_file("Dpt03", 4, [-1, 0, 5, 6])
        self.plan = self.base / "plan-2048"
        write_plan(build_plan(self.root, target_mib=2048), self.plan, self.root)

    def add_file(self, department, rows, bbox, *, crs=None):
        folder = self.root / department
        folder.mkdir(exist_ok=True)
        path = folder / "adresse.parquet"
        geo = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "geometry_types": ["Point"],
                    "crs": crs or {"type": "name", "properties": {"name": "EPSG:2154"}},
                    "bbox": bbox,
                }
            },
        }
        schema = pa.schema([
            pa.field("id", pa.int64(), metadata={b"origin": b"stable"}),
            pa.field("geometry", pa.binary()),
        ], metadata={
            b"geo": json.dumps(geo, sort_keys=True).encode(),
            b"pandas": b"identical-pandas-metadata",
        })
        xs = list(range(int(department[-2:]), int(department[-2:]) + rows))
        pq.write_table(pa.Table.from_arrays([
            pa.array(xs, type=pa.int64()),
            pa.array([wkb_point(x, x + 1) for x in xs], type=pa.binary()),
        ], schema=schema), path, compression="zstd")
        return path

    def bytes_snapshot(self):
        return {
            str(p): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in self.root.rglob("*") if p.is_file()
        }

    def test_preview_does_not_write_anything(self):
        before = self.bytes_snapshot()
        self.assertEqual(
            main([
                "--plan-dir", str(self.plan),
                "--source-root", str(self.root),
            ]),
            0,
        )
        self.assertEqual(before, self.bytes_snapshot())
        self.assertFalse((self.base / "output").exists())

    def test_pilot_full_arrow_values_geo_bbox_and_unchanged_source(self):
        before = self.bytes_snapshot()
        output_root = self.base / "new-pilot"
        report = pilot(
            self.plan, self.root, output_root=output_root,
            execute=True, threads=2, batch_rows=2, row_group_rows=3,
            max_buffer_mib=1,
        )
        self.assertFalse(report["publication_performed"])
        self.assertEqual(report["verified_rows"], 9)
        self.assertEqual(report["source_files"], 3)
        self.assertEqual(report["source_provenance"], [
            {"original": "Dpt01/adresse.parquet",
             "department": "Dpt01", "first_row": 0, "rows": 2},
            {"original": "Dpt02/adresse.parquet",
             "department": "Dpt02", "first_row": 2, "rows": 3},
            {"original": "Dpt03/adresse.parquet",
             "department": "Dpt03", "first_row": 5, "rows": 4},
        ])
        output = Path(report["output"])
        self.assertTrue(output.is_file())
        self.assertTrue(output.with_suffix(".json").is_file())
        self.assertLessEqual(pq.ParquetFile(output).metadata.row_group(0).num_rows, 3)
        schema = pq.read_schema(output)
        geo = json.loads(schema.metadata[b"geo"])
        self.assertEqual(
            geo["columns"]["geometry"]["bbox"], [-1.0, 0.0, 5.0, 6.0],
        )
        self.assertEqual(
            schema.field("id").metadata, {b"origin": b"stable"},
        )
        self.assertEqual(schema.metadata[b"pandas"], b"identical-pandas-metadata")
        self.assertEqual(before, self.bytes_snapshot())

    def test_requires_new_external_root(self):
        existing = self.base / "existing-output"
        existing.mkdir()
        with self.assertRaisesRegex(FileExistsError, "NEW directory"):
            pilot(
                self.plan, self.root, output_root=existing,
                execute=True,
            )
        with self.assertRaisesRegex(ValueError, "must not overlap"):
            pilot(
                self.plan, self.root, output_root=self.root / "pilot",
                execute=True,
            )

    def test_rejects_modified_source_after_plan(self):
        self.add_file("Dpt02", 5, [2, 3, 4, 5])
        with self.assertRaisesRegex(ValueError, "changed since planning"):
            read_pilot(self.plan, self.root)

    def test_requires_2048_mib_plan(self):
        alt = self.base / "plan-512"
        write_plan(build_plan(self.root, target_mib=512), alt, self.root)
        with self.assertRaisesRegex(ValueError, "exactly 2048 MiB"):
            read_pilot(alt, self.root)

    def test_output_bytes_never_overwritten(self):
        new = self.base / "published-pilot"
        pilot(self.plan, self.root, output_root=new, execute=True)
        before = {
            str(path): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in new.rglob("*") if path.is_file()
        }
        with self.assertRaises(FileExistsError):
            pilot(self.plan, self.root, output_root=new, execute=True)
        after = {
            str(path): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in new.rglob("*") if path.is_file()
        }
        self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
