"""Safety and provenance tests for the IMoPE v2 metadata-only planner."""

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from bdnb_parquet.imope_v2_plan import (
    SourceInfo, build_plan, main, write_plan,
)


class ImopeV2PlanTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.root = self.base / "imope"
        self.root.mkdir()

    def make_file(
        self, dept, name="adresse.parquet", *, rows=3, bbox=None,
        crs="EPSG:2154", pandas=b"stable", field_meta=b"stable",
        geo=True,
    ):
        path = self.root / dept / name
        path.parent.mkdir(exist_ok=True, parents=True)
        metadata = {b"pandas": pandas}
        if geo:
            info = {
                "version": "1.1.0",
                "primary_column": "geometry",
                "columns": {
                    "geometry": {
                        "encoding": "WKB",
                        "crs": crs,
                        "geometry_types": ["Point"],
                    },
                },
            }
            if bbox is not None:
                info["columns"]["geometry"]["bbox"] = bbox
            metadata[b"geo"] = json.dumps(info).encode()
        schema = pa.schema(
            [pa.field("id", pa.int64(), metadata={b"origin": field_meta})],
            metadata=metadata,
        )
        table = pa.Table.from_arrays(
            [pa.array(list(range(rows)), type=pa.int64())],
            schema=schema,
        )
        pq.write_table(table, path)
        return path

    def test_normalizes_only_geo_bbox_and_preserves_department_row_ranges(self):
        originals = [
            self.make_file("Dpt01", rows=2, bbox=[0, 0, 1, 1]),
            self.make_file("Dpt02", rows=3, bbox=[1, 2, 3, 4]),
            self.make_file("Dpt03", rows=4, bbox=[-2, 0, 5, 6]),
        ]
        before = {
            str(file): hashlib.sha256(file.read_bytes()).hexdigest()
            for file in originals
        }
        plan = build_plan(self.root)
        s = plan["summary"]
        self.assertEqual(s["physical_files"], 3)
        self.assertEqual(s["compatibility_cohorts"], 1)
        self.assertEqual(s["candidate_source_files"], 3)
        self.assertEqual(s["proposed_batches"], 1)
        self.assertEqual(s["estimated_file_reduction"], 2)
        self.assertEqual(s["projected_physical_files"], 1)
        self.assertEqual(s["candidate_rows"], 9)
        batch = plan["batches"][0]
        self.assertEqual(batch["bbox_union"], {
            "columns/geometry": [-2.0, 0.0, 5.0, 6.0],
        })
        self.assertEqual(batch["metadata_review"], [])
        self.assertEqual(
            [row["department"] for row in plan["sources"]],
            ["Dpt01", "Dpt02", "Dpt03"],
        )
        self.assertEqual(
            [row["first_row"] for row in plan["sources"]],
            [0, 2, 5],
        )
        self.assertEqual(
            [row["rows"] for row in plan["sources"]],
            [2, 3, 4],
        )
        self.assertEqual(
            before,
            {
                str(file): hashlib.sha256(file.read_bytes()).hexdigest()
                for file in originals
            },
        )
        self.assertEqual(sorted(self.base.iterdir()), [self.root])

    def test_different_crs_pandas_field_metadata_and_structure_never_merge(self):
        self.make_file("Dpt01", bbox=[0, 0, 1, 1])
        self.make_file("Dpt02", bbox=[1, 1, 2, 2])
        self.make_file("Dpt03", bbox=[2, 2, 3, 3], crs="EPSG:4326")
        self.make_file("Dpt04", bbox=[2, 2, 3, 3], pandas=b"different")
        self.make_file("Dpt05", bbox=[2, 2, 3, 3], field_meta=b"different")
        self.make_file("Dpt06", name="ban.parquet", bbox=[2, 2, 3, 3])
        result = build_plan(self.root)
        s = result["summary"]
        self.assertEqual(s["physical_files"], 6)
        self.assertEqual(s["compatibility_cohorts"], 5)
        self.assertEqual(s["candidate_source_files"], 2)
        self.assertEqual(s["proposed_batches"], 1)
        self.assertEqual(s["projected_physical_files"], 5)
        self.assertEqual(s["uncompacted_files_not_selected"], 4)

    def test_previous_compacted_files_are_excluded_from_second_pass(self):
        self.make_file("Dpt01")
        self.make_file("Dpt02")
        self.make_file("Dpt03", name="compact-previous.parquet")
        result = build_plan(self.root)
        self.assertEqual(result["summary"]["already_compacted_files"], 1)
        self.assertEqual(result["summary"]["candidate_source_files"], 2)
        self.assertEqual(result["summary"]["projected_physical_files"], 2)
        self.assertNotIn("compact-previous.parquet", json.dumps(result["sources"]))

    def test_missing_bbox_requires_further_review_not_silent_approval(self):
        self.make_file("Dpt01", rows=2, bbox=[0, 0, 1, 1])
        self.make_file("Dpt02", rows=3)
        result = build_plan(self.root)
        self.assertEqual(result["summary"]["proposed_batches"], 1)
        self.assertEqual(result["summary"]["metadata_review_batches"], 1)
        self.assertIn(
            "missing-nonempty-bbox:columns/geometry",
            result["batches"][0]["metadata_review"],
        )
        self.assertEqual(result["batches"][0]["bbox_union"], {})

    def test_no_bbox_anywhere_needs_geometry_scan(self):
        self.make_file("Dpt01")
        self.make_file("Dpt02")
        result = build_plan(self.root)
        self.assertIn(
            "missing-all-geo-bboxes: geometry scan needed",
            result["batches"][0]["metadata_review"],
        )

    def test_invalid_bbox_fails_closed(self):
        self.make_file("Dpt01", bbox=[2, 0, 1, 4])
        self.make_file("Dpt02", bbox=[0, 0, 1, 1])
        with self.assertRaisesRegex(ValueError, "Inverted GeoParquet bbox"):
            build_plan(self.root)

    def test_small_target_packs_independent_batches_without_reordering(self):
        # No need to generate large files to test a 1-MiB input-size target.
        paths = [
            self.make_file(f"Dpt{number:02d}")
            for number in range(1, 6)
        ]
        normal = build_plan(self.root)
        template = normal["sources"][0]
        item = SourceInfo(
            path=template["original"], department="Dpt01",
            table="adresse.parquet", bytes=300_000, rows=3,
            structure_sha256=normal["batches"][0]["structure_sha256"],
            metadata_sha256=normal["batches"][0]["metadata_ignoring_bbox_sha256"],
            group_sha256=normal["batches"][0]["group_sha256"],
            geo=True,
            bboxes={"columns/geometry": (0, 0, 1, 1)},
        )

        def fake_inspect(path, root):
            return SourceInfo(
                **{
                    **item.__dict__,
                    "path": path.relative_to(root).as_posix(),
                    "department": path.parent.name,
                },
            )

        with patch("bdnb_parquet.imope_v2_plan._inspect", side_effect=fake_inspect):
            result = build_plan(self.root, target_mib=1)
        self.assertEqual(result["summary"]["candidate_source_files"], 5)
        self.assertEqual(result["summary"]["proposed_batches"], 2)
        self.assertEqual(result["summary"]["estimated_file_reduction"], 3)
        self.assertEqual(
            [batch["sources"] for batch in result["batches"]], [3, 2],
        )
        self.assertEqual(
            [r["first_row"] for r in result["sources"]], [0, 3, 6, 0, 3],
        )
        self.assertEqual(len(paths), 5)

    def test_external_manifest_export_and_no_clobber(self):
        self.make_file("Dpt01", rows=2, bbox=[0, 0, 1, 1])
        self.make_file("Dpt02", rows=3, bbox=[1, 1, 2, 2])
        plan = build_plan(self.root)
        output = self.base / "v2-proposal"
        write_plan(plan, output, self.root)
        self.assertEqual(
            len(json.loads((output / "batches.json").read_text())), 1,
        )
        self.assertIn("Dpt02", (output / "sources.tsv").read_text())
        self.assertEqual(
            json.loads((output / "summary.json").read_text())[
                "projected_physical_files"
            ], 1,
        )
        with self.assertRaises(FileExistsError):
            write_plan(plan, output, self.root)
        with self.assertRaisesRegex(ValueError, "must not overlap"):
            write_plan(plan, self.root / "plan", self.root)
        with self.assertRaisesRegex(ValueError, "must not overlap"):
            write_plan(plan, self.base, self.root)

    def test_cli_default_is_strict_read_only(self):
        self.make_file("Dpt01", bbox=[0, 0, 1, 1])
        self.make_file("Dpt02", bbox=[1, 1, 2, 2])
        self.assertEqual(main(["--root", str(self.root)]), 0)
        self.assertEqual(list(self.base.iterdir()), [self.root])


if __name__ == "__main__":
    unittest.main()
