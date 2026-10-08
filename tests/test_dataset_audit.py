import tempfile
import unittest
from pathlib import Path

from bdnb_parquet.dataset_audit import (
    best_candidate,
    build_rows,
    scan_leaf_datasets,
)


class DatasetAuditTests(unittest.TestCase):
    def test_exact_and_legacy_matches(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for layer in ("raw", "numpy", "parquet"):
                (root / layer).mkdir()

            raw_a = root / "raw" / "domain" / "dataset-a"
            raw_b = root / "raw" / "domain" / "dataset-b"
            numpy_a = root / "numpy" / "domain" / "dataset-a"
            parquet_b = root / "parquet" / "domain" / "dataset-b"

            for path in (raw_a, raw_b, numpy_a, parquet_b):
                path.mkdir(parents=True)

            (raw_a / "source.gpkg").write_bytes(b"a")
            (raw_b / "source.csv").write_bytes(b"b")
            (numpy_a / "old.parquet").write_bytes(b"c")
            (parquet_b / "part.parquet").write_bytes(b"d")

            datasets = {
                layer: scan_leaf_datasets(root / layer, layer)
                for layer in ("raw", "numpy", "parquet")
            }
            rows = build_rows(datasets)
            by_raw = {
                row["raw_path"]: row
                for row in rows
                if row["raw_path"]
            }

            self.assertEqual(
                by_raw["domain/dataset-a"]["status"],
                "legacy-only",
            )
            self.assertEqual(
                by_raw["domain/dataset-b"]["status"],
                "parquet-candidate-present",
            )

    def test_same_name_candidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for layer in ("raw", "numpy"):
                (root / layer).mkdir()

            raw = root / "raw" / "a" / "b" / "dataset-x"
            old = root / "numpy" / "other" / "dataset-x"
            raw.mkdir(parents=True)
            old.mkdir(parents=True)
            (raw / "x.csv").write_bytes(b"x")
            (old / "x.parquet").write_bytes(b"x")

            raw_ds = scan_leaf_datasets(root / "raw", "raw")[0]
            old_ds = scan_leaf_datasets(root / "numpy", "numpy")[0]
            match, score, reason = best_candidate(raw_ds, [old_ds])

            self.assertIsNotNone(match)
            self.assertEqual(score, 70)
            self.assertEqual(reason, "same-name")


if __name__ == "__main__":
    unittest.main()
