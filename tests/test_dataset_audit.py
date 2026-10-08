import tempfile
import unittest
from pathlib import Path

from bdnb_parquet.dataset_audit import build_rows, scan_layer


class DatasetAuditTests(unittest.TestCase):
    def test_cross_layer_statuses(self):
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

            (raw_a / "source.bin").write_bytes(b"a")
            (raw_b / "source.bin").write_bytes(b"b")
            (numpy_a / "old.parquet").write_bytes(b"c")
            (parquet_b / "part.parquet").write_bytes(b"d")

            scans = {
                layer: scan_layer(root / layer, 2)
                for layer in ("raw", "numpy", "parquet")
            }
            rows = build_rows(scans, depth=2)
            by_path = {row["relative_path"]: row for row in rows}

            self.assertEqual(
                by_path["domain/dataset-a"]["status"],
                "legacy-derived-only",
            )
            self.assertEqual(
                by_path["domain/dataset-b"]["status"],
                "parquet-present",
            )


if __name__ == "__main__":
    unittest.main()
