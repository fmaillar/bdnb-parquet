import io
import unittest

from bdnb_parquet.resume import _scan_sql_member


class ResumeIndexTests(unittest.TestCase):
    def test_copy_offsets_and_ranges(self):
        sql = (
            'ALTER TABLE "s"."a" ADD COLUMN "x" INTEGER;\n'
            'ALTER TABLE "s"."b" ADD COLUMN "y" VARCHAR;\n'
            'COPY "s"."a" ("x") FROM STDIN;\n'
            '1\n'
            '2\n'
            '\\.\n'
            'COPY "s"."b" ("y") FROM STDIN;\n'
            'hello\n'
            '\\.\n'
        ).encode("utf-8")
        tar_data_offset = 4096
        identity = {"path": "/tmp/source", "size": 1, "mtime_ns": 2}

        catalog = _scan_sql_member(
            io.BytesIO(sql),
            tar_data_offset=tar_data_offset,
            sql_size=len(sql),
            identity=identity,
        )

        self.assertEqual([t["table"] for t in catalog["tables"]], ["a", "b"])
        first, second = catalog["tables"]

        first_bytes = sql[
            first["sql_offset"] : first["sql_offset"] + first["range_size"]
        ]
        second_bytes = sql[
            second["sql_offset"] : second["sql_offset"] + second["range_size"]
        ]

        self.assertTrue(first_bytes.startswith(b'COPY "s"."a"'))
        self.assertTrue(first_bytes.endswith(b"\\.\n"))
        self.assertTrue(second_bytes.startswith(b'COPY "s"."b"'))
        self.assertTrue(second_bytes.endswith(b"\\.\n"))
        self.assertEqual(
            first["tar_offset"],
            tar_data_offset + first["sql_offset"],
        )
        self.assertEqual(first["pg_types"], {"x": "INTEGER"})
        self.assertEqual(second["pg_types"], {"y": "VARCHAR"})


if __name__ == "__main__":
    unittest.main()
