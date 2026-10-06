import unittest
from datetime import date, datetime
from decimal import Decimal

from bdnb_parquet.parser import (
    convert_value,
    ewkb_to_wkb,
    parse_ddl_line,
    parse_pg_array,
    pg_copy_unescape,
    split_copy_columns,
)


class ParserTests(unittest.TestCase):
    def test_copy_columns(self):
        self.assertEqual(
            split_copy_columns('"a", "b", "c"'),
            ["a", "b", "c"],
        )

    def test_ddl(self):
        got = parse_ddl_line(
            'ALTER TABLE "s"."t" ADD COLUMN "x" numeric(17, 3);\n'
        )
        self.assertEqual(got, ("column", ("s", "t", "x", "numeric(17, 3)")))

    def test_geometry_ddl(self):
        got = parse_ddl_line(
            "SELECT AddGeometryColumn('bdnb_2026_02_a_open_data','adresse','geom_adresse',2154,'POINT',2);\n"
        )
        self.assertIsNotNone(got)
        kind, payload = got
        self.assertEqual(kind, "geometry")
        self.assertEqual(payload[0], "bdnb_2026_02_a_open_data")
        self.assertEqual(payload[1], "adresse")
        self.assertEqual(payload[2].name, "geom_adresse")
        self.assertEqual(payload[2].srid, 2154)

    def test_copy_unescape(self):
        self.assertIsNone(pg_copy_unescape(r"\N"))
        self.assertEqual(pg_copy_unescape(r"a\tb\n"), "a\tb\n")
        self.assertEqual(pg_copy_unescape(r"a\\b"), r"a\b")

    def test_array(self):
        self.assertEqual(parse_pg_array('{a,b,"c,d",NULL,"NULL"}'), ["a", "b", "c,d", None, "NULL"])

    def test_ewkb_srid_to_wkb(self):
        # Point with EWKB SRID=2154, x=1.0, y=2.0
        ewkb = "01010000206a080000000000000000f03f0000000000000040"
        expected = "0101000000000000000000f03f0000000000000040"
        self.assertEqual(ewkb_to_wkb(ewkb).hex(), expected)

    def test_types(self):
        self.assertEqual(convert_value("12", "int4"), 12)
        self.assertEqual(convert_value("12.340", "numeric(17, 3)"), Decimal("12.340"))
        self.assertEqual(convert_value("2026-05-01", "date"), date(2026, 5, 1))
        self.assertEqual(convert_value("2026/05/01", "date"), date(2026, 5, 1))
        self.assertEqual(convert_value("2026.05.01", "date"), date(2026, 5, 1))
        self.assertEqual(
            convert_value("2025/04/22T22:00:00", "timestamp"),
            datetime(2025, 4, 22, 22, 0, 0),
        )
        self.assertEqual(
            convert_value("2025.04.22T22:00:00", "timestamp"),
            datetime(2025, 4, 22, 22, 0, 0),
        )
        self.assertEqual(convert_value("1", "bool"), True)
        self.assertEqual(convert_value("0", "bool"), False)
        self.assertEqual(convert_value("{a,b}", "text[]"), ["a", "b"])


if __name__ == "__main__":
    unittest.main()
