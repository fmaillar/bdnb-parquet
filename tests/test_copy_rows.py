import unittest

from bdnb_parquet.convert import parse_copy_row, split_copy_fields


class CopyRowTests(unittest.TestCase):
    def test_plain_tab_delimiters(self):
        self.assertEqual(split_copy_fields("a\tb\tc"), ["a", "b", "c"])

    def test_backslash_escaped_literal_tab(self):
        # COPY text allows a delimiter character inside a value when quoted
        # with a backslash. It must not create an extra column.
        line = "a\\\tinside\tb"
        self.assertEqual(split_copy_fields(line), ["a\\\tinside", "b"])
        self.assertEqual(parse_copy_row(line), ["a\tinside", "b"])

    def test_even_backslashes_before_tab_leave_delimiter(self):
        line = "a\\\\\tb"
        self.assertEqual(split_copy_fields(line), ["a\\\\", "b"])
        self.assertEqual(parse_copy_row(line), ["a\\", "b"])


if __name__ == "__main__":
    unittest.main()
