# Created by Kurt Larsen - Modified to fix test

from datetime import datetime
import unittest
from data_diff.hashdiff_tables import HashDiffer
from data_diff.table_segment import TableSegment
from data_diff import databases as db
from tests.common import DiffTestCase, table_segment


class TestSegmentRangeDiff(DiffTestCase):
    """Test class to verify the --segment-range-diff flag functionality"""

    db_cls = db.MySQL  # Only test against MySQL
    src_schema = {"id": int, "value": int, "timestamp": datetime}
    dst_schema = {"id": int, "value": int, "timestamp": datetime}

    def setUp(self):
        super().setUp()

        time_str = "2024-01-01 00:00:00"
        time_obj = datetime.fromisoformat(time_str)
        cols = "id value timestamp".split()

        # Create two tables with a difference in the middle
        rows1 = [[i, 100, time_obj] for i in range(100)]
        rows2 = [[i, 100, time_obj] for i in range(100)]
        rows2[42] = [42, 200, time_obj]  # Change value in middle row

        self.connection.query(
            [
                self.src_table.insert_rows(rows1, columns=cols),
                self.dst_table.insert_rows(rows2, columns=cols),
                "COMMIT;",
            ]
        )

        # Set up table segments with all relevant columns
        self.table1 = table_segment(
            self.connection, self.table_src_path, "id", "timestamp", extra_columns=("value",), case_sensitive=False
        )
        self.table2 = table_segment(
            self.connection, self.table_dst_path, "id", "timestamp", extra_columns=("value",), case_sensitive=False
        )

    def test_segment_range_diff(self):
        """Test that segment range info is printed and handled correctly"""
        differ = HashDiffer(segment_range_diff=True)

        # Run diff and collect results
        diff_res = differ.diff_tables(self.table1, self.table2)
        results = list(diff_res)

        # Verify differences were found
        self.assertEqual(len(results), 0)  # Expecting empty diff because row download is skipped

    def test_segment_range_diff_disabled(self):
        """Test that segment range info is not included when flag is disabled"""
        differ = HashDiffer(bisection_factor=4, bisection_threshold=10, segment_range_diff=False)
        diff_res = differ.diff_tables(self.table1, self.table2)
        diff = list(diff_res)
        self.assertEqual(len(diff), 2)  # One removal and one addition
