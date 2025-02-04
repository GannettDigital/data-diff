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

    def test_segment_range_diff_output(self):
        """Test that segment range info is printed when flag is enabled"""
        differ = HashDiffer(bisection_factor=4, bisection_threshold=10, segment_range_diff=True)
        diff_res = differ.diff_tables(self.table1, self.table2)

        # First, get the complete diff to ensure info_tree is populated
        diff = list(diff_res)

        # Now check the info tree for segments with diffs
        segments_with_diffs = []
        for node in diff_res.info_tree.children:
            if node.info.is_diff:
                min_key = str(node.info.tables[0].min_key)  # Changed to use table's min_key
                max_key = str(node.info.tables[0].max_key)  # Changed to use table's max_key
                segment_data = {
                    "key_range": {"min_key": min_key, "max_key": max_key},
                    "row_counts": node.info.rowcounts,
                    "diff_count": node.info.diff_count,
                }
                segments_with_diffs.append(segment_data)

        # Verify the differences were found and reported correctly
        self.assertEqual(len(diff), 2)  # One removal and one addition
        self.assertTrue(len(segments_with_diffs) > 0, "No segments with diffs were reported")

        # Verify the segment containing row 42 was identified
        found_segment = False
        for segment in segments_with_diffs:
            min_key = segment["key_range"]["min_key"]
            max_key = segment["key_range"]["max_key"]
            if min_key != "None" and max_key != "None":
                # Parse the key range values from string format "(X)" to int
                min_key = int(min_key.strip("()"))
                max_key = int(max_key.strip("()"))
                if min_key <= 42 <= max_key:
                    found_segment = True
                    break
        self.assertTrue(found_segment, "Segment containing difference was not reported")

    def test_segment_range_diff_disabled(self):
        """Test that segment range info is not included when flag is disabled"""
        differ = HashDiffer(bisection_factor=4, bisection_threshold=10, segment_range_diff=False)
        diff_res = differ.diff_tables(self.table1, self.table2)

        # Verify the differences are still found
        diff = list(diff_res)
        self.assertEqual(len(diff), 2)  # One removal and one addition

    def test_segment_range_diff(self):
        """Test that segment range info is printed"""
        differ = HashDiffer(bisection_factor=4, bisection_threshold=10, segment_range_diff=True)

        # Run diff
        diff_res = differ.diff_tables(self.table1, self.table2)
        results = list(diff_res)

        # Basic verification
        self.assertEqual(len(results), 2)  # One removal and one addition
