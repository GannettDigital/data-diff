"""Provides classes for performing a table diff"""

import threading
import time
import os
from abc import ABC, abstractmethod
from enum import Enum
from contextlib import contextmanager
from operator import methodcaller
from typing import Any, Dict, Set, List, Tuple, Iterator, Optional, Union
from concurrent.futures import ThreadPoolExecutor, as_completed

import attrs

from data_diff.errors import DataDiffMismatchingKeyTypesError
from data_diff.info_tree import InfoTree, SegmentInfo
from data_diff.utils import dbt_diff_string_template, run_as_daemon, safezip, getLogger, truncate_error, Vector
from data_diff.thread_utils import ThreadedYielder
from data_diff.table_segment import TableSegment, create_mesh_from_points
from data_diff.tracking import create_end_event_json, create_start_event_json, send_event_json, is_tracking_enabled
from data_diff.abcs.database_types import IKey

logger = getLogger(__name__)


class Algorithm(Enum):
    AUTO = "auto"
    JOINDIFF = "joindiff"
    HASHDIFF = "hashdiff"


DiffResult = Iterator[Tuple[str, tuple]]  # Iterator[Tuple[Literal["+", "-"], tuple]]
DiffResultList = Iterator[List[Tuple[str, tuple]]]


@attrs.define(frozen=False)
class ThreadBase:
    "Provides utility methods for optional threading"

    threaded: bool = True
    max_threadpool_size: Optional[int] = 1

    def _thread_map(self, func, iterable):
        if not self.threaded:
            return map(func, iterable)

        with ThreadPoolExecutor(max_workers=self.max_threadpool_size) as task_pool:
            return task_pool.map(func, iterable)

    def _threaded_call(self, func, iterable):
        "Calls a method for each object in iterable."
        return list(self._thread_map(methodcaller(func), iterable))

    def _thread_as_completed(self, func, iterable):
        if not self.threaded:
            yield from map(func, iterable)
            return

        with ThreadPoolExecutor(max_workers=self.max_threadpool_size) as task_pool:
            futures = [task_pool.submit(func, item) for item in iterable]
            for future in as_completed(futures):
                yield future.result()

    def _threaded_call_as_completed(self, func, iterable):
        "Calls a method for each object in iterable. Returned in order of completion."
        return self._thread_as_completed(methodcaller(func), iterable)

    @contextmanager
    def _run_in_background(self, *funcs):
        with ThreadPoolExecutor(max_workers=self.max_threadpool_size) as task_pool:
            futures = [task_pool.submit(f) for f in funcs if f is not None]
            yield futures
            for f in futures:
                f.result()


@attrs.define(frozen=True)
class DiffStats:
    diff_by_sign: Dict[str, int]
    table1_count: int
    table2_count: int
    unchanged: int
    diff_percent: float
    extra_column_diffs: Optional[Dict[str, int]]


@attrs.define(frozen=True)
class DiffResultWrapper:
    diff: iter  # DiffResult
    info_tree: InfoTree
    stats: dict
    result_list: list = attrs.field(factory=list)

    def __iter__(self) -> Iterator[Any]:
        yield from self.result_list
        for i in self.diff:
            self.result_list.append(i)
            yield i

    def _get_stats(self, is_dbt: bool = False) -> DiffStats:
        list(self)  # Consume the iterator into result_list, if we haven't already

        key_columns = self.info_tree.info.tables[0].key_columns
        len_key_columns = len(key_columns)
        diff_by_key = {}
        extra_column_diffs = None
        if is_dbt:
            extra_column_values_store = {}
            extra_columns = self.info_tree.info.tables[0].extra_columns
            extra_column_diffs = {k: 0 for k in extra_columns}

        for sign, values in self.result_list:
            k = values[:len_key_columns]
            if is_dbt:
                extra_column_values = values[len_key_columns:]
            if k in diff_by_key:
                assert sign != diff_by_key[k]
                diff_by_key[k] = "!"
                if is_dbt:
                    for i in range(0, len(extra_columns)):
                        if extra_column_values[i] != extra_column_values_store[k][i]:
                            extra_column_diffs[extra_columns[i]] += 1
            else:
                diff_by_key[k] = sign
                if is_dbt:
                    extra_column_values_store[k] = extra_column_values

        diff_by_sign = {k: 0 for k in "+-!"}
        for sign in diff_by_key.values():
            diff_by_sign[sign] += 1

        table1_count = self.info_tree.info.rowcounts[1]
        table2_count = self.info_tree.info.rowcounts[2]
        unchanged = table1_count - diff_by_sign["-"] - diff_by_sign["!"]
        diff_percent = 1 - unchanged / max(table1_count, table2_count)

        return DiffStats(diff_by_sign, table1_count, table2_count, unchanged, diff_percent, extra_column_diffs)

    def get_stats_string(self, is_dbt: bool = False):
        diff_stats = self._get_stats(is_dbt)

        total_rows_diff = diff_stats.table2_count - diff_stats.table1_count

        if is_dbt:
            string_output = dbt_diff_string_template(
                total_rows_table1=diff_stats.table1_count,
                total_rows_table2=diff_stats.table2_count,
                total_rows_diff=total_rows_diff,
                rows_added=diff_stats.diff_by_sign["+"],
                rows_removed=diff_stats.diff_by_sign["-"],
                rows_updated=diff_stats.diff_by_sign["!"],
                rows_unchanged=diff_stats.unchanged,
                extra_info_dict=diff_stats.extra_column_diffs,
                extra_info_str="[u]Values Changed[/u]",
            )

        else:
            string_output = ""
            string_output += f"{diff_stats.table1_count} rows in table A\n"
            string_output += f"{diff_stats.table2_count} rows in table B\n"
            string_output += f"{diff_stats.diff_by_sign['-']} rows exclusive to table A (not present in B)\n"
            string_output += f"{diff_stats.diff_by_sign['+']} rows exclusive to table B (not present in A)\n"
            string_output += f"{diff_stats.diff_by_sign['!']} rows updated\n"
            string_output += f"{diff_stats.unchanged} rows unchanged\n"
            string_output += f"{100 * diff_stats.diff_percent:.2f}% difference score\n"

            if self.stats:
                string_output += "\nExtra-Info:\n"
                for k, v in sorted(self.stats.items()):
                    string_output += f"  {k} = {v}\n"

        return string_output

    def get_stats_dict(self, is_dbt: bool = False):
        diff_stats = self._get_stats(is_dbt)
        json_output = {
            "rows_A": diff_stats.table1_count,
            "rows_B": diff_stats.table2_count,
            "exclusive_A": diff_stats.diff_by_sign["-"],
            "exclusive_B": diff_stats.diff_by_sign["+"],
            "updated": diff_stats.diff_by_sign["!"],
            "unchanged": diff_stats.unchanged,
            "total": sum(diff_stats.diff_by_sign.values()),
            "stats": self.stats,
        }
        json_output["values"] = diff_stats.extra_column_diffs or {}
        return json_output


@attrs.define(frozen=False)
class TableDiffer(ThreadBase, ABC):
    INFO_TREE_CLASS = InfoTree

    bisection_factor = 32
    auto_bisection_factor = False
    segment_rows: int = attrs.field(factory=lambda: int(os.environ.get("DEFAULT_SEGMENT_ROWS", 50000)))
    stats: dict = {}

    ignored_columns1: Set[str] = attrs.field(factory=set)
    ignored_columns2: Set[str] = attrs.field(factory=set)
    _ignored_columns_lock: threading.Lock = attrs.field(factory=threading.Lock, init=False)
    yield_list: bool = False

    def calculate_bisection_factor(self, rows):
        """Calculate biscetion factor based on row count

        Parameters:
            rows: amount of rows in the given table or segment
        """
        ratio = rows / self.segment_rows
        logger.info(f"rows: {rows}, self.segment_rows: {self.segment_rows}, ratio: {ratio}")
        if 0 < ratio < 2:
            return 2
        else:
            return round(ratio)

    def diff_tables(self, table1: TableSegment, table2: TableSegment, info_tree: InfoTree = None) -> DiffResultWrapper:
        """Diff the given tables.

        Parameters:
            table1 (TableSegment): The "before" table to compare. Or: source table
            table2 (TableSegment): The "after" table to compare. Or: target table

        Returns:
            An iterator that yield pair-tuples, representing the diff. Items can be either -
            ('-', row) for items in table1 but not in table2.
            ('+', row) for items in table2 but not in table1.
            Where `row` is a tuple of values, corresponding to the diffed columns.
        """
        if info_tree is None:
            segment_info = self.INFO_TREE_CLASS.SEGMENT_INFO_CLASS([table1, table2])
            info_tree = self.INFO_TREE_CLASS(segment_info)
        return DiffResultWrapper(self._diff_tables_wrapper(table1, table2, info_tree), info_tree, self.stats)

    def _diff_tables_wrapper(self, table1: TableSegment, table2: TableSegment, info_tree: InfoTree) -> DiffResult:
        if is_tracking_enabled():
            options = attrs.asdict(self, recurse=False)
            # not a useful event attribute
            options.pop("_ignored_columns_lock")
            options["differ_name"] = type(self).__name__
            event_json = create_start_event_json(options)
            run_as_daemon(send_event_json, event_json)

        if table1.database.dialect.PREVENT_OVERFLOW_WHEN_CONCAT or table2.database.dialect.PREVENT_OVERFLOW_WHEN_CONCAT:
            table1.database.dialect.enable_preventing_type_overflow()
            table2.database.dialect.enable_preventing_type_overflow()

        start = time.monotonic()
        error = None
        try:
            # Query and validate schema
            table1, table2 = self._threaded_call("with_schema", [table1, table2])
            self._validate_and_adjust_columns(table1, table2)

            yield from self._diff_tables_root(table1, table2, info_tree)

        except BaseException as e:  # Catch KeyboardInterrupt too
            error = e
        finally:
            info_tree.aggregate_info()

            if is_tracking_enabled():
                runtime = time.monotonic() - start
                rowcounts = info_tree.info.rowcounts
                table1_count = rowcounts[1] if rowcounts else None
                table2_count = rowcounts[2] if rowcounts else None
                diff_count = info_tree.info.diff_count
                err_message = truncate_error(repr(error))
                event_json = create_end_event_json(
                    error is None,
                    runtime,
                    table1.database.name,
                    table2.database.name,
                    table1_count,
                    table2_count,
                    diff_count,
                    err_message,
                )
                send_event_json(event_json)

            if error:
                raise error

    def _validate_and_adjust_columns(self, table1: TableSegment, table2: TableSegment) -> None:
        pass

    def _diff_tables_root(
        self, table1: TableSegment, table2: TableSegment, info_tree: InfoTree
    ) -> Union[DiffResult, DiffResultList]:
        return self._bisect_and_diff_tables(table1, table2, info_tree)

    @abstractmethod
    def _diff_segments(
        self,
        ti: ThreadedYielder,
        table1: TableSegment,
        table2: TableSegment,
        info_tree: InfoTree,
        max_rows: int,
        level=0,
        segment_index=None,
        segment_count=None,
    ): ...

    def _bisect_and_diff_tables(self, table1: TableSegment, table2: TableSegment, info_tree):
        if len(table1.key_columns) != len(table2.key_columns):
            raise ValueError("Tables should have an equivalent number of key columns!")

        key_types1 = [table1._schema[i] for i in table1.key_columns]
        key_types2 = [table2._schema[i] for i in table2.key_columns]

        for kt in key_types1 + key_types2:
            if not isinstance(kt, IKey):
                raise NotImplementedError(f"Cannot use a column of type {kt} as a key")

        for i, (kt1, kt2) in enumerate(safezip(key_types1, key_types2)):
            if kt1.python_type is not kt2.python_type:
                k1 = table1.key_columns[i]
                k2 = table2.key_columns[i]
                raise DataDiffMismatchingKeyTypesError(
                    f"Key columns {k1} and {k2} can't be compared due to different types."
                )

        # Query min/max values
        key_ranges = self._threaded_call_as_completed("query_key_range", [table1, table2])

        # Start with the first completed value, so we don't waste time waiting
        min_key1, max_key1 = self._parse_key_range_result(key_types1, next(key_ranges))

        btable1 = table1.new_key_bounds(min_key=min_key1, max_key=max_key1, key_types=key_types1)
        btable2 = table2.new_key_bounds(min_key=min_key1, max_key=max_key1, key_types=key_types2)

        logger.info(
            f"Diffing segments at key-range: {btable1.min_key}..{btable2.max_key}. "
            f"size: table1 <= {btable1.approximate_size()}, table2 <= {btable2.approximate_size()}"
        )

        ti = ThreadedYielder(self.max_threadpool_size, self.yield_list)
        # Bisect (split) the table into segments, and diff them recursively.
        ti.submit(self._bisect_and_diff_segments, ti, btable1, btable2, info_tree, priority=999)

        # Now we check for the second min-max, to diff the portions we "missed".
        # This is achieved by subtracting the table ranges, and dividing the resulting space into aligned boxes.
        # For example, given tables A & B, and a 2D compound key, where A was queried first for key-range,
        # the regions of B we need to diff in this second pass are marked by B1..8:
        # ┌──┬──────┬──┐
        # │B1│  B2  │B3│
        # ├──┼──────┼──┤
        # │B4│  A   │B5│
        # ├──┼──────┼──┤
        # │B6│  B7  │B8│
        # └──┴──────┴──┘
        # Overall, the max number of new regions in this 2nd pass is 3^|k| - 1

        # Note: python types can be the same, but the rendering parameters (e.g. casing) can differ.
        min_key2, max_key2 = self._parse_key_range_result(key_types2, next(key_ranges))

        points = [list(sorted(p)) for p in safezip(min_key1, min_key2, max_key1, max_key2)]
        box_mesh = create_mesh_from_points(*points)

        new_regions = [(p1, p2) for p1, p2 in box_mesh if p1 < p2 and not (p1 >= min_key1 and p2 <= max_key1)]

        for p1, p2 in new_regions:
            extra_table1 = table1.new_key_bounds(min_key=p1, max_key=p2, key_types=key_types1)
            extra_table2 = table2.new_key_bounds(min_key=p1, max_key=p2, key_types=key_types2)
            ti.submit(self._bisect_and_diff_segments, ti, extra_table1, extra_table2, info_tree, priority=999)

        return ti

    def _parse_key_range_result(self, key_types, key_range) -> Tuple[Vector, Vector]:
        min_key_values, max_key_values = key_range

        # We add 1 because our ranges are exclusive of the end (like in Python)
        try:
            min_key = Vector(key_type.make_value(mn) for key_type, mn in safezip(key_types, min_key_values))
            max_key = Vector(key_type.make_value(mx) + 1 for key_type, mx in safezip(key_types, max_key_values))
        except (TypeError, ValueError) as e:
            raise type(e)(f"Cannot apply {key_types} to '{min_key_values}', '{max_key_values}'.") from e

        return min_key, max_key

    def _bisect_and_diff_segments(
        self,
        ti: ThreadedYielder,
        table1: TableSegment,
        table2: TableSegment,
        info_tree: InfoTree,
        level=0,
        max_rows=None,
    ):
        assert table1.is_bounded and table2.is_bounded

        # Choose evenly spaced checkpoints (according to min_key and max_key)
        biggest_table = max(table1, table2, key=methodcaller("count"))
        logger.info(f"Diff segments level: {level}")
        if self.auto_bisection_factor:
            self.bisection_factor = self.calculate_bisection_factor(biggest_table.count())
            logger.info(
                f"Automatically setting bisection_factor, max_rows: {max_rows}, self.bisection_factor: {self.bisection_factor}"
            )
        checkpoints = biggest_table.choose_checkpoints(self.bisection_factor - 1)

        # Get it thread-safe, to avoid segment misalignment because of bad timing.
        with self._ignored_columns_lock:
            table1 = attrs.evolve(table1, ignored_columns=frozenset(self.ignored_columns1))
            table2 = attrs.evolve(table2, ignored_columns=frozenset(self.ignored_columns2))

        # Create new instances of TableSegment between each checkpoint
        segmented1 = table1.segment_by_checkpoints(checkpoints)
        segmented2 = table2.segment_by_checkpoints(checkpoints)

        # Recursively compare each pair of corresponding segments between table1 and table2
        for i, (t1, t2) in enumerate(safezip(segmented1, segmented2)):
            info_node = info_tree.add_node(t1, t2, max_rows=max_rows)
            ti.submit(
                self._diff_segments, ti, t1, t2, info_node, max_rows, level + 1, i + 1, len(segmented1), priority=level
            )

    def ignore_column(self, column_name1: str, column_name2: str) -> None:
        """
        Ignore the column (by name on sides A & B) in md5s & diffs from now on.

        This affects 2 places:

        - The columns are not checksumed for new(!) segments.
        - The columns are ignored in in-memory diffing for running segments.

        The columns are never ignored in the fetched values, whether they are
        the same or different — for data consistency.

        Use this feature to collect relatively well-represented differences
        across all columns if one of them is highly different in the beginning
        of a table (as per the order of segmentation/bisection). Otherwise,
        that one column might easily hit the limit and stop the whole diff.
        """
        with self._ignored_columns_lock:
            self.ignored_columns1.add(column_name1)
            self.ignored_columns2.add(column_name2)
