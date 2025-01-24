import os
from numbers import Number
import logging
from collections import defaultdict
from typing import Any, Collection, Dict, Iterator, List, Sequence, Set, Tuple, Union

import attrs
from typing_extensions import Literal

from data_diff.abcs.database_types import ColType_UUID, NumericType, PrecisionType, StringType, Boolean, JSON
from data_diff.info_tree import InfoTree
from data_diff.utils import safezip, diffs_are_equiv_jsons
from data_diff.thread_utils import ThreadedYielder
from data_diff.table_segment import TableSegment
from data_diff.diff_tables import TableDiffer

BENCHMARK = os.environ.get("BENCHMARK", False)

DEFAULT_BISECTION_THRESHOLD = 1024 * 16
DEFAULT_BISECTION_FACTOR = 32

logger = logging.getLogger("hashdiff_tables")

# Just for local readability: TODO: later switch to real type declarations of these.
_Op = Literal["+", "-"]
_PK = Sequence[Any]
_Row = Tuple[Any]

# Add these near the top with the other type definitions - Kurt
DiffResult = Iterator[Tuple[str, tuple]]  # Iterator[Tuple[Literal["+", "-"], tuple]]
DiffResultList = Iterator[List[Tuple[str, tuple]]]

# We do not want the diff_sets function in line 237 when specific flag is set -  flag segment-level-diff. We want to know that there is a diff and print it. - Kurt
def diff_sets(
    a: Sequence[_Row],
    b: Sequence[_Row],
    *,
    json_cols: dict = None,
    columns1: Sequence[str],
    columns2: Sequence[str],
    key_columns1: Sequence[str],
    key_columns2: Sequence[str],
    ignored_columns1: Collection[str],
    ignored_columns2: Collection[str],
) -> Iterator:
    # Group full rows by PKs on each side. The first items are the PK: TableSegment.relevant_columns
    rows_by_pks1: Dict[_PK, List[_Row]] = defaultdict(list)
    rows_by_pks2: Dict[_PK, List[_Row]] = defaultdict(list)
    for row in a:
        pk: _PK = tuple(val for col, val in zip(key_columns1, row))
        rows_by_pks1[pk].append(row)
    for row in b:
        pk: _PK = tuple(val for col, val in zip(key_columns2, row))
        rows_by_pks2[pk].append(row)

    # Mind that the same pk MUST go in full with all the -/+ rows all at once, for grouping.
    diffs_by_pks: Dict[_PK, List[Tuple[_Op, _Row]]] = defaultdict(list)
    for pk in sorted(set(rows_by_pks1) | set(rows_by_pks2)):
        cutrows1: List[_Row] = [
            tuple(val for col, val in zip(columns1, row1) if col not in ignored_columns1) for row1 in rows_by_pks1[pk]
        ]
        cutrows2: List[_Row] = [
            tuple(val for col, val in zip(columns2, row2) if col not in ignored_columns2) for row2 in rows_by_pks2[pk]
        ]

        # Either side has 0 rows: a clearly exclusive row.
        # Either side has 2+ rows: duplicates on either side, yield it all regardless of values.
        # Both sides == 1: non-duplicate, non-exclusive, so check for values of interest.
        if len(cutrows1) != 1 or len(cutrows2) != 1 or cutrows1 != cutrows2:
            for row1 in rows_by_pks1[pk]:
                diffs_by_pks[pk].append(("-", row1))
            for row2 in rows_by_pks2[pk]:
                diffs_by_pks[pk].append(("+", row2))

    warned_diff_cols = set()
    for diffs in (diffs_by_pks[pk] for pk in sorted(diffs_by_pks)):
        if json_cols:
            parsed_match, overriden_diff_cols = diffs_are_equiv_jsons(diffs, json_cols)
            if parsed_match:
                to_warn = overriden_diff_cols - warned_diff_cols
                for w in to_warn:
                    logger.warning(
                        f"Equivalent JSON objects with different string representations detected "
                        f"in column '{w}'. These cases are NOT reported as differences."
                    )
                    warned_diff_cols.add(w)
                continue
        yield from diffs


@attrs.define(frozen=False)
class HashDiffer(TableDiffer):
    """Finds the diff between two SQL tables

    The algorithm uses hashing to quickly check if the tables are different, and then applies a
    bisection search recursively to find the differences efficiently.

    Works best for comparing tables that are mostly the same, with minor discrepancies.

    Parameters:
        bisection_factor (int): Into how many segments to bisect per iteration.
        bisection_threshold (Number): When should we stop bisecting and compare locally (in row count).
        threaded (bool): Enable/disable threaded diffing. Needed to take advantage of database threads.
        max_threadpool_size (int): Maximum size of each threadpool. ``None`` means auto.
                                   Only relevant when `threaded` is ``True``.
                                   There may be many pools, so number of actual threads can be a lot higher.
    """

    bisection_factor: int = DEFAULT_BISECTION_FACTOR
    bisection_threshold: int = DEFAULT_BISECTION_THRESHOLD
    bisection_disabled: bool = False  # i.e. always download the rows (used in tests)
    auto_bisection_factor: bool = False
    segment_level_diff: bool = False  # Add this line to enable/disable segment-level diff

    stats: dict = attrs.field(factory=dict)

    def __attrs_post_init__(self) -> None:
        # Validate options
        if self.bisection_factor >= self.bisection_threshold:
            raise ValueError("Incorrect param values (bisection factor must be lower than threshold)")
        if self.bisection_factor < 2:
            raise ValueError("Must have at least two segments per iteration (i.e. bisection_factor >= 2)")

    def _validate_and_adjust_columns(self, table1: TableSegment, table2: TableSegment, *, strict: bool = True) -> None:
        for c1, c2 in safezip(table1.relevant_columns, table2.relevant_columns):
            if c1 not in table1._schema:
                raise ValueError(f"Column '{c1}' not found in schema for table {table1}")
            if c2 not in table2._schema:
                raise ValueError(f"Column '{c2}' not found in schema for table {table2}")

            # Update schemas to minimal mutual precision
            col1 = table1._schema[c1]
            col2 = table2._schema[c2]
            if isinstance(col1, PrecisionType):
                if not isinstance(col2, PrecisionType):
                    if strict:
                        raise TypeError(f"Incompatible types for column '{c1}':  {col1} <-> {col2}")
                    else:
                        continue

                lowest = min(col1, col2, key=lambda col: col.precision)

                if col1.precision != col2.precision:
                    logger.warning(f"Using reduced precision {lowest} for column '{c1}'. Types={col1}, {col2}")

                table1._schema[c1] = attrs.evolve(col1, precision=lowest.precision, rounds=lowest.rounds)
                table2._schema[c2] = attrs.evolve(col2, precision=lowest.precision, rounds=lowest.rounds)

            elif isinstance(col1, (NumericType, Boolean)):
                if not isinstance(col2, (NumericType, Boolean)):
                    if strict:
                        raise TypeError(f"Incompatible types for column '{c1}':  {col1} <-> {col2}")
                    else:
                        continue

                lowest = min(col1, col2, key=lambda col: col.precision)

                if col1.precision != col2.precision:
                    logger.warning(f"Using reduced precision {lowest} for column '{c1}'. Types={col1}, {col2}")

                if lowest.precision != col1.precision:
                    table1._schema[c1] = attrs.evolve(col1, precision=lowest.precision)
                if lowest.precision != col2.precision:
                    table2._schema[c2] = attrs.evolve(col2, precision=lowest.precision)

        for t in [table1, table2]:
            for c in t.relevant_columns:
                ctype = t._schema[c]
                if not ctype.supported:
                    logger.warning(
                        f"[{t.database.name}] Column '{c}' of type '{ctype}' has no compatibility handling. "
                        "If encoding/formatting differs between databases, it may result in false positives."
                    )

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
    ):
        # Get initial counts and key range - Kurt
        (count1, checksum1), (count2, checksum2) = self._threaded_call("count_and_checksum", [table1, table2])
        
        # Store counts for the segment
        info_tree.info.rowcounts = {1: count1 or 0, 2: count2 or 0}
        info_tree.info.key_range = (
            str(table1.min_key if table1.min_key is not None else "start"),
            str(table2.max_key if table2.max_key is not None else "end")
        )

        if checksum1 == checksum2:
            info_tree.info.is_diff = False
            return

        if self.segment_level_diff:
            # Report this segment as differing - Kurt
            info_tree.info.is_diff = True
            logger.info(f". " * level + f"Difference detected in segment {table1.min_key}..{table2.max_key}")
            # Don't return here, so we can keep bisecting - Kurt

        # Continue normal bisection process if not using segment_level_diff
        return self._bisect_and_diff_segments(ti, table1, table2, info_tree, level=level, max_rows=max(count1 or 0, count2 or 0))

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

        max_space_size = max(table1.approximate_size(), table2.approximate_size())
        if max_rows is None:
            # We can be sure that row_count <= max_rows iff the table key is unique
            max_rows = max_space_size
            info_tree.info.max_rows = max_rows

        # If count is below the threshold, just download and compare the columns locally
        # This saves time, as bisection speed is limited by ping and query performance.
        if self.bisection_disabled or max_rows < self.bisection_threshold or max_space_size < self.bisection_factor * 2:
            if self.segment_level_diff:
                # Skip downloading rows, just check counts and checksum - Kurt
                (count1, checksum1), (count2, checksum2) = self._threaded_call("count_and_checksum", [table1, table2])
                if checksum1 != checksum2:
                    info_tree.info.is_diff = True
                    info_tree.info.key_range = (
                        str(table1.min_key if table1.min_key is not None else "start"),
                        str(table2.max_key if table2.max_key is not None else "end")
                    )
                    info_tree.info.rowcounts = {1: count1 or 0, 2: count2 or 0}
                    logger.info(
                        f". " * level + f"Difference detected in segment {table1.min_key}..{table2.max_key} "
                        f"(counts: {count1 or 0}/{count2 or 0})"
                    )
                    # Store info but return simplified diff format - Kurt
                    return [('diff', None)]
                return []

            # Original row download and comparison logic
            rows1, rows2 = self._threaded_call("get_values", [table1, table2])
            json_cols = {
                i: colname
                for i, colname in enumerate(table1.extra_columns)
                if isinstance(table1._schema[colname], JSON)
            }

            # Original detailed diff behavior
            diff = list(
                diff_sets(
                    rows1,
                    rows2,
                    json_cols=json_cols,
                    columns1=table1.relevant_columns,
                    columns2=table2.relevant_columns,
                    key_columns1=table1.key_columns,
                    key_columns2=table2.key_columns,
                    ignored_columns1=self.ignored_columns1,
                    ignored_columns2=self.ignored_columns2,
                )
            )

            info_tree.info.set_diff(diff)
            info_tree.info.rowcounts = {1: len(rows1), 2: len(rows2)}
            logger.info(". " * level + f"Diff found {len(diff)} different rows.")
            return diff

        if self.segment_level_diff:
            # For each sub-segment, just mark them as diff without downloading rows - Kurt
            info_tree.info.is_diff = True
            info_tree.info.key_range = (
                str(table1.min_key if table1.min_key else "start"),
                str(table2.max_key if table2.max_key else "end")
            )
            return [('diff', None)]

        return super()._bisect_and_diff_segments(ti, table1, table2, info_tree, level, max_rows)

    def _diff_tables_root(self, table1: TableSegment, table2: TableSegment, info_tree: InfoTree) -> Union[DiffResult, DiffResultList]:
        # Get initial total counts for the tables - Kurt
        if self.segment_level_diff:
            (count1, _), (count2, _) = self._threaded_call("count_and_checksum", [table1, table2])
            info_tree.info.rowcounts = {1: count1 or 0, 2: count2 or 0}
            info_tree.info.key_range = (
                str(table1.min_key if table1.min_key is not None else "start"),
                str(table2.max_key if table2.max_key is not None else "end")
            )

        return self._bisect_and_diff_tables(table1, table2, info_tree)
