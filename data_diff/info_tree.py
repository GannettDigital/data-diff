from typing import List, Dict, Optional, Any, Tuple, Union

import attrs
from typing_extensions import Self

from data_diff.table_segment import TableSegment


@attrs.define(frozen=False)
class SegmentInfo:
    tables: List[TableSegment]

    diff: Optional[List[Union[Tuple[Any, ...], List[Any]]]] = None
    diff_schema: Optional[Tuple[Tuple[str, type], ...]] = None
    is_diff: Optional[bool] = None
    diff_count: Optional[int] = None
    #key_range: Optional[Tuple[Any, Any]] = None

    rowcounts: Dict[int, int] = attrs.field(factory=dict)
    max_rows: Optional[int] = None
   
    
    def set_diff(
        self, diff: List[Union[Tuple[Any, ...], List[Any]]], schema: Optional[Tuple[Tuple[str, type]]] = None
    ) -> None:
        self.diff_schema = schema
        self.diff = diff
        self.diff_count = len(diff)
        self.is_diff = self.diff_count > 0

    def update_from_children(self, child_infos) -> None:
        child_infos = list(child_infos)
        assert child_infos

        # self.diff = list(chain(*[c.diff for c in child_infos]))
        self.diff_count = sum(c.diff_count for c in child_infos if c.diff_count is not None)
        self.is_diff = any(c.is_diff for c in child_infos)
        self.diff_schema = next((child.diff_schema for child in child_infos if child.diff_schema is not None), None)
        self.diff = sum((c.diff for c in child_infos if c.diff is not None), [])

        self.rowcounts = {
            1: sum(c.rowcounts[1] for c in child_infos if c.rowcounts),
            2: sum(c.rowcounts[2] for c in child_infos if c.rowcounts),
        }

    def to_dict(self) -> Dict[str, Any]:
        # Convert tables to something JSON-serializable (e.g., their names)
        # or call table.to_dict() if TableSegment provides such a method
        return {
            "tables": [str(t) for t in self.tables],  # or t.to_dict() if available
            "diff": self.diff,
            "diff_schema": self.diff_schema,
            "is_diff": self.is_diff,
            "diff_count": self.diff_count,
            "rowcounts": self.rowcounts,
            "max_rows": self.max_rows,
        }


@attrs.define(frozen=True)
class InfoTree:
    SEGMENT_INFO_CLASS = SegmentInfo

    info: SegmentInfo
    children: List["InfoTree"] = attrs.field(factory=list)

    def add_node(self, table1: TableSegment, table2: TableSegment, max_rows: Optional[int] = None) -> Self:
        cls = self.__class__
        node = cls(cls.SEGMENT_INFO_CLASS([table1, table2], max_rows=max_rows))
        self.children.append(node)
        return node

    def aggregate_info(self) -> None:
        if self.children:
            for c in self.children:
                c.aggregate_info()
            self.info.update_from_children(c.info for c in self.children)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "info": self.info.to_dict(),
            "children": [child.to_dict() for child in self.children],
        }
