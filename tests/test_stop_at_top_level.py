import pytest
from data_diff.hashdiff_tables import HashDiffer

def test_stop_at_top_level():
    differ = HashDiffer(stop_at_top_level=True)
    # Test data with nested structures
    data1 = [
        {"top1": {"nested": "value1"},
         "top2": {"nested": "value2"}}
    ]
    data2 = [
        {"top1": {"nested": "changed1"},
         "top2": {"nested": "value2"}}
    ]
    
    # Test without stop_at_top_level
    differ.stop_at_top_level = False
    full_diff = differ.diff_tables(data1, data2)
    assert any("nested" in str(d) for d in full_diff)
    
    # Test with stop_at_top_level
    differ.stop_at_top_level = True
    top_level_diff = differ.diff_tables(data1, data2)
    assert any("top1" in str(d) for d in top_level_diff)
    assert not any("nested" in str(d) for d in top_level_diff)

def test_stop_at_top_level_equal_nested():
    differ = HashDiffer(stop_at_top_level=True)
    # Test case where nested values are equal
    data1 = [
        {"top": {"nested": "same"}}
    ]
    data2 = [
        {"top": {"nested": "same"}}
    ]
    
    diff = differ.diff_tables(data1, data2)
    assert len(diff) == 0  # Should show no differences
