"""Tests for CellRange dataclass."""

from eftoolkit.gsheets.runner import CellLocation, CellRange


def test_create_with_cell_locations():
    """CellRange with start and end CellLocations."""
    start = CellLocation(cell='B4')
    end = CellLocation(cell='E14')

    cell_range = CellRange(start=start, end=end)

    assert cell_range.start == start
    assert cell_range.end == end


def test_frozen_immutable():
    """CellRange is immutable (frozen=True)."""
    import pytest

    cell_range = CellRange.from_string('B4:E14')

    with pytest.raises(AttributeError):
        cell_range.start = CellLocation(cell='A1')


def test_equality():
    """CellRange instances with same values are equal."""
    range1 = CellRange.from_string('B4:E14')
    range2 = CellRange.from_string('B4:E14')

    assert range1 == range2


def test_inequality():
    """CellRange instances with different values are not equal."""
    range1 = CellRange.from_string('B4:E14')
    range2 = CellRange.from_string('A1:C5')

    assert range1 != range2


def test_hashable():
    """CellRange is hashable (can be used in sets/dicts)."""
    cell_range = CellRange.from_string('B4:E14')

    ranges_set = {cell_range}

    assert cell_range in ranges_set


# from_string tests


def test_from_string_multi_cell_range():
    """from_string parses multi-cell range 'B4:E14'."""
    cell_range = CellRange.from_string('B4:E14')

    assert cell_range.start.cell == 'B4'
    assert cell_range.end.cell == 'E14'


def test_from_string_single_cell():
    """from_string parses single cell 'A1' (start == end)."""
    cell_range = CellRange.from_string('A1')

    assert cell_range.start.cell == 'A1'
    assert cell_range.end.cell == 'A1'
    assert cell_range.is_single_cell


def test_from_string_explicit_single_cell():
    """from_string parses 'A1:A1' as single cell."""
    cell_range = CellRange.from_string('A1:A1')

    assert cell_range.start.cell == 'A1'
    assert cell_range.end.cell == 'A1'
    assert cell_range.is_single_cell


def test_from_string_double_letter_columns():
    """from_string handles double-letter columns (AA, AB, etc.)."""
    cell_range = CellRange.from_string('AA1:AD10')

    assert cell_range.start.cell == 'AA1'
    assert cell_range.end.cell == 'AD10'
    assert cell_range.start_col == 26
    assert cell_range.end_col == 29


def test_from_string_lowercase():
    """from_string handles lowercase cell references."""
    cell_range = CellRange.from_string('b4:e14')

    assert cell_range.start.cell == 'b4'
    assert cell_range.end.cell == 'e14'


# from_bounds tests


def test_from_bounds_basic():
    """from_bounds creates CellRange from 0-indexed bounds."""
    # B4:E14 has start_row=3, start_col=1, end_row=13, end_col=4
    cell_range = CellRange.from_bounds(
        start_row=3,
        start_col=1,
        end_row=13,
        end_col=4,
    )

    assert cell_range.start.cell == 'B4'
    assert cell_range.end.cell == 'E14'


def test_from_bounds_single_cell():
    """from_bounds creates single cell when bounds are equal."""
    cell_range = CellRange.from_bounds(
        start_row=0,
        start_col=0,
        end_row=0,
        end_col=0,
    )

    assert cell_range.start.cell == 'A1'
    assert cell_range.end.cell == 'A1'
    assert cell_range.is_single_cell


def test_from_bounds_double_letter_columns():
    """from_bounds handles columns beyond Z."""
    cell_range = CellRange.from_bounds(
        start_row=0,
        start_col=26,  # AA
        end_row=9,
        end_col=29,  # AD
    )

    assert cell_range.start.cell == 'AA1'
    assert cell_range.end.cell == 'AD10'


# Computed property tests


def test_start_row_returns_0_indexed():
    """start_row property returns 0-indexed start row."""
    cell_range = CellRange.from_string('B4:E14')

    assert cell_range.start_row == 3


def test_end_row_returns_0_indexed():
    """end_row property returns 0-indexed end row."""
    cell_range = CellRange.from_string('B4:E14')

    assert cell_range.end_row == 13


def test_start_col_returns_0_indexed():
    """start_col property returns 0-indexed start column."""
    cell_range = CellRange.from_string('B4:E14')

    assert cell_range.start_col == 1


def test_end_col_returns_0_indexed():
    """end_col property returns 0-indexed end column."""
    cell_range = CellRange.from_string('B4:E14')

    assert cell_range.end_col == 4


def test_start_row_1indexed():
    """start_row_1indexed returns 1-indexed start row for API."""
    cell_range = CellRange.from_string('B4:E14')

    assert cell_range.start_row_1indexed == 4


def test_end_row_1indexed():
    """end_row_1indexed returns 1-indexed end row for API."""
    cell_range = CellRange.from_string('B4:E14')

    assert cell_range.end_row_1indexed == 14


def test_start_col_letter():
    """start_col_letter returns start column letter(s)."""
    cell_range = CellRange.from_string('B4:E14')

    assert cell_range.start_col_letter == 'B'


def test_end_col_letter():
    """end_col_letter returns end column letter(s)."""
    cell_range = CellRange.from_string('B4:E14')

    assert cell_range.end_col_letter == 'E'


def test_num_rows():
    """num_rows returns number of rows in the range."""
    cell_range = CellRange.from_string('B4:E14')

    # Rows 4-14 inclusive = 11 rows
    assert cell_range.num_rows == 11


def test_num_cols():
    """num_cols returns number of columns in the range."""
    cell_range = CellRange.from_string('B4:E14')

    # Columns B-E inclusive = 4 columns
    assert cell_range.num_cols == 4


def test_num_rows_single_cell():
    """num_rows returns 1 for single cell."""
    cell_range = CellRange.from_string('A1')

    assert cell_range.num_rows == 1


def test_num_cols_single_cell():
    """num_cols returns 1 for single cell."""
    cell_range = CellRange.from_string('A1')

    assert cell_range.num_cols == 1


def test_is_single_cell_true():
    """is_single_cell returns True for single cell."""
    cell_range = CellRange.from_string('A1')

    assert cell_range.is_single_cell is True


def test_is_single_cell_false():
    """is_single_cell returns False for multi-cell range."""
    cell_range = CellRange.from_string('B4:E14')

    assert cell_range.is_single_cell is False


# __str__ tests


def test_str_multi_cell_range():
    """__str__ returns A1 notation for multi-cell range."""
    cell_range = CellRange.from_string('B4:E14')

    assert str(cell_range) == 'B4:E14'


def test_str_single_cell():
    """__str__ returns single cell notation (not 'A1:A1')."""
    cell_range = CellRange.from_string('A1')

    assert str(cell_range) == 'A1'


def test_str_explicit_single_cell():
    """__str__ returns single cell notation even when created as 'A1:A1'."""
    cell_range = CellRange.from_string('A1:A1')

    assert str(cell_range) == 'A1'


# Properties with edge cases


def test_properties_with_a1_range():
    """Properties work correctly with A1 starting point."""
    cell_range = CellRange.from_string('A1:C5')

    assert cell_range.start_row == 0
    assert cell_range.start_col == 0
    assert cell_range.end_row == 4
    assert cell_range.end_col == 2
    assert cell_range.start_row_1indexed == 1
    assert cell_range.end_row_1indexed == 5
    assert cell_range.start_col_letter == 'A'
    assert cell_range.end_col_letter == 'C'
    assert cell_range.num_rows == 5
    assert cell_range.num_cols == 3


def test_properties_with_large_range():
    """Properties work correctly with large row/column numbers."""
    cell_range = CellRange.from_string('Z100:AAA1000')

    assert cell_range.start_row == 99
    assert cell_range.start_col == 25
    assert cell_range.end_row == 999
    assert cell_range.end_col == 702
    assert cell_range.num_rows == 901
    assert cell_range.num_cols == 678


# value property tests


def test_value_multi_cell_range():
    """value property returns A1 notation for multi-cell range."""
    cell_range = CellRange.from_string('B4:E14')

    assert cell_range.value == 'B4:E14'


def test_value_single_cell():
    """value property returns single cell notation (not 'A1:A1')."""
    cell_range = CellRange.from_string('A1')

    assert cell_range.value == 'A1'


def test_value_equals_str():
    """value property equals __str__."""
    cell_range = CellRange.from_string('B4:E14')

    assert cell_range.value == str(cell_range)


# __contains__ tests


def test_contains_cell_inside_range():
    """Cell inside range returns True."""
    cell_range = CellRange.from_string('B4:E14')

    assert CellLocation(cell='C5') in cell_range


def test_contains_cell_at_start():
    """Cell at start corner of range returns True."""
    cell_range = CellRange.from_string('B4:E14')

    assert CellLocation(cell='B4') in cell_range


def test_contains_cell_at_end():
    """Cell at end corner of range returns True."""
    cell_range = CellRange.from_string('B4:E14')

    assert CellLocation(cell='E14') in cell_range


def test_contains_cell_at_top_right():
    """Cell at top-right corner of range returns True."""
    cell_range = CellRange.from_string('B4:E14')

    assert CellLocation(cell='E4') in cell_range


def test_contains_cell_at_bottom_left():
    """Cell at bottom-left corner of range returns True."""
    cell_range = CellRange.from_string('B4:E14')

    assert CellLocation(cell='B14') in cell_range


def test_contains_cell_outside_left():
    """Cell outside range (left) returns False."""
    cell_range = CellRange.from_string('B4:E14')

    assert CellLocation(cell='A5') not in cell_range


def test_contains_cell_outside_right():
    """Cell outside range (right) returns False."""
    cell_range = CellRange.from_string('B4:E14')

    assert CellLocation(cell='F5') not in cell_range


def test_contains_cell_outside_above():
    """Cell outside range (above) returns False."""
    cell_range = CellRange.from_string('B4:E14')

    assert CellLocation(cell='C3') not in cell_range


def test_contains_cell_outside_below():
    """Cell outside range (below) returns False."""
    cell_range = CellRange.from_string('B4:E14')

    assert CellLocation(cell='C15') not in cell_range


def test_contains_single_cell_range():
    """Single cell range contains only itself."""
    cell_range = CellRange.from_string('B4')

    assert CellLocation(cell='B4') in cell_range
    assert CellLocation(cell='A4') not in cell_range
    assert CellLocation(cell='C4') not in cell_range
    assert CellLocation(cell='B3') not in cell_range
    assert CellLocation(cell='B5') not in cell_range


def test_contains_with_double_letter_columns():
    """Contains works with double-letter columns."""
    cell_range = CellRange.from_string('AA1:AD10')

    assert CellLocation(cell='AB5') in cell_range
    assert CellLocation(cell='Z5') not in cell_range
    assert CellLocation(cell='AE5') not in cell_range


# __contains__ tests for CellRange in CellRange


def test_contains_range_fully_inside():
    """Inner range fully inside outer range returns True."""
    outer = CellRange.from_string('B4:E14')
    inner = CellRange.from_string('C5:D10')

    assert inner in outer


def test_contains_range_equal():
    """Range contains itself."""
    cell_range = CellRange.from_string('B4:E14')

    assert cell_range in cell_range


def test_contains_range_at_corners():
    """Range that exactly matches outer range returns True."""
    outer = CellRange.from_string('B4:E14')
    inner = CellRange.from_string('B4:E14')

    assert inner in outer


def test_contains_range_at_top_left():
    """Inner range at top-left corner returns True."""
    outer = CellRange.from_string('B4:E14')
    inner = CellRange.from_string('B4:C6')

    assert inner in outer


def test_contains_range_at_bottom_right():
    """Inner range at bottom-right corner returns True."""
    outer = CellRange.from_string('B4:E14')
    inner = CellRange.from_string('D10:E14')

    assert inner in outer


def test_contains_range_extends_left():
    """Range extending beyond left boundary returns False."""
    outer = CellRange.from_string('B4:E14')
    inner = CellRange.from_string('A5:D10')

    assert inner not in outer


def test_contains_range_extends_right():
    """Range extending beyond right boundary returns False."""
    outer = CellRange.from_string('B4:E14')
    inner = CellRange.from_string('C5:F10')

    assert inner not in outer


def test_contains_range_extends_above():
    """Range extending beyond top boundary returns False."""
    outer = CellRange.from_string('B4:E14')
    inner = CellRange.from_string('C2:D10')

    assert inner not in outer


def test_contains_range_extends_below():
    """Range extending beyond bottom boundary returns False."""
    outer = CellRange.from_string('B4:E14')
    inner = CellRange.from_string('C5:D16')

    assert inner not in outer


def test_contains_range_completely_outside():
    """Range completely outside returns False."""
    outer = CellRange.from_string('B4:E14')
    inner = CellRange.from_string('G20:H25')

    assert inner not in outer


def test_contains_range_partial_overlap():
    """Partially overlapping range returns False."""
    outer = CellRange.from_string('B4:E14')
    inner = CellRange.from_string('A1:C5')

    assert inner not in outer


def test_contains_single_cell_range_inside():
    """Single cell as CellRange inside outer range returns True."""
    outer = CellRange.from_string('B4:E14')
    inner = CellRange.from_string('C5')

    assert inner in outer


def test_contains_single_cell_range_outside():
    """Single cell as CellRange outside outer range returns False."""
    outer = CellRange.from_string('B4:E14')
    inner = CellRange.from_string('A1')

    assert inner not in outer


def test_contains_range_larger_than_outer():
    """Range larger than outer returns False."""
    outer = CellRange.from_string('C5:D10')
    inner = CellRange.from_string('B4:E14')

    assert inner not in outer
