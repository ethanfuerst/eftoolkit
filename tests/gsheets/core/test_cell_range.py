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
