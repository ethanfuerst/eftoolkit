"""Tests for CellLocation dataclass."""

import pytest

from eftoolkit.gsheets.runner import CellLocation


def test_create_with_cell():
    """CellLocation with cell address."""
    location = CellLocation(cell='B4')

    assert location.cell == 'B4'


def test_frozen_immutable():
    """CellLocation is immutable (frozen=True)."""
    location = CellLocation(cell='B4')

    with pytest.raises(AttributeError):
        location.cell = 'C5'


def test_equality():
    """CellLocation instances with same values are equal."""
    loc1 = CellLocation(cell='B4')
    loc2 = CellLocation(cell='B4')

    assert loc1 == loc2


def test_inequality():
    """CellLocation instances with different cells are not equal."""
    loc1 = CellLocation(cell='B4')
    loc2 = CellLocation(cell='C5')

    assert loc1 != loc2


def test_hashable():
    """CellLocation is hashable (can be used in sets/dicts)."""
    location = CellLocation(cell='B4')

    locations_set = {location}

    assert location in locations_set


# Computed property tests


def test_row_returns_0_indexed():
    """row property returns 0-indexed row number."""
    location = CellLocation(cell='B4')

    assert location.row == 3


def test_col_returns_0_indexed():
    """col property returns 0-indexed column number."""
    location = CellLocation(cell='B4')

    assert location.col == 1


def test_row_1indexed_returns_1_indexed():
    """row_1indexed property returns 1-indexed row number."""
    location = CellLocation(cell='B4')

    assert location.row_1indexed == 4


def test_col_letter_returns_column_letters():
    """col_letter property returns column letter(s)."""
    location = CellLocation(cell='B4')

    assert location.col_letter == 'B'


def test_properties_with_a1():
    """Properties work correctly with A1 cell reference."""
    location = CellLocation(cell='A1')

    assert location.row == 0
    assert location.col == 0
    assert location.row_1indexed == 1
    assert location.col_letter == 'A'


def test_properties_with_double_letter_column():
    """Properties work correctly with double-letter columns (AA, AB, etc.)."""
    location = CellLocation(cell='AA10')

    assert location.row == 9
    assert location.col == 26
    assert location.row_1indexed == 10
    assert location.col_letter == 'AA'


def test_properties_with_triple_letter_column():
    """Properties work correctly with triple-letter columns (AAA, etc.)."""
    location = CellLocation(cell='AAA1')

    assert location.row == 0
    assert location.col == 702  # 26 + 26*26 = 702
    assert location.row_1indexed == 1
    assert location.col_letter == 'AAA'


def test_properties_with_large_row():
    """Properties work correctly with large row numbers."""
    location = CellLocation(cell='Z1000')

    assert location.row == 999
    assert location.col == 25
    assert location.row_1indexed == 1000
    assert location.col_letter == 'Z'


def test_col_letter_to_index_single_letters():
    """_col_letter_to_index handles single letters A-Z."""
    assert CellLocation._col_letter_to_index('A') == 0
    assert CellLocation._col_letter_to_index('B') == 1
    assert CellLocation._col_letter_to_index('Z') == 25


def test_col_letter_to_index_double_letters():
    """_col_letter_to_index handles double letters AA, AB, etc."""
    assert CellLocation._col_letter_to_index('AA') == 26
    assert CellLocation._col_letter_to_index('AB') == 27
    assert CellLocation._col_letter_to_index('AZ') == 51
    assert CellLocation._col_letter_to_index('BA') == 52


def test_col_letter_to_index_lowercase():
    """_col_letter_to_index handles lowercase letters."""
    assert CellLocation._col_letter_to_index('a') == 0
    assert CellLocation._col_letter_to_index('aa') == 26


def test_parse_cell_simple():
    """_parse_cell parses simple cell references."""
    col, row = CellLocation._parse_cell('B4')

    assert col == 'B'
    assert row == 4


def test_parse_cell_double_letter():
    """_parse_cell parses double-letter column references."""
    col, row = CellLocation._parse_cell('AA10')

    assert col == 'AA'
    assert row == 10


def test_properties_with_lowercase_cell():
    """Properties handle lowercase cell references."""
    location = CellLocation(cell='b4')

    assert location.row == 3
    assert location.col == 1
    assert location.row_1indexed == 4
    assert location.col_letter == 'b'


# value and __str__ tests


def test_value_returns_cell_string():
    """value property returns the cell string."""
    location = CellLocation(cell='B4')

    assert location.value == 'B4'


def test_str_returns_cell_string():
    """__str__ returns the cell string."""
    location = CellLocation(cell='B4')

    assert str(location) == 'B4'


def test_value_equals_str():
    """value property equals __str__."""
    location = CellLocation(cell='AA10')

    assert location.value == str(location)
