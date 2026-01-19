"""Tests for CellLocation dataclass."""

import pytest

from eftoolkit.gsheets.runner import CellLocation
from eftoolkit.gsheets.utils import column_index_to_letter, column_letter_to_index


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
    """column_letter_to_index handles single letters A-Z."""
    assert column_letter_to_index('A') == 0
    assert column_letter_to_index('B') == 1
    assert column_letter_to_index('Z') == 25


def test_col_letter_to_index_double_letters():
    """column_letter_to_index handles double letters AA, AB, etc."""
    assert column_letter_to_index('AA') == 26
    assert column_letter_to_index('AB') == 27
    assert column_letter_to_index('AZ') == 51
    assert column_letter_to_index('BA') == 52


def test_col_letter_to_index_lowercase():
    """column_letter_to_index handles lowercase letters."""
    assert column_letter_to_index('a') == 0
    assert column_letter_to_index('aa') == 26


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
    """Properties handle lowercase cell references and normalize to uppercase."""
    location = CellLocation(cell='b4')

    assert location.row == 3
    assert location.col == 1
    assert location.row_1indexed == 4
    assert location.col_letter == 'B'  # Normalized to uppercase
    assert location.value == 'B4'  # Normalized to uppercase


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


# Offset tests


def test_offset_cols_positive():
    """offset_cols moves column right."""
    location = CellLocation(cell='I2', offset_cols=1)

    assert location.value == 'J2'
    assert location.col_letter == 'J'
    assert location.col == 9  # I=8, +1=9


def test_offset_cols_negative():
    """offset_cols moves column left."""
    location = CellLocation(cell='I2', offset_cols=-1)

    assert location.value == 'H2'
    assert location.col_letter == 'H'
    assert location.col == 7  # I=8, -1=7


def test_offset_rows_positive():
    """offset_rows moves row down."""
    location = CellLocation(cell='I2', offset_rows=1)

    assert location.value == 'I3'
    assert location.row_1indexed == 3
    assert location.row == 2


def test_offset_rows_negative():
    """offset_rows moves row up."""
    location = CellLocation(cell='I2', offset_rows=-1)

    assert location.value == 'I1'
    assert location.row_1indexed == 1
    assert location.row == 0


def test_offset_both_rows_and_cols():
    """Both offset_rows and offset_cols can be applied together."""
    location = CellLocation(cell='B2', offset_rows=2, offset_cols=3)

    assert location.value == 'E4'
    assert location.row_1indexed == 4
    assert location.col == 4  # B=1, +3=4
    assert location.col_letter == 'E'


def test_offset_zero_is_default():
    """Default offsets are 0, making value equal to cell."""
    location = CellLocation(cell='B4')

    assert location.offset_rows == 0
    assert location.offset_cols == 0
    assert location.value == 'B4'


def test_offset_preserves_base_cell():
    """cell attribute remains the original base cell."""
    location = CellLocation(cell='I2', offset_cols=1, offset_rows=1)

    assert location.cell == 'I2'
    assert location.value == 'J3'


def test_offset_with_double_letter_column():
    """Offset works correctly with double-letter columns."""
    location = CellLocation(cell='AA10', offset_cols=1)

    assert location.value == 'AB10'
    assert location.col_letter == 'AB'


def test_offset_from_single_to_double_letter():
    """Offset from Z to AA (single to double letter)."""
    location = CellLocation(cell='Z1', offset_cols=1)

    assert location.value == 'AA1'
    assert location.col_letter == 'AA'


def test_offset_from_double_to_single_letter():
    """Offset from AA to Z (double to single letter)."""
    location = CellLocation(cell='AA1', offset_cols=-1)

    assert location.value == 'Z1'
    assert location.col_letter == 'Z'


def test_offset_large_positive():
    """Large positive offset works correctly."""
    location = CellLocation(cell='A1', offset_cols=26, offset_rows=99)

    assert location.value == 'AA100'
    assert location.col == 26
    assert location.row == 99


def test_offset_col_index_to_letter_single():
    """column_index_to_letter handles single letters A-Z."""
    assert column_index_to_letter(0) == 'A'
    assert column_index_to_letter(1) == 'B'
    assert column_index_to_letter(25) == 'Z'


def test_offset_col_index_to_letter_double():
    """column_index_to_letter handles double letters AA, AB, etc."""
    assert column_index_to_letter(26) == 'AA'
    assert column_index_to_letter(27) == 'AB'
    assert column_index_to_letter(51) == 'AZ'
    assert column_index_to_letter(52) == 'BA'


def test_offset_equality_with_same_offsets():
    """CellLocation instances with same cell and offsets are equal."""
    loc1 = CellLocation(cell='B4', offset_rows=1, offset_cols=2)
    loc2 = CellLocation(cell='B4', offset_rows=1, offset_cols=2)

    assert loc1 == loc2


def test_offset_inequality_with_different_offsets():
    """CellLocation instances with different offsets are not equal."""
    loc1 = CellLocation(cell='B4', offset_rows=1)
    loc2 = CellLocation(cell='B4', offset_cols=1)

    assert loc1 != loc2


def test_offset_hashable():
    """CellLocation with offsets is hashable."""
    location = CellLocation(cell='B4', offset_rows=1, offset_cols=2)

    locations_set = {location}

    assert location in locations_set


def test_offset_str_returns_offset_value():
    """__str__ returns the cell value with offsets applied."""
    location = CellLocation(cell='B4', offset_rows=1, offset_cols=1)

    assert str(location) == 'C5'
