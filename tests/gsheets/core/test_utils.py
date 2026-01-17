"""Tests for gsheets utility functions."""

from eftoolkit.gsheets.utils import parse_cell_reference


class TestParseCellReference:
    """Tests for parse_cell_reference function."""

    def test_simple_cell(self):
        """Parse simple cell reference like A1."""
        row, col = parse_cell_reference('A1')

        assert row == 0
        assert col == 0

    def test_cell_with_larger_indices(self):
        """Parse cell reference with larger row/col like C5."""
        row, col = parse_cell_reference('C5')

        assert row == 4  # 5 - 1 = 4 (0-indexed)
        assert col == 2  # C = 2 (0-indexed)

    def test_multi_letter_column(self):
        """Parse cell with multi-letter column like AA1."""
        row, col = parse_cell_reference('AA1')

        assert row == 0
        assert col == 26  # AA = 26 (0-indexed)

    def test_cell_with_sheet_name_prefix(self):
        """Parse cell reference with sheet name prefix like Sheet1!B2."""
        row, col = parse_cell_reference('Sheet1!B2')

        assert row == 1  # 2 - 1 = 1
        assert col == 1  # B = 1

    def test_range_extracts_start_cell(self):
        """Parse range extracts the start cell like A1:C3 -> A1."""
        row, col = parse_cell_reference('A1:C3')

        assert row == 0
        assert col == 0

    def test_sheet_name_and_range(self):
        """Parse reference with both sheet name and range."""
        row, col = parse_cell_reference('Data!D10:F20')

        assert row == 9  # 10 - 1 = 9
        assert col == 3  # D = 3

    def test_invalid_reference_returns_default(self):
        """Invalid cell reference returns (0, 0)."""
        row, col = parse_cell_reference('123')  # Numbers only, no column letters

        assert row == 0
        assert col == 0

    def test_empty_string_returns_default(self):
        """Empty string returns (0, 0)."""
        row, col = parse_cell_reference('')

        assert row == 0
        assert col == 0

    def test_lowercase_column(self):
        """Lowercase column letters are handled correctly."""
        row, col = parse_cell_reference('b3')

        assert row == 2
        assert col == 1

    def test_column_only_reference(self):
        """Column-only reference like 'X' returns None for row."""
        row, col = parse_cell_reference('X')

        assert row is None
        assert col == 23  # X = 23 (0-indexed)

    def test_column_only_multi_letter(self):
        """Multi-letter column-only reference like 'AA' returns None for row."""
        row, col = parse_cell_reference('AA')

        assert row is None
        assert col == 26  # AA = 26 (0-indexed)

    def test_column_only_lowercase(self):
        """Lowercase column-only reference is handled correctly."""
        row, col = parse_cell_reference('c')

        assert row is None
        assert col == 2  # C = 2 (0-indexed)
