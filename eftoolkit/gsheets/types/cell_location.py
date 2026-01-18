"""CellLocation type for specifying DataFrame locations within worksheets."""

from dataclasses import dataclass
from functools import cached_property


@dataclass(frozen=True)
class CellLocation:
    """Where a DataFrame should be written within a worksheet.

    The worksheet name comes from the WorksheetDefinition.name property,
    so CellLocation only needs the cell address within that worksheet.

    Attributes:
        cell: The cell address where the DataFrame starts (e.g., 'B4', 'A1').

    Computed Properties:
        row: 0-indexed row number (e.g., 'B4' → 3).
        col: 0-indexed column number (e.g., 'B4' → 1).
        row_1indexed: 1-indexed row number for Google Sheets API (e.g., 'B4' → 4).
        col_letter: Column letter(s) (e.g., 'B4' → 'B', 'AA1' → 'AA').
        value: String representation of the cell (same as cell attribute).

    Example:
        >>> location = CellLocation(cell='B4')
        >>> location.cell
        'B4'
        >>> location.row
        3
        >>> location.col
        1
        >>> location.row_1indexed
        4
        >>> location.col_letter
        'B'
    """

    cell: str

    @staticmethod
    def _parse_cell(cell: str) -> tuple[str, int]:
        """Parse a cell reference like 'B4' into column ('B') and row (4).

        Args:
            cell: Cell reference string (e.g., 'B4', 'AA10').

        Returns:
            Tuple of (column_letter, row_1indexed).
        """
        col = ''.join(c for c in cell if c.isalpha())
        row = int(''.join(c for c in cell if c.isdigit()))
        return col, row

    @staticmethod
    def _col_letter_to_index(col_letter: str) -> int:
        """Convert column letter(s) to 0-indexed column number.

        Args:
            col_letter: Column letter(s) (e.g., 'A', 'B', 'AA').

        Returns:
            0-indexed column number (A=0, B=1, Z=25, AA=26).
        """
        col = 0
        for char in col_letter.upper():
            col = col * 26 + (ord(char) - ord('A') + 1)
        return col - 1

    @cached_property
    def _parsed(self) -> tuple[str, int]:
        """Cached parsed cell reference."""
        return self._parse_cell(self.cell)

    @property
    def col_letter(self) -> str:
        """Column letter(s) from the cell reference.

        Example: 'B4' → 'B', 'AA10' → 'AA'.
        """
        return self._parsed[0]

    @property
    def row_1indexed(self) -> int:
        """1-indexed row number for Google Sheets API.

        Example: 'B4' → 4.
        """
        return self._parsed[1]

    @property
    def row(self) -> int:
        """0-indexed row number.

        Example: 'B4' → 3.
        """
        return self.row_1indexed - 1

    @property
    def col(self) -> int:
        """0-indexed column number.

        Example: 'B4' → 1, 'AA10' → 26.
        """
        return self._col_letter_to_index(self.col_letter)

    @property
    def value(self) -> str:
        """String representation of the cell.

        Same as the cell attribute and __str__. Useful for API calls.

        Example: 'B4' → 'B4'.
        """
        return self.cell

    def __str__(self) -> str:
        """Return string representation of the cell."""
        return self.cell
