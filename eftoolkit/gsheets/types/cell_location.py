"""CellLocation type for specifying DataFrame locations within worksheets."""

from dataclasses import dataclass
from functools import cached_property


@dataclass(frozen=True)
class CellLocation:
    """Where a DataFrame should be written within a worksheet.

    The worksheet name comes from the WorksheetDefinition.name property,
    so CellLocation only needs the cell address within that worksheet.

    Attributes:
        cell: The base cell address (e.g., 'B4', 'A1').
        offset_rows: Number of rows to offset from the base cell. Positive moves down,
            negative moves up. Defaults to 0.
        offset_cols: Number of columns to offset from the base cell. Positive moves right,
            negative moves left. Defaults to 0.

    Computed Properties:
        row: 0-indexed row number after applying offset.
        col: 0-indexed column number after applying offset.
        row_1indexed: 1-indexed row number for Google Sheets API after applying offset.
        col_letter: Column letter(s) after applying offset.
        value: String representation of the cell after applying offsets.

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

        >>> # With offsets
        >>> offset_loc = CellLocation(cell='I2', offset_cols=1)
        >>> offset_loc.value
        'J2'
        >>> CellLocation(cell='I2', offset_cols=-1).value
        'H2'
        >>> CellLocation(cell='I2', offset_rows=1).value
        'I3'
        >>> CellLocation(cell='I2', offset_rows=-1).value
        'I1'
    """

    cell: str
    offset_rows: int = 0
    offset_cols: int = 0

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

    @staticmethod
    def _col_index_to_letter(index: int) -> str:
        """Convert 0-indexed column number to column letter(s).

        Args:
            index: 0-indexed column number (0=A, 1=B, 25=Z, 26=AA).

        Returns:
            Column letter(s) (e.g., 'A', 'B', 'AA').
        """
        result = ''
        index += 1  # Convert to 1-indexed for calculation
        while index > 0:
            index, remainder = divmod(index - 1, 26)
            result = chr(ord('A') + remainder) + result
        return result

    @cached_property
    def _parsed(self) -> tuple[str, int]:
        """Cached parsed cell reference (base cell without offsets)."""
        return self._parse_cell(self.cell)

    @cached_property
    def _base_col_index(self) -> int:
        """0-indexed column number of the base cell (without offset)."""
        return self._col_letter_to_index(self._parsed[0])

    @cached_property
    def _base_row_1indexed(self) -> int:
        """1-indexed row number of the base cell (without offset)."""
        return self._parsed[1]

    @property
    def col_letter(self) -> str:
        """Column letter(s) after applying offset.

        Example: 'B4' → 'B', CellLocation('I2', offset_cols=1) → 'J'.
        """
        return self._col_index_to_letter(self.col)

    @property
    def row_1indexed(self) -> int:
        """1-indexed row number for Google Sheets API after applying offset.

        Example: 'B4' → 4, CellLocation('I2', offset_rows=1) → 3.
        """
        return self._base_row_1indexed + self.offset_rows

    @property
    def row(self) -> int:
        """0-indexed row number after applying offset.

        Example: 'B4' → 3, CellLocation('I2', offset_rows=1) → 2.
        """
        return self.row_1indexed - 1

    @property
    def col(self) -> int:
        """0-indexed column number after applying offset.

        Example: 'B4' → 1, CellLocation('I2', offset_cols=1) → 9.
        """
        return self._base_col_index + self.offset_cols

    @property
    def value(self) -> str:
        """String representation of the cell after applying offsets.

        Useful for API calls.

        Example: 'B4' → 'B4', CellLocation('I2', offset_cols=1) → 'J2'.
        """
        return f'{self.col_letter}{self.row_1indexed}'

    def __str__(self) -> str:
        """Return string representation of the cell after applying offsets."""
        return self.value
