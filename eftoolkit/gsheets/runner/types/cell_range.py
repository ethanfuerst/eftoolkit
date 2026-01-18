"""CellRange type for representing cell ranges with computed properties."""

from __future__ import annotations

from dataclasses import dataclass

from eftoolkit.gsheets.runner.types.cell_location import CellLocation
from eftoolkit.gsheets.utils import column_index_to_letter


@dataclass(frozen=True)
class CellRange:
    """A range of cells in A1 notation.

    Can represent:
    - Single cells: 'A1' (start == end)
    - Multi-cell ranges: 'B4:E14'

    Attributes:
        start: The top-left cell of the range.
        end: The bottom-right cell of the range.

    Computed Properties:
        start_row: 0-indexed start row.
        end_row: 0-indexed end row.
        start_col: 0-indexed start column.
        end_col: 0-indexed end column.
        start_row_1indexed: 1-indexed start row for Google Sheets API.
        end_row_1indexed: 1-indexed end row for Google Sheets API.
        start_col_letter: Start column letter(s).
        end_col_letter: End column letter(s).
        num_rows: Number of rows in the range.
        num_cols: Number of columns in the range.
        is_single_cell: True if range is a single cell.
        value: A1 notation string representation of the range.

    Example:
        >>> cell_range = CellRange.from_string('B4:E14')
        >>> cell_range.start_row
        3
        >>> cell_range.end_row
        13
        >>> cell_range.num_rows
        11
        >>> cell_range.num_cols
        4
        >>> str(cell_range)
        'B4:E14'
    """

    start: CellLocation
    end: CellLocation

    @classmethod
    def from_string(cls, range_str: str) -> CellRange:
        """Parse A1 notation like 'B4:E14' or 'A1'.

        Args:
            range_str: A1 notation range string (e.g., 'B4:E14', 'A1').

        Returns:
            CellRange instance.

        Example:
            >>> CellRange.from_string('B4:E14')
            CellRange(start=CellLocation(cell='B4'), end=CellLocation(cell='E14'))
            >>> CellRange.from_string('A1')
            CellRange(start=CellLocation(cell='A1'), end=CellLocation(cell='A1'))
        """
        if ':' in range_str:
            start_str, end_str = range_str.split(':')
        else:
            start_str = end_str = range_str
        return cls(
            start=CellLocation(cell=start_str),
            end=CellLocation(cell=end_str),
        )

    @classmethod
    def from_bounds(
        cls,
        start_row: int,
        start_col: int,
        end_row: int,
        end_col: int,
    ) -> CellRange:
        """Create from 0-indexed row/col bounds.

        Args:
            start_row: 0-indexed start row.
            start_col: 0-indexed start column.
            end_row: 0-indexed end row.
            end_col: 0-indexed end column.

        Returns:
            CellRange instance.

        Example:
            >>> CellRange.from_bounds(3, 1, 13, 4)  # B4:E14
            CellRange(start=CellLocation(cell='B4'), end=CellLocation(cell='E14'))
        """
        start_col_letter = column_index_to_letter(start_col)
        end_col_letter = column_index_to_letter(end_col)
        start_row_1indexed = start_row + 1
        end_row_1indexed = end_row + 1
        return cls(
            start=CellLocation(cell=f'{start_col_letter}{start_row_1indexed}'),
            end=CellLocation(cell=f'{end_col_letter}{end_row_1indexed}'),
        )

    @property
    def start_row(self) -> int:
        """0-indexed start row."""
        return self.start.row

    @property
    def end_row(self) -> int:
        """0-indexed end row."""
        return self.end.row

    @property
    def start_col(self) -> int:
        """0-indexed start column."""
        return self.start.col

    @property
    def end_col(self) -> int:
        """0-indexed end column."""
        return self.end.col

    @property
    def start_row_1indexed(self) -> int:
        """1-indexed start row for Google Sheets API."""
        return self.start.row_1indexed

    @property
    def end_row_1indexed(self) -> int:
        """1-indexed end row for Google Sheets API."""
        return self.end.row_1indexed

    @property
    def start_col_letter(self) -> str:
        """Start column letter(s)."""
        return self.start.col_letter

    @property
    def end_col_letter(self) -> str:
        """End column letter(s)."""
        return self.end.col_letter

    @property
    def num_rows(self) -> int:
        """Number of rows in the range."""
        return self.end_row - self.start_row + 1

    @property
    def num_cols(self) -> int:
        """Number of columns in the range."""
        return self.end_col - self.start_col + 1

    @property
    def is_single_cell(self) -> bool:
        """True if range is a single cell."""
        return self.start == self.end

    @property
    def value(self) -> str:
        """A1 notation string representation of the range.

        Same as __str__. Useful for API calls.

        Returns 'A1' for single cells, 'B4:E14' for multi-cell ranges.
        """
        if self.is_single_cell:
            return self.start.cell
        return f'{self.start.cell}:{self.end.cell}'

    def __str__(self) -> str:
        """Return A1 notation string.

        Returns 'A1' for single cells, 'B4:E14' for multi-cell ranges.
        """
        return self.value

    def __contains__(self, item: CellLocation | CellRange) -> bool:
        """Check if a CellLocation or CellRange is within this range.

        For CellLocation: Returns True if the cell is within the range bounds.
        For CellRange: Returns True if the entire range is contained within this range.

        Args:
            item: A CellLocation or CellRange to check.

        Returns:
            True if the item is fully within this range.

        Example:
            >>> outer = CellRange.from_string('B4:E14')
            >>> CellLocation(cell='C5') in outer
            True
            >>> CellRange.from_string('C5:D10') in outer
            True
            >>> CellRange.from_string('A1:C5') in outer
            False
        """
        if isinstance(item, CellRange):
            return (
                self.start_row <= item.start_row
                and item.end_row <= self.end_row
                and self.start_col <= item.start_col
                and item.end_col <= self.end_col
            )
        return (
            self.start_row <= item.row <= self.end_row
            and self.start_col <= item.col <= self.end_col
        )
