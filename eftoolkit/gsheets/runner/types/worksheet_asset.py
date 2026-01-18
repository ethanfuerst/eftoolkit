"""WorksheetAsset type for representing data to write to a worksheet."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from functools import cached_property
from typing import TYPE_CHECKING

from pandas import DataFrame

from eftoolkit.gsheets.runner.types.cell_location import CellLocation
from eftoolkit.gsheets.utils import column_index_to_letter, parse_cell_reference

if TYPE_CHECKING:
    from eftoolkit.gsheets.runner.types.hook_context import HookContext


@dataclass
class WorksheetAsset:
    """An asset to be written to a worksheet.

    Contains a DataFrame to write, its target location within the worksheet,
    and post-write hooks. Formatting is handled at the worksheet level via
    WorksheetDefinition.get_formatting().

    A WorksheetDefinition.generate() returns a list of WorksheetAssets, allowing
    multiple DataFrames to be written to different locations on the same worksheet.

    Attributes:
        df: The DataFrame to write to the sheet.
        location: Where to write the DataFrame within the worksheet.
        post_write_hooks: Callables that receive a HookContext and run after writing.
            The HookContext provides access to the worksheet, asset, and runner context.

    Computed Properties:
        header_range: A1-notation range for the header row (e.g., 'B4:E4').
        data_range: A1-notation range for data rows excluding header (e.g., 'B5:E14').
        full_range: A1-notation range for header + data (e.g., 'B4:E14').
        column_ranges: Dict mapping column name to full column range including header.
        data_column_ranges: Dict mapping column name to data-only column range.
        num_rows: Number of data rows (excluding header).
        num_cols: Number of columns.
        start_row: 1-based row index of the header row.
        end_row: 1-based row index of the last data row.
        start_col: Letter of the first column.
        end_col: Letter of the last column.

    Example:
        >>> def my_hook(ctx: HookContext) -> None:
        ...     # Use computed ranges for formatting
        ...     ctx.worksheet.format_range(ctx.asset.header_range, {'bold': True})
        ...
        >>> asset = WorksheetAsset(
        ...     df=my_dataframe,
        ...     location=CellLocation(cell='B4'),
        ...     post_write_hooks=[my_hook],
        ... )
    """

    df: DataFrame
    location: CellLocation
    post_write_hooks: list[Callable[[HookContext], None]] = field(default_factory=list)

    def __hash__(self) -> int:
        """Hash based on location since DataFrames aren't hashable."""
        return hash(self.location)

    @cached_property
    def _parsed_location(self) -> tuple[int, int]:
        """Parse the location cell reference into (row, col) 0-indexed."""
        row, col = parse_cell_reference(self.location.cell)
        # parse_cell_reference returns None for column-only refs, default to 0
        return (row if row is not None else 0, col)

    @property
    def num_rows(self) -> int:
        """Number of data rows (excluding header)."""
        return len(self.df)

    @property
    def num_cols(self) -> int:
        """Number of columns."""
        return len(self.df.columns)

    @property
    def start_row(self) -> int:
        """1-based row index of the header row."""
        return self._parsed_location[0] + 1

    @property
    def end_row(self) -> int:
        """1-based row index of the last data row."""
        return self.start_row + self.num_rows

    @property
    def start_col(self) -> str:
        """Letter of the first column."""
        return column_index_to_letter(self._parsed_location[1])

    @property
    def end_col(self) -> str:
        """Letter of the last column."""
        return column_index_to_letter(self._parsed_location[1] + self.num_cols - 1)

    @property
    def header_range(self) -> str:
        """A1-notation range for the header row.

        Example: 'B4:E4' for a 4-column DataFrame starting at B4.
        """
        return f'{self.start_col}{self.start_row}:{self.end_col}{self.start_row}'

    @property
    def data_range(self) -> str:
        """A1-notation range for data rows (excluding header).

        Example: 'B5:E14' for a 10-row, 4-column DataFrame starting at B4.
        Returns empty range (same start and end) if DataFrame has no rows.
        """
        data_start_row = self.start_row + 1
        return f'{self.start_col}{data_start_row}:{self.end_col}{self.end_row}'

    @property
    def full_range(self) -> str:
        """A1-notation range for header + data.

        Example: 'B4:E14' for a 10-row, 4-column DataFrame starting at B4.
        """
        return f'{self.start_col}{self.start_row}:{self.end_col}{self.end_row}'

    @property
    def column_ranges(self) -> dict[str, str]:
        """Dict mapping column name to full column range (including header).

        Example: {'Name': 'B4:B14', 'Score': 'C4:C14'} for columns starting at B4.
        """
        result = {}
        start_col_idx = self._parsed_location[1]
        for i, col_name in enumerate(self.df.columns):
            col_letter = column_index_to_letter(start_col_idx + i)
            result[col_name] = (
                f'{col_letter}{self.start_row}:{col_letter}{self.end_row}'
            )
        return result

    @property
    def data_column_ranges(self) -> dict[str, str]:
        """Dict mapping column name to data-only column range (excluding header).

        Example: {'Name': 'B5:B14', 'Score': 'C5:C14'} for columns starting at B4.
        """
        result = {}
        start_col_idx = self._parsed_location[1]
        data_start_row = self.start_row + 1
        for i, col_name in enumerate(self.df.columns):
            col_letter = column_index_to_letter(start_col_idx + i)
            result[col_name] = (
                f'{col_letter}{data_start_row}:{col_letter}{self.end_row}'
            )
        return result
