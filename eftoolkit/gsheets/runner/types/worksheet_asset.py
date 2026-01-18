"""WorksheetAsset type for representing data to write to a worksheet."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from pandas import DataFrame

from eftoolkit.gsheets.runner.types.cell_location import CellLocation
from eftoolkit.gsheets.runner.types.cell_range import CellRange
from eftoolkit.gsheets.utils import column_index_to_letter

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
        header_range: CellRange for the header row (e.g., 'B4:E4').
        data_range: CellRange for data rows excluding header (e.g., 'B5:E14').
        full_range: CellRange for header + data (e.g., 'B4:E14').
        column_ranges: Dict mapping column name to CellRange including header.
        data_column_ranges: Dict mapping column name to data-only CellRange.
        num_rows: Number of data rows (excluding header).
        num_cols: Number of columns.
        start_row: 1-based row index of the header row.
        end_row: 1-based row index of the last data row.
        start_col: Letter of the first column.
        end_col: Letter of the last column.

    Example:
        >>> def my_hook(ctx: HookContext) -> None:
        ...     # Use computed ranges for formatting (use .value for API calls)
        ...     ctx.worksheet.format_range(ctx.asset.header_range.value, {'bold': True})
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
        return self.location.row_1indexed

    @property
    def end_row(self) -> int:
        """1-based row index of the last data row."""
        return self.start_row + self.num_rows

    @property
    def start_col(self) -> str:
        """Letter of the first column."""
        return self.location.col_letter

    @property
    def end_col(self) -> str:
        """Letter of the last column."""
        return column_index_to_letter(self.location.col + self.num_cols - 1)

    @property
    def header_range(self) -> CellRange:
        """CellRange for the header row.

        Example: 'B4:E4' for a 4-column DataFrame starting at B4.
        Use .value to get the A1-notation string for API calls.
        """
        return CellRange.from_string(
            f'{self.start_col}{self.start_row}:{self.end_col}{self.start_row}'
        )

    @property
    def data_range(self) -> CellRange:
        """CellRange for data rows (excluding header).

        Example: 'B5:E14' for a 10-row, 4-column DataFrame starting at B4.
        Returns empty range (same start and end) if DataFrame has no rows.
        Use .value to get the A1-notation string for API calls.
        """
        data_start_row = self.start_row + 1
        return CellRange.from_string(
            f'{self.start_col}{data_start_row}:{self.end_col}{self.end_row}'
        )

    @property
    def full_range(self) -> CellRange:
        """CellRange for header + data.

        Example: 'B4:E14' for a 10-row, 4-column DataFrame starting at B4.
        Use .value to get the A1-notation string for API calls.
        """
        return CellRange.from_string(
            f'{self.start_col}{self.start_row}:{self.end_col}{self.end_row}'
        )

    @property
    def column_ranges(self) -> dict[str, CellRange]:
        """Dict mapping column name to CellRange (including header).

        Example: {'Name': CellRange('B4:B14'), 'Score': CellRange('C4:C14')}.
        Use .value on each CellRange to get the A1-notation string for API calls.
        """
        result: dict[str, CellRange] = {}
        for i, col_name in enumerate(self.df.columns):
            col_letter = column_index_to_letter(self.location.col + i)
            result[col_name] = CellRange.from_string(
                f'{col_letter}{self.start_row}:{col_letter}{self.end_row}'
            )
        return result

    @property
    def data_column_ranges(self) -> dict[str, CellRange]:
        """Dict mapping column name to data-only CellRange (excluding header).

        Example: {'Name': CellRange('B5:B14'), 'Score': CellRange('C5:C14')}.
        Use .value on each CellRange to get the A1-notation string for API calls.
        """
        result: dict[str, CellRange] = {}
        data_start_row = self.start_row + 1
        for i, col_name in enumerate(self.df.columns):
            col_letter = column_index_to_letter(self.location.col + i)
            result[col_name] = CellRange.from_string(
                f'{col_letter}{data_start_row}:{col_letter}{self.end_row}'
            )
        return result
