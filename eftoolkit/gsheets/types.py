"""Core type definitions for the Dashboard Runner feature.

This module provides typed abstractions for worksheet definitions,
replacing implicit tuple contracts with clear, IDE-friendly types.

Example usage:
    from eftoolkit.gsheets.types import CellLocation, WorksheetAsset, WorksheetDefinition

    # Define where a DataFrame goes within a worksheet
    location = CellLocation(cell='B4')

    # Create an asset with data and formatting
    asset = WorksheetAsset(
        df=my_dataframe,
        location=location,
        format_config_path=Path('formats/summary.json'),
    )

    # Implement a worksheet definition (one worksheet can have multiple DataFrames)
    class RevenueWorksheet:
        @property
        def name(self) -> str:
            return 'Revenue'  # This becomes the worksheet name

        def generate(self, config: dict, context: dict) -> list[WorksheetAsset]:
            summary_df = build_summary(config)
            breakdown_df = build_breakdown(config)
            return [
                WorksheetAsset(df=summary_df, location=CellLocation(cell='B2')),
                WorksheetAsset(df=breakdown_df, location=CellLocation(cell='B10')),
            ]

        def get_format_overrides(self, context: dict) -> dict:
            return {'header_color': '#4a86e8'}

    # Multiple worksheets can be passed to DashboardRunner:
    worksheets = [SummaryWorksheet(), RevenueWorksheet(), ExpensesWorksheet()]
    runner = DashboardRunner(config={'sheet_name': 'Q1 Report'}, worksheets=worksheets)
    runner.run()
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from pandas import DataFrame


@dataclass(frozen=True)
class CellLocation:
    """Where a DataFrame should be written within a worksheet.

    The worksheet name comes from the WorksheetDefinition.name property,
    so CellLocation only needs the cell address within that worksheet.

    Attributes:
        cell: The cell address where the DataFrame starts (e.g., 'B4', 'A1').

    Example:
        >>> location = CellLocation(cell='B4')
        >>> location.cell
        'B4'
    """

    cell: str


@dataclass
class WorksheetAsset:
    """An asset to be written to a worksheet.

    Contains a DataFrame to write, its target location within the worksheet,
    optional formatting configuration, and post-write hooks.

    A WorksheetDefinition.generate() returns a list of WorksheetAssets, allowing
    multiple DataFrames to be written to different locations on the same worksheet.

    Attributes:
        df: The DataFrame to write to the sheet.
        location: Where to write the DataFrame within the worksheet.
        format_config_path: Path to a JSON format configuration file.
        format_dict: Inline format configuration dictionary.
        post_write_hooks: Callables to run after writing (e.g., conditional formatting).

    Example:
        >>> asset = WorksheetAsset(
        ...     df=my_dataframe,
        ...     location=CellLocation(cell='B4'),
        ...     format_config_path=Path('formats/summary.json'),
        ... )
    """

    df: DataFrame
    location: CellLocation
    format_config_path: Path | None = None
    format_dict: dict[str, Any] | None = None
    post_write_hooks: list[Callable] = field(default_factory=list)


@runtime_checkable
class WorksheetDefinition(Protocol):
    """Protocol for defining a worksheet in a spreadsheet.

    Each WorksheetDefinition represents one worksheet in the spreadsheet.
    The generate() method returns a list of WorksheetAssets, allowing multiple
    DataFrames to be written to different locations on the same worksheet.

    A worksheet definition specifies:
    - Worksheet name (becomes the tab name in the spreadsheet)
    - Data generation logic (can produce multiple DataFrames)
    - Format configuration for each DataFrame
    - Post-write hooks for conditional formatting, notes, merges, etc.

    Example implementation:
        class RevenueWorksheet:
            @property
            def name(self) -> str:
                return 'Revenue'  # Worksheet name

            def generate(self, config: dict, context: dict) -> list[WorksheetAsset]:
                summary_df = query_summary(config['db'])
                breakdown_df = query_breakdown(config['db'])
                return [
                    WorksheetAsset(
                        df=summary_df,
                        location=CellLocation(cell='B2'),
                        format_dict={'header_color': '#4a86e8'},
                    ),
                    WorksheetAsset(
                        df=breakdown_df,
                        location=CellLocation(cell='B10'),
                        format_config_path=Path('formats/breakdown.json'),
                    ),
                ]

            def get_format_overrides(self, context: dict) -> dict:
                return {'currency_format': '$#,##0.00'}
    """

    @property
    def name(self) -> str:
        """The worksheet name in the spreadsheet."""
        ...

    def generate(self, config: dict, context: dict) -> list['WorksheetAsset']:
        """Generate the worksheet's data and return a list of WorksheetAssets.

        Args:
            config: Configuration dictionary (e.g., database connections, settings).
            context: Runtime context (e.g., date ranges, user preferences).

        Returns:
            List of WorksheetAssets, each containing a DataFrame and its location
            within this worksheet. Multiple assets = multiple DataFrames on one worksheet.
        """
        ...

    def get_format_overrides(self, context: dict) -> dict:
        """Return format overrides for this worksheet.

        Args:
            context: Runtime context for dynamic formatting decisions.

        Returns:
            Dictionary of format overrides to apply on top of base config.
        """
        ...
