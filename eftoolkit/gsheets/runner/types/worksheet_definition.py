"""WorksheetDefinition protocol for defining worksheets."""

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from eftoolkit.gsheets.runner.types.worksheet_asset import WorksheetAsset
    from eftoolkit.gsheets.runner.types.worksheet_formatting import WorksheetFormatting


@runtime_checkable
class WorksheetDefinition(Protocol):
    """Protocol for defining a worksheet in a spreadsheet.

    Each WorksheetDefinition represents one worksheet in the spreadsheet.
    The generate() method returns a list of WorksheetAssets, allowing multiple
    DataFrames to be written to different locations on the same worksheet.

    A worksheet definition specifies:
    - Worksheet name (becomes the tab name in the spreadsheet)
    - Data generation logic (can produce multiple DataFrames)
    - Worksheet-level formatting (applied after all data is written)
    - Post-write hooks for custom processing

    Example implementation:
        class RevenueWorksheet:
            @property
            def name(self) -> str:
                return 'Revenue'  # Worksheet name

            def generate(self, config: dict, context: dict) -> list[WorksheetAsset]:
                summary_df = query_summary(config['db'])
                breakdown_df = query_breakdown(config['db'])
                return [
                    WorksheetAsset(df=summary_df, location=CellLocation(cell='B2')),
                    WorksheetAsset(df=breakdown_df, location=CellLocation(cell='B10')),
                ]

            def get_formatting(self, context: dict) -> WorksheetFormatting | None:
                return WorksheetFormatting(
                    freeze_rows=1,
                    format_dict={'header_color': '#4a86e8'},
                )
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

    def get_formatting(self, context: dict) -> 'WorksheetFormatting | None':
        """Return worksheet-level formatting to apply after all assets are written.

        Args:
            context: Runtime context for dynamic formatting decisions.

        Returns:
            WorksheetFormatting with formatting options, or None if no formatting needed.
        """
        ...
