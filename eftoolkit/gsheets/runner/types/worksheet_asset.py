"""WorksheetAsset type for representing data to write to a worksheet."""

from collections.abc import Callable
from dataclasses import dataclass, field

from pandas import DataFrame

from eftoolkit.gsheets.runner.types.cell_location import CellLocation


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
        post_write_hooks: Callables to run after writing (e.g., custom post-processing).

    Example:
        >>> asset = WorksheetAsset(
        ...     df=my_dataframe,
        ...     location=CellLocation(cell='B4'),
        ... )
    """

    df: DataFrame
    location: CellLocation
    post_write_hooks: list[Callable] = field(default_factory=list)
