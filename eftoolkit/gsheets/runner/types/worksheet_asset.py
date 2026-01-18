"""WorksheetAsset type for representing data to write to a worksheet."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from pandas import DataFrame

from eftoolkit.gsheets.runner.types.cell_location import CellLocation

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

    Example:
        >>> def my_hook(ctx: HookContext) -> None:
        ...     ctx.worksheet.format_range('A1:B10', {'bold': True})
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
