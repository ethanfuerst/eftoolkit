"""HookContext type for post-write hook callbacks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from eftoolkit.gsheets.core import Worksheet
    from eftoolkit.gsheets.runner.types.worksheet_asset import WorksheetAsset


@dataclass
class HookContext:
    """Context passed to post-write hooks.

    Provides access to the worksheet and other useful information
    for performing post-write operations.

    Attributes:
        worksheet: The Worksheet instance that was written to.
        asset: The WorksheetAsset that triggered this hook.
        worksheet_name: The name of the worksheet definition.
        runner_context: The shared context dictionary from the DashboardRunner.

    Example:
        >>> def my_hook(ctx: HookContext) -> None:
        ...     # Access the worksheet for additional operations
        ...     ctx.worksheet.format_range('A1:B10', {'bold': True})
        ...     # Access the asset's data
        ...     print(f'Wrote {len(ctx.asset.df)} rows')
    """

    worksheet: Worksheet
    asset: WorksheetAsset
    worksheet_name: str
    runner_context: dict[str, Any]
