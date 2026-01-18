"""WorksheetFormatting type for worksheet-level formatting configuration."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from eftoolkit.gsheets.types import CellType, RangeType


@dataclass
class WorksheetFormatting:
    """Worksheet-level formatting configuration.

    Contains all formatting options that apply to a worksheet after data is written.
    This is returned by WorksheetDefinition.get_formatting() and applied in Phase 4
    of the DashboardRunner workflow.

    Attributes:
        merge_ranges: List of ranges to merge. Accepts A1-notation strings,
            CellLocation, or CellRange objects (e.g., ['A1:C1', CellRange.from_string('B5:D5')]).
        conditional_formats: List of conditional format rule dictionaries.
        notes: Dictionary mapping cell addresses to note text. Keys can be
            A1-notation strings or CellLocation objects.
        column_widths: Dictionary mapping column letters or indices to pixel widths.
        borders: Dictionary mapping ranges to border style configurations.
            Keys can be A1-notation strings, CellLocation, or CellRange objects.
        data_validations: List of data validation rule dictionaries.
        freeze_rows: Number of rows to freeze from the top (e.g., 1 for header row).
        freeze_columns: Number of columns to freeze from the left.
        auto_resize_columns: Tuple of (start_column, end_column) to auto-resize.
        format_config_path: Path to a JSON format configuration file.
        format_dict: Inline format configuration dictionary for cell formatting.

    Example:
        >>> from eftoolkit.gsheets.types import CellLocation, CellRange
        >>> formatting = WorksheetFormatting(
        ...     freeze_rows=1,
        ...     auto_resize_columns=(0, 5),
        ...     format_dict={'header_color': '#4a86e8'},
        ...     notes={CellLocation(cell='A1'): 'Last updated: 2024-01-15'},
        ...     merge_ranges=[CellRange.from_string('A1:C1')],
        ... )
    """

    merge_ranges: list[RangeType] = field(default_factory=list)
    conditional_formats: list[dict[str, Any]] = field(default_factory=list)
    notes: dict[CellType, str] = field(default_factory=dict)
    column_widths: dict[str | int, int] = field(default_factory=dict)
    borders: dict[RangeType, dict[str, Any]] = field(default_factory=dict)
    data_validations: list[dict[str, Any]] = field(default_factory=list)
    freeze_rows: int | None = None
    freeze_columns: int | None = None
    auto_resize_columns: tuple[int, int] | None = None
    format_config_path: Path | None = None
    format_dict: dict[str, Any] | None = None
