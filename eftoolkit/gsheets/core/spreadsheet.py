"""Spreadsheet class for Google Sheets operations."""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar

from gspread import service_account_from_dict
from gspread.exceptions import APIError, WorksheetNotFound

from eftoolkit.gsheets.core.worksheet import Worksheet

if TYPE_CHECKING:
    from eftoolkit.gsheets.runner.types import WorksheetFormatting

T = TypeVar('T')


class Spreadsheet:
    """Google Spreadsheet client for managing worksheets.

    Represents the entire spreadsheet document.
    Use worksheet() to get individual tabs for read/write operations.

    Can be used as a context manager to automatically flush all accessed
    worksheets on exit. In local_preview mode, previews open in browser:

        with Spreadsheet(credentials, 'My Sheet') as ss:
            ws1 = ss.worksheet('Tab1')
            ws1.write_dataframe(df1)
            ws2 = ss.worksheet('Tab2')
            ws2.write_dataframe(df2)
        # Both ws1 and ws2 are flushed here
        # In local_preview mode, browser tabs open automatically
    """

    def __init__(
        self,
        credentials: dict | None = None,
        spreadsheet_name: str = '',
        *,
        max_retries: int = 5,
        base_delay: float = 2.0,
        local_preview: bool = False,
        preview_dir: str | Path = 'gsheets_preview',
    ) -> None:
        """Initialize Spreadsheet client.

        Args:
            credentials: Service account credentials dict. Required unless local_preview=True.
            spreadsheet_name: Name of the spreadsheet to open.
            max_retries: Max retry attempts for API errors (429, 5xx).
            base_delay: Base delay for exponential backoff.
            local_preview: If True, skip API calls and render to local HTML.
            preview_dir: Directory for HTML preview files (only used if local_preview=True).
        """
        self._local_preview = local_preview
        self._preview_dir = Path(preview_dir)
        self._spreadsheet_name = spreadsheet_name
        self._max_retries = max_retries
        self._base_delay = base_delay
        self._gspread_spreadsheet = None
        self._worksheets: dict[str, Worksheet] = {}  # Track all accessed worksheets

        if not local_preview:
            if not credentials:
                raise ValueError('credentials required unless local_preview=True')

            gc = service_account_from_dict(credentials)
            self._gspread_spreadsheet = gc.open(spreadsheet_name)

    def _execute_with_retry(self, func: Callable[[], T], description: str = '') -> T:
        """Execute function with exponential backoff retry on transient errors.

        Args:
            func: Callable to execute.
            description: Description for logging.

        Returns:
            Result of the function call.

        Raises:
            APIError: If max retries exhausted or non-retryable error.
        """
        retryable_status_codes = (429, 500, 502, 503, 504)

        for attempt in range(self._max_retries + 1):
            try:
                return func()
            except APIError as e:
                status_code = e.response.status_code
                if status_code not in retryable_status_codes:
                    raise
                if attempt == self._max_retries:
                    raise
                delay = self._base_delay * (2**attempt) + random.uniform(0, 1)
                logging.warning(
                    f'API error {status_code} on {description} '
                    f'(attempt {attempt + 1}/{self._max_retries}). '
                    f'Retrying in {delay:.2f}s...'
                )
                time.sleep(delay)

        # This should never be reached, but satisfies type checker
        raise RuntimeError('Unexpected state in retry loop')  # pragma: no cover

    def __enter__(self) -> Spreadsheet:
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Flush all accessed worksheets on clean exit.

        In local_preview mode, also opens all preview HTML files in browser.
        """
        if exc_type is None:
            for ws in self._worksheets.values():
                ws.flush()
            if self._local_preview:
                self.open_all_previews()

    @property
    def is_local_preview(self) -> bool:
        """True if running in local preview mode."""
        return self._local_preview

    def open_all_previews(self) -> None:
        """Open all worksheet previews in browser (local_preview mode only).

        Opens each accessed worksheet's HTML preview file in the default browser.

        Raises:
            RuntimeError: If not in local_preview mode.
        """
        if not self._local_preview:
            raise RuntimeError('open_all_previews only available in local_preview mode')

        for ws in self._worksheets.values():
            ws.open_preview()

    def _preview_path_for_worksheet(self, worksheet_name: str) -> Path:
        """Generate preview file path for a worksheet."""
        safe_spreadsheet = self._spreadsheet_name.replace(' ', '_').replace('/', '_')
        safe_worksheet = worksheet_name.replace(' ', '_').replace('/', '_')
        return self._preview_dir / f'{safe_spreadsheet}_{safe_worksheet}_preview.html'

    def worksheet(self, name: str) -> Worksheet:
        """Get worksheet by name.

        Args:
            name: Worksheet title (tab name).

        Returns:
            Worksheet instance for the specified tab.

        Raises:
            WorksheetNotFound: If worksheet doesn't exist (not in local_preview mode).
        """
        if name in self._worksheets:
            return self._worksheets[name]

        if self._local_preview:
            ws = Worksheet(
                None,
                self,
                local_preview=True,
                preview_output=self._preview_path_for_worksheet(name),
                worksheet_name=name,
            )
        else:
            gspread_ws = self._gspread_spreadsheet.worksheet(name)
            ws = Worksheet(gspread_ws, self)

        self._worksheets[name] = ws
        return ws

    def get_worksheet_names(self) -> list[str]:
        """List all worksheet names.

        Returns:
            List of worksheet titles.
        """
        if self._local_preview:
            return []

        return [ws.title for ws in self._gspread_spreadsheet.worksheets()]

    def create_worksheet(
        self, name: str, rows: int = 1000, cols: int = 26, *, replace: bool = False
    ) -> Worksheet:
        """Create a new worksheet.

        Args:
            name: Title for the new worksheet.
            rows: Number of rows (default 1000).
            cols: Number of columns (default 26).
            replace: If True, delete existing worksheet with same name first.

        Returns:
            Worksheet instance for the new tab.
        """
        if self._local_preview:
            if name not in self._worksheets:
                self._worksheets[name] = Worksheet(
                    None,
                    self,
                    local_preview=True,
                    preview_output=self._preview_path_for_worksheet(name),
                    worksheet_name=name,
                )
            return self._worksheets[name]

        if replace:
            self.delete_worksheet(name, ignore_missing=True)
            # Remove from cache if it existed
            self._worksheets.pop(name, None)

        gspread_ws = self._gspread_spreadsheet.add_worksheet(
            title=name, rows=rows, cols=cols
        )
        ws = Worksheet(gspread_ws, self)
        self._worksheets[name] = ws
        return ws

    def delete_worksheet(self, name: str, *, ignore_missing: bool = True) -> None:
        """Delete worksheet by name.

        Args:
            name: Worksheet title to delete.
            ignore_missing: If True, don't raise if worksheet doesn't exist.
        """
        if self._local_preview:
            return

        try:
            ws = self._gspread_spreadsheet.worksheet(name)
            self._gspread_spreadsheet.del_worksheet(ws)
        except WorksheetNotFound:
            if not ignore_missing:
                raise

    def reorder_worksheets(self, order: list[str]) -> None:
        """Reorder worksheets (tabs) to the specified order.

        Worksheets are reordered to match the given list. Worksheets not in the
        list are moved to the end in their original relative order. Worksheet
        names in the list that don't exist in the spreadsheet are skipped.

        Args:
            order: List of worksheet titles in the desired order.

        Example:
            ss.reorder_worksheets(['Dashboard', 'Draft', 'Manual Adds'])
            # Dashboard first, then Draft, then Manual Adds, then any other tabs
        """
        if self._local_preview:
            return

        all_worksheets = self._gspread_spreadsheet.worksheets()
        worksheets_by_title = {ws.title: ws for ws in all_worksheets}

        # Build ordered list: specified worksheets first (if they exist)
        ordered = []
        for title in order:
            if title in worksheets_by_title:
                ordered.append(worksheets_by_title[title])

        # Append remaining worksheets in their original order
        ordered_titles = {ws.title for ws in ordered}
        for ws in all_worksheets:
            if ws.title not in ordered_titles:
                ordered.append(ws)

        self._execute_with_retry(
            lambda: self._gspread_spreadsheet.reorder_worksheets(ordered),
            'reorder_worksheets',
        )

    def apply_formatting(
        self,
        worksheet_name: str,
        formatting: WorksheetFormatting,
        format_dict: dict[str, Any] | None = None,
    ) -> None:
        """Apply WorksheetFormatting to a worksheet.

        Queues all formatting operations specified in the WorksheetFormatting object.
        Operations are batched and sent when the Spreadsheet context exits or when
        the worksheet's flush() method is called.

        Args:
            worksheet_name: Name of the worksheet to apply formatting to.
            formatting: WorksheetFormatting instance with formatting settings.
            format_dict: Optional pre-merged format dict (if format_config_path and
                format_dict have already been merged). If None, only formatting.format_dict
                is used.

        Example:
            >>> from eftoolkit.gsheets.runner import WorksheetFormatting
            >>> with Spreadsheet(credentials, 'My Sheet') as ss:
            ...     ss.create_worksheet('Report', replace=True)
            ...     # ... write data ...
            ...     formatting = WorksheetFormatting(
            ...         freeze_rows=1,
            ...         auto_resize_columns=(0, 5),
            ...         notes={'A1': 'Header'},
            ...     )
            ...     ss.apply_formatting('Report', formatting)
            # All formatting applied on context exit
        """
        ws = self.worksheet(worksheet_name)

        # Apply freeze rows
        if formatting.freeze_rows is not None:
            ws.freeze_rows(formatting.freeze_rows)

        # Apply freeze columns
        if formatting.freeze_columns is not None:
            ws.freeze_columns(formatting.freeze_columns)

        # Apply auto-resize columns
        if formatting.auto_resize_columns is not None:
            start_col, end_col = formatting.auto_resize_columns
            ws.auto_resize_columns(start_col, end_col)

        # Apply merge ranges
        for merge_range in formatting.merge_ranges:
            ws.merge_cells(merge_range)

        # Apply notes
        if formatting.notes:
            # Convert CellType keys to strings for the API
            notes_dict = {}
            for cell, note in formatting.notes.items():
                cell_str = cell.value if hasattr(cell, 'value') else cell
                notes_dict[cell_str] = note
            ws.set_notes(notes_dict)

        # Apply column widths
        for column, width in formatting.column_widths.items():
            ws.set_column_width(column, width)

        # Apply borders
        for range_key, border_config in formatting.borders.items():
            ws.set_borders(range_key, border_config)

        # Apply conditional formats
        for cf_rule in formatting.conditional_formats:
            # conditional_formats contains dicts with 'range' and other fields
            range_str = cf_rule.get('range', '')
            rule = {k: v for k, v in cf_rule.items() if k != 'range'}
            ws.add_conditional_format(range_str, rule)

        # Apply data validations
        for dv_rule in formatting.data_validations:
            # data_validations contains dicts with 'range' and other fields
            range_str = dv_rule.get('range', '')
            rule = {k: v for k, v in dv_rule.items() if k != 'range'}
            ws.set_data_validation(range_str, rule)

        # Apply format_dict (cell formatting)
        # Use provided format_dict (pre-merged) or fall back to formatting.format_dict
        effective_format_dict = (
            format_dict if format_dict is not None else formatting.format_dict
        )
        if effective_format_dict:
            for range_name, fmt in effective_format_dict.items():
                ws.format_range(range_name, fmt)
