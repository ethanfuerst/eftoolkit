"""Google Sheets client with automatic batching."""

import logging
import random
import time
import webbrowser
from collections.abc import Callable
from pathlib import Path
from typing import Any, TypeVar

import pandas as pd
from gspread import service_account_from_dict
from gspread.exceptions import APIError, WorksheetNotFound

from eftoolkit.gsheets.utils import parse_cell_reference

T = TypeVar('T')


class Worksheet:
    """A single worksheet (tab) within a Google Spreadsheet.

    Handles all read/write/format operations for one tab.
    Operations are queued and flushed via flush() or context manager exit.
    """

    def __init__(
        self,
        gspread_worksheet: Any,
        spreadsheet: 'Spreadsheet',
        *,
        local_preview: bool = False,
        preview_output: Path | None = None,
        worksheet_name: str | None = None,
    ) -> None:
        """Initialize worksheet.

        Args:
            gspread_worksheet: The underlying gspread Worksheet object.
            spreadsheet: Parent Spreadsheet instance.
            local_preview: If True, skip API calls and render to local HTML.
            preview_output: Path for HTML preview file.
            worksheet_name: Worksheet name (used in local_preview mode).
        """
        self._ws = gspread_worksheet
        self._spreadsheet = spreadsheet
        self._local_preview = local_preview
        self._worksheet_name = worksheet_name
        self._preview_output = preview_output or Path('sheet_preview.html')
        self._value_updates: list[dict] = []
        self._batch_requests: list[dict] = []
        self._preview_history: list[dict] = []  # Accumulates all writes for preview
        self._preview_column_widths: dict[int, int] = {}  # col_index -> width in pixels
        self._preview_notes: dict[tuple[int, int], str] = {}  # (row, col) -> note text

    def __enter__(self) -> 'Worksheet':
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Flush queued operations on clean exit."""
        if exc_type is None:
            self.flush()

    @property
    def title(self) -> str:
        """Worksheet title (tab name)."""
        if self._local_preview:
            return f'Local Preview - {self._worksheet_name}'
        return self._ws.title

    @property
    def is_local_preview(self) -> bool:
        """True if running in local preview mode."""
        return self._local_preview

    def read(self) -> pd.DataFrame:
        """Read worksheet to DataFrame (first row = headers)."""
        if self._local_preview:
            raise NotImplementedError('read not available in local preview mode')

        all_values = self._ws.get_all_values()
        if not all_values:
            return pd.DataFrame()
        return pd.DataFrame(data=all_values[1:], columns=all_values[0])

    def write_dataframe(
        self,
        df: pd.DataFrame,
        location: str = 'A1',
        *,
        include_header: bool = True,
        format_dict: dict[str, Any] | None = None,
    ) -> None:
        """Queue DataFrame write with optional formatting.

        Args:
            df: DataFrame to write.
            location: Cell location to start writing (e.g., 'A1').
            include_header: If True, include column names as first row.
            format_dict: Optional dict mapping range names to format dicts.
        """
        values = df.values.tolist()
        if include_header:
            values = [df.columns.tolist()] + values

        self._value_updates.append(
            {
                'range': f'{self.title}!{location}',
                'values': values,
            }
        )

        if format_dict:
            for range_name, fmt in format_dict.items():
                self._batch_requests.append(
                    {
                        'type': 'format',
                        'range': range_name,
                        'format': fmt,
                    }
                )

    def write_values(
        self,
        range_name: str,
        values: list[list[Any]],
    ) -> None:
        """Queue cell values update.

        Args:
            range_name: A1 notation range (e.g., 'A1:B2').
            values: 2D list of values to write.
        """
        # Prepend worksheet name if not already included
        if '!' not in range_name:
            range_name = f'{self.title}!{range_name}'
        self._value_updates.append({'range': range_name, 'values': values})

    def format_range(
        self,
        range_name: str,
        format_dict: dict[str, Any],
    ) -> None:
        """Queue cell formatting.

        Args:
            range_name: A1 notation range.
            format_dict: Format specification dict.
        """
        self._batch_requests.append(
            {
                'type': 'format',
                'range': range_name,
                'format': format_dict,
            }
        )

    def set_borders(
        self,
        range_name: str,
        borders: dict[str, Any],
    ) -> None:
        """Queue border formatting.

        Args:
            range_name: A1 notation range.
            borders: Border specification dict.
        """
        self._batch_requests.append(
            {
                'type': 'border',
                'range': range_name,
                'borders': borders,
            }
        )

    def set_column_width(
        self,
        column: str | int,
        width: int,
    ) -> None:
        """Queue column width update.

        Args:
            column: Column letter or 1-based index.
            width: Width in pixels.
        """
        self._batch_requests.append(
            {
                'type': 'column_width',
                'column': column,
                'width': width,
            }
        )

    def auto_resize_columns(
        self,
        start_col: int,
        end_col: int,
    ) -> None:
        """Queue column auto-resize.

        Args:
            start_col: 1-based start column index.
            end_col: 1-based end column index.
        """
        self._batch_requests.append(
            {
                'type': 'auto_resize',
                'start_col': start_col,
                'end_col': end_col,
            }
        )

    def set_notes(
        self,
        notes: dict[str, str],
    ) -> None:
        """Queue cell notes.

        Args:
            notes: Dict mapping cell references to note text.
        """
        self._batch_requests.append(
            {
                'type': 'notes',
                'notes': notes,
            }
        )

    def merge_cells(
        self,
        range_name: str,
        merge_type: str = 'MERGE_ALL',
    ) -> None:
        """Queue cell merge.

        Args:
            range_name: A1 notation range to merge (e.g., 'A1:C1').
            merge_type: One of 'MERGE_ALL', 'MERGE_COLUMNS', 'MERGE_ROWS'.
        """
        self._batch_requests.append(
            {
                'type': 'merge',
                'range': range_name,
                'merge_type': merge_type,
            }
        )

    def unmerge_cells(
        self,
        range_name: str,
    ) -> None:
        """Queue cell unmerge.

        Args:
            range_name: A1 notation range to unmerge.
        """
        self._batch_requests.append(
            {
                'type': 'unmerge',
                'range': range_name,
            }
        )

    def sort_range(
        self,
        range_name: str,
        sort_specs: list[dict[str, Any]],
    ) -> None:
        """Queue range sort.

        Args:
            range_name: A1 notation range to sort.
            sort_specs: List of sort specifications. Each spec should have:
                - 'column': 0-based column index within the range
                - 'ascending': True for ascending, False for descending (default True)

        Example:
            ws.sort_range('A1:C10', [{'column': 0, 'ascending': True}])
        """
        self._batch_requests.append(
            {
                'type': 'sort',
                'range': range_name,
                'sort_specs': sort_specs,
            }
        )

    def set_data_validation(
        self,
        range_name: str,
        rule: dict[str, Any],
    ) -> None:
        """Queue data validation rule.

        Args:
            range_name: A1 notation range for validation.
            rule: Validation rule dict. Common keys:
                - 'type': 'ONE_OF_LIST', 'ONE_OF_RANGE', 'NUMBER_BETWEEN', etc.
                - 'values': List of allowed values (for ONE_OF_LIST)
                - 'showDropdown': True to show dropdown (default True)
                - 'strict': True to reject invalid input (default True)

        Example:
            ws.set_data_validation('A1:A10', {
                'type': 'ONE_OF_LIST',
                'values': ['Yes', 'No', 'Maybe'],
                'showDropdown': True,
            })
        """
        self._batch_requests.append(
            {
                'type': 'data_validation',
                'range': range_name,
                'rule': rule,
            }
        )

    def clear_data_validation(
        self,
        range_name: str,
    ) -> None:
        """Queue removal of data validation rules.

        Args:
            range_name: A1 notation range to clear validation from.
        """
        self._batch_requests.append(
            {
                'type': 'clear_data_validation',
                'range': range_name,
            }
        )

    def add_conditional_format(
        self,
        range_name: str,
        rule: dict[str, Any],
    ) -> None:
        """Queue conditional formatting rule.

        Args:
            range_name: A1 notation range for conditional format.
            rule: Conditional format rule dict. Should contain:
                - 'type': 'CUSTOM_FORMULA', 'NUMBER_GREATER', 'TEXT_CONTAINS', etc.
                - 'values': Condition values (e.g., formula string)
                - 'format': Cell format to apply when condition is met

        Example:
            ws.add_conditional_format('A1:A10', {
                'type': 'CUSTOM_FORMULA',
                'values': ['=A1>100'],
                'format': {'backgroundColor': {'red': 1, 'green': 0, 'blue': 0}},
            })
        """
        self._batch_requests.append(
            {
                'type': 'conditional_format',
                'range': range_name,
                'rule': rule,
            }
        )

    def insert_rows(
        self,
        start_row: int,
        num_rows: int = 1,
    ) -> None:
        """Queue row insertion.

        Args:
            start_row: 1-based row index where new rows will be inserted.
            num_rows: Number of rows to insert (default 1).
        """
        self._batch_requests.append(
            {
                'type': 'insert_rows',
                'start_row': start_row,
                'num_rows': num_rows,
            }
        )

    def delete_rows(
        self,
        start_row: int,
        num_rows: int = 1,
    ) -> None:
        """Queue row deletion.

        Args:
            start_row: 1-based row index of first row to delete.
            num_rows: Number of rows to delete (default 1).
        """
        self._batch_requests.append(
            {
                'type': 'delete_rows',
                'start_row': start_row,
                'num_rows': num_rows,
            }
        )

    def insert_columns(
        self,
        start_col: int,
        num_cols: int = 1,
    ) -> None:
        """Queue column insertion.

        Args:
            start_col: 1-based column index where new columns will be inserted.
            num_cols: Number of columns to insert (default 1).
        """
        self._batch_requests.append(
            {
                'type': 'insert_columns',
                'start_col': start_col,
                'num_cols': num_cols,
            }
        )

    def delete_columns(
        self,
        start_col: int,
        num_cols: int = 1,
    ) -> None:
        """Queue column deletion.

        Args:
            start_col: 1-based column index of first column to delete.
            num_cols: Number of columns to delete (default 1).
        """
        self._batch_requests.append(
            {
                'type': 'delete_columns',
                'start_col': start_col,
                'num_cols': num_cols,
            }
        )

    def freeze_rows(
        self,
        num_rows: int,
    ) -> None:
        """Queue freezing rows at the top of the worksheet.

        Args:
            num_rows: Number of rows to freeze (0 to unfreeze).
        """
        self._batch_requests.append(
            {
                'type': 'freeze_rows',
                'num_rows': num_rows,
            }
        )

    def freeze_columns(
        self,
        num_cols: int,
    ) -> None:
        """Queue freezing columns at the left of the worksheet.

        Args:
            num_cols: Number of columns to freeze (0 to unfreeze).
        """
        self._batch_requests.append(
            {
                'type': 'freeze_columns',
                'num_cols': num_cols,
            }
        )

    def add_raw_request(
        self,
        request: dict[str, Any],
    ) -> None:
        """Queue a raw batchUpdate request.

        Use this for operations not covered by other methods. The request
        will be passed directly to the Google Sheets batchUpdate API.

        Args:
            request: A single batchUpdate request dict. See Google Sheets API
                documentation for available request types:
                https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/request

        Example:
            # Add a named range
            ws.add_raw_request({
                'addNamedRange': {
                    'namedRange': {
                        'name': 'MyRange',
                        'range': {
                            'sheetId': 0,
                            'startRowIndex': 0,
                            'endRowIndex': 10,
                            'startColumnIndex': 0,
                            'endColumnIndex': 5,
                        }
                    }
                }
            })
        """
        self._batch_requests.append(
            {
                'type': 'raw',
                'request': request,
            }
        )

    def flush(self) -> None:
        """Execute all queued operations.

        In normal mode: sends batched API calls to Google Sheets.
        In local_preview mode: renders HTML.
        """
        if self._local_preview:
            self._flush_to_preview()
        else:
            self._flush_to_api()

        self._value_updates.clear()
        self._batch_requests.clear()

    def _flush_to_api(self) -> None:
        """Send queued operations to Google Sheets API."""
        if not self._ws:
            return

        # Flush value updates via parent spreadsheet's batch update
        if self._value_updates:
            self._spreadsheet._execute_with_retry(
                lambda: self._spreadsheet._gspread_spreadsheet.values_batch_update(
                    {
                        'valueInputOption': 'USER_ENTERED',
                        'data': self._value_updates,
                    }
                ),
                'values_batch_update',
            )

        # Flush batch requests (format, borders, etc.)
        for req in self._batch_requests:
            if req['type'] == 'format':
                self._spreadsheet._execute_with_retry(
                    lambda r=req: self._ws.format(r['range'], r['format']),
                    'format',
                )

    def _flush_to_preview(self) -> None:
        """Render queued operations to local HTML preview as a unified grid."""
        # Accumulate current updates into history
        self._preview_history.extend(self._value_updates)

        # Process batch requests for preview metadata
        for req in self._batch_requests:
            if req['type'] == 'column_width':
                col = req['column']
                # Convert letter to index if needed
                if isinstance(col, str):
                    col_idx = 0
                    for char in col.upper():
                        col_idx = col_idx * 26 + (ord(char) - ord('A') + 1)
                    col_idx -= 1
                else:
                    col_idx = col - 1  # Convert 1-indexed to 0-indexed
                self._preview_column_widths[col_idx] = req['width']
            elif req['type'] == 'notes':
                for cell_ref, note_text in req['notes'].items():
                    row, col = parse_cell_reference(cell_ref)
                    self._preview_notes[(row, col)] = note_text

        # Build a sparse grid from all updates
        grid: dict[tuple[int, int], str] = {}
        max_row = 0
        max_col = 0

        for update in self._preview_history:
            start_row, start_col = parse_cell_reference(update['range'])
            for row_offset, row_data in enumerate(update['values']):
                for col_offset, cell_value in enumerate(row_data):
                    r = start_row + row_offset
                    c = start_col + col_offset
                    grid[(r, c)] = str(cell_value) if cell_value is not None else ''
                    max_row = max(max_row, r)
                    max_col = max(max_col, c)

        # Generate column headers (A, B, C, ... AA, AB, etc.)
        def col_to_letter(col_idx: int) -> str:
            result = ''
            col_idx += 1  # Convert to 1-indexed
            while col_idx > 0:
                col_idx, remainder = divmod(col_idx - 1, 26)
                result = chr(ord('A') + remainder) + result
            return result

        # Build HTML with Google Sheets-like styling
        html = ['<!DOCTYPE html><html><head>']
        html.append('<meta charset="utf-8">')
        html.append(f'<title>{self.title}</title>')
        html.append('<style>')
        html.append(
            'body { font-family: Arial, sans-serif; margin: 0; padding: 20px; '
            'background-color: #f8f9fa; }'
        )
        html.append(
            'h1 { color: #202124; font-size: 18px; font-weight: 400; '
            'margin-bottom: 16px; }'
        )
        html.append(
            '.sheet-container { background: white; border-radius: 8px; '
            'box-shadow: 0 1px 3px rgba(0,0,0,0.12); overflow: auto; }'
        )
        html.append('table { border-collapse: collapse; border-spacing: 0; }')
        html.append(
            'td, th { border: 1px solid #e0e0e0; padding: 4px 8px; '
            'font-size: 13px; vertical-align: middle; '
            'white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }'
        )
        html.append(
            'th { background-color: #f8f9fa; color: #5f6368; '
            'font-weight: 500; text-align: center; position: sticky; top: 0; }'
        )
        html.append(
            '.row-header { background-color: #f8f9fa; color: #5f6368; '
            'font-weight: 500; text-align: center; min-width: 46px; '
            'position: sticky; left: 0; }'
        )
        html.append(
            '.corner { background-color: #f8f9fa; position: sticky; '
            'top: 0; left: 0; z-index: 2; }'
        )
        html.append(
            'td { background-color: white; text-align: left; min-width: 80px; }'
        )
        html.append('.has-note { background-color: #fff8e1; cursor: help; }')
        html.append(
            '.has-note::after { content: ""; position: absolute; '
            'top: 0; right: 0; border-width: 0 6px 6px 0; '
            'border-style: solid; border-color: #ffc107 #ffc107 '
            'transparent transparent; }'
        )
        html.append('td { position: relative; }')
        html.append('</style>')
        html.append('</head><body>')
        html.append(f'<h1>📊 {self.title}</h1>')
        html.append('<div class="sheet-container">')
        html.append('<table>')

        # Header row with column letters
        html.append('<tr>')
        html.append('<th class="corner"></th>')  # Corner cell
        for c in range(max_col + 1):
            width = self._preview_column_widths.get(c, 80)
            html.append(
                f'<th style="width: {width}px; min-width: {width}px;">'
                f'{col_to_letter(c)}</th>'
            )
        html.append('</tr>')

        # Data rows
        for r in range(max_row + 1):
            html.append('<tr>')
            html.append(f'<td class="row-header">{r + 1}</td>')
            for c in range(max_col + 1):
                cell_value = grid.get((r, c), '')
                note = self._preview_notes.get((r, c))
                width = self._preview_column_widths.get(c, 80)
                style = f'width: {width}px; max-width: {width}px;'
                if note:
                    # Escape HTML in note text
                    escaped_note = (
                        note.replace('&', '&amp;')
                        .replace('"', '&quot;')
                        .replace('<', '&lt;')
                        .replace('>', '&gt;')
                    )
                    html.append(
                        f'<td class="has-note" style="{style}" '
                        f'title="{escaped_note}">{cell_value}</td>'
                    )
                else:
                    html.append(f'<td style="{style}">{cell_value}</td>')
            html.append('</tr>')

        html.append('</table>')
        html.append('</div>')
        html.append('</body></html>')

        self._preview_output.parent.mkdir(parents=True, exist_ok=True)
        self._preview_output.write_text('\n'.join(html))

    def open_preview(self) -> None:
        """Open the preview HTML in browser (local_preview mode only)."""
        if not self._local_preview:
            raise RuntimeError('open_preview only available in local_preview mode')

        webbrowser.open(f'file://{self._preview_output.absolute()}')


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

    def __enter__(self) -> 'Spreadsheet':
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
