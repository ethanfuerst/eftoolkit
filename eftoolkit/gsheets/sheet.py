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

T = TypeVar('T')


class Worksheet:
    """A single worksheet (tab) within a Google Spreadsheet.

    Handles all read/write/format operations for one tab.
    Operations are queued and flushed via flush() or context manager exit.
    """

    def __init__(
        self,
        gspread_worksheet,
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
        """Render queued operations to local HTML preview."""
        html = ['<html><head><style>']
        html.append('table { border-collapse: collapse; }')
        html.append('td, th { border: 1px solid #ccc; padding: 4px 8px; }')
        html.append('</style></head><body>')
        html.append(f'<h1>Sheet Preview: {self.title}</h1>')

        for update in self._value_updates:
            html.append(f'<h2>{update["range"]}</h2>')
            html.append('<table>')
            for row in update['values']:
                html.append('<tr>')
                for cell in row:
                    html.append(f'<td>{cell}</td>')
                html.append('</tr>')
            html.append('</table>')

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
    """

    def __init__(
        self,
        credentials: dict | None = None,
        spreadsheet_name: str = '',
        *,
        max_retries: int = 5,
        base_delay: float = 2.0,
        local_preview: bool = False,
        preview_dir: str | Path = 'sheet_previews',
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
        """Context manager exit (no-op, worksheets manage their own flush)."""
        pass

    @property
    def is_local_preview(self) -> bool:
        """True if running in local preview mode."""
        return self._local_preview

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
        if self._local_preview:
            return Worksheet(
                None,
                self,
                local_preview=True,
                preview_output=self._preview_path_for_worksheet(name),
                worksheet_name=name,
            )

        gspread_ws = self._gspread_spreadsheet.worksheet(name)
        return Worksheet(gspread_ws, self)

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
            return Worksheet(
                None,
                self,
                local_preview=True,
                preview_output=self._preview_path_for_worksheet(name),
                worksheet_name=name,
            )

        if replace:
            self.delete_worksheet(name, ignore_missing=True)

        gspread_ws = self._gspread_spreadsheet.add_worksheet(
            title=name, rows=rows, cols=cols
        )
        return Worksheet(gspread_ws, self)

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
