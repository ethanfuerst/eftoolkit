"""Utility functions for Google Sheets operations."""

import re
from collections.abc import Callable

# Registry for batch request handlers. Starts empty; populated at import time
BATCH_HANDLERS: dict[str, str] = {}


def batch_handler(req_type: str) -> Callable:
    """Decorator to register a Worksheet method as a batch request handler.

    Usage:
        @batch_handler('column_width')
        def _handle_column_width(self, req: dict) -> None:
            ...

    This eliminates the need to manually maintain a dispatch dict.
    When adding a new batch request type, simply decorate the handler method.
    """

    def decorator(method: Callable) -> Callable:
        BATCH_HANDLERS[req_type] = method.__name__
        return method

    return decorator


def parse_cell_reference(cell_ref: str) -> tuple[int | None, int]:
    """Parse a cell reference like 'A1' or 'Sheet1!B2' into (row, col) 0-indexed.

    Args:
        cell_ref: Cell reference string (e.g., 'A1', 'Sheet1!B2', 'AA10', 'X').
            Column-only references like 'X' are supported for open-ended ranges.

    Returns:
        Tuple of (row_index, col_index), both 0-indexed.
        row_index is None for column-only references (e.g., 'X' in 'A1:X').
    """
    # Remove sheet name if present (e.g., "Sheet1!A1" -> "A1")
    if '!' in cell_ref:
        cell_ref = cell_ref.split('!')[1]

    # Extract just the start cell from a range (e.g., "A1:B2" -> "A1")
    if ':' in cell_ref:
        cell_ref = cell_ref.split(':')[0]

    # Try parsing as column + row (e.g., 'A1', 'AA10')
    match = re.match(r'^([A-Za-z]+)(\d+)$', cell_ref)
    if match:
        col_str, row_str = match.groups()
        row: int | None = int(row_str) - 1  # Convert to 0-indexed
    else:
        # Try parsing as column-only (e.g., 'X' for open-ended ranges)
        match = re.match(r'^([A-Za-z]+)$', cell_ref)
        if match:
            col_str = match.group(1)
            row = None  # No row specified - open-ended
        else:
            return 0, 0  # Default to A1 if parsing fails

    # Convert column letters to index (A=0, B=1, ..., Z=25, AA=26, etc.)
    col = 0
    for char in col_str.upper():
        col = col * 26 + (ord(char) - ord('A') + 1)
    col -= 1  # Convert to 0-indexed

    return row, col
