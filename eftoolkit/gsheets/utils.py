"""Utility functions for Google Sheets operations."""

import json
import re
from collections.abc import Callable
from pathlib import Path

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


def column_index_to_letter(index: int) -> str:
    """Convert a 0-indexed column index to Excel-style column letter(s).

    Args:
        index: 0-indexed column number (0=A, 1=B, ..., 25=Z, 26=AA, etc.)

    Returns:
        Column letter string (e.g., 'A', 'B', 'Z', 'AA', 'AB')

    Example:
        >>> column_index_to_letter(0)
        'A'
        >>> column_index_to_letter(25)
        'Z'
        >>> column_index_to_letter(26)
        'AA'
        >>> column_index_to_letter(27)
        'AB'
    """
    result = ''
    index += 1  # Convert to 1-indexed for calculation
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        result = chr(ord('A') + remainder) + result
    return result


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


def _strip_comments(content: str) -> str:
    """Strip JSONC-style comments from content.

    Removes:
    - Single-line comments: // comment
    - Block comments: /* comment */

    Args:
        content: JSON content potentially containing comments

    Returns:
        Content with comments stripped
    """
    # Remove block comments (/* ... */) - non-greedy, handles multiline
    content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)

    # Remove single-line comments (// ...) - but not inside strings
    # Process line by line to handle // comments correctly
    lines = content.split('\n')
    result_lines = []

    for line in lines:
        # Find // that's not inside a string
        in_string = False
        escape_next = False
        comment_start = None

        for i, char in enumerate(line):
            if escape_next:
                escape_next = False
                continue

            if char == '\\':
                escape_next = True
                continue

            if char == '"' and not in_string:
                in_string = True
            elif char == '"' and in_string:
                in_string = False
            elif char == '/' and not in_string:
                if i + 1 < len(line) and line[i + 1] == '/':
                    comment_start = i
                    break

        if comment_start is not None:
            result_lines.append(line[:comment_start])
        else:
            result_lines.append(line)

    return '\n'.join(result_lines)


def load_json_config(path: str | Path, *, strip_comment_keys: bool = False) -> dict:
    """Load a JSON config file, stripping JSONC-style comments.

    Supports:
    - Standard JSON files
    - JSONC files with // single-line comments
    - JSONC files with /* */ block comments

    Args:
        path: Path to the JSON/JSONC file
        strip_comment_keys: If True, also remove keys starting with '_comment'
            from the loaded config using remove_comments(). Default: False.

    Returns:
        Parsed JSON as a dictionary

    Raises:
        FileNotFoundError: If the file does not exist
        json.JSONDecodeError: If the content is not valid JSON after stripping comments

    Example:
        >>> # Load config with _comment keys stripped
        >>> config = load_json_config('config.json', strip_comment_keys=True)
    """
    path = Path(path)
    content = path.read_text()
    stripped = _strip_comments(content)
    result = json.loads(stripped)
    if strip_comment_keys:
        result = remove_comments(result)
    return result


def remove_comments(obj: dict | list) -> dict | list:
    """Remove comment keys from a nested dictionary or list.

    Recursively filters out keys starting with '_comment' from dictionaries.
    Useful for stripping documentation keys from JSON configuration files.

    Args:
        obj: A dictionary or list, potentially nested.

    Returns:
        A copy with all '_comment*' keys removed. Returns the input unchanged
        if it's neither a dict nor a list.

    Example:
        >>> config = {
        ...     '_comment': 'This is a comment',
        ...     'setting': 'value',
        ...     'nested': {'_comment': 'Nested comment', 'key': 'value'}
        ... }
        >>> remove_comments(config)
        {'setting': 'value', 'nested': {'key': 'value'}}
    """
    if isinstance(obj, dict):
        return {
            k: remove_comments(v)
            for k, v in obj.items()
            if not k.startswith('_comment')
        }
    elif isinstance(obj, list):
        return [remove_comments(item) for item in obj]
    else:
        return obj
