"""Utility functions for Google Sheets operations."""

import json
import os
import re
from collections.abc import Callable
from pathlib import Path

from google.oauth2.service_account import Credentials

# Registry for batch request handlers. Starts empty; populated at import time
BATCH_HANDLERS: dict[str, str] = {}

_DEFAULT_SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]


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
        ```python
        column_index_to_letter(0)   # 'A'
        column_index_to_letter(25)  # 'Z'
        column_index_to_letter(26)  # 'AA'
        column_index_to_letter(27)  # 'AB'
        ```
    """
    result = ''
    index += 1  # Convert to 1-indexed for calculation
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        result = chr(ord('A') + remainder) + result
    return result


def column_letter_to_index(col_letter: str) -> int:
    """Convert Excel-style column letter(s) to a 0-indexed column index.

    Args:
        col_letter: Column letter(s) (e.g., 'A', 'B', 'Z', 'AA', 'AB')

    Returns:
        0-indexed column number (A=0, B=1, Z=25, AA=26, AB=27, etc.)

    Example:
        ```python
        column_letter_to_index('A')   # 0
        column_letter_to_index('Z')   # 25
        column_letter_to_index('AA')  # 26
        column_letter_to_index('AB')  # 27
        ```
    """
    col = 0
    for char in col_letter.upper():
        col = col * 26 + (ord(char) - ord('A') + 1)
    return col - 1


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

    col = column_letter_to_index(col_str)

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


def load_json_config(
    path: str | Path | None = None,
    *,
    env: str | None = None,
    strip_comment_keys: bool = False,
) -> dict:
    """Load a JSON config from a file or environment variable.

    Exactly one of `path` or `env` must be provided. File-based loading
    supports JSONC (// and /* */ comments). Env-var loading additionally
    fixes up literal newlines inside the string so multi-line values
    (e.g. Google service account private keys) parse correctly.

    Args:
        path: Path to a JSON/JSONC file. Mutually exclusive with `env`.
        env: Name of an environment variable holding a JSON string.
            Mutually exclusive with `path`.
        strip_comment_keys: If True, recursively remove keys starting
            with '_comment' from the parsed result.

    Returns:
        Parsed JSON as a dictionary.

    Raises:
        ValueError: If neither or both of `path` and `env` are provided,
            or if the env var is unset or empty.
        FileNotFoundError: If `path` is provided but the file does not exist.
        json.JSONDecodeError: If the content is not valid JSON.

    Example:
        ```python
        # From a file (existing behavior)
        config = load_json_config('config.jsonc')

        # From an env var (e.g. Google service account JSON)
        creds = load_json_config(env='GSPREAD_CREDENTIALS')
        ```
    """
    if (path is None) == (env is None):
        raise ValueError("Provide exactly one of 'path' or 'env'")

    if env is not None:
        content = _load_env_var_json(env)
    else:
        content = Path(path).read_text()

    stripped = _strip_comments(content)
    result = json.loads(stripped)
    if strip_comment_keys:
        result = remove_comments(result)
    return result


def _load_env_var_json(env_name: str) -> str:
    """Read a JSON string from an env var, fixing multi-line private keys.

    Env vars with multi-line values (notably Google service account private
    keys) often contain literal newlines that break `json.loads`. This helper
    replaces real newlines with the JSON escape sequence `\\n`. The
    replacement is idempotent: env vars stored with `\\n` escapes intact
    are unchanged.

    Args:
        env_name: Name of the environment variable.

    Returns:
        The env var's value with newlines escaped, ready for `json.loads`.

    Raises:
        ValueError: If the env var is unset or empty. The error message
            does not include the env var's value.
    """
    raw = os.getenv(env_name)
    if not raw:
        raise ValueError(f'Environment variable {env_name!r} is not set or empty')
    return raw.replace('\n', '\\n')


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
        ```python
        config = {
            '_comment': 'This is a comment',
            'setting': 'value',
            'nested': {'_comment': 'Nested comment', 'key': 'value'},
        }
        remove_comments(config)
        # {'setting': 'value', 'nested': {'key': 'value'}}
        ```
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


def load_service_account_credentials(
    *,
    env: str = 'GSPREAD_CREDENTIALS',
    scopes: list[str] | None = None,
) -> Credentials:
    """Load Google service account credentials from an env var.

    Reads the JSON blob from the given env var, fixes up multi-line
    private-key newlines, and builds a `google.oauth2.service_account.Credentials`
    object ready to pass to `gspread.authorize` or any other google-auth
    consumer.

    Args:
        env: Name of the environment variable holding the service account
            JSON. Defaults to 'GSPREAD_CREDENTIALS'.
        scopes: OAuth scopes for the credentials. Defaults to gspread's
            usual scopes (Sheets + Drive).

    Returns:
        A configured `Credentials` object.

    Raises:
        ValueError: If the env var is unset or empty.
        json.JSONDecodeError: If the env var does not contain valid JSON.

    Example:
        ```python
        import gspread
        from eftoolkit.gsheets.utils import load_service_account_credentials

        creds = load_service_account_credentials(env='GSPREAD_CREDENTIALS')
        gc = gspread.authorize(creds)
        ```
    """
    info = load_json_config(env=env)
    return Credentials.from_service_account_info(
        info, scopes=scopes if scopes is not None else _DEFAULT_SCOPES
    )
