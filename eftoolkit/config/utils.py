"""Configuration utilities for JSON loading and logging setup."""

import json
import logging
import re
from pathlib import Path


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


def load_json_config(path: str | Path) -> dict:
    """Load a JSON config file, stripping JSONC-style comments.

    Supports:
    - Standard JSON files
    - JSONC files with // single-line comments
    - JSONC files with /* */ block comments

    Args:
        path: Path to the JSON/JSONC file

    Returns:
        Parsed JSON as a dictionary

    Raises:
        FileNotFoundError: If the file does not exist
        json.JSONDecodeError: If the content is not valid JSON after stripping comments
    """
    path = Path(path)
    content = path.read_text()
    stripped = _strip_comments(content)
    return json.loads(stripped)


def setup_logging(
    level: int = logging.INFO,
    format: str | None = None,
) -> None:
    """Configure the root logger with the specified level and format.

    Args:
        level: Logging level (default: logging.INFO)
        format: Log format string (default: '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    """
    if format is None:
        format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    logging.basicConfig(
        level=level,
        format=format,
        force=True,
    )


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
