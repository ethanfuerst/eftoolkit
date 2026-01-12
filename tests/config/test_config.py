"""Tests for config utilities."""

import json
import logging

import pytest

from eftoolkit.config import load_json_config, setup_logging
from eftoolkit.config.utils import _strip_comments


def test_load_json_config_valid(tmp_path):
    """load_json_config loads standard JSON file."""
    config_file = tmp_path / 'config.json'
    config_file.write_text('{"key": "value", "number": 42}')

    result = load_json_config(config_file)

    assert result == {'key': 'value', 'number': 42}


def test_load_json_config_with_line_comments(tmp_path):
    """load_json_config strips // single-line comments."""
    config_file = tmp_path / 'config.jsonc'
    config_file.write_text("""{
    // This is a comment
    "key": "value",
    "number": 42 // inline comment
}""")

    result = load_json_config(config_file)

    assert result == {'key': 'value', 'number': 42}


def test_load_json_config_with_block_comments(tmp_path):
    """load_json_config strips /* */ block comments."""
    config_file = tmp_path / 'config.jsonc'
    config_file.write_text("""{
    /* This is a
       multiline block comment */
    "key": "value",
    "number": /* inline block */ 42
}""")

    result = load_json_config(config_file)

    assert result == {'key': 'value', 'number': 42}


def test_load_json_config_file_not_found():
    """load_json_config raises FileNotFoundError for missing files."""
    with pytest.raises(FileNotFoundError):
        load_json_config('/nonexistent/path/config.json')


def test_load_json_config_invalid_json(tmp_path):
    """load_json_config raises JSONDecodeError for invalid JSON."""
    config_file = tmp_path / 'invalid.json'
    config_file.write_text('{"key": invalid}')

    with pytest.raises(json.JSONDecodeError):
        load_json_config(config_file)


def test_setup_logging():
    """setup_logging configures root logger with correct level."""
    setup_logging(level=logging.WARNING)

    logger = logging.getLogger()

    assert logger.level == logging.WARNING


def test_setup_logging_default_level():
    """setup_logging uses INFO level by default."""
    setup_logging()

    logger = logging.getLogger()

    assert logger.level == logging.INFO


def test_setup_logging_custom_format():
    """setup_logging accepts custom format string."""
    custom_format = '%(levelname)s: %(message)s'

    setup_logging(format=custom_format)

    # Verify no error occurred
    logger = logging.getLogger()

    assert logger.level == logging.INFO


def test_load_json_config_preserves_url_in_string(tmp_path):
    """load_json_config does not strip // inside strings."""
    config_file = tmp_path / 'config.json'
    config_file.write_text('{"url": "https://example.com/path"}')

    result = load_json_config(config_file)

    assert result == {'url': 'https://example.com/path'}


def test_load_json_config_handles_escaped_quotes(tmp_path):
    """load_json_config handles escaped quotes in strings."""
    config_file = tmp_path / 'config.json'
    config_file.write_text('{"text": "say \\"hello\\""}')

    result = load_json_config(config_file)

    assert result == {'text': 'say "hello"'}


def test_load_json_config_handles_escaped_backslash(tmp_path):
    """load_json_config handles escaped backslashes in strings."""
    config_file = tmp_path / 'config.json'
    config_file.write_text('{"path": "C:\\\\Users\\\\name"}')

    result = load_json_config(config_file)

    assert result == {'path': 'C:\\Users\\name'}


def test_load_json_config_comment_after_string_with_escaped_quote(tmp_path):
    """load_json_config strips comment after string containing escaped quotes."""
    config_file = tmp_path / 'config.jsonc'
    config_file.write_text('{"text": "say \\"hi\\""}  // comment')

    result = load_json_config(config_file)

    assert result == {'text': 'say "hi"'}


def test_load_json_config_no_comment(tmp_path):
    """load_json_config works when line has no // at all."""
    config_file = tmp_path / 'config.json'
    config_file.write_text('{"key": 123}')

    result = load_json_config(config_file)

    assert result == {'key': 123}


def test_load_json_config_single_slash_not_comment(tmp_path):
    """load_json_config preserves single / that is not a comment."""
    config_file = tmp_path / 'config.json'
    config_file.write_text('{"ratio": "1/2"}')

    result = load_json_config(config_file)

    assert result == {'ratio': '1/2'}


def test_load_json_config_slash_followed_by_star_is_block_comment(tmp_path):
    """load_json_config handles /* as block comment start."""
    config_file = tmp_path / 'config.jsonc'
    config_file.write_text('{"key": 1 /* block */ }')

    result = load_json_config(config_file)

    assert result == {'key': 1}


def test_strip_comments_slash_followed_by_non_slash():
    """_strip_comments handles / followed by non-slash outside string."""
    # This covers the branch where / is found but next char is not /
    content = '{"key": 1} /x'

    result = _strip_comments(content)

    # The /x stays since it's not a // comment
    assert result == '{"key": 1} /x'


def test_strip_comments_slash_at_end_of_line():
    """_strip_comments handles / at end of line."""
    content = '{"key": 1}/'

    result = _strip_comments(content)

    # Single / at end stays
    assert result == '{"key": 1}/'
