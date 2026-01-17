"""Tests for config utilities."""

import json
import logging

import pytest

from eftoolkit.config import load_json_config, remove_comments, setup_logging
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


def test_remove_comments_simple_dict():
    """remove_comments removes _comment keys from a dict."""
    config = {
        '_comment': 'This is a comment',
        'setting': 'value',
    }

    result = remove_comments(config)

    assert result == {'setting': 'value'}


def test_remove_comments_nested_dict():
    """remove_comments removes _comment keys from nested dicts."""
    config = {
        '_comment': 'Top-level comment',
        'setting': 'value',
        'nested': {
            '_comment': 'Nested comment',
            'key': 'value',
        },
    }

    result = remove_comments(config)

    assert result == {'setting': 'value', 'nested': {'key': 'value'}}


def test_remove_comments_numbered_comment_keys():
    """remove_comments removes _comment_1, _comment_foo, etc."""
    config = {
        '_comment': 'First comment',
        '_comment_1': 'Second comment',
        '_comment_foo': 'Named comment',
        'setting': 'value',
    }

    result = remove_comments(config)

    assert result == {'setting': 'value'}


def test_remove_comments_list():
    """remove_comments recursively processes lists."""
    config = [
        {'_comment': 'Comment in list', 'key': 'value1'},
        {'_comment': 'Another comment', 'key': 'value2'},
    ]

    result = remove_comments(config)

    assert result == [{'key': 'value1'}, {'key': 'value2'}]


def test_remove_comments_nested_list():
    """remove_comments handles nested lists and dicts."""
    config = {
        '_comment': 'Top comment',
        'items': [
            {'_comment': 'Item comment', 'name': 'item1'},
            {'_comment': 'Item comment', 'name': 'item2'},
        ],
    }

    result = remove_comments(config)

    assert result == {'items': [{'name': 'item1'}, {'name': 'item2'}]}


def test_remove_comments_preserves_non_comment_underscore_keys():
    """remove_comments preserves keys starting with _ but not _comment."""
    config = {
        '_comment': 'This should be removed',
        '_internal': 'This should stay',
        '_private_setting': 'This too',
    }

    result = remove_comments(config)

    assert result == {'_internal': 'This should stay', '_private_setting': 'This too'}


def test_remove_comments_empty_dict():
    """remove_comments handles empty dict."""
    result = remove_comments({})

    assert result == {}


def test_remove_comments_empty_list():
    """remove_comments handles empty list."""
    result = remove_comments([])

    assert result == []


def test_remove_comments_non_dict_values():
    """remove_comments preserves non-dict/list values."""
    config = {
        '_comment': 'Comment',
        'string': 'value',
        'number': 42,
        'boolean': True,
        'null': None,
    }

    result = remove_comments(config)

    assert result == {'string': 'value', 'number': 42, 'boolean': True, 'null': None}


def test_remove_comments_deeply_nested():
    """remove_comments handles deeply nested structures."""
    config = {
        '_comment': 'Level 0',
        'level1': {
            '_comment': 'Level 1',
            'level2': {
                '_comment': 'Level 2',
                'level3': {
                    '_comment': 'Level 3',
                    'value': 'deep',
                },
            },
        },
    }

    result = remove_comments(config)

    assert result == {'level1': {'level2': {'level3': {'value': 'deep'}}}}
