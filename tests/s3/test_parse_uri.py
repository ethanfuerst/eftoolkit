"""Tests for _parse_s3_uri function."""

import pytest

from eftoolkit.s3.filesystem import _parse_s3_uri


def test_parse_s3_uri_with_key():
    """_parse_s3_uri extracts bucket and key from full URI."""
    bucket, key = _parse_s3_uri('s3://my-bucket/path/to/file.txt')

    assert bucket == 'my-bucket'
    assert key == 'path/to/file.txt'


def test_parse_s3_uri_bucket_only():
    """_parse_s3_uri handles bucket-only URI."""
    bucket, key = _parse_s3_uri('s3://my-bucket')

    assert bucket == 'my-bucket'
    assert key == ''


def test_parse_s3_uri_bucket_with_trailing_slash():
    """_parse_s3_uri handles bucket with trailing slash."""
    bucket, key = _parse_s3_uri('s3://my-bucket/')

    assert bucket == 'my-bucket'
    assert key == ''


def test_parse_s3_uri_deeply_nested_key():
    """_parse_s3_uri handles deeply nested key paths."""
    bucket, key = _parse_s3_uri('s3://bucket/a/b/c/d/e/file.parquet')

    assert bucket == 'bucket'
    assert key == 'a/b/c/d/e/file.parquet'


def test_parse_s3_uri_invalid_scheme_raises():
    """_parse_s3_uri raises ValueError for non-s3 URI."""
    with pytest.raises(ValueError) as exc_info:
        _parse_s3_uri('https://my-bucket/file.txt')

    assert "must start with 's3://'" in str(exc_info.value)


def test_parse_s3_uri_empty_bucket_raises():
    """_parse_s3_uri raises ValueError for empty bucket."""
    with pytest.raises(ValueError) as exc_info:
        _parse_s3_uri('s3:///path/to/file.txt')

    assert 'bucket name is empty' in str(exc_info.value)


def test_parse_s3_uri_no_scheme_raises():
    """_parse_s3_uri raises ValueError for URI without scheme."""
    with pytest.raises(ValueError) as exc_info:
        _parse_s3_uri('my-bucket/path/to/file.txt')

    assert "must start with 's3://'" in str(exc_info.value)
