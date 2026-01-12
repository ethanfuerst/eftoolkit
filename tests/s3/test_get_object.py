"""Tests for S3FileSystem get_object method."""

from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from eftoolkit.s3 import S3FileSystem


def test_get_object_returns_exact_bytes(mock_s3_bucket):
    """get_object returns exact bytes that were uploaded."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    data = b'test data with special chars: \x00\xff\n\t'
    fs.put_object(f's3://{mock_s3_bucket}/test/data.bin', data)

    result = fs.get_object(f's3://{mock_s3_bucket}/test/data.bin')

    assert result == data


def test_get_object_missing_raises_file_not_found(mock_s3_bucket):
    """get_object raises FileNotFoundError for missing object."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    with pytest.raises(FileNotFoundError) as exc_info:
        fs.get_object(f's3://{mock_s3_bucket}/nonexistent/file.txt')

    assert 's3://' in str(exc_info.value)
    assert 'does not exist' in str(exc_info.value)


def test_get_object_empty_file(mock_s3_bucket):
    """get_object returns empty bytes for empty file."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    fs.put_object(f's3://{mock_s3_bucket}/test/empty.txt', b'')

    result = fs.get_object(f's3://{mock_s3_bucket}/test/empty.txt')

    assert result == b''


def test_get_object_large_file(mock_s3_bucket):
    """get_object handles larger files."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    # 1MB of data
    data = b'x' * (1024 * 1024)
    fs.put_object(f's3://{mock_s3_bucket}/test/large.bin', data)

    result = fs.get_object(f's3://{mock_s3_bucket}/test/large.bin')

    assert result == data
    assert len(result) == 1024 * 1024


def test_get_object_reraises_unexpected_client_error():
    """get_object re-raises unexpected ClientError (not NoSuchKey)."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    mock_client = MagicMock()
    mock_client.get_object.side_effect = ClientError(
        {'Error': {'Code': 'AccessDenied', 'Message': 'Access Denied'}},
        'GetObject',
    )

    with patch.object(fs, '_get_client', return_value=mock_client):
        with pytest.raises(ClientError) as exc_info:
            fs.get_object('s3://any-bucket/test.txt')

    assert exc_info.value.response['Error']['Code'] == 'AccessDenied'
