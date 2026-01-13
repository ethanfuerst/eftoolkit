"""Tests for S3FileSystem cp method."""

from unittest.mock import MagicMock, patch

import boto3
import pytest
from botocore.exceptions import ClientError

from eftoolkit.s3 import S3FileSystem


def test_cp_copies_object_same_bucket(mock_s3_bucket):
    """cp copies object within the same bucket."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    data = b'original data'
    fs.put_object(f's3://{mock_s3_bucket}/src/file.txt', data)

    fs.cp(
        f's3://{mock_s3_bucket}/src/file.txt',
        f's3://{mock_s3_bucket}/dst/file.txt',
    )

    # Source should still exist
    assert fs.file_exists(f's3://{mock_s3_bucket}/src/file.txt') is True

    # Destination should have same content
    result = fs.get_object(f's3://{mock_s3_bucket}/dst/file.txt')

    assert result == data


def test_cp_destination_bytes_match_source(mock_s3_bucket):
    """cp produces exact byte-for-byte copy."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    # Binary data with various bytes
    data = bytes(range(256)) * 100
    fs.put_object(f's3://{mock_s3_bucket}/src/binary.bin', data)

    fs.cp(
        f's3://{mock_s3_bucket}/src/binary.bin',
        f's3://{mock_s3_bucket}/dst/binary.bin',
    )

    result = fs.get_object(f's3://{mock_s3_bucket}/dst/binary.bin')

    assert result == data


def test_cp_source_remains_after_copy(mock_s3_bucket):
    """cp does not remove the source object."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    data = b'keep me'
    fs.put_object(f's3://{mock_s3_bucket}/src/keep.txt', data)

    fs.cp(
        f's3://{mock_s3_bucket}/src/keep.txt',
        f's3://{mock_s3_bucket}/dst/copy.txt',
    )

    # Source should still exist with original data
    source_data = fs.get_object(f's3://{mock_s3_bucket}/src/keep.txt')

    assert source_data == data


def test_cp_missing_source_raises_file_not_found(mock_s3_bucket):
    """cp raises FileNotFoundError if source does not exist."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    with pytest.raises(FileNotFoundError) as exc_info:
        fs.cp(
            f's3://{mock_s3_bucket}/nonexistent/file.txt',
            f's3://{mock_s3_bucket}/dst/file.txt',
        )

    assert 's3://' in str(exc_info.value)
    assert 'does not exist' in str(exc_info.value)


def test_cp_overwrites_destination(mock_s3_bucket):
    """cp overwrites existing destination object."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    fs.put_object(f's3://{mock_s3_bucket}/src/file.txt', b'new data')
    fs.put_object(f's3://{mock_s3_bucket}/dst/file.txt', b'old data')

    fs.cp(
        f's3://{mock_s3_bucket}/src/file.txt',
        f's3://{mock_s3_bucket}/dst/file.txt',
    )

    result = fs.get_object(f's3://{mock_s3_bucket}/dst/file.txt')

    assert result == b'new data'


def test_cp_across_buckets(mock_s3_bucket):
    """cp works across different buckets."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    # Create a second bucket
    conn = boto3.client('s3', region_name='us-east-1')
    second_bucket = 'test-bucket-2'
    conn.create_bucket(Bucket=second_bucket)

    data = b'cross bucket data'
    fs.put_object(f's3://{mock_s3_bucket}/src/file.txt', data)

    fs.cp(
        f's3://{mock_s3_bucket}/src/file.txt',
        f's3://{second_bucket}/dst/file.txt',
    )

    # Verify in second bucket
    result = fs.get_object(f's3://{second_bucket}/dst/file.txt')

    assert result == data


def test_cp_nested_paths(mock_s3_bucket):
    """cp works with deeply nested paths."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    data = b'deep data'
    fs.put_object(f's3://{mock_s3_bucket}/a/b/c/d/source.txt', data)

    fs.cp(
        f's3://{mock_s3_bucket}/a/b/c/d/source.txt',
        f's3://{mock_s3_bucket}/x/y/z/dest.txt',
    )

    result = fs.get_object(f's3://{mock_s3_bucket}/x/y/z/dest.txt')

    assert result == data


def test_cp_reraises_unexpected_client_error():
    """cp re-raises unexpected ClientError (not NoSuchKey)."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    mock_client = MagicMock()
    mock_client.copy_object.side_effect = ClientError(
        {'Error': {'Code': 'AccessDenied', 'Message': 'Access Denied'}},
        'CopyObject',
    )

    with patch.object(fs, '_get_client', return_value=mock_client):
        with pytest.raises(ClientError) as exc_info:
            fs.cp('s3://any-bucket/src.txt', 's3://any-bucket/dst.txt')

    assert exc_info.value.response['Error']['Code'] == 'AccessDenied'
