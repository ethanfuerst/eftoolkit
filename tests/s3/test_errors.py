"""Tests for S3FileSystem error handling."""

from unittest.mock import MagicMock, patch

import boto3
import pytest
from botocore.exceptions import ClientError

from eftoolkit.s3 import S3FileSystem


def test_read_missing_file_raises_error(mock_s3_bucket):
    """Reading non-existent file raises FileNotFoundError."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    with pytest.raises(FileNotFoundError) as exc_info:
        fs.read_df_from_parquet(f's3://{mock_s3_bucket}/error/missing.parquet')

    assert 'does not exist' in str(exc_info.value)


def test_read_parquet_reraises_unexpected_client_error():
    """Unexpected ClientError during read is re-raised."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    mock_client = MagicMock()
    mock_client.get_object.side_effect = ClientError(
        {'Error': {'Code': 'AccessDenied', 'Message': 'Access Denied'}}, 'GetObject'
    )

    with patch.object(fs, '_get_client', return_value=mock_client):
        with pytest.raises(ClientError) as exc_info:
            fs.read_df_from_parquet('s3://any-bucket/test.parquet')

    assert exc_info.value.response['Error']['Code'] == 'AccessDenied'


def test_file_exists_reraises_unexpected_client_error():
    """Unexpected ClientError during file_exists is re-raised."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    mock_client = MagicMock()
    mock_client.head_object.side_effect = ClientError(
        {'Error': {'Code': 'AccessDenied', 'Message': 'Access Denied'}},
        'HeadObject',
    )

    with patch.object(fs, '_get_client', return_value=mock_client):
        with pytest.raises(ClientError) as exc_info:
            fs.file_exists('s3://any-bucket/test.parquet')

    assert exc_info.value.response['Error']['Code'] == 'AccessDenied'


def test_read_empty_directory_raises_error(mock_s3_bucket):
    """Reading directory with no parquet files raises error."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    conn = boto3.client('s3', region_name='us-east-1')
    conn.put_object(
        Bucket=mock_s3_bucket,
        Key='error_empty/empty_dir/readme.txt',
        Body=b'readme',
    )

    with pytest.raises(FileNotFoundError) as exc_info:
        fs.read_df_from_parquet(f's3://{mock_s3_bucket}/error_empty/empty_dir')

    assert 'contains no .parquet files' in str(exc_info.value)


def test_read_missing_prefix_raises_error(mock_s3_bucket):
    """Reading non-existent directory/prefix raises error."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    with pytest.raises(FileNotFoundError) as exc_info:
        fs.read_df_from_parquet(f's3://{mock_s3_bucket}/error_prefix/nonexistent_dir')

    assert 'does not exist' in str(exc_info.value)
