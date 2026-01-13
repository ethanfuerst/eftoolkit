"""Tests for S3FileSystem write_df_to_parquet method."""

import io

import boto3
import pandas as pd
import pytest

from eftoolkit.s3 import S3FileSystem


def test_write_df_to_parquet_creates_file(mock_s3_bucket, sample_df):
    """Write a DataFrame and verify file is created."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    fs.write_df_to_parquet(sample_df, f's3://{mock_s3_bucket}/write/data.parquet')

    conn = boto3.client('s3', region_name='us-east-1')
    response = conn.list_objects_v2(Bucket=mock_s3_bucket)
    keys = [obj['Key'] for obj in response.get('Contents', [])]

    assert 'write/data.parquet' in keys


def test_write_df_to_parquet_empty_df(mock_s3_bucket):
    """Write an empty DataFrame."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    empty_df = pd.DataFrame({'col': []})
    fs.write_df_to_parquet(empty_df, f's3://{mock_s3_bucket}/write_empty/empty.parquet')

    conn = boto3.client('s3', region_name='us-east-1')
    response = conn.list_objects_v2(Bucket=mock_s3_bucket)
    keys = [obj['Key'] for obj in response.get('Contents', [])]

    assert 'write_empty/empty.parquet' in keys


def test_write_df_to_parquet_content_is_valid(mock_s3_bucket, sample_df):
    """Verify written parquet content can be read back."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    fs.write_df_to_parquet(sample_df, f's3://{mock_s3_bucket}/write_valid/test.parquet')

    conn = boto3.client('s3', region_name='us-east-1')
    response = conn.get_object(Bucket=mock_s3_bucket, Key='write_valid/test.parquet')
    result_df = pd.read_parquet(io.BytesIO(response['Body'].read()))

    pd.testing.assert_frame_equal(result_df, sample_df)


def test_write_df_to_parquet_requires_parquet_extension(mock_s3_bucket):
    """Writing without .parquet extension raises ValueError."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    df = pd.DataFrame({'col': [1, 2, 3]})

    with pytest.raises(ValueError) as exc_info:
        fs.write_df_to_parquet(df, f's3://{mock_s3_bucket}/write/data')

    assert '.parquet' in str(exc_info.value)
