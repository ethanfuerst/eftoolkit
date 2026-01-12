"""Tests for DuckDB S3 integration using moto mock."""

import pandas as pd

from eftoolkit.s3 import S3FileSystem
from eftoolkit.sql import DuckDB


def test_read_parquet_from_s3(mock_s3_bucket, sample_df):
    """Read parquet from mock S3 via DuckDB."""
    # Setup: write parquet to mock S3
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    fs.write_df_to_parquet(sample_df, f's3://{mock_s3_bucket}/test/data.parquet')

    # Create DuckDB with S3 credentials
    db = DuckDB(
        s3_access_key_id='testing',
        s3_secret_access_key='testing',
        s3_region='us-east-1',
    )

    result = db.read_parquet_from_s3(f's3://{mock_s3_bucket}/test/data.parquet')

    pd.testing.assert_frame_equal(result, sample_df)


def test_write_df_to_s3_parquet(mock_s3_bucket, sample_df):
    """Write DataFrame to mock S3 via DuckDB."""
    db = DuckDB(
        s3_access_key_id='testing',
        s3_secret_access_key='testing',
        s3_region='us-east-1',
    )

    db.write_df_to_s3_parquet(
        sample_df, f's3://{mock_s3_bucket}/duckdb_write/output.parquet'
    )

    # Verify file exists and content matches
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )
    assert fs.file_exists(f's3://{mock_s3_bucket}/duckdb_write/output.parquet')

    result = fs.read_df_from_parquet(
        f's3://{mock_s3_bucket}/duckdb_write/output.parquet'
    )
    pd.testing.assert_frame_equal(result, sample_df)


def test_s3_configured_via_s3_instance(mock_s3_bucket, sample_df):
    """DuckDB can be initialized with existing S3FileSystem instance."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    fs.write_df_to_parquet(sample_df, f's3://{mock_s3_bucket}/s3inst/data.parquet')

    db = DuckDB(s3=fs)

    result = db.read_parquet_from_s3(f's3://{mock_s3_bucket}/s3inst/data.parquet')

    pd.testing.assert_frame_equal(result, sample_df)


def test_s3_property_returns_filesystem(mock_s3_bucket):
    """s3 property returns the S3FileSystem instance."""
    db = DuckDB(
        s3_access_key_id='testing',
        s3_secret_access_key='testing',
        s3_region='us-east-1',
    )

    assert db.s3 is not None
    assert isinstance(db.s3, S3FileSystem)


def test_s3_property_returns_none_when_not_configured():
    """s3 property returns None when S3 is not configured."""
    db = DuckDB()

    assert db.s3 is None
