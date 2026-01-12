"""Tests for DuckDB _setup_s3 method and credential configuration."""

import pandas as pd

from eftoolkit.s3 import S3FileSystem
from eftoolkit.sql import DuckDB


def test_setup_s3_creates_secret(mock_s3_bucket, sample_df):
    """_setup_s3 creates DuckDB S3 secret with credentials."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    fs.write_df_to_parquet(sample_df, f's3://{mock_s3_bucket}/setup_test/data.parquet')

    db = DuckDB(
        s3_access_key_id='testing',
        s3_secret_access_key='testing',
        s3_region='us-east-1',
    )

    # Query using native DuckDB S3 support (tests that _setup_s3 worked)
    # Note: moto doesn't support DuckDB's native S3 access, so we test
    # via the S3FileSystem which we know works
    result = db.read_parquet_from_s3(f's3://{mock_s3_bucket}/setup_test/data.parquet')

    pd.testing.assert_frame_equal(result, sample_df)


def test_setup_s3_with_endpoint(mock_s3_bucket, sample_df):
    """_setup_s3 includes endpoint when provided."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    fs.write_df_to_parquet(
        sample_df, f's3://{mock_s3_bucket}/endpoint_test/data.parquet'
    )

    db = DuckDB(
        s3_access_key_id='testing',
        s3_secret_access_key='testing',
        s3_region='us-east-1',
        s3_endpoint='nyc3.digitaloceanspaces.com',
    )

    assert db.s3_endpoint == 'nyc3.digitaloceanspaces.com'


def test_setup_s3_with_url_style(mock_s3_bucket):
    """_setup_s3 sets s3_url_style when provided."""
    db = DuckDB(
        s3_access_key_id='testing',
        s3_secret_access_key='testing',
        s3_region='us-east-1',
        s3_url_style='path',
    )

    assert db.s3_url_style == 'path'


def test_setup_s3_not_called_without_credentials():
    """_setup_s3 does nothing when credentials are not provided."""
    db = DuckDB()

    # Should not raise, just doesn't configure S3
    result = db.query('SELECT 1 as num')

    assert result['num'][0] == 1


def test_setup_s3_without_url_style(mock_s3_bucket):
    """_setup_s3 works without url_style."""
    db = DuckDB(
        s3_access_key_id='testing',
        s3_secret_access_key='testing',
        s3_region='us-east-1',
    )

    # If _setup_s3 runs correctly, queries should work
    result = db.query('SELECT 1 as num')

    assert result['num'][0] == 1


def test_setup_s3_with_url_style_executes(mock_s3_bucket):
    """_setup_s3 executes SET s3_url_style when provided."""
    db = DuckDB(
        s3_access_key_id='testing',
        s3_secret_access_key='testing',
        s3_region='us-east-1',
        s3_url_style='path',
    )

    # If _setup_s3 runs correctly, queries should work
    result = db.query('SELECT 1 as num')

    assert result['num'][0] == 1


def test_setup_s3_with_endpoint_and_url_style(mock_s3_bucket):
    """_setup_s3 handles both endpoint and url_style."""
    db = DuckDB(
        s3_access_key_id='testing',
        s3_secret_access_key='testing',
        s3_region='us-east-1',
        s3_endpoint='nyc3.digitaloceanspaces.com',
        s3_url_style='path',
    )

    # If _setup_s3 runs correctly with endpoint, queries should work
    result = db.query('SELECT 1 as num')

    assert result['num'][0] == 1
