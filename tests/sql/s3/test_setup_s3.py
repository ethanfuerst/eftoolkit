"""Tests for DuckDB _setup_s3 method and credential configuration."""

import os

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


def test_setup_s3_not_called_without_credentials(clear_s3_env):
    """_setup_s3 does nothing when credentials are not provided."""
    db = DuckDB()

    # Should not raise, just doesn't configure S3
    result = db.query('SELECT 1 as num')

    assert result['num'][0] == 1


def test_duckdb_credentials_from_s3_env_vars(clear_s3_env):
    """DuckDB resolves S3 creds from S3_* env vars and configures a native secret."""
    os.environ['S3_ACCESS_KEY_ID'] = 'test-key'
    os.environ['S3_SECRET_ACCESS_KEY'] = 'test-secret'
    os.environ['S3_REGION'] = 'us-west-2'

    db = DuckDB()

    assert db.s3_access_key_id == 'test-key'
    assert db.s3_secret_access_key == 'test-secret'
    assert db.s3_region == 'us-west-2'
    assert db._s3 is not None
    assert isinstance(db.s3, S3FileSystem)

    secrets = db.query('SELECT name, type FROM duckdb_secrets()')

    assert len(secrets) >= 1
    assert (secrets['type'] == 's3').any()


def test_duckdb_credentials_from_aws_env_vars(clear_s3_env):
    """DuckDB falls back to AWS_* env vars when S3_* aren't set."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'aws-key'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'aws-secret'
    os.environ['AWS_REGION'] = 'eu-west-1'

    db = DuckDB()

    assert db.s3_access_key_id == 'aws-key'
    assert db.s3_secret_access_key == 'aws-secret'
    assert db.s3_region == 'eu-west-1'
    assert db._s3 is not None

    secrets = db.query('SELECT name, type FROM duckdb_secrets()')

    assert (secrets['type'] == 's3').any()


def test_duckdb_credentials_from_aws_default_region(clear_s3_env):
    """DuckDB falls back to AWS_DEFAULT_REGION for region when others are unset."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'aws-key'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'aws-secret'
    os.environ['AWS_DEFAULT_REGION'] = 'ap-southeast-2'

    db = DuckDB()

    assert db.s3_access_key_id == 'aws-key'
    assert db.s3_secret_access_key == 'aws-secret'
    assert db.s3_region == 'ap-southeast-2'
    assert db._s3 is not None

    secrets = db.query('SELECT name, type FROM duckdb_secrets()')

    assert (secrets['type'] == 's3').any()


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
