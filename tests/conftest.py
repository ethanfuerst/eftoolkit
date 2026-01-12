"""Shared pytest fixtures."""

import os
from pathlib import Path

import boto3
import pandas as pd
import pytest
from moto import mock_aws

from eftoolkit.sql import DuckDB

TEST_BUCKET = 'test-bucket'


@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame(
        {
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [10.0, 20.0, 30.0],
        }
    )


@pytest.fixture
def db_file():
    """Provide a test database file and clean it up after."""
    db_path = 'test.db'
    yield db_path
    if os.path.exists(db_path):
        os.remove(db_path)


@pytest.fixture
def persistent_db(db_file):
    """Provide a DuckDB instance with persistent storage."""
    return DuckDB(database=db_file)


@pytest.fixture
def memory_db():
    """Provide a DuckDB instance with in-memory storage."""
    return DuckDB(database=':memory:')


@pytest.fixture
def s3_test_dir():
    """Return the path to the test S3-like directory."""
    return Path('tests/s3')


@pytest.fixture
def s3_db(persistent_db, s3_test_dir):
    """Create a DuckDB instance configured for local S3-like operations."""
    db = persistent_db

    # Configure local filesystem for DuckDB
    with db._get_connection() as conn:
        conn.execute('INSTALL httpfs;')
        conn.execute('LOAD httpfs;')
        # Tell DuckDB to replace s3:// with our local path
        conn.execute(f"SET s3_endpoint='file://{s3_test_dir.absolute()}';")
        conn.execute("SET s3_url_style='path';")
        conn.execute("SET s3_region='local';")
        conn.execute("SET s3_access_key_id='dummy';")
        conn.execute("SET s3_secret_access_key='dummy';")

    return db


@pytest.fixture
def mock_s3_bucket():
    """Create a mocked S3 bucket for testing.

    Yields the bucket name for S3FileSystem tests.
    """
    with mock_aws():
        conn = boto3.client('s3', region_name='us-east-1')
        conn.create_bucket(Bucket=TEST_BUCKET)
        yield TEST_BUCKET
