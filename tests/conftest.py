"""Shared pytest fixtures."""

import os

import boto3
import pandas as pd
import pytest
from moto import mock_aws

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
def mock_s3_bucket():
    """Create a mocked S3 bucket for testing.

    Yields the bucket name for S3FileSystem tests.
    """
    with mock_aws():
        conn = boto3.client('s3', region_name='us-east-1')
        conn.create_bucket(Bucket=TEST_BUCKET)
        yield TEST_BUCKET


@pytest.fixture
def clear_s3_env():
    """Clear AWS_* env vars for the duration of the test.

    Keeps tests that assume "no S3 configured" deterministic regardless
    of what env vars the host machine has set. Restores the host's
    original values on teardown.
    """
    keys = [
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY',
        'AWS_REGION',
        'AWS_DEFAULT_REGION',
        'AWS_ENDPOINT_URL_S3',
    ]
    saved = {k: os.environ.pop(k, None) for k in keys}
    try:
        yield
    finally:
        for k in keys:
            os.environ.pop(k, None)
        for k, v in saved.items():
            if v is not None:
                os.environ[k] = v
