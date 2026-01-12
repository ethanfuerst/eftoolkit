"""Shared pytest fixtures."""

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
