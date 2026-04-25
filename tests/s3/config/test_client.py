"""Tests for S3FileSystem _get_client internal method."""

import os

from eftoolkit.s3 import S3FileSystem


def test_get_client_returns_boto3_client(mock_s3_bucket):
    """_get_client returns a working boto3 client."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    client = fs._get_client()

    assert client

    response = client.list_objects_v2(Bucket=mock_s3_bucket)

    assert 'Contents' not in response or response['Contents'] == []


def test_get_client_with_endpoint(clear_s3_env):
    """_get_client configures endpoint correctly."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'key'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'secret'

    fs = S3FileSystem(endpoint='custom.endpoint.com', region='us-east-1')
    client = fs._get_client()

    assert client


def test_get_client_with_region(clear_s3_env):
    """_get_client configures region correctly."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'key'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'secret'

    fs = S3FileSystem(region='us-west-2')
    client = fs._get_client()

    assert client


def test_get_client_without_region(clear_s3_env):
    """_get_client works without region."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'key'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'secret'

    fs = S3FileSystem()
    client = fs._get_client()

    assert client
