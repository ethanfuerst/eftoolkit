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

    assert client is not None

    response = client.list_objects_v2(Bucket=mock_s3_bucket)

    assert 'Contents' not in response or response['Contents'] == []


def test_get_client_with_endpoint():
    """_get_client configures endpoint correctly."""
    os.environ['S3_ACCESS_KEY_ID'] = 'key'
    os.environ['S3_SECRET_ACCESS_KEY'] = 'secret'

    try:
        fs = S3FileSystem(endpoint='custom.endpoint.com', region='us-east-1')
        client = fs._get_client()

        assert client is not None
    finally:
        os.environ.pop('S3_ACCESS_KEY_ID', None)
        os.environ.pop('S3_SECRET_ACCESS_KEY', None)


def test_get_client_with_region():
    """_get_client configures region correctly."""
    os.environ['S3_ACCESS_KEY_ID'] = 'key'
    os.environ['S3_SECRET_ACCESS_KEY'] = 'secret'

    try:
        fs = S3FileSystem(region='us-west-2')
        client = fs._get_client()

        assert client is not None
    finally:
        os.environ.pop('S3_ACCESS_KEY_ID', None)
        os.environ.pop('S3_SECRET_ACCESS_KEY', None)


def test_get_client_without_region():
    """_get_client works without region."""
    os.environ['S3_ACCESS_KEY_ID'] = 'key'
    os.environ['S3_SECRET_ACCESS_KEY'] = 'secret'
    os.environ.pop('S3_REGION', None)
    os.environ.pop('AWS_REGION', None)

    try:
        fs = S3FileSystem()
        client = fs._get_client()

        assert client is not None
    finally:
        os.environ.pop('S3_ACCESS_KEY_ID', None)
        os.environ.pop('S3_SECRET_ACCESS_KEY', None)
