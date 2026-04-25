"""Tests for S3FileSystem custom endpoint handling."""

import os

from eftoolkit.s3 import S3FileSystem


def test_endpoint_stored_correctly(clear_s3_env):
    """Endpoint is stored correctly."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'key'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'secret'

    fs = S3FileSystem(endpoint='nyc3.digitaloceanspaces.com')

    assert fs.endpoint == 'nyc3.digitaloceanspaces.com'


def test_no_endpoint_returns_none(clear_s3_env):
    """No endpoint results in None."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'key'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'secret'

    fs = S3FileSystem()

    assert fs.endpoint is None


def test_endpoint_from_aws_env_var(clear_s3_env):
    """Endpoint falls back to AWS_ENDPOINT_URL_S3 env var."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'key'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'secret'
    os.environ['AWS_ENDPOINT_URL_S3'] = 'minio.local:9000'

    fs = S3FileSystem()

    assert fs.endpoint == 'minio.local:9000'


def test_explicit_endpoint_overrides_aws_env_var(clear_s3_env):
    """Explicit endpoint kwarg wins over AWS_ENDPOINT_URL_S3."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'key'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'secret'
    os.environ['AWS_ENDPOINT_URL_S3'] = 'env.endpoint.com'

    fs = S3FileSystem(endpoint='kwarg.endpoint.com')

    assert fs.endpoint == 'kwarg.endpoint.com'
