"""Tests for S3FileSystem credential handling."""

import os

import pytest

from eftoolkit.s3 import S3FileSystem


def test_missing_credentials_raises_error():
    """Missing credentials raises ValueError."""
    # Clear any env vars
    for key in [
        'S3_ACCESS_KEY_ID',
        'AWS_ACCESS_KEY_ID',
        'S3_SECRET_ACCESS_KEY',
        'AWS_SECRET_ACCESS_KEY',
    ]:
        os.environ.pop(key, None)

    with pytest.raises(ValueError) as exc_info:
        S3FileSystem()

    assert 'S3 credentials required' in str(exc_info.value)


def test_credentials_from_s3_env_vars():
    """Credentials are read from S3_* env vars."""
    os.environ['S3_ACCESS_KEY_ID'] = 'test-key'
    os.environ['S3_SECRET_ACCESS_KEY'] = 'test-secret'
    os.environ['S3_REGION'] = 'us-west-2'
    os.environ['S3_ENDPOINT'] = 'custom.endpoint.com'

    try:
        fs = S3FileSystem()

        assert fs.access_key_id == 'test-key'
        assert fs.secret_access_key == 'test-secret'
        assert fs.region == 'us-west-2'
        assert fs.endpoint == 'custom.endpoint.com'
    finally:
        os.environ.pop('S3_ACCESS_KEY_ID', None)
        os.environ.pop('S3_SECRET_ACCESS_KEY', None)
        os.environ.pop('S3_REGION', None)
        os.environ.pop('S3_ENDPOINT', None)


def test_credentials_from_aws_env_vars():
    """Credentials are read from AWS_* env vars as fallback."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'aws-key'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'aws-secret'
    os.environ['AWS_REGION'] = 'eu-west-1'

    try:
        fs = S3FileSystem()

        assert fs.access_key_id == 'aws-key'
        assert fs.secret_access_key == 'aws-secret'
        assert fs.region == 'eu-west-1'
    finally:
        os.environ.pop('AWS_ACCESS_KEY_ID', None)
        os.environ.pop('AWS_SECRET_ACCESS_KEY', None)
        os.environ.pop('AWS_REGION', None)


def test_explicit_credentials_override_env_vars():
    """Explicit credentials take precedence over env vars."""
    os.environ['S3_ACCESS_KEY_ID'] = 'env-key'
    os.environ['S3_SECRET_ACCESS_KEY'] = 'env-secret'

    try:
        fs = S3FileSystem(
            access_key_id='explicit-key', secret_access_key='explicit-secret'
        )

        assert fs.access_key_id == 'explicit-key'
        assert fs.secret_access_key == 'explicit-secret'
    finally:
        os.environ.pop('S3_ACCESS_KEY_ID', None)
        os.environ.pop('S3_SECRET_ACCESS_KEY', None)


def test_s3_env_vars_take_precedence_over_aws():
    """S3_* env vars take precedence over AWS_* env vars."""
    os.environ['S3_ACCESS_KEY_ID'] = 's3-key'
    os.environ['AWS_ACCESS_KEY_ID'] = 'aws-key'
    os.environ['S3_SECRET_ACCESS_KEY'] = 's3-secret'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'aws-secret'

    try:
        fs = S3FileSystem()

        assert fs.access_key_id == 's3-key'
        assert fs.secret_access_key == 's3-secret'
    finally:
        os.environ.pop('S3_ACCESS_KEY_ID', None)
        os.environ.pop('AWS_ACCESS_KEY_ID', None)
        os.environ.pop('S3_SECRET_ACCESS_KEY', None)
        os.environ.pop('AWS_SECRET_ACCESS_KEY', None)
