"""Tests for S3FileSystem credential handling."""

import os

import pytest

from eftoolkit.s3 import S3FileSystem


def test_missing_credentials_raises_error(clear_s3_env):
    """Missing credentials raises ValueError."""
    with pytest.raises(ValueError) as exc_info:
        S3FileSystem()

    assert 'S3 credentials required' in str(exc_info.value)
    assert 'AWS_ACCESS_KEY_ID' in str(exc_info.value)
    assert 'AWS_SECRET_ACCESS_KEY' in str(exc_info.value)


def test_credentials_from_aws_env_vars(clear_s3_env):
    """Credentials are read from AWS-standard env vars."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'aws-key'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'aws-secret'
    os.environ['AWS_REGION'] = 'eu-west-1'

    fs = S3FileSystem()

    assert fs.access_key_id == 'aws-key'
    assert fs.secret_access_key == 'aws-secret'
    assert fs.region == 'eu-west-1'


def test_credentials_from_aws_default_region(clear_s3_env):
    """Region falls back to AWS_DEFAULT_REGION when AWS_REGION is unset."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'aws-key'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'aws-secret'
    os.environ['AWS_DEFAULT_REGION'] = 'ap-southeast-2'

    fs = S3FileSystem()

    assert fs.access_key_id == 'aws-key'
    assert fs.secret_access_key == 'aws-secret'
    assert fs.region == 'ap-southeast-2'


def test_explicit_credentials_override_env_vars(clear_s3_env):
    """Explicit credentials take precedence over env vars."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'env-key'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'env-secret'

    fs = S3FileSystem(access_key_id='explicit-key', secret_access_key='explicit-secret')

    assert fs.access_key_id == 'explicit-key'
    assert fs.secret_access_key == 'explicit-secret'


def test_aws_region_takes_precedence_over_aws_default_region(clear_s3_env):
    """AWS_REGION wins over AWS_DEFAULT_REGION (matches boto3 ordering)."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'aws-key'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'aws-secret'
    os.environ['AWS_REGION'] = 'eu-west-1'
    os.environ['AWS_DEFAULT_REGION'] = 'ap-southeast-2'

    fs = S3FileSystem()

    assert fs.region == 'eu-west-1'


def test_legacy_s3_env_vars_are_ignored(clear_s3_env):
    """Legacy S3_* env vars are no longer consulted (ETH-429)."""
    os.environ['S3_ACCESS_KEY_ID'] = 'legacy-key'
    os.environ['S3_SECRET_ACCESS_KEY'] = 'legacy-secret'

    try:
        with pytest.raises(ValueError):
            S3FileSystem()
    finally:
        os.environ.pop('S3_ACCESS_KEY_ID', None)
        os.environ.pop('S3_SECRET_ACCESS_KEY', None)
