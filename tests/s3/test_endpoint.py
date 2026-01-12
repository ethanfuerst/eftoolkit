"""Tests for S3FileSystem custom endpoint handling."""

import os

from eftoolkit.s3 import S3FileSystem


def test_endpoint_stored_correctly():
    """Endpoint is stored correctly."""
    os.environ['S3_ACCESS_KEY_ID'] = 'key'
    os.environ['S3_SECRET_ACCESS_KEY'] = 'secret'

    try:
        fs = S3FileSystem(endpoint='nyc3.digitaloceanspaces.com')

        assert fs.endpoint == 'nyc3.digitaloceanspaces.com'
    finally:
        os.environ.pop('S3_ACCESS_KEY_ID', None)
        os.environ.pop('S3_SECRET_ACCESS_KEY', None)


def test_no_endpoint_returns_none():
    """No endpoint results in None."""
    os.environ['S3_ACCESS_KEY_ID'] = 'key'
    os.environ['S3_SECRET_ACCESS_KEY'] = 'secret'

    try:
        fs = S3FileSystem()

        assert fs.endpoint is None
    finally:
        os.environ.pop('S3_ACCESS_KEY_ID', None)
        os.environ.pop('S3_SECRET_ACCESS_KEY', None)
