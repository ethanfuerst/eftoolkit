"""Tests for S3FileSystem delete_object method."""

from eftoolkit.s3 import S3FileSystem


def test_delete_object_removes_object(mock_s3_bucket):
    """delete_object removes an existing object."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    fs.put_object(f's3://{mock_s3_bucket}/test/to_delete.txt', b'data')

    assert fs.file_exists(f's3://{mock_s3_bucket}/test/to_delete.txt') is True

    fs.delete_object(f's3://{mock_s3_bucket}/test/to_delete.txt')

    assert fs.file_exists(f's3://{mock_s3_bucket}/test/to_delete.txt') is False


def test_delete_object_missing_is_noop(mock_s3_bucket):
    """delete_object on non-existent object does not error (idempotent)."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    # Should not raise any exception
    fs.delete_object(f's3://{mock_s3_bucket}/nonexistent/file.txt')


def test_delete_object_twice_is_idempotent(mock_s3_bucket):
    """delete_object can be called multiple times without error."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    fs.put_object(f's3://{mock_s3_bucket}/test/file.txt', b'data')

    fs.delete_object(f's3://{mock_s3_bucket}/test/file.txt')
    fs.delete_object(f's3://{mock_s3_bucket}/test/file.txt')

    assert fs.file_exists(f's3://{mock_s3_bucket}/test/file.txt') is False


def test_delete_object_does_not_affect_other_objects(mock_s3_bucket):
    """delete_object only removes the specified object."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    fs.put_object(f's3://{mock_s3_bucket}/test/file1.txt', b'data1')
    fs.put_object(f's3://{mock_s3_bucket}/test/file2.txt', b'data2')

    fs.delete_object(f's3://{mock_s3_bucket}/test/file1.txt')

    assert fs.file_exists(f's3://{mock_s3_bucket}/test/file1.txt') is False
    assert fs.file_exists(f's3://{mock_s3_bucket}/test/file2.txt') is True


def test_delete_object_nested_path(mock_s3_bucket):
    """delete_object works with deeply nested paths."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    fs.put_object(f's3://{mock_s3_bucket}/a/b/c/d/e/file.txt', b'nested data')

    fs.delete_object(f's3://{mock_s3_bucket}/a/b/c/d/e/file.txt')

    assert fs.file_exists(f's3://{mock_s3_bucket}/a/b/c/d/e/file.txt') is False
