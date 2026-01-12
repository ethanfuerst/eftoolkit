"""Tests for S3FileSystem put_object method."""

from eftoolkit.s3 import S3FileSystem


def test_put_object_uploads_bytes(mock_s3_bucket):
    """put_object uploads raw bytes to S3."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    data = b'hello world'
    fs.put_object(f's3://{mock_s3_bucket}/test/hello.txt', data)

    # Verify using get_object
    result = fs.get_object(f's3://{mock_s3_bucket}/test/hello.txt')

    assert result == data


def test_put_object_with_content_type(mock_s3_bucket):
    """put_object accepts optional content_type."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    data = b'{"key": "value"}'
    fs.put_object(
        f's3://{mock_s3_bucket}/test/data.json',
        data,
        content_type='application/json',
    )

    # Verify data was uploaded
    result = fs.get_object(f's3://{mock_s3_bucket}/test/data.json')

    assert result == data


def test_put_object_overwrites_existing(mock_s3_bucket):
    """put_object overwrites existing object."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    fs.put_object(f's3://{mock_s3_bucket}/test/file.txt', b'original')
    fs.put_object(f's3://{mock_s3_bucket}/test/file.txt', b'updated')

    result = fs.get_object(f's3://{mock_s3_bucket}/test/file.txt')

    assert result == b'updated'


def test_put_object_empty_bytes(mock_s3_bucket):
    """put_object handles empty bytes."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    fs.put_object(f's3://{mock_s3_bucket}/test/empty.txt', b'')

    result = fs.get_object(f's3://{mock_s3_bucket}/test/empty.txt')

    assert result == b''


def test_put_object_binary_data(mock_s3_bucket):
    """put_object handles binary data."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    # Binary data with null bytes
    data = bytes(range(256))
    fs.put_object(f's3://{mock_s3_bucket}/test/binary.bin', data)

    result = fs.get_object(f's3://{mock_s3_bucket}/test/binary.bin')

    assert result == data
