"""Tests for S3FileSystem file_exists method."""

from eftoolkit.s3 import S3FileSystem


def test_file_exists_returns_true_for_existing(mock_s3_bucket, sample_df):
    """file_exists returns True for existing file."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    fs.write_df_to_parquet(sample_df, f's3://{mock_s3_bucket}/exists/exists.parquet')

    assert fs.file_exists(f's3://{mock_s3_bucket}/exists/exists.parquet') is True


def test_file_exists_returns_false_for_missing(mock_s3_bucket):
    """file_exists returns False for non-existent file."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    assert (
        fs.file_exists(f's3://{mock_s3_bucket}/missing/does-not-exist.parquet') is False
    )
