"""End-to-end integration tests for S3FileSystem."""

import pandas as pd

from eftoolkit.s3 import S3FileSystem


def test_full_workflow(mock_s3_bucket, sample_df):
    """Test complete workflow: write, exists, list, read."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    assert list(fs.ls(f's3://{mock_s3_bucket}')) == []
    assert fs.file_exists(f's3://{mock_s3_bucket}/integ/data.parquet') is False

    fs.write_df_to_parquet(sample_df, f's3://{mock_s3_bucket}/integ/data.parquet')

    assert fs.file_exists(f's3://{mock_s3_bucket}/integ/data.parquet') is True

    objects = list(fs.ls(f's3://{mock_s3_bucket}'))

    assert len(objects) == 1
    assert objects[0].key == 'integ/data.parquet'

    result = fs.read_df_from_parquet(f's3://{mock_s3_bucket}/integ/data.parquet')
    pd.testing.assert_frame_equal(result, sample_df)


def test_overwrite_existing_file(mock_s3_bucket):
    """Writing to same key overwrites the file."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    df1 = pd.DataFrame({'value': [1, 2, 3]})
    df2 = pd.DataFrame({'value': [10, 20, 30]})

    fs.write_df_to_parquet(
        df1, f's3://{mock_s3_bucket}/integ_overwrite/overwrite.parquet'
    )
    fs.write_df_to_parquet(
        df2, f's3://{mock_s3_bucket}/integ_overwrite/overwrite.parquet'
    )

    result = fs.read_df_from_parquet(
        f's3://{mock_s3_bucket}/integ_overwrite/overwrite.parquet'
    )

    pd.testing.assert_frame_equal(result, df2)


def test_nested_keys(mock_s3_bucket, sample_df):
    """Test deeply nested key paths."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    fs.write_df_to_parquet(
        sample_df, f's3://{mock_s3_bucket}/integ_nested/a/b/c/d/data.parquet'
    )

    assert (
        fs.file_exists(f's3://{mock_s3_bucket}/integ_nested/a/b/c/d/data.parquet')
        is True
    )

    keys = [obj.key for obj in fs.ls(f's3://{mock_s3_bucket}/integ_nested/a/b/c')]

    assert 'integ_nested/a/b/c/d/data.parquet' in keys

    result = fs.read_df_from_parquet(
        f's3://{mock_s3_bucket}/integ_nested/a/b/c/d/data.parquet'
    )

    pd.testing.assert_frame_equal(result, sample_df)
