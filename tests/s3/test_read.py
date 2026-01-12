"""Tests for S3FileSystem read_df_from_parquet method."""

import boto3
import pandas as pd

from eftoolkit.s3 import S3FileSystem


def test_read_single_file_with_parquet_extension(mock_s3_bucket, sample_df):
    """Read a single file using .parquet extension."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    fs.write_df_to_parquet(sample_df, f's3://{mock_s3_bucket}/read_single/data.parquet')

    result = fs.read_df_from_parquet(f's3://{mock_s3_bucket}/read_single/data.parquet')

    pd.testing.assert_frame_equal(result, sample_df)


def test_read_preserves_data_types(mock_s3_bucket):
    """Verify data types are preserved after round-trip."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    df = pd.DataFrame(
        {
            'int_col': [1, 2, 3],
            'float_col': [1.1, 2.2, 3.3],
            'str_col': ['a', 'b', 'c'],
            'bool_col': [True, False, True],
        }
    )

    fs.write_df_to_parquet(df, f's3://{mock_s3_bucket}/read_types/types.parquet')
    result = fs.read_df_from_parquet(f's3://{mock_s3_bucket}/read_types/types.parquet')

    assert result['int_col'].dtype == df['int_col'].dtype
    assert result['float_col'].dtype == df['float_col'].dtype
    assert result['str_col'].dtype == df['str_col'].dtype
    assert result['bool_col'].dtype == df['bool_col'].dtype


def test_read_directory_concatenates_files(mock_s3_bucket):
    """Read multiple parquet files from a directory."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    df1 = pd.DataFrame({'id': [1, 2], 'name': ['Alice', 'Bob']})
    df2 = pd.DataFrame({'id': [3, 4], 'name': ['Charlie', 'Diana']})
    expected = pd.concat([df1, df2], ignore_index=True)

    fs.write_df_to_parquet(df1, f's3://{mock_s3_bucket}/read_dir/multi/part1.parquet')
    fs.write_df_to_parquet(df2, f's3://{mock_s3_bucket}/read_dir/multi/part2.parquet')

    result = fs.read_df_from_parquet(f's3://{mock_s3_bucket}/read_dir/multi')

    assert len(result) == 4
    assert set(result['id'].tolist()) == {1, 2, 3, 4}
    pd.testing.assert_frame_equal(
        result.sort_values('id').reset_index(drop=True), expected
    )


def test_read_directory_ignores_non_parquet_files(mock_s3_bucket):
    """Only .parquet files are read from directory."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    df = pd.DataFrame({'id': [1, 2]})
    fs.write_df_to_parquet(
        df, f's3://{mock_s3_bucket}/read_dir_mixed/mixed/data.parquet'
    )

    conn = boto3.client('s3', region_name='us-east-1')
    conn.put_object(
        Bucket=mock_s3_bucket, Key='read_dir_mixed/mixed/readme.txt', Body=b'readme'
    )

    result = fs.read_df_from_parquet(f's3://{mock_s3_bucket}/read_dir_mixed/mixed')

    assert len(result) == 2
    pd.testing.assert_frame_equal(result.sort_values('id').reset_index(drop=True), df)
