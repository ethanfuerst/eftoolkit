"""Tests for S3FileSystem ls method."""

from eftoolkit.s3 import S3FileSystem, S3Object


def test_ls_returns_iterator_of_s3_objects(mock_s3_bucket, sample_df):
    """ls returns an iterator of S3Object instances."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    fs.write_df_to_parquet(sample_df, f's3://{mock_s3_bucket}/ls_iter/data.parquet')

    result = fs.ls(f's3://{mock_s3_bucket}')

    # Should be an iterator, not a list
    assert hasattr(result, '__iter__')
    assert hasattr(result, '__next__')

    # Should yield S3Object instances
    objects = list(result)

    assert len(objects) == 1
    assert isinstance(objects[0], S3Object)
    assert objects[0].key == 'ls_iter/data.parquet'


def test_ls_s3_object_has_metadata(mock_s3_bucket, sample_df):
    """S3Object includes metadata like size and last_modified."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    fs.write_df_to_parquet(sample_df, f's3://{mock_s3_bucket}/ls_meta/data.parquet')

    objects = list(fs.ls(f's3://{mock_s3_bucket}'))
    obj = objects[0]

    assert obj.key == 'ls_meta/data.parquet'
    assert obj.size is not None
    assert obj.size > 0
    assert obj.last_modified is not None
    assert obj.etag is not None


def test_ls_s3_object_str_returns_key(mock_s3_bucket, sample_df):
    """S3Object __str__ returns the key."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    fs.write_df_to_parquet(sample_df, f's3://{mock_s3_bucket}/ls_str/data.parquet')

    obj = next(fs.ls(f's3://{mock_s3_bucket}'))

    assert str(obj) == 'ls_str/data.parquet'


def test_ls_recursive_returns_all_keys(mock_s3_bucket, sample_df):
    """ls with recursive=True returns all keys in bucket."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    fs.write_df_to_parquet(sample_df, f's3://{mock_s3_bucket}/ls_all/a/data1.parquet')
    fs.write_df_to_parquet(sample_df, f's3://{mock_s3_bucket}/ls_all/b/data2.parquet')

    keys = [obj.key for obj in fs.ls(f's3://{mock_s3_bucket}')]

    assert 'ls_all/a/data1.parquet' in keys
    assert 'ls_all/b/data2.parquet' in keys


def test_ls_with_prefix(mock_s3_bucket, sample_df):
    """ls filters by prefix."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    fs.write_df_to_parquet(
        sample_df, f's3://{mock_s3_bucket}/ls_prefix/prefix1/data.parquet'
    )
    fs.write_df_to_parquet(
        sample_df, f's3://{mock_s3_bucket}/ls_prefix/prefix2/data.parquet'
    )

    objects = list(fs.ls(f's3://{mock_s3_bucket}/ls_prefix/prefix1'))

    assert len(objects) == 1
    assert objects[0].key == 'ls_prefix/prefix1/data.parquet'


def test_ls_empty_bucket(mock_s3_bucket):
    """ls returns empty iterator for empty bucket."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    objects = list(fs.ls(f's3://{mock_s3_bucket}'))

    assert objects == []


def test_ls_non_recursive_returns_only_immediate_files(mock_s3_bucket, sample_df):
    """ls with recursive=False returns only files at immediate level."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    # Create nested structure
    fs.write_df_to_parquet(
        sample_df, f's3://{mock_s3_bucket}/ls_nonrec/a/nested.parquet'
    )
    fs.write_df_to_parquet(
        sample_df, f's3://{mock_s3_bucket}/ls_nonrec/b/deep/file.parquet'
    )
    fs.write_df_to_parquet(sample_df, f's3://{mock_s3_bucket}/ls_nonrec/root.parquet')

    # Non-recursive ls should only show files at the immediate level
    objects = list(fs.ls(f's3://{mock_s3_bucket}/ls_nonrec', recursive=False))
    keys = [obj.key for obj in objects]

    # Should contain only the root file
    assert keys == ['ls_nonrec/root.parquet']

    # Should NOT contain nested files or directories
    assert 'ls_nonrec/a/' not in keys
    assert 'ls_nonrec/b/' not in keys
    assert 'ls_nonrec/a/nested.parquet' not in keys
    assert 'ls_nonrec/b/deep/file.parquet' not in keys


def test_ls_non_recursive_at_subdirectory(mock_s3_bucket, sample_df):
    """ls with recursive=False at a subdirectory level."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    fs.write_df_to_parquet(
        sample_df, f's3://{mock_s3_bucket}/ls_subdir/level1/file1.parquet'
    )
    fs.write_df_to_parquet(
        sample_df, f's3://{mock_s3_bucket}/ls_subdir/level1/level2/file2.parquet'
    )

    objects = list(fs.ls(f's3://{mock_s3_bucket}/ls_subdir/level1', recursive=False))
    keys = [obj.key for obj in objects]

    # Should contain only immediate files
    assert keys == ['ls_subdir/level1/file1.parquet']
    assert 'ls_subdir/level1/level2/' not in keys
    assert 'ls_subdir/level1/level2/file2.parquet' not in keys
