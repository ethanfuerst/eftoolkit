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

    # Core attributes
    assert obj.key == 'ls_meta/data.parquet'
    assert obj.bucket == mock_s3_bucket
    assert obj.uri == f's3://{mock_s3_bucket}/ls_meta/data.parquet'

    # Metadata object
    assert obj.metadata is not None
    assert obj.metadata.size is not None
    assert obj.metadata.size > 0
    assert obj.metadata.last_modified_timestamp_utc is not None
    assert obj.metadata.etag is not None
    assert obj.metadata.is_prefix is False


def test_ls_s3_object_str_returns_uri(mock_s3_bucket, sample_df):
    """S3Object __str__ returns the full S3 URI."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    fs.write_df_to_parquet(sample_df, f's3://{mock_s3_bucket}/ls_str/data.parquet')

    obj = next(fs.ls(f's3://{mock_s3_bucket}'))

    assert str(obj) == f's3://{mock_s3_bucket}/ls_str/data.parquet'
    assert obj.uri == f's3://{mock_s3_bucket}/ls_str/data.parquet'


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


def test_s3_object_metadata_items(mock_s3_bucket, sample_df):
    """S3ObjectMetadata.items() yields key-value pairs."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    fs.write_df_to_parquet(sample_df, f's3://{mock_s3_bucket}/ls_items/data.parquet')

    obj = next(fs.ls(f's3://{mock_s3_bucket}/ls_items'))
    result = dict(obj.metadata.items())

    assert isinstance(result, dict)
    assert 'is_prefix' in result
    assert 'last_modified_timestamp_utc' in result
    assert 'size' in result
    assert 'etag' in result
    assert 'storage_class' in result
    assert result['is_prefix'] is False
    assert result['size'] > 0


def test_s3_object_metadata_dict_conversion(mock_s3_bucket, sample_df):
    """dict(metadata) works via __iter__."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    fs.write_df_to_parquet(
        sample_df, f's3://{mock_s3_bucket}/ls_dict_conv/data.parquet'
    )

    obj = next(fs.ls(f's3://{mock_s3_bucket}/ls_dict_conv'))
    result = dict(obj.metadata)

    assert isinstance(result, dict)
    assert result == dict(obj.metadata.items())


def test_ls_include_prefixes_returns_directories(mock_s3_bucket, sample_df):
    """ls with include_prefixes=True yields prefix entries."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    # Create nested structure
    fs.write_df_to_parquet(
        sample_df, f's3://{mock_s3_bucket}/ls_prefixes/subdir1/file1.parquet'
    )
    fs.write_df_to_parquet(
        sample_df, f's3://{mock_s3_bucket}/ls_prefixes/subdir2/file2.parquet'
    )
    fs.write_df_to_parquet(sample_df, f's3://{mock_s3_bucket}/ls_prefixes/root.parquet')

    # With include_prefixes=True, should see both files and prefixes
    objects = list(
        fs.ls(
            f's3://{mock_s3_bucket}/ls_prefixes', recursive=False, include_prefixes=True
        )
    )

    keys = [obj.key for obj in objects]
    prefixes = [obj for obj in objects if obj.metadata.is_prefix]
    files = [obj for obj in objects if not obj.metadata.is_prefix]

    # Should have root file + 2 prefixes
    assert 'ls_prefixes/root.parquet' in keys
    assert 'ls_prefixes/subdir1/' in keys
    assert 'ls_prefixes/subdir2/' in keys
    assert len(files) == 1
    assert len(prefixes) == 2

    # Prefixes should have is_prefix=True
    for p in prefixes:
        assert p.metadata.is_prefix is True
        assert p.metadata.size is None  # Prefixes don't have size


def test_ls_include_prefixes_default_false(mock_s3_bucket, sample_df):
    """ls with include_prefixes=False (default) does not yield prefixes."""
    fs = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    fs.write_df_to_parquet(
        sample_df, f's3://{mock_s3_bucket}/ls_no_prefixes/subdir/file.parquet'
    )
    fs.write_df_to_parquet(
        sample_df, f's3://{mock_s3_bucket}/ls_no_prefixes/root.parquet'
    )

    # Default behavior - no prefixes
    objects = list(fs.ls(f's3://{mock_s3_bucket}/ls_no_prefixes', recursive=False))
    keys = [obj.key for obj in objects]

    assert keys == ['ls_no_prefixes/root.parquet']
    assert 'ls_no_prefixes/subdir/' not in keys
