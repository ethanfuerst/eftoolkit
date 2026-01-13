"""S3 filesystem utilities."""

import io
import os
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime

import boto3
import pandas as pd
from botocore.exceptions import ClientError


@dataclass(frozen=True)
class S3ObjectMetadata:
    """Metadata for an S3 object from boto3 response.

    Attributes:
        is_prefix: True if this represents a prefix/directory, not an actual object
        last_modified_timestamp_utc: When the object was last modified (UTC)
        size: Object size in bytes
        etag: Object ETag hash
        storage_class: S3 storage class (STANDARD, GLACIER, etc.)
    """

    is_prefix: bool = False
    last_modified_timestamp_utc: datetime | None = None
    size: int | None = None
    etag: str | None = None
    storage_class: str | None = None

    def items(self):
        """Yield key-value pairs of metadata fields.

        Enables dict(metadata.items()) and `for k, v in metadata.items()`.
        """
        yield ('is_prefix', self.is_prefix)
        yield ('last_modified_timestamp_utc', self.last_modified_timestamp_utc)
        yield ('size', self.size)
        yield ('etag', self.etag)
        yield ('storage_class', self.storage_class)

    def __iter__(self):
        """Allow dict(metadata) to work by yielding key-value pairs."""
        yield from self.items()

    @classmethod
    def from_boto_response(
        cls, obj: dict, *, is_prefix: bool = False
    ) -> 'S3ObjectMetadata':
        """Create S3ObjectMetadata from boto3 list_objects_v2 response dict."""
        return cls(
            is_prefix=is_prefix,
            last_modified_timestamp_utc=obj.get('LastModified'),
            size=obj.get('Size'),
            etag=obj.get('ETag', '').strip('"') if obj.get('ETag') else None,
            storage_class=obj.get('StorageClass'),
        )


@dataclass(frozen=True)
class S3Object:
    """Represents an S3 object with its location and metadata.

    Attributes:
        key: Object key (path within bucket)
        bucket: Bucket name
        uri: Full S3 URI (s3://bucket/key)
        metadata: Object metadata (size, last_modified, etc.)
    """

    key: str
    bucket: str
    metadata: S3ObjectMetadata

    @property
    def uri(self) -> str:
        """Return the full S3 URI."""
        return f's3://{self.bucket}/{self.key}'

    @classmethod
    def from_boto_response(
        cls, obj: dict, *, bucket: str, is_prefix: bool = False
    ) -> 'S3Object':
        """Create S3Object from boto3 list_objects_v2 response dict."""
        return cls(
            key=obj['Key'],
            bucket=bucket,
            metadata=S3ObjectMetadata.from_boto_response(obj, is_prefix=is_prefix),
        )

    def __str__(self) -> str:
        """Return the full S3 URI."""
        return self.uri


def _parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    """Parse an S3 URI into bucket and key components.

    Args:
        s3_uri: S3 URI in format 's3://bucket/key' or 's3://bucket' (for bucket-only)

    Returns:
        Tuple of (bucket, key). Key is empty string for bucket-only URIs.

    Raises:
        ValueError: If URI doesn't start with 's3://'
    """
    if not s3_uri.startswith('s3://'):
        raise ValueError(f"Invalid S3 URI: must start with 's3://', got: '{s3_uri}'")

    path = s3_uri[5:]  # Remove 's3://'
    if '/' in path:
        bucket, key = path.split('/', 1)
    else:
        bucket, key = path, ''

    if not bucket:
        raise ValueError(f"Invalid S3 URI: bucket name is empty in '{s3_uri}'")

    return bucket, key


class S3FileSystem:
    """S3 filesystem client for reading/writing parquet files.

    Falls back to environment variables if credentials are not provided:
      - S3_ACCESS_KEY_ID / AWS_ACCESS_KEY_ID
      - S3_SECRET_ACCESS_KEY / AWS_SECRET_ACCESS_KEY
      - S3_REGION / AWS_REGION
      - S3_ENDPOINT
    """

    def __init__(
        self,
        *,
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
        region: str | None = None,
        endpoint: str | None = None,
    ) -> None:
        """Initialize S3 filesystem.

        Args:
            access_key_id: AWS access key ID
            secret_access_key: AWS secret access key
            region: AWS region
            endpoint: Custom S3 endpoint (e.g., 'nyc3.digitaloceanspaces.com')
        """
        self.access_key_id = access_key_id or os.getenv(
            'S3_ACCESS_KEY_ID', os.getenv('AWS_ACCESS_KEY_ID')
        )
        self.secret_access_key = secret_access_key or os.getenv(
            'S3_SECRET_ACCESS_KEY', os.getenv('AWS_SECRET_ACCESS_KEY')
        )
        self.region = region or os.getenv('S3_REGION', os.getenv('AWS_REGION'))
        self.endpoint = endpoint or os.getenv('S3_ENDPOINT')

        if not self.access_key_id or not self.secret_access_key:
            raise ValueError(
                'S3 credentials required. Pass access_key_id/secret_access_key '
                'or set S3_ACCESS_KEY_ID/S3_SECRET_ACCESS_KEY environment variables.'
            )

    def _get_client(self):
        """Get boto3 S3 client."""
        endpoint_url = f'https://{self.endpoint}' if self.endpoint else None
        return boto3.client(
            's3',
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            region_name=self.region,
            endpoint_url=endpoint_url,
        )

    def put_object(
        self,
        s3_uri: str,
        body: bytes,
        *,
        content_type: str | None = None,
    ) -> None:
        """Upload raw bytes to S3.

        Args:
            s3_uri: S3 URI (e.g., 's3://bucket/key')
            body: Raw bytes to upload
            content_type: Optional content type (e.g., 'application/octet-stream')
        """
        bucket, key = _parse_s3_uri(s3_uri)
        client = self._get_client()
        params = {'Bucket': bucket, 'Key': key, 'Body': body}
        if content_type:
            params['ContentType'] = content_type
        client.put_object(**params)

    def get_object(self, s3_uri: str) -> bytes:
        """Download raw bytes from S3.

        Args:
            s3_uri: S3 URI (e.g., 's3://bucket/key')

        Returns:
            Raw bytes of the object

        Raises:
            FileNotFoundError: If the object does not exist
        """
        bucket, key = _parse_s3_uri(s3_uri)
        client = self._get_client()
        try:
            response = client.get_object(Bucket=bucket, Key=key)
            return response['Body'].read()
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                raise FileNotFoundError(f'{s3_uri} does not exist') from e
            raise

    def delete_object(self, s3_uri: str) -> None:
        """Delete an object from S3.

        Args:
            s3_uri: S3 URI (e.g., 's3://bucket/key')

        Note:
            This is idempotent - deleting a non-existent object does not error.
        """
        bucket, key = _parse_s3_uri(s3_uri)
        client = self._get_client()
        client.delete_object(Bucket=bucket, Key=key)

    def cp(self, src_uri: str, dst_uri: str) -> None:
        """Copy an object within or across buckets.

        Args:
            src_uri: Source S3 URI (e.g., 's3://bucket/key')
            dst_uri: Destination S3 URI (e.g., 's3://bucket/key')

        Raises:
            FileNotFoundError: If the source object does not exist
        """
        src_bucket, src_key = _parse_s3_uri(src_uri)
        dst_bucket, dst_key = _parse_s3_uri(dst_uri)
        client = self._get_client()
        try:
            client.copy_object(
                CopySource={'Bucket': src_bucket, 'Key': src_key},
                Bucket=dst_bucket,
                Key=dst_key,
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                raise FileNotFoundError(f'{src_uri} does not exist') from e
            raise

    def write_df_to_parquet(self, df: pd.DataFrame, s3_uri: str) -> None:
        """Write DataFrame as parquet to S3.

        Args:
            df: DataFrame to write
            s3_uri: S3 URI (e.g., 's3://bucket/path/file.parquet')

        Raises:
            ValueError: If URI does not end with .parquet
        """
        _, key = _parse_s3_uri(s3_uri)
        if not key.endswith('.parquet'):
            raise ValueError(f"S3 URI must end with .parquet, got: '{s3_uri}'")

        buffer = io.BytesIO()
        df.to_parquet(buffer, engine='pyarrow', index=False)
        buffer.seek(0)

        self.put_object(
            s3_uri,
            buffer.getvalue(),
            content_type='application/octet-stream',
        )

    def read_df_from_parquet(self, s3_uri: str) -> pd.DataFrame:
        """Read parquet file(s) from S3.

        Supports both single files and directories containing parquet files.

        Args:
            s3_uri: S3 URI. Can be:
                - A URI ending in .parquet (reads that exact file)
                - A prefix/directory URI (reads all .parquet files and concatenates)

        Returns:
            DataFrame with parquet contents
        """
        bucket, key = _parse_s3_uri(s3_uri)

        if key.endswith('.parquet'):
            # Single file read - use get_object
            data = self.get_object(s3_uri)
            return pd.read_parquet(io.BytesIO(data))

        # Key is a prefix - list all parquet files under it
        client = self._get_client()
        prefix = key.rstrip('/') + '/'
        paginator = client.get_paginator('list_objects_v2')
        parquet_keys = []

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                if obj['Key'].endswith('.parquet'):
                    parquet_keys.append(obj['Key'])

        if not parquet_keys:
            # Check if the prefix exists at all
            response = client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
            if not response.get('Contents'):
                raise FileNotFoundError(
                    f'{s3_uri} does not exist. '
                    f'For single files, use a URI ending in .parquet'
                )
            raise FileNotFoundError(f'{s3_uri} exists but contains no .parquet files')

        dfs = []
        for pq_key in parquet_keys:
            data = self.get_object(f's3://{bucket}/{pq_key}')
            dfs.append(pd.read_parquet(io.BytesIO(data)))

        return pd.concat(dfs, ignore_index=True)

    def file_exists(self, s3_uri: str) -> bool:
        """Check if object exists.

        Args:
            s3_uri: S3 URI (e.g., 's3://bucket/key')

        Returns:
            True if object exists
        """
        bucket, key = _parse_s3_uri(s3_uri)
        client = self._get_client()
        try:
            client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise

    def ls(
        self,
        s3_uri: str,
        *,
        recursive: bool = True,
        include_prefixes: bool = False,
    ) -> Iterator[S3Object]:
        """List objects at an S3 URI.

        Args:
            s3_uri: S3 URI (e.g., 's3://bucket' or 's3://bucket/prefix')
            recursive: If True, list all objects under prefix recursively.
                If False, list only files at the immediate level.
            include_prefixes: If True and recursive=False, also yield prefix
                (directory) entries with is_prefix=True in metadata.
                Ignored when recursive=True.

        Yields:
            S3Object instances with metadata for each file (and prefix if requested)
        """
        bucket, prefix = _parse_s3_uri(s3_uri)
        client = self._get_client()

        if recursive:
            paginator = client.get_paginator('list_objects_v2')
            paginate_params = {'Bucket': bucket}
            if prefix:
                paginate_params['Prefix'] = prefix

            for page in paginator.paginate(**paginate_params):
                for obj in page.get('Contents', []):
                    yield S3Object.from_boto_response(obj, bucket=bucket)
        else:
            # Non-recursive: use delimiter to get only immediate files/prefixes
            normalized_prefix = prefix.rstrip('/') + '/' if prefix else ''
            paginator = client.get_paginator('list_objects_v2')
            paginate_params = {
                'Bucket': bucket,
                'Prefix': normalized_prefix,
                'Delimiter': '/',
            }

            for page in paginator.paginate(**paginate_params):
                # Yield files at this level
                for obj in page.get('Contents', []):
                    yield S3Object.from_boto_response(obj, bucket=bucket)

                # Optionally yield prefixes (directories)
                if include_prefixes:
                    for prefix_entry in page.get('CommonPrefixes', []):
                        yield S3Object(
                            key=prefix_entry['Prefix'],
                            bucket=bucket,
                            metadata=S3ObjectMetadata(is_prefix=True),
                        )
