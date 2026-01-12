"""S3 filesystem utilities."""

import os

import pandas as pd
import s3fs


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

    def _get_fs(self):
        """Get s3fs filesystem instance."""
        endpoint_url = f'https://{self.endpoint}' if self.endpoint else None
        return s3fs.S3FileSystem(
            key=self.access_key_id,
            secret=self.secret_access_key,
            endpoint_url=endpoint_url,
            client_kwargs={'region_name': self.region} if self.region else None,
        )

    def write_df_to_parquet(self, df: pd.DataFrame, bucket: str, key: str) -> int:
        """Write DataFrame as parquet to s3://bucket/key.parquet.

        Args:
            df: DataFrame to write
            bucket: S3 bucket name
            key: Object key (without .parquet extension)

        Returns:
            Number of rows written
        """
        fs = self._get_fs()
        s3_path = f'{bucket}/{key}.parquet'

        with fs.open(s3_path, 'wb') as f:
            df.to_parquet(f, engine='pyarrow', index=False)

        return len(df)

    def read_df_from_parquet(self, bucket: str, key: str) -> pd.DataFrame:
        """Read parquet file(s) from S3.

        Supports both single files and directories containing parquet files.

        Args:
            bucket: S3 bucket name
            key: Object key. Can be:
                - A key without extension (reads bucket/key.parquet)
                - A key ending in .parquet (reads that exact file)
                - A prefix/directory containing .parquet files (reads all and concatenates)

        Returns:
            DataFrame with parquet contents
        """
        fs = self._get_fs()

        # Determine the S3 path to check
        if key.endswith('.parquet'):
            s3_path = f'{bucket}/{key}'
            # Check if it's a single file
            if fs.exists(s3_path) and fs.isfile(s3_path):
                with fs.open(s3_path, 'rb') as f:
                    return pd.read_parquet(f)
            raise FileNotFoundError(f's3://{s3_path} does not exist')

        # Key doesn't end with .parquet - must be a directory/prefix containing parquet files
        prefix_path = f'{bucket}/{key}'
        if not fs.exists(prefix_path):
            raise FileNotFoundError(
                f's3://{prefix_path} does not exist. '
                f'For single files, use a key ending in .parquet'
            )

        if not fs.isdir(prefix_path):
            raise FileNotFoundError(
                f's3://{prefix_path} exists but is not a directory. '
                f'For single files, use a key ending in .parquet'
            )

        parquet_files = [
            f for f in fs.ls(prefix_path, detail=False) if f.endswith('.parquet')
        ]
        if not parquet_files:
            raise FileNotFoundError(
                f's3://{prefix_path} exists but contains no .parquet files'
            )

        dfs = []
        for pf in parquet_files:
            with fs.open(pf, 'rb') as f:
                dfs.append(pd.read_parquet(f))
        return pd.concat(dfs, ignore_index=True)

    def file_exists(self, bucket: str, key: str) -> bool:
        """Check if object exists.

        Args:
            bucket: S3 bucket name
            key: Object key

        Returns:
            True if object exists
        """
        fs = self._get_fs()
        return fs.exists(f'{bucket}/{key}')

    def list_keys(self, bucket: str, prefix: str = '') -> list[str]:
        """List object keys with optional prefix.

        Args:
            bucket: S3 bucket name
            prefix: Key prefix to filter by

        Returns:
            List of object keys
        """
        fs = self._get_fs()
        path = f'{bucket}/{prefix}' if prefix else bucket
        return [key.replace(f'{bucket}/', '') for key in fs.ls(path)]
