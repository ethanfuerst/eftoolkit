#!/usr/bin/env python3
"""S3 operations examples using moto mock.

This example demonstrates S3 file operations using eftoolkit's S3FileSystem
with moto to mock AWS S3. No real AWS credentials needed!

Run with: uv run python examples/s3_operations.py
"""

import boto3
import pandas as pd
from moto import mock_aws

from eftoolkit.s3 import S3FileSystem


@mock_aws
def main():
    """Demonstrate S3 operations with mocked AWS."""
    # Create mock S3 bucket
    conn = boto3.client('s3', region_name='us-east-1')
    conn.create_bucket(Bucket='demo-bucket')

    # Initialize S3FileSystem with mock credentials
    s3 = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    bucket = 'demo-bucket'
    base_path = 'examples/demo'

    # --- Write DataFrame to Parquet ---
    print('=== DataFrame to Parquet ===')
    df = pd.DataFrame(
        {
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'score': [95.5, 87.3, 92.1],
        }
    )
    s3.write_df_to_parquet(df, f's3://{bucket}/{base_path}/data.parquet')
    print(f'Wrote DataFrame to s3://{bucket}/{base_path}/data.parquet')

    # --- Read Parquet to DataFrame ---
    print('\n=== Parquet to DataFrame ===')
    df_read = s3.read_df_from_parquet(f's3://{bucket}/{base_path}/data.parquet')
    print('Read DataFrame:')
    print(df_read.to_string(index=False))

    # --- Raw Object Operations ---
    print('\n=== Raw Object Operations ===')

    # Upload raw bytes
    s3.put_object(
        f's3://{bucket}/{base_path}/config.json',
        b'{"version": "1.0"}',
        content_type='application/json',
    )
    print('Uploaded config.json')

    # Download raw bytes
    data = s3.get_object(f's3://{bucket}/{base_path}/config.json')
    print(f'Downloaded config.json: {data.decode("utf-8")}')

    # Copy object
    s3.cp(
        f's3://{bucket}/{base_path}/config.json',
        f's3://{bucket}/{base_path}/config_backup.json',
    )
    print('Copied config.json to config_backup.json')

    # --- List Objects ---
    print('\n=== List Objects ===')
    print(f'Objects in s3://{bucket}/{base_path}/:')
    for obj in s3.ls(f's3://{bucket}/{base_path}/'):
        print(f'  {obj.key} - {obj.size} bytes')

    # Check if object exists
    print('\n=== Check Existence ===')
    exists = s3.file_exists(f's3://{bucket}/{base_path}/data.parquet')
    print(f'data.parquet exists: {exists}')

    missing = s3.file_exists(f's3://{bucket}/{base_path}/missing.parquet')
    print(f'missing.parquet exists: {missing}')

    # --- Delete object ---
    print('\n=== Delete Object ===')
    s3.delete_object(f's3://{bucket}/{base_path}/config_backup.json')
    print('Deleted config_backup.json')

    # Verify deletion
    print('\nRemaining objects:')
    for obj in s3.ls(f's3://{bucket}/{base_path}/'):
        print(f'  {obj.key}')

    print('\n=== Done! ===')


if __name__ == '__main__':
    main()
