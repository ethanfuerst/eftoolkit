# Quickstart

This guide walks through the core features of eftoolkit with practical examples.

## DuckDB: SQL Queries Made Easy

The `DuckDB` class provides a thin wrapper around DuckDB with S3 integration.

```python
from eftoolkit import DuckDB

# Create an in-memory database
db = DuckDB()

# Run SQL queries
df = db.query("SELECT 1 as id, 'Alice' as name UNION SELECT 2, 'Bob'")
print(df)
#    id   name
# 0   1  Alice
# 1   2    Bob

# Create tables from SQL
db.create_table('users', "SELECT 1 as id, 'Alice' as name")

# Get table as DataFrame
users = db.get_table('users')
```

### With S3 Integration

```python
db = DuckDB(
    s3_access_key_id='your-key',
    s3_secret_access_key='your-secret',
    s3_region='us-east-1',
)

# Read parquet from S3
df = db.read_parquet_from_s3('s3://my-bucket/data.parquet')

# Write DataFrame to S3
db.write_df_to_s3_parquet(df, 's3://my-bucket/output.parquet')
```

## S3FileSystem: Parquet Read/Write

The `S3FileSystem` class handles S3 operations for parquet files.

```python
from eftoolkit import S3FileSystem
import pandas as pd

s3 = S3FileSystem(
    access_key_id='your-key',
    secret_access_key='your-secret',
    region='us-east-1',
)

# Write DataFrame to S3
df = pd.DataFrame({'id': [1, 2], 'value': ['a', 'b']})
s3.write_df_to_parquet(df, 's3://my-bucket/data.parquet')

# Read from S3
df = s3.read_df_from_parquet('s3://my-bucket/data.parquet')

# List objects
for obj in s3.ls('s3://my-bucket/'):
    print(f"{obj.key}: {obj.metadata.size} bytes")

# Check if file exists
if s3.file_exists('s3://my-bucket/data.parquet'):
    print("File exists!")

# Copy, delete operations
s3.cp('s3://bucket/source.parquet', 's3://bucket/dest.parquet')
s3.delete_object('s3://bucket/old-file.parquet')
```

## Spreadsheet: Google Sheets with Batching

The `Spreadsheet` class provides efficient Google Sheets operations.

### Local Preview Mode (No Credentials)

Test your workflows without API credentials:

```python
from eftoolkit import Spreadsheet
import pandas as pd

# Local preview mode - generates HTML instead of API calls
ss = Spreadsheet(local_preview=True, spreadsheet_name='My Report')

with ss.worksheet('Data') as ws:
    df = pd.DataFrame({'Name': ['Alice', 'Bob'], 'Score': [95, 87]})
    ws.write_dataframe(df)
    ws.format_range('A1:B1', {'textFormat': {'bold': True}})
    ws.set_column_width('A', 150)

# Open the preview in browser
ws.open_preview()
```

### Live Mode (With Credentials)

```python
import json
from pathlib import Path

# Load service account credentials
credentials = json.loads(Path('credentials.json').read_text())

ss = Spreadsheet(
    credentials=credentials,
    spreadsheet_name='Production Report',
)

with ss.worksheet('Sheet1') as ws:
    # Read existing data
    df = ws.read()

    # Write new data
    ws.write_dataframe(df, location='A1')

    # Format cells
    ws.format_range('A1:C1', {
        'textFormat': {'bold': True},
        'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9},
    })

    # Operations are batched and flushed on context exit
```

## Configuration Utilities

### Load JSON Config (with JSONC support)

```python
from eftoolkit import load_json_config

# Supports standard JSON and JSONC (with comments)
config = load_json_config('config.jsonc')
```

Example `config.jsonc`:

```jsonc
{
  // Database settings
  "database": {
    "host": "localhost",
    "port": 5432
  },
  /*
   * Feature flags
   */
  "features": {
    "debug": true
  }
}
```

### Setup Logging

```python
from eftoolkit import setup_logging
import logging

# Configure root logger
setup_logging(level=logging.DEBUG)

# Now all loggers use this config
logger = logging.getLogger(__name__)
logger.info("Logging configured!")
```

## Next Steps

- [User Guide](../user-guide/index.md) - Deep dive into each module
- [How-To Guides](../how-to/index.md) - Common recipes and patterns
- [API Reference](../api/index.md) - Complete API documentation
