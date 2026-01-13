# How-To Guides

Practical recipes for common tasks with eftoolkit.

## Data Pipeline Patterns

### ETL: S3 → DuckDB → Google Sheets

```python
from eftoolkit import DuckDB, S3FileSystem, Spreadsheet

# Extract from S3
s3 = S3FileSystem(
    access_key_id='...',
    secret_access_key='...',
    region='us-east-1',
)
raw_df = s3.read_df_from_parquet('s3://data-lake/raw/sales.parquet')

# Transform with DuckDB
db = DuckDB()
db.create_table_from_df('sales', raw_df)
summary = db.query("""
    SELECT
        region,
        SUM(amount) as total_sales,
        COUNT(*) as num_orders
    FROM sales
    GROUP BY region
    ORDER BY total_sales DESC
""")

# Load to Google Sheets
ss = Spreadsheet(credentials={...}, spreadsheet_name='Sales Report')
with ss.worksheet('Summary') as ws:
    ws.write_dataframe(summary)
    ws.format_range('A1:C1', {'textFormat': {'bold': True}})
    ws.format_range('B2:B100', {
        'numberFormat': {'type': 'CURRENCY', 'pattern': '$#,##0'}
    })
```

### Backup Google Sheet to S3

```python
from eftoolkit import S3FileSystem, Spreadsheet
from datetime import datetime

ss = Spreadsheet(credentials={...}, spreadsheet_name='Important Data')
s3 = S3FileSystem(access_key_id='...', secret_access_key='...', region='us-east-1')

# Read from Google Sheets
with ss.worksheet('Sheet1') as ws:
    df = ws.read()

# Write to S3 with timestamp
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
s3.write_df_to_parquet(df, f's3://backups/sheets/data_{timestamp}.parquet')
```

## S3 Patterns

### Process All Files in a Prefix

```python
from eftoolkit import S3FileSystem
import pandas as pd

s3 = S3FileSystem(access_key_id='...', secret_access_key='...', region='us-east-1')

all_dfs = []
for obj in s3.ls('s3://my-bucket/data/2024/'):
    if obj.key.endswith('.parquet'):
        df = s3.read_df_from_parquet(obj.uri)
        all_dfs.append(df)

combined = pd.concat(all_dfs, ignore_index=True)
```

### Copy Files Between Buckets

```python
from eftoolkit import S3FileSystem

s3 = S3FileSystem(access_key_id='...', secret_access_key='...', region='us-east-1')

# Copy all parquet files from one bucket to another
for obj in s3.ls('s3://source-bucket/data/'):
    if obj.key.endswith('.parquet'):
        dest_uri = f's3://dest-bucket/{obj.key}'
        s3.cp(obj.uri, dest_uri)
        print(f"Copied {obj.key}")
```

### Clean Up Old Files

```python
from eftoolkit import S3FileSystem
from datetime import datetime, timedelta

s3 = S3FileSystem(access_key_id='...', secret_access_key='...', region='us-east-1')
cutoff = datetime.now() - timedelta(days=30)

for obj in s3.ls('s3://my-bucket/temp/'):
    if obj.metadata.last_modified_timestamp_utc < cutoff:
        s3.delete_object(obj.uri)
        print(f"Deleted {obj.key}")
```

## DuckDB Patterns

### Query CSV and Parquet Directly

```python
from eftoolkit import DuckDB

db = DuckDB()

# Query CSV
df = db.query("SELECT * FROM 'data.csv' WHERE value > 100")

# Query Parquet
df = db.query("SELECT * FROM 'data/*.parquet'")

# Join across formats
df = db.query("""
    SELECT a.*, b.name
    FROM 'orders.parquet' a
    JOIN 'customers.csv' b ON a.customer_id = b.id
""")
```

### Incremental Processing

```python
from eftoolkit import DuckDB

db = DuckDB(database='pipeline.duckdb')

# Track last processed ID
last_id = db.query("SELECT COALESCE(MAX(id), 0) as last FROM processed").iloc[0]['last']

# Process new records
new_records = db.query(f"""
    SELECT * FROM 's3://bucket/incoming/*.parquet'
    WHERE id > {last_id}
""")

if not new_records.empty:
    db.create_table_from_df('new_batch', new_records)
    db.execute("""
        INSERT INTO processed
        SELECT * FROM new_batch
    """)
```

## Google Sheets Patterns

### Format Report with Conditional Colors

```python
from eftoolkit import Spreadsheet
import pandas as pd

ss = Spreadsheet(credentials={...}, spreadsheet_name='Performance Report')

with ss.worksheet('Metrics') as ws:
    df = pd.DataFrame({
        'Metric': ['Revenue', 'Costs', 'Profit'],
        'Actual': [150000, 80000, 70000],
        'Target': [140000, 85000, 55000],
    })
    ws.write_dataframe(df)

    # Bold headers
    ws.format_range('A1:C1', {'textFormat': {'bold': True}})

    # Currency format
    ws.format_range('B2:C4', {
        'numberFormat': {'type': 'CURRENCY', 'pattern': '$#,##0'}
    })

    # Green for exceeding target
    ws.add_conditional_format('B2:B4', {
        'type': 'CUSTOM_FORMULA',
        'values': ['=B2>C2'],
        'format': {'backgroundColor': {'red': 0.8, 'green': 1, 'blue': 0.8}},
    })

    # Red for missing target
    ws.add_conditional_format('B2:B4', {
        'type': 'CUSTOM_FORMULA',
        'values': ['=B2<C2'],
        'format': {'backgroundColor': {'red': 1, 'green': 0.8, 'blue': 0.8}},
    })
```

### Build Multiple Tabs

```python
from eftoolkit import Spreadsheet

ss = Spreadsheet(credentials={...}, spreadsheet_name='Monthly Report')

# Summary tab
with ss.worksheet('Summary') as ws:
    ws.write_dataframe(summary_df)

# Regional breakdowns
for region in ['North', 'South', 'East', 'West']:
    ws = ss.create_worksheet(region, replace=True)
    region_df = full_df[full_df['region'] == region]
    ws.write_dataframe(region_df)
    ws.flush()
```

### Preview Before Live Update

```python
from eftoolkit import Spreadsheet

# First, test with local preview
ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
with ss.worksheet('Data') as ws:
    ws.write_dataframe(df)
    ws.format_range('A1:C1', {'textFormat': {'bold': True}})
ws.open_preview()  # Review in browser

# If preview looks good, switch to live mode
# (Comment out local_preview=True and add credentials)
```

## Testing Patterns

### Mock S3 with moto

```python
import pytest
from moto import mock_aws
import boto3
from eftoolkit import S3FileSystem

@pytest.fixture
def mock_s3_bucket():
    with mock_aws():
        conn = boto3.client('s3', region_name='us-east-1')
        conn.create_bucket(Bucket='test-bucket')
        yield 'test-bucket'

def test_s3_operations(mock_s3_bucket):
    s3 = S3FileSystem(
        access_key_id='testing',
        secret_access_key='testing',
        region='us-east-1',
    )

    df = pd.DataFrame({'id': [1, 2]})
    s3.write_df_to_parquet(df, f's3://{mock_s3_bucket}/test.parquet')

    result = s3.read_df_from_parquet(f's3://{mock_s3_bucket}/test.parquet')
    assert len(result) == 2
```

### Test Google Sheets with Local Preview

```python
def test_report_generation():
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test Report')

    with ss.worksheet('Data') as ws:
        df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
        ws.write_dataframe(df)
        ws.format_range('A1:B1', {'textFormat': {'bold': True}})

    # Assertions on the worksheet state
    assert ws.is_local_preview
    assert 'Local Preview' in ws.title
```
