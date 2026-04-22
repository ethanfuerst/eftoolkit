# DuckDB Wrapper

The `DuckDB` class provides a thin wrapper around DuckDB with S3 integration.

## Overview

```python
from eftoolkit import DuckDB

db = DuckDB()  # In-memory database
df = db.query("SELECT 1 as value")
```

## Basic Usage

### Creating a Database

```python
# In-memory (default)
db = DuckDB()

# File-based (persistent)
db = DuckDB(database='my_data.duckdb')
```

### Running Queries

```python
# Query returns DataFrame
df = db.query("SELECT * FROM 'data.csv'")

# Execute for side effects (DDL, DML)
db.execute("CREATE TABLE test AS SELECT 1 as id")
```

### Table Operations

```python
# Create table from SQL
db.create_table('users', "SELECT 1 as id, 'Alice' as name")

# Create table from DataFrame
import pandas as pd
df = pd.DataFrame({'id': [1, 2], 'name': ['Alice', 'Bob']})
db.create_table_from_df('users', df)

# Get table as DataFrame with optional filter
users = db.get_table('users')
active_users = db.get_table('users', where="active = true")
```

## S3 Integration

### Constructor kwargs

All S3-related kwargs on `DuckDB(...)` are keyword-only.

| Kwarg | Default | Purpose |
|-------|---------|---------|
| `database` | `':memory:'` | DuckDB file path or in-memory sentinel. **Positional.** |
| `s3` | `None` | Existing `S3FileSystem` instance. Takes precedence over the `s3_*` kwargs. |
| `s3_access_key_id` | `None` | Plumbed to both the DuckDB native S3 layer (`CREATE SECRET`) and an internal `S3FileSystem`. |
| `s3_secret_access_key` | `None` | Same plumbing as `s3_access_key_id`. |
| `s3_region` | `None` | AWS region. |
| `s3_endpoint` | `None` | Custom S3 endpoint for non-AWS services (DO Spaces, R2, MinIO). |
| `s3_url_style` | `None` | `'path'` or `'vhost'`. Emitted as `SET s3_url_style='<v>'`. Not forwarded to `S3FileSystem`. |

If both `s3` and the credential kwargs are supplied, `s3` wins.

### Configuration

Provide S3 credentials at initialization:

```python
db = DuckDB(
    s3_access_key_id='AKIAIOSFODNN7EXAMPLE',
    s3_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    s3_region='us-east-1',
)
```

Or use an existing `S3FileSystem`:

```python
from eftoolkit import S3FileSystem

s3 = S3FileSystem(
    access_key_id='...',
    secret_access_key='...',
    region='us-east-1',
)

db = DuckDB(s3=s3)
```

### Reading from S3

```python
# Read parquet file
df = db.read_parquet_from_s3('s3://my-bucket/data.parquet')

# Or use native DuckDB syntax
df = db.query("SELECT * FROM 's3://my-bucket/data.parquet'")
```

### Writing to S3

```python
# Write DataFrame to S3
db.write_df_to_s3_parquet(df, 's3://my-bucket/output.parquet')

# Or use native DuckDB COPY command
db.execute("""
    COPY (SELECT * FROM my_table)
    TO 's3://my-bucket/output.parquet' (FORMAT PARQUET)
""")
```

!!! note "Return value"
    `write_df_to_s3_parquet` returns `None`. Row count is `len(df)`; the
    destination URI is the one you passed in.

### Custom Endpoints

For S3-compatible services (DigitalOcean Spaces, MinIO, etc.):

```python
db = DuckDB(
    s3_access_key_id='...',
    s3_secret_access_key='...',
    s3_region='nyc3',
    s3_endpoint='nyc3.digitaloceanspaces.com',
    s3_url_style='path',  # or 'vhost'
)
```

## Data Cleaning

The `get_table()` method automatically cleans `inf`, `-inf`, and `NaN` values:

```python
# Values are replaced with None
df = db.get_table('measurements')
# inf -> None, -inf -> None, NaN -> None
```

## Context Manager

Using `DuckDB` as a context manager opens a persistent connection that is reused for all operations within the block. This improves performance when running multiple queries:

```python
with DuckDB() as db:
    db.execute("CREATE TABLE t (x INT)")
    db.execute("INSERT INTO t VALUES (1), (2), (3)")
    result = db.query("SELECT SUM(x) as total FROM t")
# Connection closed automatically on exit
```

Without the context manager, each operation creates and closes its own connection:

```python
db = DuckDB()
db.query("SELECT 1")  # Opens connection, runs query, closes connection
db.query("SELECT 2")  # Opens new connection, runs query, closes connection
```

The context manager is especially useful for:

- Running multiple queries in sequence
- Creating tables and inserting data in an in-memory database
- Transactions that need to share state

## Accessing Native API

For operations not covered by the wrapper:

```python
# Get underlying DuckDB connection
conn = db.connection

# Use native DuckDB methods
conn.execute("INSTALL spatial")
conn.execute("LOAD spatial")
```

## Environment Variables

`DuckDB` falls back to environment variables when the `s3_*` credential
kwargs are omitted. Per field, precedence is:

**1. explicit `s3_*` kwarg** → **2. `S3_*` env var** → **3. `AWS_*` env var**

| `s3_*` kwarg | `S3_*` (primary) | `AWS_*` (fallback) |
|--------------|------------------|--------------------|
| `s3_access_key_id` | `S3_ACCESS_KEY_ID` | `AWS_ACCESS_KEY_ID` |
| `s3_secret_access_key` | `S3_SECRET_ACCESS_KEY` | `AWS_SECRET_ACCESS_KEY` |
| `s3_region` | `S3_REGION` | `AWS_REGION` |
| `s3_endpoint` | `S3_ENDPOINT` | _(no fallback)_ |

Resolved credentials apply to both the internal `S3FileSystem` (used by
`read_parquet_from_s3` / `write_df_to_s3_parquet`) and the native DuckDB
S3 layer (`INSTALL httpfs` + `CREATE SECRET`, used by raw `s3://` SQL).

```python
# All four fields resolved from the environment
db = DuckDB()

# Mix: explicit region, env-var credentials
db = DuckDB(s3_region='eu-west-1')
```

If neither kwargs nor env vars yield both `s3_access_key_id` and
`s3_secret_access_key`, `DuckDB` leaves S3 unconfigured — S3-touching
methods then raise `ValueError('S3 not configured')`.

!!! note "Not consulted"
    `AWS_DEFAULT_REGION`, `S3_URL_STYLE`, `AWS_SESSION_TOKEN`, and
    `AWS_PROFILE` are not read. See the
    [S3 env-var section](s3.md#environment-variables).

## See Also

- [S3 Operations](s3.md) - More S3 functionality
- [API Reference](../api/sql.md) - Full API documentation
