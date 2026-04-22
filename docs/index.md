# eftoolkit

A streamlined Python toolkit for everyday programming tasks and utilities.

## Overview

**eftoolkit** provides reusable utilities for common data engineering tasks:

- **DuckDB Wrapper** - Query data with SQL, seamlessly integrated with S3
- **S3 Operations** - Read/write parquet files to S3-compatible storage
- **Google Sheets** - Batch operations with automatic retry and local preview mode
- **Configuration** - JSON/JSONC loading and logging setup

## Key Features

- **Unified API** - Consistent patterns across all modules
- **S3 Integration** - DuckDB and S3FileSystem work together seamlessly
- **Batch Operations** - Google Sheets operations are queued and flushed efficiently
- **Local Preview** - Test Google Sheets workflows without API credentials
- **Type Hints** - Full type annotations for IDE support

## Quick Example

```python
from eftoolkit import DuckDB, S3FileSystem, Spreadsheet

# Query with DuckDB
db = DuckDB()
df = db.query("SELECT 1 as id, 'Hello' as message")

# Write to S3
s3 = S3FileSystem(access_key_id='...', secret_access_key='...', region='us-east-1')
s3.write_df_to_parquet(df, 's3://my-bucket/data.parquet')

# Write to Google Sheets (local preview - no credentials needed!)
ss = Spreadsheet(local_preview=True, spreadsheet_name='Demo')
with ss.worksheet('Sheet1') as ws:
    ws.write_dataframe(df)
    ws.format_range('A1:B1', {'textFormat': {'bold': True}})
```

## Getting Started

Ready to get started? Check out the [Installation](getting-started/installation.md) guide and [Quickstart](getting-started/quickstart.md) tutorial.

## Version policy

`eftoolkit` is classified **Alpha** (`Development Status :: 3 - Alpha` in
PyPI metadata). Versioning follows [Semantic Versioning](https://semver.org/):

- **Minor** bumps aim to be **additive** — new kwargs, methods, modules.
- **Breaking** changes ship with a **major** bump (the `1.4.0` → `2.0.0`
  jump that introduced `CellRange` is the reference case).
- **Patch** bumps are bug fixes and documentation.

Until the package reaches `3.0.0` / Beta status, no long-term API
compatibility guarantees apply. Suggested pins by appetite:

| Appetite | Pin | Effect |
|----------|-----|--------|
| Production-ish | `eftoolkit~=X.Y.0` | Patch-only updates. |
| Conservative | `eftoolkit>=X.Y,<NEXT_MAJOR` (e.g. `>=2.0,<3`) | Avoid the next major. |
| Bleeding edge | Track `main` | New features and occasional churn. |

See the [changelog](development/changelog.md) and
[GitHub releases](https://github.com/ethanfuerst/eftoolkit/releases) for
the breakage surface per version. (The hand-maintained changelog may lag
behind git tags.)
