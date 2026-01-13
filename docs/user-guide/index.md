# User Guide

This guide provides in-depth coverage of each eftoolkit module.

## Modules Overview

| Module | Primary Class | Description |
|--------|---------------|-------------|
| [`eftoolkit.sql`](duckdb.md) | `DuckDB` | SQL queries with DuckDB, S3 integration |
| [`eftoolkit.s3`](s3.md) | `S3FileSystem` | S3 operations for parquet files |
| [`eftoolkit.gsheets`](gsheets.md) | `Spreadsheet`, `Worksheet` | Google Sheets with batching |
| [`eftoolkit.config`](config.md) | Functions | JSON loading, logging setup |

## Design Principles

### Consistency

All modules follow similar patterns:

- Explicit credential/configuration in `__init__`
- Context manager support where appropriate
- DataFrame-centric data handling
- Clear error messages

### Minimal Abstraction

eftoolkit provides thin wrappers that:

- Don't hide the underlying libraries (DuckDB, gspread, boto3)
- Allow direct access to native APIs when needed
- Add convenience methods for common operations

### S3 URI Convention

S3 paths use the standard URI format throughout:

```
s3://bucket-name/path/to/file.parquet
```

Both `DuckDB` and `S3FileSystem` understand this format.

## Quick Links

- [DuckDB Wrapper](duckdb.md) - SQL queries, table creation, S3 integration
- [S3 Operations](s3.md) - Parquet read/write, file operations, listing
- [Google Sheets](gsheets.md) - Batched operations, formatting, local preview
- [Configuration](config.md) - JSON/JSONC loading, logging setup
