# eftoolkit

A streamlined Python toolkit for everyday programming tasks and utilities.

## Status

**Work in Progress** - This package is under active development and **not yet published to PyPI**. APIs may change without notice.

## Installation

This package is not yet on PyPI. Install from source:

```bash
# Clone and install with dev dependencies
git clone https://github.com/ethanfuerst/eftoolkit.git
cd eftoolkit
uv sync
```

## Quick Start

```python
from eftoolkit.sql import DuckDB
from eftoolkit.s3 import S3FileSystem
from eftoolkit.gsheets import Spreadsheet

# DuckDB with in-memory database
db = DuckDB(database=':memory:')
df = db.query('SELECT 1 as num')

# S3 operations (requires credentials)
s3 = S3FileSystem(
    access_key_id='...',
    secret_access_key='...',
    region='us-east-1',
)
s3.write_df_to_parquet(df, 'my-bucket', 'data/output')

# Google Sheets (requires service account credentials)
spreadsheet = Spreadsheet(credentials={...}, spreadsheet_name='My Sheet')
ws = spreadsheet.worksheet('Sheet1')
ws.write_dataframe(df)
ws.flush()
```

## Development

```bash
# Install dev dependencies
uv sync

# Run linting and formatting
uv run pre-commit run --all-files

# Run tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=eftoolkit --cov-report=term-missing

# Coverage report
uv run coverage report -m
```
