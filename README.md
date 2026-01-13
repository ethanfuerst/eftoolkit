# eftoolkit

[![Documentation](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://ethanfuerst.github.io/eftoolkit/)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A streamlined Python toolkit for everyday programming tasks and utilities.

**[Documentation](https://ethanfuerst.github.io/eftoolkit/)** | [Installation](https://ethanfuerst.github.io/eftoolkit/getting-started/installation/) | [Quickstart](https://ethanfuerst.github.io/eftoolkit/getting-started/quickstart/)

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
db = DuckDB()
db.create_table('users', "SELECT 1 as id, 'Alice' as name")
df = db.get_table('users')

# S3 operations (requires credentials)
s3 = S3FileSystem(
    access_key_id='...',
    secret_access_key='...',
    region='us-east-1',
)
s3.write_df_to_parquet(df, 's3://my-bucket/data/output.parquet')

# Google Sheets (requires service account credentials)
ss = Spreadsheet(credentials={...}, spreadsheet_name='My Sheet')
with ss.worksheet('Sheet1') as ws:
    ws.write_dataframe(df)
    ws.format_range('A1:B1', {'textFormat': {'bold': True}})
    # flush() called automatically on exit

# Google Sheets local preview (no credentials needed!)
ss = Spreadsheet(local_preview=True, spreadsheet_name='Preview')
ws = ss.worksheet('Sheet1')
ws.write_dataframe(df)
ws.flush()
ws.open_preview()  # Opens HTML in browser
```

## Examples

See the [`examples/`](examples/) directory for detailed usage examples:

- **[`basic_duckdb.py`](examples/basic_duckdb.py)** - DuckDB queries, table creation, DataFrame integration
- **[`s3_operations.py`](examples/s3_operations.py)** - S3 read/write with moto mock (no credentials needed)
- **[`google_sheets.py`](examples/google_sheets.py)** - Google Sheets operations (uses local preview if no credentials)

Run an example:
```bash
uv run python examples/basic_duckdb.py
uv run python examples/s3_operations.py      # Uses moto mock
uv run python examples/google_sheets.py      # Local preview, or set GOOGLE_CREDENTIALS_PATH for live
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

## Project Structure

```
eftoolkit/
├── eftoolkit/          # Main package
│   ├── sql/            # DuckDB wrapper with S3 integration
│   ├── s3/             # S3FileSystem for parquet read/write
│   ├── gsheets/        # Google Sheets client with batching
│   └── config/         # Configuration utilities
├── examples/           # Usage examples (see above)
├── tests/              # pytest test suite
└── example_usage/      # Reference code from other projects (read-only)
```

### About `example_usage/`

The `example_usage/` directory contains reference code extracted from real projects
(`boxoffice_tracking`, `boxoffice_drafting`, `ynab_report`). This code demonstrates
production patterns that informed eftoolkit's design. It is **read-only reference
material** and excluded from linting/formatting. The patterns from these projects
are documented in `examples/` with cleaner, standalone examples.
