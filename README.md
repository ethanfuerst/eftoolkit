# eftoolkit

[![PyPI version](https://img.shields.io/pypi/v/eftoolkit.svg)](https://pypi.org/project/eftoolkit/)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Documentation](https://img.shields.io/badge/docs-online-blue)](https://tidbitstatistics.com/eftoolkit/)

A streamlined Python toolkit for everyday programming tasks and utilities.

**[Documentation](https://tidbitstatistics.com/eftoolkit/)** | [Installation](https://tidbitstatistics.com/eftoolkit/getting-started/installation/) | [Quickstart](https://tidbitstatistics.com/eftoolkit/getting-started/quickstart/)

## Installation

```bash
uv add eftoolkit
```

Or with pip:

```bash
pip install eftoolkit
```

For development:

```bash
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

# Build documentation locally
uv run mkdocs serve
```

## Releasing (Maintainers Only)

Releases are automated via the release script. You must be on the `main` branch with no uncommitted changes:

```bash
./scripts/release.sh patch  # 0.1.0 -> 0.1.1
./scripts/release.sh minor  # 0.1.0 -> 0.2.0
./scripts/release.sh major  # 0.1.0 -> 1.0.0
```

This runs all checks, auto-bumps the version, generates release notes, and triggers the PyPI publish workflow.

## Project Structure

```
eftoolkit/
├── eftoolkit/          # Main package
│   ├── sql/            # DuckDB wrapper with S3 integration
│   ├── s3/             # S3FileSystem for parquet read/write
│   ├── gsheets/        # Google Sheets client with batching
│   └── config/         # Configuration utilities
├── docs/               # Documentation (MkDocs)
└── tests/              # pytest test suite
```
