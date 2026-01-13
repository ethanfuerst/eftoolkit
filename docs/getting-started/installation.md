# Installation

## Requirements

- Python 3.10 or higher
- pip, uv, or another Python package manager

## Install from PyPI

!!! note "Not Yet Published"
    This package is not yet available on PyPI. Use the source installation method below.

Once published, install with:

=== "pip"

    ```bash
    pip install eftoolkit
    ```

=== "uv"

    ```bash
    uv add eftoolkit
    ```

## Install from Source

Clone the repository and install in development mode:

```bash
git clone https://github.com/ethanfuerst/eftoolkit.git
cd eftoolkit
```

=== "uv (recommended)"

    ```bash
    uv sync
    ```

=== "pip"

    ```bash
    pip install -e ".[dev]"
    ```

## Optional Dependencies

Install only the modules you need:

=== "SQL only (DuckDB)"

    ```bash
    pip install eftoolkit[sql-only]
    # Includes: pandas, duckdb
    ```

=== "S3 only"

    ```bash
    pip install eftoolkit[s3-only]
    # Includes: pandas, s3fs, pyarrow
    ```

## Verify Installation

```python
from eftoolkit import DuckDB, S3FileSystem, Spreadsheet
print("Installation successful!")
```

## Dependencies

The full package includes:

| Package | Version | Purpose |
|---------|---------|---------|
| pandas | >=2.0 | DataFrame operations |
| duckdb | >=1.0 | SQL queries |
| gspread | >=6.0 | Google Sheets API |
| s3fs | >=2024.0 | S3 filesystem |
| pyarrow | >=15.0 | Parquet support |
