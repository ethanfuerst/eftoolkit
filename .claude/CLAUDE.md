# eftoolkit

A personal Python toolkit providing reusable utilities for common tasks. Includes a DuckDB wrapper with S3 support, an S3 filesystem client, and a Google Sheets client with batching.

## Key directories/files

- `eftoolkit/` - Main package
  - `sql/duckdb.py` - `DuckDB` class: query, execute, S3 read/write
  - `s3/filesystem.py` - `S3FileSystem` class: parquet read/write, file operations
  - `gsheets/sheet.py` - `Spreadsheet` and `Worksheet` classes: worksheet operations with batching
  - `gsheets/runner.py` - `DashboardRunner`: 6-phase workflow orchestrator
  - `gsheets/registry.py` - `WorksheetRegistry`: worksheet definition registry
- `tests/` - pytest test suite
  - `conftest.py` - shared fixtures (sample DataFrames, mock S3)
- `pyproject.toml` - project metadata, dependencies, tool configs

## Setup

```bash
# Install with uv (preferred)
uv pip install -e ".[dev]"

# Or sync dependencies
uv sync
```

## Common commands

```bash
# Run linting and formatting
uv run pre-commit run --all-files

# Run tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=eftoolkit --cov-report=term-missing
```

## Package imports

```python
from eftoolkit.sql import DuckDB
from eftoolkit.s3 import S3FileSystem
from eftoolkit.gsheets import Spreadsheet, Worksheet, DashboardRunner, WorksheetRegistry
```

## Code style

- **Strings**: Prefer single quotes unless the string contains a single quote.
- **None checks**: Use `is` for None checks, `==` for value comparisons.
- **Naming**: `snake_case` for variables/functions, `PascalCase` for classes, `UPPER_SNAKE_CASE` for constants.
- **Interpolation**: Prefer f-strings.
- **Docstrings**: Write docstrings for public functions/classes.
- **Comments**: Only when explaining non-obvious intent or tricky logic. Avoid narrating obvious code.

## Testing conventions

- **Coverage**: 100% coverage on touched files.
- **Structure**: Organize by area (e.g., `tests/s3/`, `tests/gsheets/`, `tests/sql/`). Split by behavior when helpful (e.g., `test_write.py`, `test_read.py`).
- **No wrapper classes**: Use plain `test_*` functions, not `class TestFoo:`.
- **Tests as demos**: Write tests as small usage examples of the public API.
- **Construct in test**: Instantiate the class under test inside each test, not as a fixture.
- **Unique identifiers**: Each test should use its own unique paths/identifiers.
- **Fixtures**: Use for shared primitives (tmp_path, sample DataFrames, moto setup), not for the primary class under test.
- **Assertion spacing**: Place a blank line above the first `assert`. Group multiple asserts together without blank lines between them.

## Project-specific notes

- **Flat package layout**: Code lives directly in `eftoolkit/sql/`, `eftoolkit/s3/`, `eftoolkit/gsheets/`.
- **Pre-commit with ruff**: Uses ruff for linting and formatting.
- **S3FileSystem**: Uses `boto3`. Requires credentials (explicit args or env vars).
- **Spreadsheet**: Has local preview mode (`local_preview=True`) for development without API calls.
