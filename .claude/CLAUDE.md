# eftoolkit

A personal Python toolkit providing reusable utilities for common tasks. Includes a DuckDB wrapper with S3 support, an S3 filesystem client, and a Google Sheets client with batching.

## Key directories/files

- `eftoolkit/` - Main package
  - `sql/duckdb.py` - `DuckDB` class: query, execute, S3 read/write
  - `s3/filesystem.py` - `S3FileSystem` class: parquet read/write, file operations
  - `gsheets/core/` - `Spreadsheet` and `Worksheet` classes: worksheet operations with batching
  - `gsheets/runner/` - `DashboardRunner`: 7-phase workflow orchestrator (phases 0–6)
  - `gsheets/runner/registry.py` - `WorksheetRegistry`: worksheet definition registry
  - `gsheets/runner/types/` - Type definitions for dashboard runner
  - `gsheets/utils.py` - JSON/JSONC config loading (file or env var), service account credential loading, cell parsing utilities
  - `utils.py` - General utilities (logging setup)
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

`eftoolkit/__init__.py` only exposes `__version__`. Always import classes/functions from their submodules:

```python
from eftoolkit import __version__
from eftoolkit.sql import DuckDB
from eftoolkit.s3 import S3FileSystem
from eftoolkit.gsheets import Spreadsheet, Worksheet
from eftoolkit.gsheets.runner import DashboardRunner, WorksheetRegistry
from eftoolkit.gsheets.utils import load_json_config, load_service_account_credentials, remove_comments
from eftoolkit.utils import setup_logging
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

- **Package layout**: `eftoolkit/sql/`, `eftoolkit/s3/`, `eftoolkit/gsheets/` (with `core/`, `runner/` subdirs).
- **Pre-commit with ruff**: Uses ruff for linting and formatting.
- **S3FileSystem**: Uses `boto3`. Requires credentials (explicit args or env vars).
- **Spreadsheet**: Has local preview mode (`local_preview=True`) for development without API calls.
- **Row/column indexing on `Worksheet`**: User-facing integer row/col params are **1-based** (`insert_rows`, `delete_rows`, `insert_columns`, `delete_columns`, `set_column_width(int)`, `auto_resize_columns`, `Worksheet.read(header_row=...)`). Outlier: `sort_range`'s sort-spec `'column'` key is 0-based. Internal types (`CellLocation.row`, `CellRange.start_row`) are 0-indexed and expose `*_1indexed` siblings for the Sheets API.
- **Read methods don't retry**: `Worksheet.read_cell` / `read_range` call `gspread` directly and bypass `_execute_with_retry` (unlike every write handler). A 429 on a read fails immediately.
- **`gspread.get_all_values()` returns all strings**: every cell value is `str`. `Worksheet.read()` accepts a `dtype` kwarg (scalar or per-column dict) that delegates to `DataFrame.astype` after construction.
- **Env-var credential loading**: `load_json_config(env='GSPREAD_CREDENTIALS')` and `load_service_account_credentials(env=...)` apply an idempotent `\n` → `\\n` fixup on the raw env-var string before `json.loads` so multi-line private keys pasted through UIs still parse. `Spreadsheet.__init__` still takes a dict; env-var integration on that API is tracked in ETH-436.
- **`DuckDB.__init__` does not read env vars** (unlike `S3FileSystem`). For env-var-driven auth, pass `s3=S3FileSystem()` — but note that only covers `read_parquet_from_s3` / `write_df_to_s3_parquet`, not native DuckDB SQL against `s3://` URIs. `AWS_DEFAULT_REGION` and `S3_URL_STYLE` are also not consulted today (follow-ups in the ETH-424 plan appendix).
- **Exception model**: no custom hierarchy. `eftoolkit` raises stdlib (`ValueError`, `FileNotFoundError`, …) and propagates `gspread.APIError` / `botocore.ClientError`. Transient-vs-permanent split and retry behavior are documented in `docs/user-guide/errors.md` — treat that page as authoritative when answering "what does X raise?".
