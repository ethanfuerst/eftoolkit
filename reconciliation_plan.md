# Reconciliation Plan: `example_usage/` → `eftoolkit/`

## 1. Inventory Table

| Project | File | Utilities Found | Category | Notes |
|---------|------|-----------------|----------|-------|
| **boxoffice_drafting** | `db_connection.py` | `DuckDBConnection`, `duckdb_connection()` context manager | sql-duckdb | Uses config dict for S3 creds; hardcodes endpoint/region constants; has `df()` method |
| | `format.py` | `remove_comments()`, `load_format_config()` | other | JSON config loader with `_comment` key stripping |
| | `gspread_format.py` | `df_to_sheet()` | gsheets | Simple DataFrame→sheet write + format dict application |
| | `gsheet.py` | `GoogleSheetDashboard`, `update_dashboard()`, many helpers | gsheets | Complex dashboard builder; uses formatting, notes, conditional formatting |
| | `query.py` | `table_to_df()` | sql-duckdb | Query table→DataFrame with column renaming and inf/nan cleaning |
| | `logging_config.py` | `setup_logging()` | other | Basic logging.basicConfig wrapper |
| **boxoffice_tracking** | `s3_utils.py` | `load_df_to_s3_parquet()`, `load_duckdb_table_to_s3_parquet()` | s3 | Uses `s3fs` library directly (not DuckDB httpfs); reads env vars |
| | `logging_config.py` | `setup_logging()` | other | Identical to other projects |
| **ynab_report** | `db_connection.py` | `DuckDBConnection` | sql-duckdb | Hardcoded paths/env vars; READ vs WRITE secret distinction |
| | `s3_utils.py` | `load_df_to_s3_table()` | s3 | Uses DuckDB httpfs for S3 write; returns row count |
| | `utils.py` | `get_df_from_table()` | sql-duckdb | Simple table query helper with optional WHERE clause |
| | `batcher.py` | `SheetBatcher` | gsheets | **Most sophisticated**: batches API calls, retry with exponential backoff, queues values/formats/borders/widths/notes |
| | `refresh_sheets.py` | `create_worksheet()`, `queue_df_to_sheet()`, `queue_*` helpers | gsheets | High-level dashboard refresh using SheetBatcher |
| | `logging_config.py` | `setup_logging()` | other | Identical to other projects |

### Key Differences by Category

**sql-duckdb:**

| Aspect | boxoffice_drafting | ynab_report | Current eftoolkit |
|--------|-------------------|-------------|-------------------|
| DB path | Config dict + project_root | Hardcoded project_root | Explicit `database` arg ✓ |
| S3 creds | Config dict → env vars | Hardcoded env var names | Explicit args ✓ |
| Endpoint | Constants (DigitalOcean) | Hardcoded DigitalOcean | Explicit arg ✓ |
| Context manager | `duckdb_connection()` | None (manual close) | `_get_connection()` internal |
| `df()` method | Yes | Yes | No (uses `query()` → `fetchdf()`) |
| inf/nan cleaning | In `table_to_df()` | In `get_df_from_table()` | In `_clean_df()` ✓ |

**s3:**

| Aspect | boxoffice_tracking | ynab_report | Current eftoolkit |
|--------|-------------------|-------------|-------------------|
| Library | `s3fs` + pandas | DuckDB httpfs | DuckDB httpfs ✓ |
| Returns | Row count | Row count | None (logs only) |
| Bucket default | Env var fallback | Explicit arg | Explicit arg ✓ |

**gsheets:**

| Aspect | boxoffice_drafting | ynab_report | Current eftoolkit |
|--------|-------------------|-------------|-------------------|
| Auth | Env var → JSON parse | Env var → JSON parse | Explicit dict arg ✓ |
| Batching | None | `SheetBatcher` class | None |
| Retry logic | None | Exponential backoff (429, 5xx) | None |
| Format helpers | `df_to_sheet()` | `queue_*` functions | None |
| Border/notes | Manual in gsheet.py | `queue_border()`, `queue_notes()` | None |

---

## 2. Proposed Unified Public API

### `eftoolkit.s3.S3FileSystem`

**Note:** This is the foundational S3 module. `DuckDB` uses it internally for S3 operations.

```python
class S3FileSystem:
    def __init__(
        self,
        *,
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
        region: str | None = None,
        endpoint: str | None = None,  # e.g., 'nyc3.digitaloceanspaces.com'
    ) -> None:
        '''Initialize S3 filesystem. Falls back to env vars if args are None:
          - S3_ACCESS_KEY_ID / AWS_ACCESS_KEY_ID
          - S3_SECRET_ACCESS_KEY / AWS_SECRET_ACCESS_KEY
          - S3_REGION / AWS_REGION
          - S3_ENDPOINT
        '''

    def write_df_to_parquet(self, df: pd.DataFrame, bucket: str, key: str) -> int:
        '''Write DataFrame as parquet to s3://bucket/key.parquet. Returns row count.'''

    def read_df_from_parquet(self, bucket: str, key: str) -> pd.DataFrame:
        '''Read parquet from s3://bucket/key.parquet.'''

    def file_exists(self, bucket: str, key: str) -> bool:
        '''Check if object exists.'''

    def list_keys(self, bucket: str, prefix: str = '') -> list[str]:
        '''List object keys with optional prefix.'''
```

**Error behavior:**
- Raises `ValueError` if credentials not available (neither args nor env)
- Raises `FileNotFoundError` on read of nonexistent key
- Raises `botocore.exceptions.ClientError` for S3 API errors

---

### `eftoolkit.sql.DuckDB`

**Design principle:** Wrap `duckdb.DuckDBPyConnection` to inherit the native DuckDB Python API. S3 operations use `S3FileSystem` internally for a unified credential/storage layer.

```python
class DuckDB:
    '''Thin wrapper around duckdb.DuckDBPyConnection with S3 integration.

    Inherits all native DuckDB methods (query, execute, sql, fetchone, fetchall, etc.)
    via delegation to the underlying connection.

    S3 operations use eftoolkit.s3.S3FileSystem internally.
    '''

    def __init__(
        self,
        database: str = ':memory:',
        *,
        s3: S3FileSystem | None = None,  # Pass existing S3FileSystem, or...
        # ...configure S3 inline (creates S3FileSystem internally):
        s3_region: str | None = None,
        s3_access_key_id: str | None = None,
        s3_secret_access_key: str | None = None,
        s3_endpoint: str | None = None,
        s3_url_style: str | None = None,  # 'path' or 'vhost'
    ) -> None:
        '''Initialize DuckDB with optional S3 integration.

        Option 1: Pass an existing S3FileSystem via `s3=` parameter.
        Option 2: Pass S3 credentials directly (creates S3FileSystem internally).
        Option 3: No S3 (S3 methods will raise if called).
        '''

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        '''Underlying DuckDB connection (for direct access to native API).'''

    @property
    def s3(self) -> S3FileSystem | None:
        '''S3FileSystem instance used for S3 operations, or None if not configured.'''

    # --- Delegated native methods (examples, not exhaustive) ---
    def query(self, sql: str) -> duckdb.DuckDBPyRelation: ...
    def execute(self, sql: str, *args, **kwargs) -> duckdb.DuckDBPyConnection: ...
    def sql(self, sql: str) -> duckdb.DuckDBPyRelation: ...
    def fetchone(self) -> tuple | None: ...
    def fetchall(self) -> list[tuple]: ...
    def fetchdf(self) -> pd.DataFrame: ...

    # --- Custom convenience methods (not in native API) ---
    def get_table(self, table_name: str, where: str | None = None) -> pd.DataFrame:
        '''SELECT * FROM table with optional WHERE clause. Cleans inf/nan → None.'''

    def create_table_from_df(self, table_name: str, df: pd.DataFrame) -> None:
        '''CREATE OR REPLACE TABLE from DataFrame.'''

    # --- S3 operations (use self.s3 internally) ---
    def read_parquet_from_s3(self, bucket: str, key: str) -> pd.DataFrame:
        '''Read parquet from S3. Delegates to self.s3.read_df_from_parquet().'''

    def write_df_to_s3_parquet(self, df: pd.DataFrame, bucket: str, key: str) -> int:
        '''Write DataFrame to S3 as parquet. Delegates to self.s3.write_df_to_parquet().'''

    # --- Context manager ---
    def __enter__(self) -> 'DuckDB': ...
    def __exit__(self, *args) -> None: ...
    def close(self) -> None: ...
```

**Error behavior:**
- Native methods raise `duckdb.Error` per DuckDB behavior
- S3 methods raise `ValueError` if `self.s3` is None
- S3 methods propagate errors from `S3FileSystem`

---

### `eftoolkit.gsheets.GoogleSheet`

**Design principle:** Single class for all spreadsheet operations. Batching is internal—all write methods queue operations and flush automatically or on `flush()`. Supports local preview mode for development.

```python
class GoogleSheet:
    '''Google Sheets client with automatic batching, retry logic, and local preview.

    All write operations are queued internally and flushed:
    - Automatically when exiting context manager
    - Explicitly via flush()
    - Automatically when queue reaches batch limits

    Local preview mode (local_preview=True):
    - Skips Google Sheets API calls
    - Renders queued operations to a local HTML file
    - Opens in browser for visual inspection
    - Useful for development/testing without API quota usage
    '''

    def __init__(
        self,
        credentials: dict | None = None,  # Required unless local_preview=True
        spreadsheet_name: str = '',
        *,
        max_retries: int = 5,
        base_delay: float = 2.0,
        local_preview: bool = False,
        preview_output: str | Path = 'sheet_preview.html',
    ) -> None:
        '''Initialize Google Sheets client.

        Args:
            credentials: Service account credentials dict. Required unless local_preview=True.
            spreadsheet_name: Name of the spreadsheet to open.
            max_retries: Max retry attempts for API errors (429, 5xx).
            base_delay: Base delay for exponential backoff.
            local_preview: If True, skip API calls and render to local HTML instead.
            preview_output: Path for HTML preview file (only used if local_preview=True).
        '''

    def __enter__(self) -> 'GoogleSheet': ...
    def __exit__(self, *args) -> None:
        '''Flush queued operations on clean exit.'''

    @property
    def spreadsheet(self) -> gspread.Spreadsheet | None:
        '''Underlying gspread Spreadsheet object (None if local_preview=True).'''

    @property
    def is_local_preview(self) -> bool:
        '''True if running in local preview mode.'''

    # --- Worksheet management ---
    def get_worksheet(self, name: str) -> gspread.Worksheet:
        '''Get worksheet by name. Raises WorksheetNotFound if missing.'''

    def create_worksheet(
        self, name: str, rows: int, cols: int, *, replace: bool = False
    ) -> gspread.Worksheet:
        '''Create worksheet. If replace=True, deletes existing first.'''

    def delete_worksheet(self, name: str, *, ignore_missing: bool = True) -> None:
        '''Delete worksheet by name.'''

    # --- Read operations (immediate, not batched) ---
    def read_worksheet(self, name: str) -> pd.DataFrame:
        '''Read worksheet to DataFrame (first row = headers).'''

    # --- Write operations (queued, batched) ---
    def write_dataframe(
        self,
        df: pd.DataFrame,
        worksheet: gspread.Worksheet | str,
        location: str = 'A1',
        *,
        include_header: bool = True,
        format_dict: dict[str, Any] | None = None,
    ) -> None:
        '''Queue DataFrame write with optional formatting.'''

    def write_values(
        self,
        range_name: str,  # e.g., 'Sheet1!A1'
        values: list[list[Any]],
    ) -> None:
        '''Queue cell values update.'''

    def format_range(
        self,
        range_name: str,
        format_dict: dict[str, Any],
        worksheet: gspread.Worksheet | str,
    ) -> None:
        '''Queue cell formatting.'''

    def set_borders(
        self,
        range_name: str,
        borders: dict[str, Any],  # keys: top, bottom, left, right
        worksheet: gspread.Worksheet | str,
    ) -> None:
        '''Queue border formatting.'''

    def set_column_width(
        self,
        column: str | int,
        width: int,
        worksheet: gspread.Worksheet | str,
    ) -> None:
        '''Queue column width update.'''

    def auto_resize_columns(
        self,
        start_col: int,
        end_col: int,
        worksheet: gspread.Worksheet | str,
    ) -> None:
        '''Queue column auto-resize. Indices are 1-based.'''

    def set_notes(
        self,
        notes: dict[str, str],  # cell -> note text
        worksheet: gspread.Worksheet | str,
    ) -> None:
        '''Queue cell notes.'''

    # --- Batch control ---
    def flush(self) -> None:
        '''Execute all queued operations.

        In normal mode: sends batched API calls to Google Sheets.
        In local_preview mode: renders HTML and opens in browser.
        '''

    def open_preview(self) -> None:
        '''Open the preview HTML in browser (local_preview mode only).'''
```

**Local preview implementation notes:**
- Generates a simple HTML table representation of the sheet
- Shows cell values, basic formatting (bold, colors), column widths
- Borders and notes rendered as CSS/tooltips
- Auto-opens in default browser on `flush()` (can be disabled)
- Useful for CI/testing: set `local_preview=True` to avoid API calls

**Error behavior:**
- Constructor raises `gspread.exceptions.SpreadsheetNotFound` if spreadsheet doesn't exist (unless local_preview)
- Constructor raises `ValueError` if credentials missing and local_preview=False
- Operations raise `gspread.exceptions.APIError` after exhausting retries
- Context manager flushes on clean exit, skips flush on exception

---

### `eftoolkit.config` (optional utility)

```python
def load_json_config(path: str | Path) -> dict:
    '''Load JSON file, stripping keys starting with "_comment".'''

def setup_logging(
    level: int = logging.INFO,
    format: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
) -> None:
    '''Configure basic logging.'''
```

---

## 3. Proposed Package Layout + Dependencies

### Directory Structure

```
eftoolkit/
├── __init__.py              # Re-exports: DuckDB, GoogleSheet, S3FileSystem
├── sql/
│   ├── __init__.py          # from .duckdb import DuckDB
│   └── duckdb.py            # DuckDB class
├── gsheets/
│   ├── __init__.py          # from .sheet import GoogleSheet
│   ├── sheet.py             # GoogleSheet class (batching internal)
│   └── preview.py           # Local HTML preview renderer
├── s3/
│   ├── __init__.py          # from .filesystem import S3FileSystem
│   └── filesystem.py        # S3FileSystem class
└── config.py                # load_json_config(), setup_logging()

tests/
├── conftest.py
├── sql/
│   └── test_duckdb.py
├── gsheets/
│   ├── test_sheet.py        # Mock gspread
│   └── test_preview.py      # Test local preview rendering
├── s3/
│   └── test_filesystem.py   # Use moto
└── test_config.py
```

### Dependencies

**pyproject.toml structure:**

Default install includes all dependencies (like sqlmesh). Extras allow minimal installs.

```toml
[project]
dependencies = [
    "pandas>=2.0",
    "duckdb>=1.0",
    "gspread>=6.0",
    "s3fs>=2024.0",
    "pyarrow>=15.0",
]

[project.optional-dependencies]
# Minimal extras for users who want subset
sql-only = [
    "pandas>=2.0",
    "duckdb>=1.0",
]
gsheets-only = [
    "pandas>=2.0",
    "gspread>=6.0",
]
s3-only = [
    "pandas>=2.0",
    "s3fs>=2024.0",
    "pyarrow>=15.0",
]
dev = [
    "pytest>=8.0",
    "pytest-cov>=5.0",
    "black",
    "isort",
    "moto[s3]>=4.0",
]
```

**Usage examples:**
```bash
pip install eftoolkit              # Everything (default)
pip install eftoolkit[sql-only]    # Just DuckDB + pandas
pip install eftoolkit[gsheets-only] # Just gspread + pandas
```

### Auth/Credentials Handling

| Module | Explicit Args | Env Var Fallback |
|--------|--------------|------------------|
| `S3FileSystem` | `access_key_id`, `secret_access_key`, `region`, `endpoint` | `AWS_ACCESS_KEY_ID` / `S3_ACCESS_KEY_ID`, etc. |
| `DuckDB` | `s3=S3FileSystem(...)` or inline S3 args | Via `S3FileSystem` fallback |
| `GoogleSheet` | `credentials` (dict, required unless local_preview) | None - must be explicit |

---

## 4. Implementation Order

1. **`eftoolkit/s3/filesystem.py`** - New (moved up: DuckDB depends on it)
   - Implement using `s3fs`
   - Methods: `write_df_to_parquet`, `read_df_from_parquet`, `file_exists`, `list_keys`
   - Env var fallback
   - **Tests:** Use moto for mocking

2. **`eftoolkit/sql/duckdb.py`** - Refactor existing
   - Wrap `duckdb.DuckDBPyConnection` instead of reimplementing methods
   - Accept `s3: S3FileSystem` parameter or inline S3 args
   - S3 methods delegate to `self.s3`
   - Keep custom helpers: `get_table`, `create_table_from_df`
   - **Tests:** Extend existing; test S3 integration with mocked S3FileSystem

3. **`eftoolkit/config.py`** - New, simple
   - `load_json_config()` from boxoffice_drafting/format.py
   - `setup_logging()` from logging_config.py
   - **Tests:** Unit tests for comment stripping

4. **`eftoolkit/gsheets/sheet.py`** - Replace existing
   - Single `GoogleSheet` class with internal batching
   - Port retry logic from ynab_report/batcher.py
   - All write methods queue internally, flush on context exit
   - **Tests:** Mock gspread API calls

5. **`eftoolkit/gsheets/preview.py`** - New
   - HTML renderer for local preview mode
   - Generates table with values, formatting, borders
   - Opens in browser on flush
   - **Tests:** Verify HTML output structure

6. **Update `__init__.py` files and pyproject.toml**
   - Default deps = all; add `-only` extras for minimal installs
   - Re-export public API

7. **Delete `example_usage/` folders as reconciled**
   - Remove each project folder after its patterns are absorbed

---

### Priority Phases

- **Phase 1 (storage):** Steps 1-2 — S3FileSystem + DuckDB refactor (S3 is dependency)
- **Phase 2 (utils):** Step 3 — Config utilities
- **Phase 3 (sheets):** Steps 4-5 — GoogleSheet with batching + local preview
- **Phase 4 (cleanup):** Steps 6-7 — Package finalization
