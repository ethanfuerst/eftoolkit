# Google Sheets

The `Spreadsheet` and `Worksheet` classes provide efficient Google Sheets operations with automatic batching.

## Overview

```python
from eftoolkit.gsheets import Spreadsheet

# Local preview (no credentials)
ss = Spreadsheet(local_preview=True, spreadsheet_name='My Sheet')

# Live mode (with credentials)
ss = Spreadsheet(credentials={...}, spreadsheet_name='My Sheet')
```

## Local Preview Mode

Test your workflows without API credentials:

```python
import pandas as pd
from eftoolkit.gsheets import Spreadsheet

ss = Spreadsheet(local_preview=True, spreadsheet_name='Report')

with ss.worksheet('Data') as ws:
    df = pd.DataFrame({'Name': ['Alice', 'Bob'], 'Score': [95, 87]})
    ws.write_dataframe(df)
    ws.format_range('A1:B1', {'textFormat': {'bold': True}})

# Open HTML preview in browser
ws.open_preview()
```

Preview files are saved to `gsheets_preview/` by default.

## Live Mode

### Setup Credentials

1. Create a Google Cloud project
2. Enable the Google Sheets API
3. Create a service account
4. Download the JSON credentials
5. Share your spreadsheet with the service account email

### Connect to Spreadsheet

```python
import json
from pathlib import Path
from eftoolkit.gsheets import Spreadsheet

credentials = json.loads(Path('credentials.json').read_text())

ss = Spreadsheet(
    credentials=credentials,
    spreadsheet_name='Production Report',
)
```

## Worksheet Operations

### Read Data

```python
with ss.worksheet('Sheet1') as ws:
    df = ws.read()                          # Full sheet → DataFrame (row 1 = headers)
    value = ws.read_cell('B2')              # Single cell → str | int | float | ''
    values = ws.read_range('A1:C10')        # Range → list[list[Any]]
    values = ws.read_range(CellRange.from_string('A1:C10'))  # Same, via CellRange
```

#### Header-less sheets and non-row-1 headers

`read()` accepts a 1-based `header_row` kwarg:

```python
with ss.worksheet('Sheet1') as ws:
    # Header on row 3 — rows 1 and 2 are skipped
    df = ws.read(header_row=3)

    # No header — columns default to 0, 1, 2, ...
    df = ws.read(header_row=None)
```

!!! note "1-based, not 0-based"
    `header_row` uses 1-based row numbering to match spreadsheet A1 notation,
    unlike `pandas.read_csv`'s 0-based `header` kwarg.

#### Type coercion

`gspread.get_all_values()` returns every cell as a string. Pass a `dtype` to
coerce columns after construction (delegates to `DataFrame.astype`):

```python
with ss.worksheet('Sheet1') as ws:
    df = ws.read(dtype=float)                                         # Every column
    df = ws.read(dtype={'amount': float, 'date': 'datetime64[ns]'})   # Per column
```

Coercion errors surface as-is from pandas — blank cells (`''`) will raise on
`int` coercion. Pre-clean with `df.replace('', pd.NA)` if needed.

!!! note "Read methods raise in local preview"
    `read`, `read_cell`, and `read_range` all raise `NotImplementedError` in `local_preview=True` mode. There is no fake-data path.

### Write DataFrame

```python
with ss.worksheet('Sheet1') as ws:
    ws.write_dataframe(df)  # Writes to A1 with headers

    # Custom location
    ws.write_dataframe(df, location='C5')

    # Without headers
    ws.write_dataframe(df, include_header=False)
```

### Write Values and Single Cells

```python
with ss.worksheet('Sheet1') as ws:
    # Range write (auto-prepends '{title}!' if missing)
    ws.write_values('A1:B2', [
        ['Name', 'Score'],
        ['Alice', 95],
    ])

    # Single-cell write — there is no `write_value` method; use write_values with a 1x1 list
    ws.write_values('A1', [['Hello']])

    # Or write a 1x1 DataFrame without a header
    ws.write_dataframe(pd.DataFrame([['Hello']]), location='A1', include_header=False)
```

!!! warning "`Worksheet.__exit__` does not flush on exception"
    Exiting a `with ss.worksheet(...) as ws:` block via an exception **silently drops** queued operations. Only clean exits flush. If you need partial writes on error, call `ws.flush()` explicitly inside the `try` block.

## Formatting

### Cell Formatting

```python
with ss.worksheet('Sheet1') as ws:
    # Bold headers
    ws.format_range('A1:C1', {'textFormat': {'bold': True}})

    # Background color
    ws.format_range('A1:A10', {
        'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
    })

    # Number format
    ws.format_range('B2:B100', {
        'numberFormat': {'type': 'CURRENCY', 'pattern': '$#,##0.00'}
    })
```

#### `format_range` accepted dict shapes

`format_range` passes the dict straight through to `gspread.Worksheet.format`. gspread accepts **two equivalent shapes**:

- **Top-level gspread keys**: `{'bold': True, 'backgroundColor': ...}`
- **Sheets-API `userEnteredFormat` keys**: `{'textFormat': {'bold': True}, 'numberFormat': {...}, 'backgroundColor': {...}}`

The fields mask is derived by gspread from which keys are present; eftoolkit does not normalise or validate the dict. The examples above use the Sheets-API shape, which is the recommended form because it mirrors the REST API documentation.

!!! note "`set_notes` is not batched"
    Despite the plural name, `set_notes` issues one API call per note inside a loop (each call wrapped in the standard retry). For large note dictionaries, expect N round-trips.

### Borders

```python
ws.set_borders('A1:C10', {
    'top': {'style': 'SOLID'},
    'bottom': {'style': 'SOLID'},
    'left': {'style': 'SOLID'},
    'right': {'style': 'SOLID'},
})
```

### Column Width

```python
# Set specific width
ws.set_column_width('A', 200)  # By letter
ws.set_column_width(1, 200)    # By index (1-based)

# Auto-resize
ws.auto_resize_columns(1, 5)  # Columns A-E (1-based inclusive range)
```

### Freeze Rows/Columns

```python
ws.freeze_rows(1)    # Freeze header row
ws.freeze_columns(2) # Freeze first two columns
```

## Advanced Operations

### Merge Cells

```python
ws.merge_cells('A1:C1')
ws.unmerge_cells('A1:C1')
```

### Cell Notes

```python
ws.set_notes({
    'A1': 'This is the header',
    'B2': 'Important value',
})
```

### Data Validation

```python
# Dropdown list
ws.set_data_validation('A2:A100', {
    'type': 'ONE_OF_LIST',
    'values': ['Yes', 'No', 'Maybe'],
    'showDropdown': True,
})

# Clear validation
ws.clear_data_validation('A2:A100')
```

### Conditional Formatting

```python
ws.add_conditional_format('B2:B100', {
    'type': 'CUSTOM_FORMULA',
    'values': ['=B2>100'],
    'format': {
        'backgroundColor': {'red': 0.8, 'green': 1, 'blue': 0.8}
    },
})
```

### Insert/Delete Rows and Columns

```python
ws.insert_rows(5, num_rows=3)    # Insert 3 rows at row 5
ws.delete_rows(10, num_rows=2)   # Delete 2 rows starting at row 10

ws.insert_columns(2, num_cols=1) # Insert column at B
ws.delete_columns(3, num_cols=1) # Delete column C
```

### Resize Sheet

Resize the worksheet to keep only a specified number of rows and/or columns:

```python
# Keep only the first 3 rows and 4 columns
ws.resize_sheet(rows=3, columns=4)

# Keep only the first 5 rows (columns unchanged)
ws.resize_sheet(rows=5)

# Keep only the first 10 columns (rows unchanged)
ws.resize_sheet(columns=10)
```

### Sort Range

```python
ws.sort_range('A1:C10', [
    {'column': 0, 'ascending': True},   # Sort by column A
    {'column': 2, 'ascending': False},  # Then by column C descending
])
```

### Raw Requests

For operations not covered by the wrapper:

```python
ws.add_raw_request({
    'addNamedRange': {
        'namedRange': {
            'name': 'MyRange',
            'range': {
                'sheetId': 0,
                'startRowIndex': 0,
                'endRowIndex': 10,
                'startColumnIndex': 0,
                'endColumnIndex': 5,
            }
        }
    }
})
```

## Batch Operations

All operations are queued until `flush()` is called:

```python
with ss.worksheet('Sheet1') as ws:
    ws.write_dataframe(df)           # Queued
    ws.format_range('A1:B1', {...})  # Queued
    ws.set_column_width('A', 200)    # Queued
    # flush() called automatically on context exit

# Or manually:
ws = ss.worksheet('Sheet1')
ws.write_dataframe(df)
ws.flush()  # Execute all queued operations
```

## Spreadsheet Management

### List Worksheets

```python
names = ss.get_worksheet_names()
# ['Sheet1', 'Sheet2', 'Data']
```

### Create Worksheet

```python
ws = ss.create_worksheet('New Tab')
ws = ss.create_worksheet('New Tab', rows=100, cols=10)
ws = ss.create_worksheet('New Tab', replace=True)  # Delete existing first
```

### Delete Worksheet

```python
ss.delete_worksheet('Old Tab')
ss.delete_worksheet('Old Tab', ignore_missing=True)  # No error if missing
```

### Reorder Worksheets

```python
# Reorder tabs to specified order
ss.reorder_worksheets(['Dashboard', 'Draft', 'Manual Adds'])

# Tabs not in the list are moved to the end in their original order
# Missing tab names are gracefully skipped
```

## Retry Behavior

API calls automatically retry on transient errors:

- 429 (Rate limit)
- 500, 502, 503, 504 (Server errors)

Configure retry behavior:

```python
ss = Spreadsheet(
    credentials={...},
    spreadsheet_name='My Sheet',
    max_retries=10,    # Default: 5
    base_delay=1.0,    # Default: 2.0 seconds
)
```

## Dashboard Runner

For complex dashboards with multiple worksheets, `DashboardRunner` provides a structured 7-phase workflow:

0. **Run pre-run hooks** — optional setup (create/delete/reorder worksheets). Skipped in `local_preview` mode.
1. **Validate structure** — open the spreadsheet to confirm access and credentials. Skipped in `local_preview` mode.
2. **Generate data** — call `generate(config, context)` on each definition. Pure Python, no API calls. `context[<worksheet_name>]` is seeded with `{'assets', 'total_rows', 'asset_count'}` after each call so later worksheets can reference earlier ones.
3. **Write data and run post-write hooks** — inside one `Spreadsheet` context, call `create_worksheet(name, replace=True)` for each definition, write each `WorksheetAsset.df` via `write_dataframe`, then run each asset's `post_write_hooks` with a `HookContext`.
4. **Apply formatting** — call `get_formatting(context)` on each definition; merge `format_config_path` JSON with inline `format_dict`; apply via `Spreadsheet.apply_formatting`.
5. **Log summary** — INFO logs per-worksheet asset/row counts and totals.
6. **Run post-run hooks** — optional cleanup / notifications / reordering. Skipped in `local_preview` mode.

!!! warning "Phase 3 replaces existing tabs"
    Phase 3 calls `create_worksheet(name, replace=True)`, which **deletes any existing tab with the same name** before writing. Hand-maintained tabs whose names collide with a `WorksheetDefinition.name` will be lost. Rename the tab or the definition to avoid this.

### Basic Usage

```python
from eftoolkit.gsheets.runner import (
    CellLocation,
    DashboardRunner,
    WorksheetAsset,
    WorksheetFormatting,
)
import pandas as pd


class RevenueWorksheet:
    @property
    def name(self) -> str:
        return 'Revenue'

    def generate(self, config: dict, context: dict) -> list[WorksheetAsset]:
        df = pd.DataFrame({
            'Month': ['Jan', 'Feb', 'Mar'],
            'Revenue': [10000, 12000, 11500],
        })
        return [WorksheetAsset(df=df, location=CellLocation(cell='A1'))]

    def get_formatting(self, context: dict) -> WorksheetFormatting | None:
        return None  # No special formatting


runner = DashboardRunner(
    config={'sheet_name': 'Q1 Report'},
    credentials=credentials,
    worksheets=[RevenueWorksheet()],
)
runner.run()
```

### Worksheet Definition Protocol

A worksheet definition is any object that satisfies this `runtime_checkable` `Protocol`:

```python
from eftoolkit.gsheets.runner import WorksheetAsset, WorksheetFormatting

class WorksheetDefinition(Protocol):
    @property
    def name(self) -> str: ...

    def generate(self, config: dict, context: dict) -> list[WorksheetAsset]: ...

    def get_formatting(self, context: dict) -> WorksheetFormatting | None: ...
```

- `name` is a `@property` returning the tab name. A plain attribute also satisfies `isinstance(obj, WorksheetDefinition)` at runtime, but the documented contract is a property.
- `generate(config, context)` takes **both** as positional arguments. `config` is passed per call, not stored as `self.config`. `context` is shared state that `DashboardRunner` seeds after every generate call (see Phase 2).
- `get_formatting(context)` may return `None` to skip Phase 4 for that worksheet.
- **There are no `setup`, `teardown`, or `validate` lifecycle hooks.** For pre/post orchestration use `pre_run_hooks` / `post_run_hooks` on `DashboardRunner`, and for per-asset side effects use `post_write_hooks` on `WorksheetAsset`.

### Multiple DataFrames per Worksheet

A single worksheet can contain multiple DataFrames at different locations:

```python
class SummaryWorksheet:
    @property
    def name(self) -> str:
        return 'Summary'

    def generate(self, config: dict, context: dict) -> list[WorksheetAsset]:
        totals = pd.DataFrame({'Metric': ['Revenue', 'Expenses'], 'Value': [100000, 75000]})
        breakdown = pd.DataFrame({'Category': ['Sales', 'Support'], 'Amount': [60000, 40000]})

        return [
            WorksheetAsset(df=totals, location=CellLocation(cell='A1')),
            WorksheetAsset(df=breakdown, location=CellLocation(cell='A10')),
        ]

    def get_formatting(self, context: dict) -> WorksheetFormatting | None:
        return None
```

### Using WorksheetRegistry

For larger dashboards, use `WorksheetRegistry` to manage worksheet definitions:

```python
from eftoolkit.gsheets.runner import DashboardRunner, WorksheetRegistry

# Register worksheets (order is preserved)
WorksheetRegistry.register([
    SummaryWorksheet(),
    RevenueWorksheet(),
    ExpensesWorksheet(),
])

# Runner uses registered worksheets by default
runner = DashboardRunner(
    config={'sheet_name': 'Q1 Report'},
    credentials=credentials,
)
runner.run()
```

Registry methods:

```python
# Register one at a time
WorksheetRegistry.register(SummaryWorksheet())

# Retrieve in registration order
worksheets = WorksheetRegistry.get_ordered_worksheets()

# Get a specific worksheet
revenue = WorksheetRegistry.get_worksheet('Revenue')

# Reorder worksheets
WorksheetRegistry.reorder(['Expenses', 'Summary', 'Revenue'])

# Clear registry (useful in tests)
WorksheetRegistry.clear()
```

### Worksheet-Level Formatting

Formatting is applied at the worksheet level via `get_formatting()`, which returns a `WorksheetFormatting` object. This provides clear separation between data (in `WorksheetAsset`) and formatting:

```python
from pathlib import Path

class FormattedWorksheet:
    @property
    def name(self) -> str:
        return 'Formatted'

    def generate(self, config: dict, context: dict) -> list[WorksheetAsset]:
        df = pd.DataFrame({'Name': ['Alice'], 'Score': [95]})
        return [WorksheetAsset(df=df, location=CellLocation(cell='A1'))]

    def get_formatting(self, context: dict) -> WorksheetFormatting | None:
        return WorksheetFormatting(
            freeze_rows=1,                              # Freeze header row
            auto_resize_columns=(0, 5),                 # Auto-resize columns A-E
            format_config_path=Path('formats/summary.json'),  # Load from file
            format_dict={'header_color': '#4a86e8'},    # Inline overrides
        )
```

### Worksheet Assets

`WorksheetAsset` pairs a DataFrame with a location and optional post-write callbacks:

```python
from eftoolkit.gsheets.runner import CellLocation, WorksheetAsset

asset = WorksheetAsset(
    df=my_dataframe,                    # note: field is `df`, not `dataframe`
    location=CellLocation(cell='A1'),
    post_write_hooks=[my_hook],         # always a list[Callable[[HookContext], None]]
)
```

| Field | Type | Notes |
|-------|------|-------|
| `df` | `pd.DataFrame` | Required. The data to write. Field name is `df`. |
| `location` | `CellLocation` | Required. Top-left cell (header row if `include_header=True`). |
| `post_write_hooks` | `list[Callable[[HookContext], None]]` | Defaults to empty list. Always a list — never a single callable. |

Hooks run in **Phase 3**, after the asset's data is written via `write_dataframe`, inside the same `Spreadsheet` context. Any queued operations a hook adds flush when the context exits, after the writes. Hooks receive a `HookContext` (not `config`) — to read config, stash it in `context` during `generate()` and pull it back from `ctx.runner_context`.

#### WorksheetFormatting Options

`WorksheetFormatting` supports these options:

| Option | Type | Description |
|--------|------|-------------|
| `freeze_rows` | `int \| None` | Number of rows to freeze from the top. |
| `freeze_columns` | `int \| None` | Number of columns to freeze from the left. |
| `auto_resize_columns` | `tuple[int, int] \| None` | **1-based inclusive** (start, end) column indices to auto-resize. |
| `merge_ranges` | `list[RangeType]` | Ranges to merge. Each item may be an A1 string, a `CellLocation`, or a `CellRange`. |
| `notes` | `dict[CellType, str]` | Cell → note text. Keys may be A1 strings or `CellLocation`. |
| `column_widths` | `dict[str \| int, int]` | Column letter or 1-based index → width in pixels. |
| `borders` | `dict[RangeType, dict]` | Range → border spec passed verbatim to `updateBorders`. |
| `conditional_formats` | `list[dict]` | Conditional-format rules (see `add_conditional_format`). |
| `data_validations` | `list[dict]` | Data-validation rules (see `set_data_validation`). |
| `format_config_path` | `Path \| None` | Path to a JSONC format config file. |
| `format_dict` | `dict \| None` | Inline format config. Merged with `format_config_path`; `format_dict` wins on collision. |

When both `format_config_path` and `format_dict` are provided, they are merged with `format_dict` taking precedence.

### Pre-Run Hooks

Execute setup operations before the main workflow runs. Pre-run hooks receive a `Spreadsheet` instance for operations like creating, deleting, or reordering worksheets:

```python
from eftoolkit.gsheets import Spreadsheet

def ensure_worksheets_exist(ss: Spreadsheet) -> None:
    """Create worksheets that aren't managed by DashboardRunner."""
    existing = ss.get_worksheet_names()
    for name in ['Manual Input', 'Reference Data']:
        if name not in existing:
            ss.create_worksheet(name, rows=100, cols=10)

def reorder_tabs(ss: Spreadsheet) -> None:
    """Ensure tabs appear in the desired order."""
    ss.reorder_worksheets(['Summary', 'Details', 'Manual Input', 'Reference Data'])

runner = DashboardRunner(
    config={'sheet_name': 'My Report'},
    credentials=credentials,
    worksheets=[SummaryWorksheet(), DetailsWorksheet()],
    pre_run_hooks=[ensure_worksheets_exist, reorder_tabs],
)
runner.run()
```

Pre-run hooks are skipped in `local_preview` mode since they typically perform API operations.

### Post-Write Hooks

Execute callbacks after data is written. Hooks receive a `HookContext` that provides access to the worksheet, asset, and runner context:

```python
from eftoolkit.gsheets.runner import HookContext

def highlight_high_values(ctx: HookContext) -> None:
    """Add conditional formatting to highlight values > 100."""
    # Access the worksheet to apply formatting
    ctx.worksheet.add_conditional_format('A2:A100', {
        'type': 'NUMBER_GREATER',
        'values': ['100'],
        'format': {'backgroundColor': {'red': 1, 'green': 0.8, 'blue': 0.8}},
    })
    print(f'Applied formatting to {ctx.worksheet_name}')

def log_row_count(ctx: HookContext) -> None:
    """Log how many rows were written."""
    print(f'Wrote {len(ctx.asset.df)} rows to {ctx.worksheet_name}!{ctx.asset.location.cell}')

class HookedWorksheet:
    @property
    def name(self) -> str:
        return 'Hooked'

    def generate(self, config: dict, context: dict) -> list[WorksheetAsset]:
        df = pd.DataFrame({'Value': [50, 150, 200, 75]})
        return [
            WorksheetAsset(
                df=df,
                location=CellLocation(cell='A1'),
                post_write_hooks=[highlight_high_values, log_row_count],
            )
        ]

    def get_formatting(self, context: dict) -> WorksheetFormatting | None:
        return None
```

#### HookContext

The `HookContext` provides:

| Attribute | Type | Description |
|-----------|------|-------------|
| `worksheet` | `Worksheet` | The worksheet instance for additional operations |
| `asset` | `WorksheetAsset` | The asset that triggered this hook |
| `worksheet_name` | `str` | Name of the worksheet definition |
| `runner_context` | `dict` | Shared context dictionary from the DashboardRunner |

#### WorksheetAsset Computed Ranges

`WorksheetAsset` provides computed properties that return `CellRange` objects for easy access to cell ranges, eliminating the need to manually calculate ranges based on location and DataFrame dimensions:

| Property | Type | Description | Example |
|----------|------|-------------|---------|
| `header_range` | `CellRange` | Range for the header row | `CellRange('B4:E4')` |
| `data_range` | `CellRange` | Range for data rows (excluding header) | `CellRange('B5:E14')` |
| `full_range` | `CellRange` | Range for header + data | `CellRange('B4:E14')` |
| `column_ranges` | `dict[str, CellRange]` | Column name → full column range (including header) | `{'Name': CellRange('B4:B14')}` |
| `data_column_ranges` | `dict[str, CellRange]` | Column name → data-only range (excluding header) | `{'Name': CellRange('B5:B14')}` |
| `num_rows` | `int` | Number of data rows (excluding header) | `10` |
| `num_cols` | `int` | Number of columns | `4` |
| `start_row` | `int` | 1-based row index of header row | `4` |
| `end_row` | `int` | 1-based row index of last data row | `14` |
| `start_col` | `str` | Letter of first column | `'B'` |
| `end_col` | `str` | Letter of last column | `'E'` |

Example using computed ranges in hooks (use `.value` for API calls):

```python
def format_header(ctx: HookContext) -> None:
    """Bold the header row using computed range."""
    ctx.worksheet.format_range(ctx.asset.header_range.value, {'textFormat': {'bold': True}})

def format_currency_column(ctx: HookContext) -> None:
    """Apply currency format to the Amount column."""
    amount_range = ctx.asset.data_column_ranges['Amount']
    ctx.worksheet.format_range(amount_range.value, {
        'numberFormat': {'type': 'CURRENCY', 'pattern': '$#,##0.00'}
    })

def highlight_data_area(ctx: HookContext) -> None:
    """Add background color to all data rows."""
    ctx.worksheet.format_range(ctx.asset.data_range.value, {
        'backgroundColor': {'red': 0.95, 'green': 0.95, 'blue': 0.95}
    })

def log_range_info(ctx: HookContext) -> None:
    """Access range computed properties."""
    r = ctx.asset.full_range
    print(f'Range {r.value} has {r.num_rows} rows and {r.num_cols} cols')
```

#### CellLocation Properties

`CellLocation(cell='B4', offset_rows=0, offset_cols=0)` is a frozen dataclass with computed properties for row/column indices.

| Property | Type | Description | Example (`CellLocation('B4', offset_cols=1)`) |
|----------|------|-------------|-----------------------------------------------|
| `cell` | `str` | **Raw input string, offset NOT applied.** | `'B4'` |
| `value` | `str` | **Offset-aware A1 address.** Same as `str(loc)`. Use this for API calls. | `'C4'` |
| `row` | `int` | 0-indexed row (offset-aware). | `3` |
| `col` | `int` | 0-indexed column (offset-aware). | `2` |
| `row_1indexed` | `int` | 1-indexed row (offset-aware). | `4` |
| `col_letter` | `str` | Column letter(s) (offset-aware). | `'C'` |

!!! warning "`.cell` vs `.value`"
    `.cell` is the raw input string you passed in — offsets are not applied. `.value` (and `str(loc)`) apply offsets and return the resolved A1 address. Always use `.value` when passing to range APIs. Mixing them up is the most common bug with this type.

```python
loc = CellLocation(cell='I2', offset_cols=1)
loc.cell    # 'I2'  — raw input
loc.value   # 'J2'  — offset applied; use this for API calls
str(loc)    # 'J2'  — same as .value
```

#### CellRange Type

`CellRange` represents a range of cells in A1 notation with computed properties:

```python
from eftoolkit.gsheets.runner import CellRange

# Parse from A1 notation
cell_range = CellRange.from_string('B4:E14')

# Or create from 0-indexed bounds
cell_range = CellRange.from_bounds(start_row=3, start_col=1, end_row=13, end_col=4)
```

| Property | Type | Description | Example (`'B4:E14'`) |
|----------|------|-------------|----------------------|
| `start_row` | `int` | 0-indexed start row | `3` |
| `end_row` | `int` | 0-indexed end row | `13` |
| `start_col` | `int` | 0-indexed start column | `1` |
| `end_col` | `int` | 0-indexed end column | `4` |
| `start_row_1indexed` | `int` | 1-indexed start row | `4` |
| `end_row_1indexed` | `int` | 1-indexed end row | `14` |
| `start_col_letter` | `str` | Start column letter(s) | `'B'` |
| `end_col_letter` | `str` | End column letter(s) | `'E'` |
| `num_rows` | `int` | Number of rows in range | `11` |
| `num_cols` | `int` | Number of columns in range | `4` |
| `is_single_cell` | `bool` | True if range is a single cell | `False` |
| `value` | `str` | A1 notation string (same as `str()`) | `'B4:E14'` |

Single cells are represented as `CellRange` where `start == end`:

```python
single = CellRange.from_string('A1')
single.is_single_cell  # True
single.num_rows        # 1
single.num_cols        # 1
single.value           # 'A1' (not 'A1:A1')
str(single)            # 'A1' (same as .value)
```

Use `.value` or `str()` to convert back to A1 notation for API calls:

```python
cell_range = CellRange.from_string('B4:E14')
cell_range.value  # 'B4:E14'
str(cell_range)   # 'B4:E14'
```

Check if a cell or range is within a range using the `in` operator:

```python
from eftoolkit.gsheets.runner import CellLocation, CellRange

outer = CellRange.from_string('B4:E14')

# Check if a CellLocation is within the range
CellLocation(cell='C5') in outer   # True (inside range)
CellLocation(cell='A1') in outer   # False (outside range)

# Check if a CellRange is fully contained within another range
CellRange.from_string('C5:D10') in outer  # True (fully inside)
CellRange.from_string('A1:C5') in outer   # False (extends outside)
CellRange.from_string('B4:E14') in outer  # True (exact match)
```

This is useful for validating that formatting operations target ranges within an asset's bounds.

### Local Preview Mode

Test your dashboard without API credentials:

```python
runner = DashboardRunner(
    config={'sheet_name': 'Test Report'},
    credentials=None,
    worksheets=[RevenueWorksheet()],
    local_preview=True,
)
runner.run()  # Writes to local HTML files instead of Google Sheets
```

### Shared Context

Worksheets can share data via the `context` dictionary:

```python
class FirstWorksheet:
    @property
    def name(self) -> str:
        return 'First'

    def generate(self, config: dict, context: dict) -> list[WorksheetAsset]:
        total = 50000
        context['running_total'] = total  # Share with later worksheets
        df = pd.DataFrame({'Total': [total]})
        return [WorksheetAsset(df=df, location=CellLocation(cell='A1'))]

    def get_formatting(self, context: dict) -> WorksheetFormatting | None:
        return None


class SecondWorksheet:
    @property
    def name(self) -> str:
        return 'Second'

    def generate(self, config: dict, context: dict) -> list[WorksheetAsset]:
        previous_total = context.get('running_total', 0)
        df = pd.DataFrame({'Previous': [previous_total], 'New': [60000]})
        return [WorksheetAsset(df=df, location=CellLocation(cell='A1'))]

    def get_formatting(self, context: dict) -> WorksheetFormatting | None:
        return None
```

## See Also

- [API Reference](../api/gsheets.md) - Full API documentation
