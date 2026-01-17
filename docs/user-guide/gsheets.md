# Google Sheets

The `Spreadsheet` and `Worksheet` classes provide efficient Google Sheets operations with automatic batching.

## Overview

```python
from eftoolkit import Spreadsheet

# Local preview (no credentials)
ss = Spreadsheet(local_preview=True, spreadsheet_name='My Sheet')

# Live mode (with credentials)
ss = Spreadsheet(credentials={...}, spreadsheet_name='My Sheet')
```

## Local Preview Mode

Test your workflows without API credentials:

```python
import pandas as pd
from eftoolkit import Spreadsheet

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
from eftoolkit import Spreadsheet

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
    df = ws.read()  # Returns DataFrame
```

### Write DataFrame

```python
with ss.worksheet('Sheet1') as ws:
    ws.write_dataframe(df)  # Writes to A1 with headers

    # Custom location
    ws.write_dataframe(df, location='C5')

    # Without headers
    ws.write_dataframe(df, include_header=False)
```

### Write Values

```python
with ss.worksheet('Sheet1') as ws:
    ws.write_values('A1:B2', [
        ['Name', 'Score'],
        ['Alice', 95],
    ])
```

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
ws.auto_resize_columns(1, 5)  # Columns A-E
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

For complex dashboards with multiple worksheets, `DashboardRunner` provides a structured 6-phase workflow:

1. **Validate structure** - Check spreadsheet access and permissions
2. **Generate data** - Create all DataFrames (no API calls)
3. **Write data** - Write DataFrames to worksheets
4. **Apply formatting** - Apply worksheet-level formatting
5. **Run hooks** - Execute post-write hooks
6. **Log summary** - Report what was written

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

#### WorksheetFormatting Options

`WorksheetFormatting` supports these options:

| Option | Type | Description |
|--------|------|-------------|
| `freeze_rows` | `int \| None` | Number of rows to freeze from the top |
| `freeze_columns` | `int \| None` | Number of columns to freeze from the left |
| `auto_resize_columns` | `tuple[int, int] \| None` | (start, end) column indices to auto-resize |
| `merge_ranges` | `list[str]` | A1-notation ranges to merge (e.g., `['A1:C1']`) |
| `notes` | `dict[str, str]` | Cell address → note text mapping |
| `column_widths` | `dict[str \| int, int]` | Column → width in pixels |
| `borders` | `dict[str, dict]` | Range → border style configuration |
| `conditional_formats` | `list[dict]` | Conditional formatting rules |
| `data_validations` | `list[dict]` | Data validation rules |
| `format_config_path` | `Path \| None` | Path to JSON format config file |
| `format_dict` | `dict \| None` | Inline format configuration |

When both `format_config_path` and `format_dict` are provided, they are merged with `format_dict` taking precedence.

### Post-Write Hooks

Execute callbacks after data is written (e.g., custom post-processing):

```python
def add_conditional_formatting():
    print('Adding conditional formatting...')

class HookedWorksheet:
    @property
    def name(self) -> str:
        return 'Hooked'

    def generate(self, config: dict, context: dict) -> list[WorksheetAsset]:
        df = pd.DataFrame({'Value': [1, 2, 3]})
        return [
            WorksheetAsset(
                df=df,
                location=CellLocation(cell='A1'),
                post_write_hooks=[add_conditional_formatting],
            )
        ]

    def get_formatting(self, context: dict) -> WorksheetFormatting | None:
        return None
```

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
