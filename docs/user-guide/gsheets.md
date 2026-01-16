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

## See Also

- [API Reference](../api/gsheets.md) - Full API documentation
