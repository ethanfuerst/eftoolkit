# eftoolkit.gsheets

Google Sheets client with automatic batching and dashboard orchestration.

## Core Classes

### Spreadsheet

::: eftoolkit.gsheets.core.spreadsheet.Spreadsheet
    options:
      show_root_heading: true
      show_source: true
      members:
        - __init__
        - worksheet
        - get_worksheet_names
        - create_worksheet
        - delete_worksheet
        - reorder_worksheets
        - is_local_preview

### Worksheet

::: eftoolkit.gsheets.core.worksheet.Worksheet
    options:
      show_root_heading: true
      show_source: true
      members:
        - __init__
        - read
        - write_dataframe
        - write_values
        - format_range
        - set_borders
        - set_column_width
        - auto_resize_columns
        - set_notes
        - merge_cells
        - unmerge_cells
        - sort_range
        - set_data_validation
        - clear_data_validation
        - add_conditional_format
        - insert_rows
        - delete_rows
        - insert_columns
        - delete_columns
        - freeze_rows
        - freeze_columns
        - add_raw_request
        - flush
        - open_preview
        - title
        - is_local_preview

## Dashboard Runner

For structured dashboard workflows, import from `eftoolkit.gsheets.runner`:

```python
from eftoolkit.gsheets.runner import (
    DashboardRunner,
    WorksheetRegistry,
    CellLocation,
    HookContext,
    WorksheetAsset,
    WorksheetDefinition,
    WorksheetFormatting,
)
```

### DashboardRunner

::: eftoolkit.gsheets.runner.dashboard_runner.DashboardRunner
    options:
      show_root_heading: true
      show_source: true
      members:
        - __init__
        - run

### WorksheetRegistry

::: eftoolkit.gsheets.runner.registry.WorksheetRegistry
    options:
      show_root_heading: true
      show_source: true
      members:
        - register
        - get_ordered_worksheets
        - get_worksheet
        - reorder
        - clear

## Runner Types

### CellLocation

::: eftoolkit.gsheets.runner.types.cell_location.CellLocation
    options:
      show_root_heading: true
      show_source: true

### HookContext

::: eftoolkit.gsheets.runner.types.hook_context.HookContext
    options:
      show_root_heading: true
      show_source: true

### WorksheetFormatting

::: eftoolkit.gsheets.runner.types.worksheet_formatting.WorksheetFormatting
    options:
      show_root_heading: true
      show_source: true

### WorksheetAsset

::: eftoolkit.gsheets.runner.types.worksheet_asset.WorksheetAsset
    options:
      show_root_heading: true
      show_source: true

### WorksheetDefinition

::: eftoolkit.gsheets.runner.types.worksheet_definition.WorksheetDefinition
    options:
      show_root_heading: true
      show_source: true

## Utilities

JSON config utilities for loading JSONC files with comment stripping:

```python
from eftoolkit.gsheets.utils import load_json_config, remove_comments
```

### load_json_config

::: eftoolkit.gsheets.utils.load_json_config
    options:
      show_root_heading: true
      show_source: true

### remove_comments

::: eftoolkit.gsheets.utils.remove_comments
    options:
      show_root_heading: true
      show_source: true
