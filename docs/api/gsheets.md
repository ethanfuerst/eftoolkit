# eftoolkit.gsheets

Google Sheets client with automatic batching and dashboard orchestration.

## Core Classes

::: eftoolkit.gsheets.core.spreadsheet.Spreadsheet
    options:
      heading_level: 3
      show_root_heading: true
      show_source: true
      members:
        - __init__
        - worksheet
        - get_worksheet_names
        - create_worksheet
        - delete_worksheet
        - reorder_worksheets
        - apply_formatting
        - open_all_previews
        - is_local_preview

::: eftoolkit.gsheets.core.worksheet.Worksheet
    options:
      heading_level: 3
      show_root_heading: true
      show_source: true
      members:
        - __init__
        - read
        - read_cell
        - read_range
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
        - resize_sheet
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

::: eftoolkit.gsheets.runner.dashboard_runner.DashboardRunner
    options:
      heading_level: 3
      show_root_heading: true
      show_source: true
      members:
        - __init__
        - run

::: eftoolkit.gsheets.runner.registry.WorksheetRegistry
    options:
      heading_level: 3
      show_root_heading: true
      show_source: true
      members:
        - register
        - get_ordered_worksheets
        - get_worksheet
        - reorder
        - clear

## Runner Types

::: eftoolkit.gsheets.runner.types.cell_location.CellLocation
    options:
      heading_level: 3
      show_root_heading: true
      show_source: true

::: eftoolkit.gsheets.runner.CellRange
    options:
      heading_level: 3
      show_root_heading: true
      show_source: true

::: eftoolkit.gsheets.runner.types.hook_context.HookContext
    options:
      heading_level: 3
      show_root_heading: true
      show_source: true

::: eftoolkit.gsheets.runner.types.worksheet_formatting.WorksheetFormatting
    options:
      heading_level: 3
      show_root_heading: true
      show_source: true

::: eftoolkit.gsheets.runner.types.worksheet_asset.WorksheetAsset
    options:
      heading_level: 3
      show_root_heading: true
      show_source: true

::: eftoolkit.gsheets.runner.types.worksheet_definition.WorksheetDefinition
    options:
      heading_level: 3
      show_root_heading: true
      show_source: true

## Utilities

JSON config utilities for loading JSONC files with comment stripping:

```python
from eftoolkit.gsheets.utils import load_json_config, remove_comments
```

::: eftoolkit.gsheets.utils.load_json_config
    options:
      heading_level: 3
      show_root_heading: true
      show_source: true

::: eftoolkit.gsheets.utils.load_service_account_credentials
    options:
      heading_level: 3
      show_root_heading: true
      show_source: true

::: eftoolkit.gsheets.utils.remove_comments
    options:
      heading_level: 3
      show_root_heading: true
      show_source: true
