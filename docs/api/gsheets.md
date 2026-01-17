# eftoolkit.gsheets

Google Sheets client with automatic batching.

## Classes

### Spreadsheet

::: eftoolkit.gsheets.sheet.Spreadsheet
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

::: eftoolkit.gsheets.sheet.Worksheet
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
