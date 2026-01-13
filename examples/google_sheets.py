#!/usr/bin/env python3
"""Google Sheets example - works with or without credentials.

This example demonstrates Google Sheets operations using eftoolkit's Spreadsheet
and Worksheet classes. It automatically detects whether credentials are available:

- With credentials: Connects to a real Google Sheet
- Without credentials: Uses local_preview mode to render HTML

Environment variables (for real Google Sheets):
    GOOGLE_CREDENTIALS_PATH: Path to service account JSON or JSON content directly
    GOOGLE_SPREADSHEET_NAME: Name of the spreadsheet to use (default: 'eftoolkit-demo')

Run with: uv run python examples/google_sheets.py
"""

import json
import os
from pathlib import Path

import pandas as pd

from eftoolkit.gsheets import Spreadsheet


def load_credentials() -> dict | None:
    """Load Google service account credentials if available.

    Returns:
        Credentials dict if GOOGLE_CREDENTIALS_PATH is set, None otherwise.
    """
    creds_value = os.environ.get('GOOGLE_CREDENTIALS_PATH')
    if not creds_value:
        return None

    # Check if it's JSON content or a file path
    if creds_value.strip().startswith('{'):
        return json.loads(creds_value)

    with open(creds_value) as f:
        return json.load(f)


def main():
    """Demonstrate Google Sheets operations."""
    credentials = load_credentials()
    spreadsheet_name = os.environ.get('GOOGLE_SPREADSHEET_NAME', 'eftoolkit-demo')

    if credentials:
        print('=== Google Sheets Demo (Live Mode) ===')
        print(f'Connecting to: {spreadsheet_name}')
        ss = Spreadsheet(
            credentials=credentials,
            spreadsheet_name=spreadsheet_name,
        )
    else:
        print('=== Google Sheets Demo (Local Preview Mode) ===')
        print('No credentials found. Using local preview mode.')
        print('Set GOOGLE_CREDENTIALS_PATH to connect to real Google Sheets.')
        print()
        ss = Spreadsheet(
            local_preview=True,
            spreadsheet_name=spreadsheet_name,
        )

    print(f'Preview mode: {ss.is_local_preview}')
    print()

    # --- Create/get a worksheet ---
    worksheet_name = 'Dashboard'
    if ss.is_local_preview:
        ws = ss.worksheet(worksheet_name)
    else:
        ws = ss.create_worksheet(worksheet_name, replace=True)
    print(f'Working with worksheet: {worksheet_name}')

    # --- Write sample data ---
    sales_df = pd.DataFrame(
        {
            'Region': ['North', 'South', 'East', 'West'],
            'Q1': [125000, 98000, 115000, 142000],
            'Q2': [132000, 105000, 118000, 155000],
            'Q3': [128000, 112000, 125000, 148000],
            'Q4': [145000, 125000, 135000, 168000],
        }
    )

    # Write title and headers
    ws.write_values('A1', [['Sales Report - 2024']])
    ws.write_values('A3:E3', [['Region', 'Q1', 'Q2', 'Q3', 'Q4']])
    ws.write_dataframe(sales_df, location='A4', include_header=False)

    # Add totals row
    ws.write_values('A8:E8', [['Total', 480000, 510000, 513000, 573000]])
    ws.flush()
    print('Wrote sales data')

    # --- Apply formatting ---
    # Title formatting
    ws.format_range(
        'A1',
        {
            'textFormat': {'bold': True, 'fontSize': 16},
        },
    )

    # Header formatting
    ws.format_range(
        'A3:E3',
        {
            'textFormat': {'bold': True},
            'backgroundColor': {'red': 0.2, 'green': 0.4, 'blue': 0.8},
        },
    )

    # Set column widths
    ws.set_column_width('A', 100)
    ws.set_column_width('B', 90)
    ws.set_column_width('C', 90)
    ws.set_column_width('D', 90)
    ws.set_column_width('E', 90)

    # Add notes
    ws.set_notes(
        {
            'A1': 'Annual sales by region',
            'E4': 'Best performing region!',
        }
    )
    ws.flush()
    print('Applied formatting')

    # --- Add a second section ---
    products_df = pd.DataFrame(
        {
            'Product': ['Widget', 'Gadget', 'Gizmo', 'Doohickey'],
            'Units': [15000, 12500, 8900, 6200],
            'Revenue': [299000, 312500, 178000, 124000],
        }
    )

    ws.write_values('G1', [['Top Products']])
    ws.write_values('G3:I3', [['Product', 'Units', 'Revenue']])
    ws.write_dataframe(products_df, location='G4', include_header=False)

    ws.set_column_width('G', 100)
    ws.set_column_width('H', 80)
    ws.set_column_width('I', 100)
    ws.flush()
    print('Added products section')

    # --- Show result ---
    print()
    if ss.is_local_preview:
        preview_path = Path(
            f'gsheets_preview/{spreadsheet_name}_Dashboard_preview.html'
        )
        print('=== Preview Generated ===')
        print(f'Preview file: {preview_path.absolute()}')
        print()
        print('Opening preview in browser...')
        ws.open_preview()
    else:
        print('=== Done ===')
        print(f'View your spreadsheet: {spreadsheet_name}')

    print()
    print('=== Features Demonstrated ===')
    print('- write_values(): Write raw cell values')
    print('- write_dataframe(): Write pandas DataFrame')
    print('- format_range(): Apply cell formatting')
    print('- set_column_width(): Set column widths')
    print('- set_notes(): Add cell notes/comments')
    print('- flush(): Send batched operations to API')


if __name__ == '__main__':
    main()
