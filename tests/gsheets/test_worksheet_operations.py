"""Tests for Worksheet batch operations (queuing methods)."""

import pandas as pd

from eftoolkit.gsheets import Spreadsheet

# --- Write Operations ---


def test_worksheet_write_dataframe_queues():
    """write_dataframe adds to value_updates queue."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    ws.write_dataframe(df)

    assert len(ws._value_updates) == 1
    assert ws._value_updates[0]['values'] == [['a', 'b'], [1, 3], [2, 4]]


def test_worksheet_write_dataframe_without_header():
    """write_dataframe with include_header=False omits headers."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    ws.write_dataframe(df, include_header=False)

    assert ws._value_updates[0]['values'] == [[1, 3], [2, 4]]


def test_worksheet_write_dataframe_with_format():
    """write_dataframe with format_dict queues format requests."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    df = pd.DataFrame({'a': [1]})
    ws.write_dataframe(df, format_dict={'A1:B1': {'bold': True}})

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'format'


def test_worksheet_write_values_queues():
    """write_values adds to value_updates queue."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.write_values('A1:B2', [[1, 2], [3, 4]])

    assert len(ws._value_updates) == 1


def test_worksheet_write_values_prepends_title():
    """write_values prepends worksheet title if not included."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.write_values('A1:B2', [[1, 2]])

    assert 'Local Preview - Sheet1!' in ws._value_updates[0]['range']


def test_worksheet_write_values_keeps_existing_title():
    """write_values keeps existing worksheet reference."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.write_values('OtherSheet!A1:B2', [[1, 2]])

    assert ws._value_updates[0]['range'] == 'OtherSheet!A1:B2'


# --- Format Operations ---


def test_worksheet_format_range_queues():
    """format_range adds to batch_requests queue."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.format_range('A1:B2', {'bold': True})

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'format'


def test_worksheet_set_borders_queues():
    """set_borders adds to batch_requests queue."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.set_borders('A1:B2', {'top': {'style': 'SOLID'}})

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'border'


def test_worksheet_set_notes_queues():
    """set_notes adds to batch_requests queue."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.set_notes({'A1': 'Note text'})

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'notes'


def test_worksheet_add_conditional_format_queues():
    """add_conditional_format adds to batch_requests queue."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    rule = {
        'type': 'CUSTOM_FORMULA',
        'values': ['=A1>100'],
        'format': {'backgroundColor': {'red': 1}},
    }
    ws.add_conditional_format('A1:A10', rule)

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'conditional_format'
    assert ws._batch_requests[0]['range'] == 'A1:A10'
    assert ws._batch_requests[0]['rule'] == rule


# --- Column/Row Operations ---


def test_worksheet_set_column_width_queues():
    """set_column_width adds to batch_requests queue."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.set_column_width('A', 100)

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'column_width'


def test_worksheet_auto_resize_columns_queues():
    """auto_resize_columns adds to batch_requests queue."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.auto_resize_columns(1, 5)

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'auto_resize'


def test_worksheet_insert_rows_queues():
    """insert_rows adds to batch_requests queue."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.insert_rows(5, num_rows=3)

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'insert_rows'
    assert ws._batch_requests[0]['start_row'] == 5
    assert ws._batch_requests[0]['num_rows'] == 3


def test_worksheet_insert_rows_default_count():
    """insert_rows defaults to inserting 1 row."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.insert_rows(5)

    assert ws._batch_requests[0]['num_rows'] == 1


def test_worksheet_delete_rows_queues():
    """delete_rows adds to batch_requests queue."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.delete_rows(5, num_rows=2)

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'delete_rows'
    assert ws._batch_requests[0]['start_row'] == 5
    assert ws._batch_requests[0]['num_rows'] == 2


def test_worksheet_insert_columns_queues():
    """insert_columns adds to batch_requests queue."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.insert_columns(3, num_cols=2)

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'insert_columns'
    assert ws._batch_requests[0]['start_col'] == 3
    assert ws._batch_requests[0]['num_cols'] == 2


def test_worksheet_delete_columns_queues():
    """delete_columns adds to batch_requests queue."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.delete_columns(3, num_cols=2)

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'delete_columns'
    assert ws._batch_requests[0]['start_col'] == 3
    assert ws._batch_requests[0]['num_cols'] == 2


def test_worksheet_freeze_rows_queues():
    """freeze_rows adds to batch_requests queue."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.freeze_rows(2)

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'freeze_rows'
    assert ws._batch_requests[0]['num_rows'] == 2


def test_worksheet_freeze_columns_queues():
    """freeze_columns adds to batch_requests queue."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.freeze_columns(1)

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'freeze_columns'
    assert ws._batch_requests[0]['num_cols'] == 1


# --- Merge Operations ---


def test_worksheet_merge_cells_queues():
    """merge_cells adds to batch_requests queue."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.merge_cells('A1:C1')

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'merge'
    assert ws._batch_requests[0]['range'] == 'A1:C1'
    assert ws._batch_requests[0]['merge_type'] == 'MERGE_ALL'


def test_worksheet_merge_cells_with_type():
    """merge_cells accepts different merge types."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.merge_cells('A1:C3', merge_type='MERGE_COLUMNS')

    assert ws._batch_requests[0]['merge_type'] == 'MERGE_COLUMNS'


def test_worksheet_unmerge_cells_queues():
    """unmerge_cells adds to batch_requests queue."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.unmerge_cells('A1:C1')

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'unmerge'
    assert ws._batch_requests[0]['range'] == 'A1:C1'


# --- Sort Operations ---


def test_worksheet_sort_range_queues():
    """sort_range adds to batch_requests queue."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    sort_specs = [{'column': 0, 'ascending': True}]
    ws.sort_range('A1:C10', sort_specs)

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'sort'
    assert ws._batch_requests[0]['range'] == 'A1:C10'
    assert ws._batch_requests[0]['sort_specs'] == sort_specs


# --- Data Validation Operations ---


def test_worksheet_set_data_validation_queues():
    """set_data_validation adds to batch_requests queue."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    rule = {'type': 'ONE_OF_LIST', 'values': ['Yes', 'No']}
    ws.set_data_validation('A1:A10', rule)

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'data_validation'
    assert ws._batch_requests[0]['range'] == 'A1:A10'
    assert ws._batch_requests[0]['rule'] == rule


def test_worksheet_clear_data_validation_queues():
    """clear_data_validation adds to batch_requests queue."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.clear_data_validation('A1:A10')

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'clear_data_validation'
    assert ws._batch_requests[0]['range'] == 'A1:A10'


# --- Raw Request Operations ---


def test_worksheet_add_raw_request_queues():
    """add_raw_request adds to batch_requests queue."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    raw_request = {
        'addNamedRange': {
            'namedRange': {
                'name': 'MyRange',
                'range': {'sheetId': 0},
            }
        }
    }
    ws.add_raw_request(raw_request)

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'raw'
    assert ws._batch_requests[0]['request'] == raw_request
