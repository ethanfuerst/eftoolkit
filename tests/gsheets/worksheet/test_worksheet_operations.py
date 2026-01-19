"""Tests for Worksheet batch operations (queuing methods)."""

import pandas as pd

from eftoolkit.gsheets import Spreadsheet
from eftoolkit.gsheets.types import CellLocation, CellRange

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


def test_worksheet_write_dataframe_with_cell_location():
    """write_dataframe accepts CellLocation for location parameter."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    ws.write_dataframe(df, location=CellLocation(cell='B3'))

    assert len(ws._value_updates) == 1
    assert 'B3' in ws._value_updates[0]['range']


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


def test_worksheet_write_values_with_cell_location():
    """write_values accepts CellLocation for range_name parameter."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.write_values(CellLocation(cell='C5'), [[1, 2]])

    assert len(ws._value_updates) == 1
    assert 'C5' in ws._value_updates[0]['range']


def test_worksheet_write_values_with_cell_range():
    """write_values accepts CellRange for range_name parameter."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.write_values(CellRange.from_string('A1:B2'), [[1, 2], [3, 4]])

    assert len(ws._value_updates) == 1
    assert 'A1:B2' in ws._value_updates[0]['range']


# --- Format Operations ---


def test_worksheet_format_range_queues():
    """format_range adds to batch_requests queue."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.format_range('A1:B2', {'bold': True})

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'format'


def test_worksheet_format_range_with_cell_location():
    """format_range accepts CellLocation for range_name parameter."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.format_range(CellLocation(cell='D7'), {'bold': True})

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'format'
    assert ws._batch_requests[0]['range'] == CellLocation(cell='D7')


def test_worksheet_format_range_with_cell_range():
    """format_range accepts CellRange for range_name parameter."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.format_range(CellRange.from_string('B2:D4'), {'bold': True})

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'format'
    assert ws._batch_requests[0]['range'] == CellRange.from_string('B2:D4')


def test_worksheet_set_borders_queues():
    """set_borders adds to batch_requests queue."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.set_borders('A1:B2', {'top': {'style': 'SOLID'}})

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'border'


def test_worksheet_set_borders_with_cell_location():
    """set_borders accepts CellLocation for range_name parameter."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.set_borders(CellLocation(cell='E5'), {'top': {'style': 'SOLID'}})

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'border'
    assert ws._batch_requests[0]['range'] == CellLocation(cell='E5')


def test_worksheet_set_borders_with_cell_range():
    """set_borders accepts CellRange for range_name parameter."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.set_borders(CellRange.from_string('C3:F6'), {'top': {'style': 'SOLID'}})

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'border'
    assert ws._batch_requests[0]['range'] == CellRange.from_string('C3:F6')


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


def test_worksheet_add_conditional_format_with_cell_location():
    """add_conditional_format accepts CellLocation for range_name parameter."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    rule = {
        'type': 'CUSTOM_FORMULA',
        'values': ['=G5>100'],
        'format': {'backgroundColor': {'red': 1}},
    }
    ws.add_conditional_format(CellLocation(cell='G5'), rule)

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'conditional_format'
    assert ws._batch_requests[0]['range'] == CellLocation(cell='G5')


def test_worksheet_add_conditional_format_with_cell_range():
    """add_conditional_format accepts CellRange for range_name parameter."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    rule = {
        'type': 'CUSTOM_FORMULA',
        'values': ['=A1>100'],
        'format': {'backgroundColor': {'red': 1}},
    }
    ws.add_conditional_format(CellRange.from_string('D2:H8'), rule)

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'conditional_format'
    assert ws._batch_requests[0]['range'] == CellRange.from_string('D2:H8')


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


# --- Resize Sheet Operations ---


def test_worksheet_resize_sheet_rows_only():
    """resize_sheet with rows only queues correct request."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.resize_sheet(rows=5)

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'resize_sheet'
    assert ws._batch_requests[0]['rows'] == 5
    assert ws._batch_requests[0]['columns'] is None


def test_worksheet_resize_sheet_columns_only():
    """resize_sheet with columns only queues correct request."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.resize_sheet(columns=10)

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'resize_sheet'
    assert ws._batch_requests[0]['rows'] is None
    assert ws._batch_requests[0]['columns'] == 10


def test_worksheet_resize_sheet_both_rows_and_columns():
    """resize_sheet with both rows and columns queues correct request."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.resize_sheet(rows=3, columns=4)

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'resize_sheet'
    assert ws._batch_requests[0]['rows'] == 3
    assert ws._batch_requests[0]['columns'] == 4


def test_worksheet_resize_sheet_requires_at_least_one_param():
    """resize_sheet raises ValueError when neither rows nor columns specified."""
    import pytest

    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    with pytest.raises(
        ValueError, match='At least one of rows or columns must be specified'
    ):
        ws.resize_sheet()


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


def test_worksheet_merge_cells_with_cell_range():
    """merge_cells accepts CellRange for range_name parameter."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.merge_cells(CellRange.from_string('B2:E2'))

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'merge'
    assert ws._batch_requests[0]['range'] == CellRange.from_string('B2:E2')


def test_worksheet_unmerge_cells_queues():
    """unmerge_cells adds to batch_requests queue."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.unmerge_cells('A1:C1')

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'unmerge'
    assert ws._batch_requests[0]['range'] == 'A1:C1'


def test_worksheet_unmerge_cells_with_cell_range():
    """unmerge_cells accepts CellRange for range_name parameter."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.unmerge_cells(CellRange.from_string('B2:E2'))

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'unmerge'
    assert ws._batch_requests[0]['range'] == CellRange.from_string('B2:E2')


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


def test_worksheet_sort_range_with_cell_range():
    """sort_range accepts CellRange for range_name parameter."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    sort_specs = [{'column': 0, 'ascending': False}]
    ws.sort_range(CellRange.from_string('B2:D15'), sort_specs)

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'sort'
    assert ws._batch_requests[0]['range'] == CellRange.from_string('B2:D15')


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


def test_worksheet_set_data_validation_with_cell_location():
    """set_data_validation accepts CellLocation for range_name parameter."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    rule = {'type': 'ONE_OF_LIST', 'values': ['Yes', 'No']}
    ws.set_data_validation(CellLocation(cell='F8'), rule)

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'data_validation'
    assert ws._batch_requests[0]['range'] == CellLocation(cell='F8')


def test_worksheet_set_data_validation_with_cell_range():
    """set_data_validation accepts CellRange for range_name parameter."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    rule = {'type': 'ONE_OF_LIST', 'values': ['Yes', 'No']}
    ws.set_data_validation(CellRange.from_string('C3:C20'), rule)

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'data_validation'
    assert ws._batch_requests[0]['range'] == CellRange.from_string('C3:C20')


def test_worksheet_clear_data_validation_queues():
    """clear_data_validation adds to batch_requests queue."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.clear_data_validation('A1:A10')

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'clear_data_validation'
    assert ws._batch_requests[0]['range'] == 'A1:A10'


def test_worksheet_clear_data_validation_with_cell_location():
    """clear_data_validation accepts CellLocation for range_name parameter."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.clear_data_validation(CellLocation(cell='F8'))

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'clear_data_validation'
    assert ws._batch_requests[0]['range'] == CellLocation(cell='F8')


def test_worksheet_clear_data_validation_with_cell_range():
    """clear_data_validation accepts CellRange for range_name parameter."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.clear_data_validation(CellRange.from_string('C3:C20'))

    assert len(ws._batch_requests) == 1
    assert ws._batch_requests[0]['type'] == 'clear_data_validation'
    assert ws._batch_requests[0]['range'] == CellRange.from_string('C3:C20')


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


# --- Read Operations ---


def test_worksheet_read_cell_raises_in_local_preview():
    """read_cell raises NotImplementedError in local preview mode."""
    import pytest

    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    with pytest.raises(NotImplementedError):
        ws.read_cell('V5')


def test_worksheet_read_range_raises_in_local_preview():
    """read_range raises NotImplementedError in local preview mode."""
    import pytest

    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    with pytest.raises(NotImplementedError):
        ws.read_range('V5:V10')
