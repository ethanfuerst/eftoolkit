"""Tests for Worksheet batch request handlers.

These tests verify that each batch request type (column_width, auto_resize, raw, etc.)
is properly flushed to the Google Sheets API.

Google Sheets API batchUpdate reference:
https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/request
"""

from unittest.mock import MagicMock

import pytest

from eftoolkit.gsheets import Spreadsheet, Worksheet


def _create_mock_worksheet_with_api():
    """Helper to create a worksheet connected to mock gspread objects."""
    mock_gspread = MagicMock()
    mock_ws = MagicMock()
    mock_ws.title = 'TestSheet'
    mock_ws.id = 12345

    ss = Spreadsheet(local_preview=True, spreadsheet_name='TestSpreadsheet')
    ss._local_preview = False
    ss._gspread_spreadsheet = mock_gspread

    ws = Worksheet(mock_ws, ss)
    return ws, mock_gspread, mock_ws


def test_set_column_width_with_letter_calls_batch_update():
    """set_column_width with letter column sends UpdateDimensionPropertiesRequest."""
    ws, mock_gspread, mock_ws = _create_mock_worksheet_with_api()

    ws.set_column_width('B', 200)
    ws.flush()

    mock_gspread.batch_update.assert_called_once()
    call_args = mock_gspread.batch_update.call_args[0][0]
    request = call_args['requests'][0]

    assert 'updateDimensionProperties' in request
    assert (
        request['updateDimensionProperties']['range']['startIndex'] == 1
    )  # B = index 1
    assert request['updateDimensionProperties']['range']['endIndex'] == 2
    assert request['updateDimensionProperties']['properties']['pixelSize'] == 200


def test_set_column_width_with_number_calls_batch_update():
    """set_column_width with numeric column queues request that calls batch_update."""
    ws, mock_gspread, mock_ws = _create_mock_worksheet_with_api()

    ws.set_column_width(3, 150)
    ws.flush()

    mock_gspread.batch_update.assert_called_once()
    call_args = mock_gspread.batch_update.call_args[0][0]
    request = call_args['requests'][0]

    assert 'updateDimensionProperties' in request
    assert (
        request['updateDimensionProperties']['range']['startIndex'] == 2
    )  # 3 is 0-indexed as 2
    assert request['updateDimensionProperties']['range']['endIndex'] == 3
    assert request['updateDimensionProperties']['properties']['pixelSize'] == 150


def test_set_column_width_uses_worksheet_id():
    """set_column_width includes the correct worksheet ID."""
    ws, mock_gspread, mock_ws = _create_mock_worksheet_with_api()
    mock_ws.id = 99999

    ws.set_column_width('A', 100)
    ws.flush()

    call_args = mock_gspread.batch_update.call_args[0][0]
    request = call_args['requests'][0]

    assert request['updateDimensionProperties']['range']['sheetId'] == 99999


def test_auto_resize_columns_calls_batch_update():
    """auto_resize_columns queues request that calls batch_update."""
    ws, mock_gspread, mock_ws = _create_mock_worksheet_with_api()

    ws.auto_resize_columns(1, 5)
    ws.flush()

    mock_gspread.batch_update.assert_called_once()
    call_args = mock_gspread.batch_update.call_args[0][0]
    request = call_args['requests'][0]

    assert 'autoResizeDimensions' in request
    dims = request['autoResizeDimensions']['dimensions']
    assert dims['startIndex'] == 0  # 1-based to 0-based
    assert dims['endIndex'] == 5  # end is exclusive
    assert dims['dimension'] == 'COLUMNS'


def test_auto_resize_columns_uses_worksheet_id():
    """auto_resize_columns includes the correct worksheet ID."""
    ws, mock_gspread, mock_ws = _create_mock_worksheet_with_api()
    mock_ws.id = 77777

    ws.auto_resize_columns(2, 4)
    ws.flush()

    call_args = mock_gspread.batch_update.call_args[0][0]
    dims = call_args['requests'][0]['autoResizeDimensions']['dimensions']

    assert dims['sheetId'] == 77777


def test_add_raw_request_calls_batch_update():
    """add_raw_request queues request that calls batch_update."""
    ws, mock_gspread, mock_ws = _create_mock_worksheet_with_api()

    raw_request = {
        'addNamedRange': {
            'namedRange': {
                'name': 'MyRange',
                'range': {'sheetId': 0, 'startRowIndex': 0, 'endRowIndex': 10},
            }
        }
    }
    ws.add_raw_request(raw_request)
    ws.flush()

    mock_gspread.batch_update.assert_called_once()
    call_args = mock_gspread.batch_update.call_args[0][0]

    assert call_args['requests'][0] == raw_request


def test_add_raw_request_preserves_request_structure():
    """add_raw_request passes the exact request structure to batch_update."""
    ws, mock_gspread, mock_ws = _create_mock_worksheet_with_api()

    complex_request = {
        'updateCells': {
            'rows': [{'values': [{'userEnteredValue': {'stringValue': 'test'}}]}],
            'fields': 'userEnteredValue',
            'start': {'sheetId': 0, 'rowIndex': 0, 'columnIndex': 0},
        }
    }
    ws.add_raw_request(complex_request)
    ws.flush()

    call_args = mock_gspread.batch_update.call_args[0][0]

    assert call_args['requests'][0] == complex_request


def test_set_borders_calls_batch_update():
    """set_borders queues request that calls batch_update."""
    ws, mock_gspread, mock_ws = _create_mock_worksheet_with_api()

    borders = {
        'top': {'style': 'SOLID', 'color': {'red': 0, 'green': 0, 'blue': 0}},
        'bottom': {'style': 'SOLID'},
    }
    ws.set_borders('A1:C3', borders)
    ws.flush()

    mock_gspread.batch_update.assert_called_once()
    call_args = mock_gspread.batch_update.call_args[0][0]
    request = call_args['requests'][0]

    assert 'updateBorders' in request
    assert 'top' in request['updateBorders']
    assert 'bottom' in request['updateBorders']


def test_merge_cells_calls_batch_update():
    """merge_cells queues request that calls batch_update."""
    ws, mock_gspread, mock_ws = _create_mock_worksheet_with_api()

    ws.merge_cells('A1:C1', 'MERGE_ALL')
    ws.flush()

    mock_gspread.batch_update.assert_called_once()
    call_args = mock_gspread.batch_update.call_args[0][0]
    request = call_args['requests'][0]

    assert 'mergeCells' in request
    assert request['mergeCells']['mergeType'] == 'MERGE_ALL'


def test_unmerge_cells_calls_batch_update():
    """unmerge_cells queues request that calls batch_update."""
    ws, mock_gspread, mock_ws = _create_mock_worksheet_with_api()

    ws.unmerge_cells('A1:C1')
    ws.flush()

    mock_gspread.batch_update.assert_called_once()
    call_args = mock_gspread.batch_update.call_args[0][0]
    request = call_args['requests'][0]

    assert 'unmergeCells' in request


def test_set_notes_calls_update_note():
    """set_notes queues request that calls update_note on worksheet."""
    ws, mock_gspread, mock_ws = _create_mock_worksheet_with_api()

    ws.set_notes({'A1': 'Note 1', 'B2': 'Note 2'})
    ws.flush()

    assert mock_ws.update_note.call_count == 2
    calls = [call[0] for call in mock_ws.update_note.call_args_list]

    assert ('A1', 'Note 1') in calls
    assert ('B2', 'Note 2') in calls


def test_sort_range_calls_batch_update():
    """sort_range queues request that calls batch_update."""
    ws, mock_gspread, mock_ws = _create_mock_worksheet_with_api()

    ws.sort_range('A1:C10', [{'column': 0, 'ascending': True}])
    ws.flush()

    mock_gspread.batch_update.assert_called_once()
    call_args = mock_gspread.batch_update.call_args[0][0]
    request = call_args['requests'][0]

    assert 'sortRange' in request
    assert request['sortRange']['sortSpecs'][0]['sortOrder'] == 'ASCENDING'


def test_sort_range_descending():
    """sort_range with ascending=False uses DESCENDING sort order."""
    ws, mock_gspread, mock_ws = _create_mock_worksheet_with_api()

    ws.sort_range('A1:C10', [{'column': 1, 'ascending': False}])
    ws.flush()

    call_args = mock_gspread.batch_update.call_args[0][0]
    request = call_args['requests'][0]

    assert request['sortRange']['sortSpecs'][0]['sortOrder'] == 'DESCENDING'


def test_set_data_validation_calls_batch_update():
    """set_data_validation queues request that calls batch_update."""
    ws, mock_gspread, mock_ws = _create_mock_worksheet_with_api()

    ws.set_data_validation(
        'A1:A10',
        {
            'type': 'ONE_OF_LIST',
            'values': ['Yes', 'No', 'Maybe'],
            'showDropdown': True,
        },
    )
    ws.flush()

    mock_gspread.batch_update.assert_called_once()
    call_args = mock_gspread.batch_update.call_args[0][0]
    request = call_args['requests'][0]

    assert 'setDataValidation' in request
    assert request['setDataValidation']['rule']['condition']['type'] == 'ONE_OF_LIST'


def test_clear_data_validation_calls_batch_update():
    """clear_data_validation queues request that calls batch_update."""
    ws, mock_gspread, mock_ws = _create_mock_worksheet_with_api()

    ws.clear_data_validation('A1:A10')
    ws.flush()

    mock_gspread.batch_update.assert_called_once()
    call_args = mock_gspread.batch_update.call_args[0][0]
    request = call_args['requests'][0]

    assert 'setDataValidation' in request
    assert request['setDataValidation']['rule'] is None


def test_add_conditional_format_calls_batch_update():
    """add_conditional_format queues request that calls batch_update."""
    ws, mock_gspread, mock_ws = _create_mock_worksheet_with_api()

    ws.add_conditional_format(
        'A1:A10',
        {
            'type': 'CUSTOM_FORMULA',
            'values': ['=A1>100'],
            'format': {'backgroundColor': {'red': 1, 'green': 0, 'blue': 0}},
        },
    )
    ws.flush()

    mock_gspread.batch_update.assert_called_once()
    call_args = mock_gspread.batch_update.call_args[0][0]
    request = call_args['requests'][0]

    assert 'addConditionalFormatRule' in request


def test_add_conditional_format_with_open_ended_range():
    """add_conditional_format with open-ended range like 'X5:X' omits endRowIndex."""
    ws, mock_gspread, mock_ws = _create_mock_worksheet_with_api()

    ws.add_conditional_format(
        'X5:X',
        {
            'type': 'CUSTOM_FORMULA',
            'values': ['=X5>0'],
            'format': {'backgroundColor': {'red': 0, 'green': 1, 'blue': 0}},
        },
    )
    ws.flush()

    mock_gspread.batch_update.assert_called_once()
    call_args = mock_gspread.batch_update.call_args[0][0]
    request = call_args['requests'][0]

    assert 'addConditionalFormatRule' in request
    ranges = request['addConditionalFormatRule']['rule']['ranges']

    assert len(ranges) == 1
    grid_range = ranges[0]
    assert grid_range['startRowIndex'] == 4  # Row 5, 0-indexed
    assert grid_range['startColumnIndex'] == 23  # Column X, 0-indexed
    assert grid_range['endColumnIndex'] == 24  # Exclusive
    assert 'endRowIndex' not in grid_range  # Open-ended, no end row


def test_insert_rows_calls_batch_update():
    """insert_rows queues request that calls batch_update."""
    ws, mock_gspread, mock_ws = _create_mock_worksheet_with_api()

    ws.insert_rows(5, 3)
    ws.flush()

    mock_gspread.batch_update.assert_called_once()
    call_args = mock_gspread.batch_update.call_args[0][0]
    request = call_args['requests'][0]

    assert 'insertDimension' in request
    assert request['insertDimension']['range']['dimension'] == 'ROWS'
    assert request['insertDimension']['range']['startIndex'] == 4  # 5 - 1
    assert request['insertDimension']['range']['endIndex'] == 7  # 4 + 3


def test_delete_rows_calls_batch_update():
    """delete_rows queues request that calls batch_update."""
    ws, mock_gspread, mock_ws = _create_mock_worksheet_with_api()

    ws.delete_rows(2, 2)
    ws.flush()

    mock_gspread.batch_update.assert_called_once()
    call_args = mock_gspread.batch_update.call_args[0][0]
    request = call_args['requests'][0]

    assert 'deleteDimension' in request
    assert request['deleteDimension']['range']['dimension'] == 'ROWS'
    assert request['deleteDimension']['range']['startIndex'] == 1  # 2 - 1
    assert request['deleteDimension']['range']['endIndex'] == 3  # 1 + 2


def test_insert_columns_calls_batch_update():
    """insert_columns queues request that calls batch_update."""
    ws, mock_gspread, mock_ws = _create_mock_worksheet_with_api()

    ws.insert_columns(3, 2)
    ws.flush()

    mock_gspread.batch_update.assert_called_once()
    call_args = mock_gspread.batch_update.call_args[0][0]
    request = call_args['requests'][0]

    assert 'insertDimension' in request
    assert request['insertDimension']['range']['dimension'] == 'COLUMNS'
    assert request['insertDimension']['range']['startIndex'] == 2  # 3 - 1
    assert request['insertDimension']['range']['endIndex'] == 4  # 2 + 2


def test_delete_columns_calls_batch_update():
    """delete_columns queues request that calls batch_update."""
    ws, mock_gspread, mock_ws = _create_mock_worksheet_with_api()

    ws.delete_columns(1, 1)
    ws.flush()

    mock_gspread.batch_update.assert_called_once()
    call_args = mock_gspread.batch_update.call_args[0][0]
    request = call_args['requests'][0]

    assert 'deleteDimension' in request
    assert request['deleteDimension']['range']['dimension'] == 'COLUMNS'
    assert request['deleteDimension']['range']['startIndex'] == 0


def test_freeze_rows_calls_batch_update():
    """freeze_rows queues request that calls batch_update."""
    ws, mock_gspread, mock_ws = _create_mock_worksheet_with_api()

    ws.freeze_rows(2)
    ws.flush()

    mock_gspread.batch_update.assert_called_once()
    call_args = mock_gspread.batch_update.call_args[0][0]
    request = call_args['requests'][0]

    assert 'updateSheetProperties' in request
    props = request['updateSheetProperties']['properties']

    assert props['gridProperties']['frozenRowCount'] == 2


def test_freeze_columns_calls_batch_update():
    """freeze_columns queues request that calls batch_update."""
    ws, mock_gspread, mock_ws = _create_mock_worksheet_with_api()

    ws.freeze_columns(1)
    ws.flush()

    mock_gspread.batch_update.assert_called_once()
    call_args = mock_gspread.batch_update.call_args[0][0]
    request = call_args['requests'][0]

    assert 'updateSheetProperties' in request
    props = request['updateSheetProperties']['properties']

    assert props['gridProperties']['frozenColumnCount'] == 1


def test_unknown_request_type_raises_value_error():
    """Unknown batch request type raises ValueError."""
    ws, mock_gspread, mock_ws = _create_mock_worksheet_with_api()

    # Manually add an unknown request type
    ws._batch_requests.append({'type': 'unknown_type', 'data': 'whatever'})

    with pytest.raises(ValueError, match="Unknown batch request type: 'unknown_type'"):
        ws.flush()


def test_multiple_batch_requests_all_executed():
    """Multiple batch requests of different types are all executed."""
    ws, mock_gspread, mock_ws = _create_mock_worksheet_with_api()

    ws.set_column_width('A', 100)
    ws.auto_resize_columns(2, 3)
    ws.freeze_rows(1)
    ws.flush()

    # batch_update should be called 3 times (once per request)
    assert mock_gspread.batch_update.call_count == 3


def test_format_and_column_width_both_executed():
    """Both format and column_width requests execute correctly."""
    ws, mock_gspread, mock_ws = _create_mock_worksheet_with_api()

    ws.format_range('A1', {'bold': True})
    ws.set_column_width('A', 200)
    ws.flush()

    mock_ws.format.assert_called_once_with('A1', {'bold': True})
    mock_gspread.batch_update.assert_called_once()


def test_context_manager_flushes_column_width():
    """Column width is applied when worksheet context manager exits."""
    ws, mock_gspread, mock_ws = _create_mock_worksheet_with_api()

    # Simulate context manager behavior
    ws.__enter__()
    ws.set_column_width('C', 300)
    ws.__exit__(None, None, None)

    mock_gspread.batch_update.assert_called_once()
    call_args = mock_gspread.batch_update.call_args[0][0]
    request = call_args['requests'][0]

    assert request['updateDimensionProperties']['properties']['pixelSize'] == 300


def test_range_with_sheet_name_prefix_is_parsed_correctly():
    """Range with sheet name prefix (Sheet1!A1:B2) is parsed correctly."""
    ws, mock_gspread, mock_ws = _create_mock_worksheet_with_api()

    # Use merge_cells which calls _parse_range_to_grid_range
    ws.merge_cells('Sheet1!A1:C3', 'MERGE_ALL')
    ws.flush()

    mock_gspread.batch_update.assert_called_once()
    call_args = mock_gspread.batch_update.call_args[0][0]
    request = call_args['requests'][0]

    # Verify the range was parsed correctly (sheet name stripped)
    range_obj = request['mergeCells']['range']
    assert range_obj['startRowIndex'] == 0  # A1 row
    assert range_obj['startColumnIndex'] == 0  # A column
    assert range_obj['endRowIndex'] == 3  # C3 row (exclusive)
    assert range_obj['endColumnIndex'] == 3  # C column (exclusive)


# --- Read Operations ---


def test_read_cell_calls_gspread_acell():
    """read_cell calls gspread worksheet's acell method."""
    ws, mock_gspread, mock_ws = _create_mock_worksheet_with_api()

    # Setup mock return value
    mock_cell = MagicMock()
    mock_cell.value = 'test_value'
    mock_ws.acell.return_value = mock_cell

    result = ws.read_cell('V5')

    mock_ws.acell.assert_called_once_with('V5')
    assert result == 'test_value'


def test_read_cell_returns_empty_string_for_blank():
    """read_cell returns empty string for blank cell."""
    ws, mock_gspread, mock_ws = _create_mock_worksheet_with_api()

    mock_cell = MagicMock()
    mock_cell.value = ''
    mock_ws.acell.return_value = mock_cell

    result = ws.read_cell('A1')

    assert result == ''


def test_read_range_calls_gspread_get():
    """read_range calls gspread worksheet's get method."""
    ws, mock_gspread, mock_ws = _create_mock_worksheet_with_api()

    mock_ws.get.return_value = [['val1'], ['val2'], ['val3']]

    result = ws.read_range('V5:V7')

    mock_ws.get.assert_called_once_with('V5:V7')
    assert result == [['val1'], ['val2'], ['val3']]


def test_read_range_returns_2d_list_for_multiple_columns():
    """read_range returns 2D list for multi-column ranges."""
    ws, mock_gspread, mock_ws = _create_mock_worksheet_with_api()

    mock_ws.get.return_value = [['a', 'b'], ['c', 'd']]

    result = ws.read_range('A1:B2')

    assert result == [['a', 'b'], ['c', 'd']]
