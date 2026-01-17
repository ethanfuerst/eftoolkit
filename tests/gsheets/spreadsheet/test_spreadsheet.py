"""Tests for Spreadsheet class."""

from unittest.mock import MagicMock, patch

import pytest
from gspread.exceptions import WorksheetNotFound

from eftoolkit.gsheets import Spreadsheet


def test_spreadsheet_local_preview_mode():
    """Spreadsheet initializes in local preview mode without credentials."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')

    assert ss.is_local_preview is True
    assert ss._gspread_spreadsheet is None


def test_spreadsheet_requires_credentials():
    """Spreadsheet raises ValueError when credentials missing in normal mode."""
    with pytest.raises(ValueError, match='credentials required'):
        Spreadsheet(spreadsheet_name='Test')


def test_spreadsheet_context_manager():
    """Spreadsheet works as context manager."""
    with Spreadsheet(local_preview=True, spreadsheet_name='Test') as ss:
        assert ss.is_local_preview is True


def test_spreadsheet_init_with_credentials():
    """Spreadsheet initializes with mocked gspread connection."""
    with patch('eftoolkit.gsheets.sheet.service_account_from_dict') as mock_sa:
        mock_gc = MagicMock()
        mock_spreadsheet = MagicMock()
        mock_gc.open.return_value = mock_spreadsheet
        mock_sa.return_value = mock_gc

        ss = Spreadsheet(
            credentials={'type': 'service_account'},
            spreadsheet_name='TestSheet',
        )

        mock_sa.assert_called_once_with({'type': 'service_account'})
        mock_gc.open.assert_called_once_with('TestSheet')
        assert ss._gspread_spreadsheet == mock_spreadsheet


def test_spreadsheet_worksheet_local_preview():
    """worksheet() returns Worksheet in local preview mode."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')

    ws = ss.worksheet('Sheet1')

    assert ws.is_local_preview is True
    assert ws._worksheet_name == 'Sheet1'


def test_spreadsheet_worksheet_local_preview_returns_cached():
    """worksheet() returns same Worksheet instance when called twice."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')

    ws1 = ss.worksheet('Sheet1')
    ws2 = ss.worksheet('Sheet1')

    assert ws1 is ws2


def test_spreadsheet_worksheet_returns_worksheet():
    """worksheet() returns Worksheet wrapping gspread worksheet."""
    mock_gspread = MagicMock()
    mock_ws = MagicMock()
    mock_ws.title = 'Sheet1'
    mock_gspread.worksheet.return_value = mock_ws

    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ss._local_preview = False
    ss._gspread_spreadsheet = mock_gspread

    ws = ss.worksheet('Sheet1')

    assert ws._ws == mock_ws
    assert ws.title == 'Sheet1'


def test_spreadsheet_get_worksheet_names_local_preview():
    """get_worksheet_names() returns empty list in local preview mode."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')

    result = ss.get_worksheet_names()

    assert result == []


def test_spreadsheet_get_worksheet_names_returns_titles():
    """get_worksheet_names() returns list of worksheet titles."""
    mock_gspread = MagicMock()
    mock_ws1 = MagicMock()
    mock_ws1.title = 'Sheet1'
    mock_ws2 = MagicMock()
    mock_ws2.title = 'Sheet2'
    mock_gspread.worksheets.return_value = [mock_ws1, mock_ws2]

    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ss._local_preview = False
    ss._gspread_spreadsheet = mock_gspread

    result = ss.get_worksheet_names()

    assert result == ['Sheet1', 'Sheet2']


def test_spreadsheet_create_worksheet_local_preview():
    """create_worksheet() returns Worksheet in local preview mode."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')

    ws = ss.create_worksheet('NewSheet')

    assert ws.is_local_preview is True
    assert ws._worksheet_name == 'NewSheet'


def test_spreadsheet_create_worksheet_local_preview_returns_cached():
    """create_worksheet() returns same Worksheet instance when called twice."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')

    ws1 = ss.create_worksheet('NewSheet')
    ws2 = ss.create_worksheet('NewSheet')

    assert ws1 is ws2


def test_spreadsheet_create_worksheet_without_replace():
    """create_worksheet without replace creates worksheet directly."""
    mock_gspread = MagicMock()
    mock_ws = MagicMock()
    mock_ws.title = 'NewSheet'
    mock_gspread.add_worksheet.return_value = mock_ws

    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ss._local_preview = False
    ss._gspread_spreadsheet = mock_gspread

    ws = ss.create_worksheet('NewSheet', replace=False)

    mock_gspread.del_worksheet.assert_not_called()
    mock_gspread.add_worksheet.assert_called_once_with(
        title='NewSheet', rows=1000, cols=26
    )
    assert ws._ws == mock_ws


def test_spreadsheet_create_worksheet_with_replace():
    """create_worksheet with replace=True deletes existing first."""
    mock_gspread = MagicMock()
    mock_ws = MagicMock()
    mock_gspread.worksheet.return_value = mock_ws
    mock_gspread.add_worksheet.return_value = MagicMock(title='NewSheet')

    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ss._local_preview = False
    ss._gspread_spreadsheet = mock_gspread

    ss.create_worksheet('NewSheet', replace=True)

    mock_gspread.del_worksheet.assert_called_once_with(mock_ws)
    mock_gspread.add_worksheet.assert_called_once()


def test_spreadsheet_delete_worksheet_local_preview():
    """delete_worksheet() is a no-op in local preview mode."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')

    # Should not raise
    ss.delete_worksheet('Sheet1')


def test_spreadsheet_delete_worksheet_success():
    """delete_worksheet deletes existing worksheet."""
    mock_gspread = MagicMock()
    mock_ws = MagicMock()
    mock_gspread.worksheet.return_value = mock_ws

    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ss._local_preview = False
    ss._gspread_spreadsheet = mock_gspread

    ss.delete_worksheet('Sheet1')

    mock_gspread.worksheet.assert_called_once_with('Sheet1')
    mock_gspread.del_worksheet.assert_called_once_with(mock_ws)


def test_spreadsheet_delete_worksheet_ignore_missing():
    """delete_worksheet with ignore_missing=True doesn't raise."""
    mock_gspread = MagicMock()
    mock_gspread.worksheet.side_effect = WorksheetNotFound('Sheet1')

    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ss._local_preview = False
    ss._gspread_spreadsheet = mock_gspread

    # Should not raise
    ss.delete_worksheet('Sheet1', ignore_missing=True)


def test_spreadsheet_delete_worksheet_raises_when_not_ignoring():
    """delete_worksheet with ignore_missing=False raises WorksheetNotFound."""
    mock_gspread = MagicMock()
    mock_gspread.worksheet.side_effect = WorksheetNotFound('Sheet1')

    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ss._local_preview = False
    ss._gspread_spreadsheet = mock_gspread

    with pytest.raises(WorksheetNotFound):
        ss.delete_worksheet('Sheet1', ignore_missing=False)


def test_preview_path_sanitizes_names():
    """_preview_path_for_worksheet sanitizes special characters."""
    ss = Spreadsheet(
        local_preview=True, spreadsheet_name='My Sheet/Test', preview_dir='previews'
    )

    path = ss._preview_path_for_worksheet('Tab/Name')

    assert '/' not in path.name
    assert ' ' not in path.name


def test_spreadsheet_context_manager_flushes_worksheets(tmp_path):
    """Spreadsheet context manager flushes all accessed worksheets on exit."""
    with patch('webbrowser.open'):  # Suppress browser opening
        with Spreadsheet(
            local_preview=True, spreadsheet_name='Test', preview_dir=str(tmp_path)
        ) as ss:
            ws1 = ss.worksheet('Sheet1')
            ws1.write_values('A1', [['data1']])
            ws2 = ss.worksheet('Sheet2')
            ws2.write_values('A1', [['data2']])

    # Both worksheets should have been flushed (queues cleared)
    assert len(ws1._value_updates) == 0
    assert len(ws2._value_updates) == 0

    # HTML files should exist for both
    html_files = list(tmp_path.glob('*.html'))
    assert len(html_files) == 2


def test_spreadsheet_context_manager_no_flush_on_error():
    """Spreadsheet context manager does not flush worksheets on exception."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    try:
        with ss:
            ws = ss.worksheet('Sheet1')
            ws.write_values('A1', [['data']])
            raise ValueError('Test error')
    except ValueError:
        pass

    # Queue should still have the value since flush was skipped
    assert len(ws._value_updates) == 1


def test_spreadsheet_context_manager_flush_is_idempotent(tmp_path):
    """Flushing via worksheet then spreadsheet context is idempotent."""
    with patch('webbrowser.open'):  # Suppress browser opening
        with Spreadsheet(
            local_preview=True, spreadsheet_name='Test', preview_dir=str(tmp_path)
        ) as ss:
            with ss.worksheet('Sheet1') as ws:
                ws.write_values('A1', [['data']])
            # ws already flushed here

    # Should not raise, and still have one HTML file
    html_files = list(tmp_path.glob('*.html'))
    assert len(html_files) == 1


def test_spreadsheet_context_manager_opens_previews_in_local_mode(tmp_path):
    """Spreadsheet context manager opens previews in browser in local_preview mode."""
    with patch('webbrowser.open') as mock_open:
        with Spreadsheet(
            local_preview=True, spreadsheet_name='Test', preview_dir=str(tmp_path)
        ) as ss:
            ss.worksheet('Sheet1').write_values('A1', [['data1']])
            ss.worksheet('Sheet2').write_values('A1', [['data2']])

    # Should have opened browser for both worksheets
    assert mock_open.call_count == 2


def test_spreadsheet_context_manager_no_preview_in_normal_mode():
    """Spreadsheet context manager does not open browser in normal mode."""
    with patch('eftoolkit.gsheets.sheet.service_account_from_dict') as mock_sa:
        mock_gc = MagicMock()
        mock_spreadsheet = MagicMock()
        mock_gc.open.return_value = mock_spreadsheet
        mock_sa.return_value = mock_gc

        with patch('webbrowser.open') as mock_open:
            with Spreadsheet(
                credentials={'type': 'service_account'},
                spreadsheet_name='Test',
            ):
                pass

        # Should not have opened browser
        mock_open.assert_not_called()


def test_spreadsheet_open_all_previews_raises_in_normal_mode():
    """open_all_previews() raises RuntimeError when not in preview mode."""
    with patch('eftoolkit.gsheets.sheet.service_account_from_dict') as mock_sa:
        mock_gc = MagicMock()
        mock_gc.open.return_value = MagicMock()
        mock_sa.return_value = mock_gc

        ss = Spreadsheet(
            credentials={'type': 'service_account'},
            spreadsheet_name='Test',
        )

        with pytest.raises(RuntimeError, match='local_preview mode'):
            ss.open_all_previews()


def test_spreadsheet_open_all_previews_opens_browser(tmp_path):
    """open_all_previews() opens browser for each worksheet."""
    ss = Spreadsheet(
        local_preview=True, spreadsheet_name='Test', preview_dir=str(tmp_path)
    )
    ws1 = ss.worksheet('Sheet1')
    ws1.write_values('A1', [['test1']])
    ws1.flush()
    ws2 = ss.worksheet('Sheet2')
    ws2.write_values('A1', [['test2']])
    ws2.flush()

    with patch('webbrowser.open') as mock_open:
        ss.open_all_previews()

        assert mock_open.call_count == 2


def test_spreadsheet_create_worksheet_with_replace_clears_cache():
    """create_worksheet with replace=True clears cached worksheet."""
    mock_gspread = MagicMock()
    mock_ws_old = MagicMock()
    mock_ws_new = MagicMock()
    mock_ws_new.title = 'Sheet1'
    mock_gspread.worksheet.return_value = mock_ws_old
    mock_gspread.add_worksheet.return_value = mock_ws_new

    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ss._local_preview = False
    ss._gspread_spreadsheet = mock_gspread

    # First, access the worksheet to cache it
    ss.worksheet('Sheet1')
    assert 'Sheet1' in ss._worksheets

    # Now create with replace
    ws2 = ss.create_worksheet('Sheet1', replace=True)

    # Should be a different instance
    assert ws2._ws == mock_ws_new
    assert ss._worksheets['Sheet1'] is ws2


def test_reorder_worksheets_local_preview():
    """reorder_worksheets() is a no-op in local preview mode."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')

    # Should not raise
    ss.reorder_worksheets(['Sheet1', 'Sheet2'])


def test_reorder_worksheets_reorders_to_specified_order():
    """reorder_worksheets() reorders worksheets to match specified order."""
    mock_gspread = MagicMock()
    mock_ws1 = MagicMock()
    mock_ws1.title = 'Alpha'
    mock_ws2 = MagicMock()
    mock_ws2.title = 'Beta'
    mock_ws3 = MagicMock()
    mock_ws3.title = 'Gamma'
    mock_gspread.worksheets.return_value = [mock_ws1, mock_ws2, mock_ws3]

    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ss._local_preview = False
    ss._gspread_spreadsheet = mock_gspread

    ss.reorder_worksheets(['Gamma', 'Alpha', 'Beta'])

    mock_gspread.reorder_worksheets.assert_called_once()
    call_args = mock_gspread.reorder_worksheets.call_args[0][0]

    assert [ws.title for ws in call_args] == ['Gamma', 'Alpha', 'Beta']


def test_reorder_worksheets_unspecified_tabs_at_end():
    """reorder_worksheets() moves unspecified tabs to end in original order."""
    mock_gspread = MagicMock()
    mock_ws1 = MagicMock()
    mock_ws1.title = 'Alpha'
    mock_ws2 = MagicMock()
    mock_ws2.title = 'Beta'
    mock_ws3 = MagicMock()
    mock_ws3.title = 'Gamma'
    mock_ws4 = MagicMock()
    mock_ws4.title = 'Delta'
    mock_gspread.worksheets.return_value = [mock_ws1, mock_ws2, mock_ws3, mock_ws4]

    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ss._local_preview = False
    ss._gspread_spreadsheet = mock_gspread

    # Only specify Gamma and Alpha, Beta and Delta should follow in original order
    ss.reorder_worksheets(['Gamma', 'Alpha'])

    call_args = mock_gspread.reorder_worksheets.call_args[0][0]

    assert [ws.title for ws in call_args] == ['Gamma', 'Alpha', 'Beta', 'Delta']


def test_reorder_worksheets_skips_missing_tabs():
    """reorder_worksheets() skips tabs that don't exist."""
    mock_gspread = MagicMock()
    mock_ws1 = MagicMock()
    mock_ws1.title = 'Alpha'
    mock_ws2 = MagicMock()
    mock_ws2.title = 'Beta'
    mock_gspread.worksheets.return_value = [mock_ws1, mock_ws2]

    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ss._local_preview = False
    ss._gspread_spreadsheet = mock_gspread

    # 'Missing' doesn't exist, should be skipped
    ss.reorder_worksheets(['Missing', 'Beta', 'Alpha'])

    call_args = mock_gspread.reorder_worksheets.call_args[0][0]

    assert [ws.title for ws in call_args] == ['Beta', 'Alpha']


def test_reorder_worksheets_empty_order_preserves_original():
    """reorder_worksheets() with empty list preserves original order."""
    mock_gspread = MagicMock()
    mock_ws1 = MagicMock()
    mock_ws1.title = 'Alpha'
    mock_ws2 = MagicMock()
    mock_ws2.title = 'Beta'
    mock_gspread.worksheets.return_value = [mock_ws1, mock_ws2]

    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ss._local_preview = False
    ss._gspread_spreadsheet = mock_gspread

    ss.reorder_worksheets([])

    call_args = mock_gspread.reorder_worksheets.call_args[0][0]

    assert [ws.title for ws in call_args] == ['Alpha', 'Beta']
