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
