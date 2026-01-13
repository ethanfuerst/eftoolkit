"""Tests for Worksheet flush, read, and preview functionality."""

from unittest.mock import MagicMock, patch

import pytest

from eftoolkit.gsheets import Spreadsheet, Worksheet

# --- Flush Tests ---


def test_worksheet_flush_clears_queues():
    """flush() clears all queues."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    ws.write_values('A1', [[1]])
    ws.format_range('A1', {'bold': True})
    ws.flush()

    assert len(ws._value_updates) == 0
    assert len(ws._batch_requests) == 0


def test_worksheet_context_manager_flushes():
    """Worksheet context manager flushes on clean exit."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')

    with ss.worksheet('Sheet1') as ws:
        ws.write_values('A1', [[1]])

    # After context, queues should be cleared
    assert len(ws._value_updates) == 0


def test_worksheet_context_manager_no_flush_on_error():
    """Worksheet context manager does not flush on exception."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')

    try:
        with ss.worksheet('Sheet1') as ws:
            ws.write_values('A1', [[1]])
            raise ValueError('Test error')
    except ValueError:
        pass

    # Queue should still have the value since flush was skipped
    assert len(ws._value_updates) == 1


def test_worksheet_flush_calls_api():
    """flush() calls gspread batch update when not in preview mode."""
    mock_gspread = MagicMock()
    mock_ws = MagicMock()
    mock_ws.title = 'Sheet1'

    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ss._local_preview = False
    ss._gspread_spreadsheet = mock_gspread

    ws = Worksheet(mock_ws, ss)
    ws.write_values('A1', [[1, 2]])
    ws.flush()

    mock_gspread.values_batch_update.assert_called_once()


def test_worksheet_flush_with_format_calls_api():
    """flush() calls format on worksheet when format requests queued."""
    mock_gspread = MagicMock()
    mock_ws = MagicMock()
    mock_ws.title = 'Sheet1'

    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ss._local_preview = False
    ss._gspread_spreadsheet = mock_gspread

    ws = Worksheet(mock_ws, ss)
    ws.format_range('A1', {'bold': True})
    ws.flush()

    mock_ws.format.assert_called_once_with('A1', {'bold': True})


def test_worksheet_flush_to_api_with_no_ws():
    """_flush_to_api returns early when _ws is None."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')
    ws._local_preview = False  # Switch to API mode but ws is None

    # Should not raise
    ws._flush_to_api()


def test_worksheet_flush_with_non_format_batch_request():
    """flush() handles batch requests that are not format type."""
    mock_gspread = MagicMock()
    mock_ws = MagicMock()
    mock_ws.title = 'Sheet1'

    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ss._local_preview = False
    ss._gspread_spreadsheet = mock_gspread

    ws = Worksheet(mock_ws, ss)
    # Add a non-format request (e.g., border)
    ws.set_borders('A1', {'top': {'style': 'SOLID'}})
    ws.flush()

    # format should not be called since it's a border request
    mock_ws.format.assert_not_called()


# --- Read Tests ---


def test_worksheet_read_returns_dataframe():
    """read() returns DataFrame from worksheet."""
    mock_ws = MagicMock()
    mock_ws.get_all_values.return_value = [['a', 'b'], [1, 2], [3, 4]]

    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = Worksheet(mock_ws, ss)

    result = ws.read()

    assert list(result.columns) == ['a', 'b']
    assert len(result) == 2


def test_worksheet_read_empty_returns_empty_dataframe():
    """read() returns empty DataFrame when worksheet is empty."""
    mock_ws = MagicMock()
    mock_ws.get_all_values.return_value = []

    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = Worksheet(mock_ws, ss)

    result = ws.read()

    assert result.empty


def test_worksheet_read_raises_in_preview_mode():
    """read() raises NotImplementedError in local preview mode."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    with pytest.raises(NotImplementedError):
        ws.read()


# --- Title Tests ---


def test_worksheet_title_in_preview_mode():
    """title property returns preview title in local preview mode."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = ss.worksheet('Sheet1')

    assert ws.title == 'Local Preview - Sheet1'


def test_worksheet_title_from_gspread():
    """title property returns gspread title in normal mode."""
    mock_ws = MagicMock()
    mock_ws.title = 'RealSheet'

    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = Worksheet(mock_ws, ss)

    assert ws.title == 'RealSheet'


# --- Preview Tests ---


def test_worksheet_flush_to_preview_creates_html(tmp_path):
    """_flush_to_preview creates HTML file."""
    ss = Spreadsheet(
        local_preview=True, spreadsheet_name='Test', preview_dir=str(tmp_path)
    )
    ws = ss.worksheet('Sheet1')

    ws.write_values('A1', [['Hello', 'World']])
    ws.flush()

    # Check that HTML file was created
    html_files = list(tmp_path.glob('*.html'))
    assert len(html_files) == 1

    content = html_files[0].read_text()
    assert 'Hello' in content
    assert 'World' in content


def test_worksheet_open_preview_raises_in_normal_mode():
    """open_preview() raises RuntimeError when not in preview mode."""
    mock_ws = MagicMock()
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test')
    ws = Worksheet(mock_ws, ss)

    with pytest.raises(RuntimeError, match='local_preview mode'):
        ws.open_preview()


def test_worksheet_open_preview_opens_browser(tmp_path):
    """open_preview() opens browser with file path."""
    ss = Spreadsheet(
        local_preview=True, spreadsheet_name='Test', preview_dir=str(tmp_path)
    )
    ws = ss.worksheet('Sheet1')
    ws.write_values('A1', [['test']])
    ws.flush()

    with patch('webbrowser.open') as mock_open:
        ws.open_preview()
        mock_open.assert_called_once()
        assert 'file://' in mock_open.call_args[0][0]
