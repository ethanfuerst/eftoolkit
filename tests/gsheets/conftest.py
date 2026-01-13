"""Shared fixtures for Google Sheets tests."""

from unittest.mock import MagicMock

import pytest
from gspread.exceptions import APIError


class MockResponse:
    """Mock response object for APIError."""

    def __init__(self, status_code: int):
        self.status_code = status_code
        self.text = f'Error {status_code}'

    def json(self):
        """Return error JSON matching gspread expected format."""
        return {
            'error': {
                'code': self.status_code,
                'message': f'Error {self.status_code}',
                'status': 'ERROR',
            }
        }


def create_api_error(status_code: int) -> APIError:
    """Create an APIError with the given status code."""
    response = MockResponse(status_code)
    error = APIError(response)
    return error


@pytest.fixture
def mock_gspread_spreadsheet():
    """Create a mock gspread spreadsheet object."""
    return MagicMock()


@pytest.fixture
def mock_gspread_worksheet():
    """Create a mock gspread worksheet object."""
    mock_ws = MagicMock()
    mock_ws.title = 'Sheet1'
    return mock_ws
