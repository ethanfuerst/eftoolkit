"""Tests for retry logic with exponential backoff."""

from unittest.mock import patch

import pytest

from eftoolkit.gsheets import Spreadsheet
from tests.gsheets.conftest import create_api_error


def test_retry_on_429_error():
    """_execute_with_retry retries on 429 error."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test', max_retries=3)

    call_count = 0

    def flaky_func():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise create_api_error(429)
        return 'success'

    with patch('time.sleep'):
        result = ss._execute_with_retry(flaky_func, 'test_op')

    assert result == 'success'
    assert call_count == 3


def test_retry_on_500_error():
    """_execute_with_retry retries on 500 error."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test', max_retries=2)

    call_count = 0

    def flaky_func():
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise create_api_error(500)
        return 'success'

    with patch('time.sleep'):
        result = ss._execute_with_retry(flaky_func, 'test_op')

    assert result == 'success'
    assert call_count == 2


def test_retry_on_502_error():
    """_execute_with_retry retries on 502 error."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test', max_retries=2)

    call_count = 0

    def flaky_func():
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise create_api_error(502)
        return 'success'

    with patch('time.sleep'):
        result = ss._execute_with_retry(flaky_func, 'test_op')

    assert result == 'success'


def test_retry_on_503_error():
    """_execute_with_retry retries on 503 error."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test', max_retries=2)

    call_count = 0

    def flaky_func():
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise create_api_error(503)
        return 'success'

    with patch('time.sleep'):
        result = ss._execute_with_retry(flaky_func, 'test_op')

    assert result == 'success'


def test_retry_on_504_error():
    """_execute_with_retry retries on 504 error."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test', max_retries=2)

    call_count = 0

    def flaky_func():
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise create_api_error(504)
        return 'success'

    with patch('time.sleep'):
        result = ss._execute_with_retry(flaky_func, 'test_op')

    assert result == 'success'


def test_retry_exponential_backoff():
    """_execute_with_retry uses exponential backoff delays."""
    ss = Spreadsheet(
        local_preview=True, spreadsheet_name='Test', max_retries=3, base_delay=1.0
    )

    call_count = 0

    def flaky_func():
        nonlocal call_count
        call_count += 1
        if call_count <= 3:
            raise create_api_error(429)
        return 'success'

    with patch('time.sleep') as mock_sleep:
        with patch('random.uniform', return_value=0.5):
            result = ss._execute_with_retry(flaky_func, 'test_op')
            delays = [call[0][0] for call in mock_sleep.call_args_list]

    assert result == 'success'
    # With base_delay=1.0 and random.uniform returning 0.5:
    # attempt 0: 1.0 * 2^0 + 0.5 = 1.5
    # attempt 1: 1.0 * 2^1 + 0.5 = 2.5
    # attempt 2: 1.0 * 2^2 + 0.5 = 4.5
    assert delays == [1.5, 2.5, 4.5]


def test_max_retries_exhausted():
    """_execute_with_retry raises after max retries exhausted."""
    from gspread.exceptions import APIError

    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test', max_retries=2)

    def always_fails():
        raise create_api_error(429)

    with patch('time.sleep'):
        with pytest.raises(APIError):
            ss._execute_with_retry(always_fails, 'test_op')


def test_non_retryable_error_raises_immediately():
    """_execute_with_retry raises immediately on non-retryable error."""
    from gspread.exceptions import APIError

    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test', max_retries=5)

    call_count = 0

    def fails_with_400():
        nonlocal call_count
        call_count += 1
        raise create_api_error(400)

    with pytest.raises(APIError):
        ss._execute_with_retry(fails_with_400, 'test_op')

    assert call_count == 1  # No retries for 400


def test_success_on_first_try():
    """_execute_with_retry returns immediately on success."""
    ss = Spreadsheet(local_preview=True, spreadsheet_name='Test', max_retries=5)

    def succeeds():
        return 'success'

    result = ss._execute_with_retry(succeeds, 'test_op')

    assert result == 'success'
