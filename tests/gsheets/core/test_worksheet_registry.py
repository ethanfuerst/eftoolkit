"""Tests for WorksheetRegistry."""

import threading
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import pytest

from eftoolkit.gsheets.registry import WorksheetRegistry
from eftoolkit.gsheets.types import CellLocation, WorksheetAsset


class MockWorksheet:
    """A mock worksheet for testing."""

    def __init__(self, worksheet_name: str):
        self._name = worksheet_name

    @property
    def name(self) -> str:
        return self._name

    def generate(self, config: dict, context: dict) -> list[WorksheetAsset]:
        return [
            WorksheetAsset(
                df=pd.DataFrame({'a': [1]}),
                location=CellLocation(cell='A1'),
            )
        ]

    def get_format_overrides(self, context: dict) -> dict:
        return {}


@pytest.fixture(autouse=True)
def clear_registry():
    """Clear registry before and after each test."""
    WorksheetRegistry.clear()
    yield
    WorksheetRegistry.clear()


def test_register_single_worksheet():
    """Register a single worksheet."""
    ws = MockWorksheet('Summary')

    WorksheetRegistry.register(ws)
    result = WorksheetRegistry.get_ordered_worksheets()

    assert len(result) == 1
    assert result[0].name == 'Summary'


def test_register_multiple_worksheets_maintains_order():
    """Multiple worksheets maintain registration order."""
    ws1 = MockWorksheet('Summary')
    ws2 = MockWorksheet('Revenue')
    ws3 = MockWorksheet('Expenses')

    WorksheetRegistry.register(ws1)
    WorksheetRegistry.register(ws2)
    WorksheetRegistry.register(ws3)

    result = WorksheetRegistry.get_ordered_worksheets()

    assert len(result) == 3
    assert [ws.name for ws in result] == ['Summary', 'Revenue', 'Expenses']


def test_register_list_maintains_order():
    """Registering a list maintains list order."""
    ws1 = MockWorksheet('Summary')
    ws2 = MockWorksheet('Revenue')
    ws3 = MockWorksheet('Expenses')

    WorksheetRegistry.register([ws1, ws2, ws3])
    result = WorksheetRegistry.get_ordered_worksheets()

    assert len(result) == 3
    assert [ws.name for ws in result] == ['Summary', 'Revenue', 'Expenses']


def test_register_empty_list():
    """Registering an empty list does nothing."""
    WorksheetRegistry.register([])

    assert WorksheetRegistry.get_ordered_worksheets() == []


def test_register_duplicate_raises_value_error():
    """Registering duplicate worksheet name raises ValueError."""
    ws1 = MockWorksheet('Summary')
    ws2 = MockWorksheet('Summary')

    WorksheetRegistry.register(ws1)

    with pytest.raises(ValueError, match="'Summary' is already registered"):
        WorksheetRegistry.register(ws2)


def test_get_worksheet_returns_registered():
    """get_worksheet returns the registered worksheet."""
    ws = MockWorksheet('Revenue')

    WorksheetRegistry.register(ws)
    result = WorksheetRegistry.get_worksheet('Revenue')

    assert result is ws


def test_get_worksheet_returns_none_for_unknown():
    """get_worksheet returns None for unknown worksheet."""
    result = WorksheetRegistry.get_worksheet('NonExistent')

    assert result is None


def test_clear_removes_all_worksheets():
    """clear() removes all registered worksheets."""
    ws1 = MockWorksheet('Summary')
    ws2 = MockWorksheet('Revenue')

    WorksheetRegistry.register(ws1)
    WorksheetRegistry.register(ws2)
    WorksheetRegistry.clear()

    assert WorksheetRegistry.get_ordered_worksheets() == []
    assert WorksheetRegistry.get_worksheet('Summary') is None


def test_clear_allows_re_registration():
    """After clear(), worksheets can be re-registered."""
    ws1 = MockWorksheet('Summary')

    WorksheetRegistry.register(ws1)
    WorksheetRegistry.clear()
    WorksheetRegistry.register(ws1)

    result = WorksheetRegistry.get_ordered_worksheets()

    assert len(result) == 1
    assert result[0].name == 'Summary'


def test_register_list_duplicate_raises_value_error():
    """Registering a list with duplicate names raises ValueError."""
    ws1 = MockWorksheet('Summary')
    ws2 = MockWorksheet('Summary')

    with pytest.raises(ValueError, match="'Summary' is already registered"):
        WorksheetRegistry.register([ws1, ws2])


def test_register_list_duplicate_with_existing_raises():
    """Registering a list fails if name already registered."""
    ws1 = MockWorksheet('Summary')
    ws2 = MockWorksheet('Revenue')

    WorksheetRegistry.register(ws1)

    with pytest.raises(ValueError, match="'Summary' is already registered"):
        WorksheetRegistry.register([MockWorksheet('Summary'), ws2])


def test_reorder_changes_order():
    """reorder changes the worksheet order."""
    ws1 = MockWorksheet('Summary')
    ws2 = MockWorksheet('Revenue')
    ws3 = MockWorksheet('Expenses')

    WorksheetRegistry.register([ws1, ws2, ws3])
    WorksheetRegistry.reorder(['Expenses', 'Summary', 'Revenue'])
    result = WorksheetRegistry.get_ordered_worksheets()

    assert [ws.name for ws in result] == ['Expenses', 'Summary', 'Revenue']


def test_reorder_with_missing_names_raises():
    """reorder raises ValueError if names are missing."""
    ws1 = MockWorksheet('Summary')
    ws2 = MockWorksheet('Revenue')

    WorksheetRegistry.register([ws1, ws2])

    with pytest.raises(ValueError, match='Missing worksheets'):
        WorksheetRegistry.reorder(['Summary'])


def test_reorder_with_extra_names_raises():
    """reorder raises ValueError if extra names provided."""
    ws1 = MockWorksheet('Summary')

    WorksheetRegistry.register(ws1)

    with pytest.raises(ValueError, match='Unknown worksheets'):
        WorksheetRegistry.reorder(['Summary', 'NonExistent'])


def test_reorder_with_duplicates_raises():
    """reorder raises ValueError if duplicate names provided."""
    ws1 = MockWorksheet('Summary')
    ws2 = MockWorksheet('Revenue')

    WorksheetRegistry.register([ws1, ws2])

    with pytest.raises(ValueError, match='Duplicate names'):
        WorksheetRegistry.reorder(['Summary', 'Summary'])


def test_reorder_empty_registry():
    """reorder with empty list on empty registry succeeds."""
    WorksheetRegistry.reorder([])

    assert WorksheetRegistry.get_ordered_worksheets() == []


def test_thread_safety_concurrent_registration():
    """Registry is thread-safe for concurrent registrations."""
    errors = []

    def register_worksheet(name: str):
        try:
            ws = MockWorksheet(name)
            WorksheetRegistry.register(ws)
        except Exception as e:
            errors.append(e)

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(register_worksheet, f'Sheet{i}') for i in range(100)]
        for future in futures:
            future.result()

    assert len(errors) == 0
    assert len(WorksheetRegistry.get_ordered_worksheets()) == 100


def test_thread_safety_concurrent_read_write():
    """Registry is thread-safe for concurrent reads and writes."""
    barrier = threading.Barrier(20)
    errors = []

    def writer(name: str):
        try:
            barrier.wait()
            ws = MockWorksheet(name)
            WorksheetRegistry.register(ws)
        except Exception as e:
            errors.append(e)

    def reader():
        try:
            barrier.wait()
            WorksheetRegistry.get_ordered_worksheets()
            WorksheetRegistry.get_worksheet('Sheet0')
        except Exception as e:
            errors.append(e)

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        for i in range(10):
            futures.append(executor.submit(writer, f'Sheet{i}'))
            futures.append(executor.submit(reader))
        for future in futures:
            future.result()

    assert len(errors) == 0
