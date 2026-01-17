"""Tests for DashboardRunner."""

import logging
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from eftoolkit.gsheets.registry import WorksheetRegistry
from eftoolkit.gsheets.runner import DashboardRunner
from eftoolkit.gsheets.types import CellLocation, WorksheetAsset


class MockWorksheetDefinition:
    """Mock worksheet definition for testing."""

    def __init__(
        self,
        name: str,
        df: pd.DataFrame | None = None,
        location: str = 'A1',
        format_overrides: dict | None = None,
        post_write_hooks: list | None = None,
    ):
        self._name = name
        self._df = df if df is not None else pd.DataFrame({'a': [1, 2, 3]})
        self._location = location
        self._format_overrides = format_overrides or {}
        self._post_write_hooks = post_write_hooks or []

    @property
    def name(self) -> str:
        return self._name

    def generate(self, config: dict, context: dict) -> list[WorksheetAsset]:
        return [
            WorksheetAsset(
                df=self._df,
                location=CellLocation(cell=self._location),
                post_write_hooks=self._post_write_hooks,
            )
        ]

    def get_format_overrides(self, context: dict) -> dict:
        return self._format_overrides


@pytest.fixture(autouse=True)
def clear_registry():
    """Clear registry before and after each test."""
    WorksheetRegistry.clear()
    yield
    WorksheetRegistry.clear()


def test_init_with_worksheets():
    """DashboardRunner can be initialized with explicit worksheets."""
    worksheets = [MockWorksheetDefinition('Sheet1')]
    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=worksheets,
    )

    assert runner.worksheets == worksheets
    assert runner.config == {'sheet_name': 'Test'}


def test_init_from_registry():
    """DashboardRunner uses WorksheetRegistry when no worksheets provided."""
    ws = MockWorksheetDefinition('FromRegistry')
    WorksheetRegistry.register(ws)

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
    )

    assert len(runner.worksheets) == 1
    assert runner.worksheets[0].name == 'FromRegistry'


def test_init_missing_sheet_name_raises():
    """DashboardRunner raises ValueError if sheet_name missing from config."""
    with pytest.raises(ValueError, match="'sheet_name'"):
        DashboardRunner(
            config={},
            credentials={'type': 'service_account'},
            worksheets=[MockWorksheetDefinition('Test')],
        )


def test_init_no_worksheets_raises():
    """DashboardRunner raises ValueError if no worksheets provided or registered."""
    with pytest.raises(ValueError, match='No worksheets provided'):
        DashboardRunner(
            config={'sheet_name': 'Test'},
            credentials={'type': 'service_account'},
        )


def test_phase_2_generate_data():
    """Phase 2 generates data from all worksheets."""
    ws1 = MockWorksheetDefinition('Sheet1', pd.DataFrame({'a': [1, 2]}))
    ws2 = MockWorksheetDefinition('Sheet2', pd.DataFrame({'b': [3, 4, 5]}))

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws1, ws2],
    )

    runner._phase_2_generate_data()

    assert 'Sheet1' in runner.results
    assert 'Sheet2' in runner.results
    assert len(runner.results['Sheet1']) == 1
    assert len(runner.results['Sheet1'][0].df) == 2
    assert len(runner.results['Sheet2'][0].df) == 3


def test_phase_2_populates_context():
    """Phase 2 populates context with worksheet metadata."""
    ws = MockWorksheetDefinition('Summary', pd.DataFrame({'x': [1, 2, 3, 4]}))

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws],
    )

    runner._phase_2_generate_data()

    assert 'Summary' in runner.context
    assert runner.context['Summary']['total_rows'] == 4
    assert runner.context['Summary']['asset_count'] == 1


def test_phase_3_write_data_local_preview(tmp_path):
    """Phase 3 writes data in local preview mode."""
    ws = MockWorksheetDefinition('TestSheet', pd.DataFrame({'col': [1, 2]}))

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={},
        worksheets=[ws],
        local_preview=True,
    )

    runner._phase_2_generate_data()

    with patch('eftoolkit.gsheets.runner.Spreadsheet') as mock_ss:
        mock_spreadsheet = MagicMock()
        mock_worksheet = MagicMock()
        mock_spreadsheet.__enter__ = MagicMock(return_value=mock_spreadsheet)
        mock_spreadsheet.__exit__ = MagicMock(return_value=None)
        mock_spreadsheet.create_worksheet.return_value = mock_worksheet
        mock_ss.return_value = mock_spreadsheet

        runner._phase_3_write_data()

        mock_ss.assert_called_once_with(
            credentials=None,
            spreadsheet_name='Test',
            local_preview=True,
        )
        mock_spreadsheet.create_worksheet.assert_called_once_with(
            'TestSheet', replace=True
        )
        mock_worksheet.write_dataframe.assert_called_once()


def test_phase_4_apply_formatting():
    """Phase 4 applies format overrides."""
    ws = MockWorksheetDefinition('Formatted', format_overrides={'color': 'blue'})

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws],
    )

    runner._phase_2_generate_data()
    # Phase 4 just logs overrides for now; verify no exceptions
    runner._phase_4_apply_formatting()


def test_phase_5_runs_hooks():
    """Phase 5 executes post-write hooks."""
    hook_called = []

    def test_hook():
        hook_called.append(True)

    ws = MockWorksheetDefinition('WithHooks', post_write_hooks=[test_hook])

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws],
    )

    runner._phase_2_generate_data()
    runner._phase_5_run_hooks()

    assert len(hook_called) == 1


def test_phase_5_runs_multiple_hooks():
    """Phase 5 executes all hooks from all assets."""
    hook_calls = []

    def hook_a():
        hook_calls.append('a')

    def hook_b():
        hook_calls.append('b')

    ws1 = MockWorksheetDefinition('Sheet1', post_write_hooks=[hook_a])
    ws2 = MockWorksheetDefinition('Sheet2', post_write_hooks=[hook_b])

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws1, ws2],
    )

    runner._phase_2_generate_data()
    runner._phase_5_run_hooks()

    assert hook_calls == ['a', 'b']


def test_phase_6_logs_summary(caplog):
    """Phase 6 logs a summary of the run."""
    ws1 = MockWorksheetDefinition('Sheet1', pd.DataFrame({'a': [1, 2]}))
    ws2 = MockWorksheetDefinition('Sheet2', pd.DataFrame({'b': [3, 4, 5]}))

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws1, ws2],
    )

    runner._phase_2_generate_data()

    with caplog.at_level(logging.INFO):
        runner._phase_6_log_summary()

    assert 'Sheet1' in caplog.text
    assert 'Sheet2' in caplog.text
    assert '2 worksheets' in caplog.text


def test_run_executes_all_phases():
    """run() executes all 6 phases."""
    ws = MockWorksheetDefinition('TestSheet')

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={},
        worksheets=[ws],
        local_preview=True,
    )

    with (
        patch.object(runner, '_phase_1_validate_structure') as p1,
        patch.object(runner, '_phase_2_generate_data') as p2,
        patch.object(runner, '_phase_3_write_data') as p3,
        patch.object(runner, '_phase_4_apply_formatting') as p4,
        patch.object(runner, '_phase_5_run_hooks') as p5,
        patch.object(runner, '_phase_6_log_summary') as p6,
    ):
        runner.run()

        p1.assert_called_once()
        p2.assert_called_once()
        p3.assert_called_once()
        p4.assert_called_once()
        p5.assert_called_once()
        p6.assert_called_once()


def test_run_with_local_preview(tmp_path):
    """Full run in local preview mode."""
    ws = MockWorksheetDefinition('PreviewSheet', pd.DataFrame({'x': [1]}))

    runner = DashboardRunner(
        config={'sheet_name': 'PreviewTest'},
        credentials={},
        worksheets=[ws],
        local_preview=True,
    )

    with patch('eftoolkit.gsheets.runner.Spreadsheet') as mock_ss:
        mock_spreadsheet = MagicMock()
        mock_worksheet = MagicMock()
        mock_spreadsheet.__enter__ = MagicMock(return_value=mock_spreadsheet)
        mock_spreadsheet.__exit__ = MagicMock(return_value=None)
        mock_spreadsheet.create_worksheet.return_value = mock_worksheet
        mock_ss.return_value = mock_spreadsheet

        runner.run()

    # Verify phases completed
    assert 'PreviewSheet' in runner.results
    assert runner.context['PreviewSheet']['total_rows'] == 1


def test_phase_1_validates_structure():
    """Phase 1 validates spreadsheet accessibility in non-preview mode."""
    ws = MockWorksheetDefinition('TestSheet')

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws],
        local_preview=False,
    )

    with patch('eftoolkit.gsheets.runner.Spreadsheet') as mock_ss:
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.__enter__ = MagicMock(return_value=mock_spreadsheet)
        mock_spreadsheet.__exit__ = MagicMock(return_value=None)
        mock_ss.return_value = mock_spreadsheet

        runner._phase_1_validate_structure()

        mock_ss.assert_called_once_with(
            credentials={'type': 'service_account'},
            spreadsheet_name='Test',
        )


def test_context_shared_between_worksheets():
    """Context from earlier worksheets is available to later ones."""
    received_context = {}

    class ContextAwareWorksheet:
        @property
        def name(self) -> str:
            return 'Dependent'

        def generate(self, config: dict, context: dict) -> list[WorksheetAsset]:
            received_context.update(context)
            return [
                WorksheetAsset(
                    df=pd.DataFrame({'a': [1]}),
                    location=CellLocation(cell='A1'),
                )
            ]

        def get_format_overrides(self, context: dict) -> dict:
            return {}

    ws1 = MockWorksheetDefinition('First', pd.DataFrame({'x': [1, 2, 3]}))
    ws2 = ContextAwareWorksheet()

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws1, ws2],
    )

    runner._phase_2_generate_data()

    assert 'First' in received_context
    assert received_context['First']['total_rows'] == 3


def test_format_config_from_file(tmp_path):
    """Asset with format_config_path loads format from file."""
    format_file = tmp_path / 'format.json'
    format_file.write_text('{"header_color": "#4a86e8"}')

    class WorksheetWithFormatPath:
        @property
        def name(self) -> str:
            return 'Formatted'

        def generate(self, config: dict, context: dict) -> list[WorksheetAsset]:
            return [
                WorksheetAsset(
                    df=pd.DataFrame({'a': [1]}),
                    location=CellLocation(cell='A1'),
                    format_config_path=format_file,
                )
            ]

        def get_format_overrides(self, context: dict) -> dict:
            return {}

    ws = WorksheetWithFormatPath()

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={},
        worksheets=[ws],
        local_preview=True,
    )

    runner._phase_2_generate_data()

    with (
        patch('eftoolkit.gsheets.runner.Spreadsheet') as mock_ss,
        patch('eftoolkit.gsheets.runner.load_json_config') as mock_load,
    ):
        mock_load.return_value = {'header_color': '#4a86e8'}
        mock_spreadsheet = MagicMock()
        mock_worksheet = MagicMock()
        mock_spreadsheet.__enter__ = MagicMock(return_value=mock_spreadsheet)
        mock_spreadsheet.__exit__ = MagicMock(return_value=None)
        mock_spreadsheet.create_worksheet.return_value = mock_worksheet
        mock_ss.return_value = mock_spreadsheet

        runner._phase_3_write_data()

        mock_load.assert_called_once_with(format_file)
        call_kwargs = mock_worksheet.write_dataframe.call_args[1]
        assert call_kwargs['format_dict'] == {'header_color': '#4a86e8'}


def test_format_dict_inline():
    """Asset with inline format_dict passes it to write_dataframe."""

    class WorksheetWithInlineFormat:
        @property
        def name(self) -> str:
            return 'Inline'

        def generate(self, config: dict, context: dict) -> list[WorksheetAsset]:
            return [
                WorksheetAsset(
                    df=pd.DataFrame({'a': [1]}),
                    location=CellLocation(cell='B2'),
                    format_dict={'bold': True},
                )
            ]

        def get_format_overrides(self, context: dict) -> dict:
            return {}

    ws = WorksheetWithInlineFormat()

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={},
        worksheets=[ws],
        local_preview=True,
    )

    runner._phase_2_generate_data()

    with patch('eftoolkit.gsheets.runner.Spreadsheet') as mock_ss:
        mock_spreadsheet = MagicMock()
        mock_worksheet = MagicMock()
        mock_spreadsheet.__enter__ = MagicMock(return_value=mock_spreadsheet)
        mock_spreadsheet.__exit__ = MagicMock(return_value=None)
        mock_spreadsheet.create_worksheet.return_value = mock_worksheet
        mock_ss.return_value = mock_spreadsheet

        runner._phase_3_write_data()

        call_kwargs = mock_worksheet.write_dataframe.call_args[1]
        assert call_kwargs['format_dict'] == {'bold': True}
        assert call_kwargs['location'] == 'B2'


def test_format_dict_merged_from_file_and_inline(tmp_path):
    """Inline format_dict merges with and overrides format_config_path."""
    format_file = tmp_path / 'base_format.json'
    format_file.write_text('{"color": "red", "size": 12}')

    class WorksheetWithBothFormats:
        @property
        def name(self) -> str:
            return 'Merged'

        def generate(self, config: dict, context: dict) -> list[WorksheetAsset]:
            return [
                WorksheetAsset(
                    df=pd.DataFrame({'a': [1]}),
                    location=CellLocation(cell='A1'),
                    format_config_path=format_file,
                    format_dict={
                        'color': 'blue',
                        'bold': True,
                    },  # Override color, add bold
                )
            ]

        def get_format_overrides(self, context: dict) -> dict:
            return {}

    ws = WorksheetWithBothFormats()

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={},
        worksheets=[ws],
        local_preview=True,
    )

    runner._phase_2_generate_data()

    with (
        patch('eftoolkit.gsheets.runner.Spreadsheet') as mock_ss,
        patch('eftoolkit.gsheets.runner.load_json_config') as mock_load,
    ):
        mock_load.return_value = {'color': 'red', 'size': 12}
        mock_spreadsheet = MagicMock()
        mock_worksheet = MagicMock()
        mock_spreadsheet.__enter__ = MagicMock(return_value=mock_spreadsheet)
        mock_spreadsheet.__exit__ = MagicMock(return_value=None)
        mock_spreadsheet.create_worksheet.return_value = mock_worksheet
        mock_ss.return_value = mock_spreadsheet

        runner._phase_3_write_data()

        call_kwargs = mock_worksheet.write_dataframe.call_args[1]
        # Inline overrides file config
        assert call_kwargs['format_dict'] == {'color': 'blue', 'size': 12, 'bold': True}


def test_multiple_assets_per_worksheet():
    """Worksheet with multiple assets writes all of them."""

    class MultiAssetWorksheet:
        @property
        def name(self) -> str:
            return 'MultiAsset'

        def generate(self, config: dict, context: dict) -> list[WorksheetAsset]:
            return [
                WorksheetAsset(
                    df=pd.DataFrame({'summary': [100]}),
                    location=CellLocation(cell='A1'),
                ),
                WorksheetAsset(
                    df=pd.DataFrame({'detail': [1, 2, 3]}),
                    location=CellLocation(cell='A5'),
                ),
            ]

        def get_format_overrides(self, context: dict) -> dict:
            return {}

    ws = MultiAssetWorksheet()

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={},
        worksheets=[ws],
        local_preview=True,
    )

    runner._phase_2_generate_data()

    with patch('eftoolkit.gsheets.runner.Spreadsheet') as mock_ss:
        mock_spreadsheet = MagicMock()
        mock_worksheet = MagicMock()
        mock_spreadsheet.__enter__ = MagicMock(return_value=mock_spreadsheet)
        mock_spreadsheet.__exit__ = MagicMock(return_value=None)
        mock_spreadsheet.create_worksheet.return_value = mock_worksheet
        mock_ss.return_value = mock_spreadsheet

        runner._phase_3_write_data()

        # Should have 2 write calls for the 2 assets
        assert mock_worksheet.write_dataframe.call_count == 2
        locations = [
            call[1]['location']
            for call in mock_worksheet.write_dataframe.call_args_list
        ]
        assert 'A1' in locations
        assert 'A5' in locations
