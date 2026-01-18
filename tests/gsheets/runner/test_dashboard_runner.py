"""Tests for DashboardRunner."""

import logging
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from eftoolkit.gsheets.runner import (
    CellLocation,
    DashboardRunner,
    HookContext,
    WorksheetAsset,
    WorksheetFormatting,
    WorksheetRegistry,
)


class MockWorksheetDefinition:
    """Mock worksheet definition for testing."""

    def __init__(
        self,
        name: str,
        df: pd.DataFrame | None = None,
        location: str = 'A1',
        formatting: WorksheetFormatting | None = None,
        post_write_hooks: list | None = None,
    ):
        self._name = name
        self._df = df if df is not None else pd.DataFrame({'a': [1, 2, 3]})
        self._location = location
        self._formatting = formatting
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

    def get_formatting(self, context: dict) -> WorksheetFormatting | None:
        return self._formatting


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

    with patch('eftoolkit.gsheets.runner.dashboard_runner.Spreadsheet') as mock_ss:
        mock_spreadsheet = MagicMock()
        mock_worksheet = MagicMock()
        mock_spreadsheet.__enter__ = MagicMock(return_value=mock_spreadsheet)
        mock_spreadsheet.__exit__ = MagicMock(return_value=None)
        mock_spreadsheet.create_worksheet.return_value = mock_worksheet
        mock_ss.return_value = mock_spreadsheet

        runner._phase_3_write_data_and_run_hooks()

        mock_ss.assert_called_once_with(
            credentials=None,
            spreadsheet_name='Test',
            local_preview=True,
        )
        mock_spreadsheet.create_worksheet.assert_called_once_with(
            'TestSheet', replace=True
        )
        mock_worksheet.write_dataframe.assert_called_once()


def test_phase_4_apply_formatting_with_formatting():
    """Phase 4 applies formatting when get_formatting returns WorksheetFormatting."""
    formatting = WorksheetFormatting(
        freeze_rows=1,
        format_dict={'color': 'blue'},
    )
    ws = MockWorksheetDefinition('Formatted', formatting=formatting)

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws],
    )

    runner._phase_2_generate_data()
    # Phase 4 logs formatting info; verify no exceptions
    runner._phase_4_apply_formatting()


def test_phase_4_apply_formatting_with_none():
    """Phase 4 handles None from get_formatting."""
    ws = MockWorksheetDefinition('NoFormatting', formatting=None)

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws],
    )

    runner._phase_2_generate_data()
    runner._phase_4_apply_formatting()


def test_phase_4_apply_formatting_with_config_path(tmp_path, caplog):
    """Phase 4 loads formatting from format_config_path."""
    format_file = tmp_path / 'format.json'
    format_file.write_text('{"header_color": "#4a86e8"}')

    formatting = WorksheetFormatting(format_config_path=format_file)
    ws = MockWorksheetDefinition('WithConfigPath', formatting=formatting)

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws],
    )

    runner._phase_2_generate_data()

    with (
        patch(
            'eftoolkit.gsheets.runner.dashboard_runner.load_json_config'
        ) as mock_load,
        caplog.at_level(logging.INFO),
    ):
        mock_load.return_value = {'header_color': '#4a86e8'}
        runner._phase_4_apply_formatting()

        mock_load.assert_called_once_with(format_file)


def test_phase_4_merges_config_path_and_format_dict(tmp_path):
    """Phase 4 merges format_config_path and format_dict, with dict taking precedence."""
    format_file = tmp_path / 'base.json'

    formatting = WorksheetFormatting(
        format_config_path=format_file,
        format_dict={'color': 'blue', 'bold': True},  # Override color, add bold
    )
    ws = MockWorksheetDefinition('Merged', formatting=formatting)

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws],
    )

    runner._phase_2_generate_data()

    with patch(
        'eftoolkit.gsheets.runner.dashboard_runner.load_json_config'
    ) as mock_load:
        mock_load.return_value = {'color': 'red', 'size': 12}
        runner._phase_4_apply_formatting()

        mock_load.assert_called_once_with(format_file)


def test_phase_3_runs_hooks_with_context():
    """Phase 3 executes post-write hooks with HookContext."""
    received_contexts = []

    def test_hook(ctx: HookContext):
        received_contexts.append(ctx)

    ws = MockWorksheetDefinition('WithHooks', post_write_hooks=[test_hook])

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={},
        worksheets=[ws],
        local_preview=True,
    )

    runner._phase_2_generate_data()

    with patch('eftoolkit.gsheets.runner.dashboard_runner.Spreadsheet') as mock_ss:
        mock_spreadsheet = MagicMock()
        mock_worksheet = MagicMock()
        mock_spreadsheet.__enter__ = MagicMock(return_value=mock_spreadsheet)
        mock_spreadsheet.__exit__ = MagicMock(return_value=None)
        mock_spreadsheet.create_worksheet.return_value = mock_worksheet
        mock_ss.return_value = mock_spreadsheet

        runner._phase_3_write_data_and_run_hooks()

    assert len(received_contexts) == 1
    ctx = received_contexts[0]
    assert ctx.worksheet == mock_worksheet
    assert ctx.worksheet_name == 'WithHooks'
    assert ctx.asset.df.equals(pd.DataFrame({'a': [1, 2, 3]}))
    assert 'WithHooks' in ctx.runner_context


def test_phase_3_runs_multiple_hooks():
    """Phase 3 executes all hooks from all assets."""
    hook_calls = []

    def hook_a(ctx: HookContext):
        hook_calls.append(('a', ctx.worksheet_name))

    def hook_b(ctx: HookContext):
        hook_calls.append(('b', ctx.worksheet_name))

    ws1 = MockWorksheetDefinition('Sheet1', post_write_hooks=[hook_a])
    ws2 = MockWorksheetDefinition('Sheet2', post_write_hooks=[hook_b])

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={},
        worksheets=[ws1, ws2],
        local_preview=True,
    )

    runner._phase_2_generate_data()

    with patch('eftoolkit.gsheets.runner.dashboard_runner.Spreadsheet') as mock_ss:
        mock_spreadsheet = MagicMock()
        mock_worksheet = MagicMock()
        mock_spreadsheet.__enter__ = MagicMock(return_value=mock_spreadsheet)
        mock_spreadsheet.__exit__ = MagicMock(return_value=None)
        mock_spreadsheet.create_worksheet.return_value = mock_worksheet
        mock_ss.return_value = mock_spreadsheet

        runner._phase_3_write_data_and_run_hooks()

    assert hook_calls == [('a', 'Sheet1'), ('b', 'Sheet2')]


def test_phase_5_logs_summary(caplog):
    """Phase 5 logs a summary of the run."""
    ws1 = MockWorksheetDefinition('Sheet1', pd.DataFrame({'a': [1, 2]}))
    ws2 = MockWorksheetDefinition('Sheet2', pd.DataFrame({'b': [3, 4, 5]}))

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws1, ws2],
    )

    runner._phase_2_generate_data()

    with caplog.at_level(logging.INFO):
        runner._phase_5_log_summary()

    assert 'Sheet1' in caplog.text
    assert 'Sheet2' in caplog.text
    assert '2 worksheets' in caplog.text


def test_run_executes_all_phases():
    """run() executes all 5 phases."""
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
        patch.object(runner, '_phase_3_write_data_and_run_hooks') as p3,
        patch.object(runner, '_phase_4_apply_formatting') as p4,
        patch.object(runner, '_phase_5_log_summary') as p5,
    ):
        runner.run()

        p1.assert_called_once()
        p2.assert_called_once()
        p3.assert_called_once()
        p4.assert_called_once()
        p5.assert_called_once()


def test_run_with_local_preview(tmp_path):
    """Full run in local preview mode."""
    ws = MockWorksheetDefinition('PreviewSheet', pd.DataFrame({'x': [1]}))

    runner = DashboardRunner(
        config={'sheet_name': 'PreviewTest'},
        credentials={},
        worksheets=[ws],
        local_preview=True,
    )

    with patch('eftoolkit.gsheets.runner.dashboard_runner.Spreadsheet') as mock_ss:
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

    with patch('eftoolkit.gsheets.runner.dashboard_runner.Spreadsheet') as mock_ss:
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

        def get_formatting(self, context: dict) -> WorksheetFormatting | None:
            return None

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


def test_phase_3_writes_data_without_formatting():
    """Phase 3 writes data without any formatting."""

    class WorksheetWithFormatting:
        @property
        def name(self) -> str:
            return 'Formatted'

        def generate(self, config: dict, context: dict) -> list[WorksheetAsset]:
            return [
                WorksheetAsset(
                    df=pd.DataFrame({'a': [1]}),
                    location=CellLocation(cell='A1'),
                )
            ]

        def get_formatting(self, context: dict) -> WorksheetFormatting | None:
            return WorksheetFormatting(
                freeze_rows=1,
                format_dict={'header_color': '#4a86e8'},
            )

    ws = WorksheetWithFormatting()

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={},
        worksheets=[ws],
        local_preview=True,
    )

    runner._phase_2_generate_data()

    with patch('eftoolkit.gsheets.runner.dashboard_runner.Spreadsheet') as mock_ss:
        mock_spreadsheet = MagicMock()
        mock_worksheet = MagicMock()
        mock_spreadsheet.__enter__ = MagicMock(return_value=mock_spreadsheet)
        mock_spreadsheet.__exit__ = MagicMock(return_value=None)
        mock_spreadsheet.create_worksheet.return_value = mock_worksheet
        mock_ss.return_value = mock_spreadsheet

        runner._phase_3_write_data_and_run_hooks()

        # write_dataframe should be called without format_dict
        call_kwargs = mock_worksheet.write_dataframe.call_args[1]
        assert (
            'format_dict' not in call_kwargs or call_kwargs.get('format_dict') is None
        )


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

        def get_formatting(self, context: dict) -> WorksheetFormatting | None:
            return None

    ws = MultiAssetWorksheet()

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={},
        worksheets=[ws],
        local_preview=True,
    )

    runner._phase_2_generate_data()

    with patch('eftoolkit.gsheets.runner.dashboard_runner.Spreadsheet') as mock_ss:
        mock_spreadsheet = MagicMock()
        mock_worksheet = MagicMock()
        mock_spreadsheet.__enter__ = MagicMock(return_value=mock_spreadsheet)
        mock_spreadsheet.__exit__ = MagicMock(return_value=None)
        mock_spreadsheet.create_worksheet.return_value = mock_worksheet
        mock_ss.return_value = mock_spreadsheet

        runner._phase_3_write_data_and_run_hooks()

        # Should have 2 write calls for the 2 assets
        assert mock_worksheet.write_dataframe.call_count == 2
        locations = [
            call[1]['location']
            for call in mock_worksheet.write_dataframe.call_args_list
        ]
        assert 'A1' in locations
        assert 'A5' in locations
