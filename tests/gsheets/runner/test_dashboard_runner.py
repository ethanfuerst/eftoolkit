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
        format_dict={'A1:B1': {'bold': True}},
    )
    ws = MockWorksheetDefinition('Formatted', formatting=formatting)

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws],
    )

    runner._phase_2_generate_data()

    with patch('eftoolkit.gsheets.runner.dashboard_runner.Spreadsheet') as mock_ss:
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.__enter__ = MagicMock(return_value=mock_spreadsheet)
        mock_spreadsheet.__exit__ = MagicMock(return_value=None)
        mock_ss.return_value = mock_spreadsheet

        runner._phase_4_apply_formatting()

        # Verify apply_formatting was called with correct arguments
        mock_spreadsheet.apply_formatting.assert_called_once()
        call_args = mock_spreadsheet.apply_formatting.call_args
        assert call_args[0][0] == 'Formatted'  # worksheet_name
        assert call_args[0][1].freeze_rows == 1  # formatting object
        assert call_args[0][2] == {'A1:B1': {'bold': True}}  # merged format_dict


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
    format_file.write_text('{"A1:B1": {"bold": true}}')

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
        patch('eftoolkit.gsheets.runner.dashboard_runner.Spreadsheet') as mock_ss,
        caplog.at_level(logging.INFO),
    ):
        mock_load.return_value = {'A1:B1': {'bold': True}}
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.__enter__ = MagicMock(return_value=mock_spreadsheet)
        mock_spreadsheet.__exit__ = MagicMock(return_value=None)
        mock_ss.return_value = mock_spreadsheet

        runner._phase_4_apply_formatting()

        mock_load.assert_called_once_with(format_file)
        # Verify apply_formatting was called with merged format_dict
        call_args = mock_spreadsheet.apply_formatting.call_args
        assert call_args[0][2] == {'A1:B1': {'bold': True}}


def test_phase_4_merges_config_path_and_format_dict(tmp_path):
    """Phase 4 merges format_config_path and format_dict, with dict taking precedence."""
    format_file = tmp_path / 'base.json'

    formatting = WorksheetFormatting(
        format_config_path=format_file,
        format_dict={'A1:B1': {'color': 'blue'}},  # Override color
    )
    ws = MockWorksheetDefinition('Merged', formatting=formatting)

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
        patch('eftoolkit.gsheets.runner.dashboard_runner.Spreadsheet') as mock_ss,
    ):
        mock_load.return_value = {'A1:B1': {'color': 'red'}, 'C1:D1': {'size': 12}}
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.__enter__ = MagicMock(return_value=mock_spreadsheet)
        mock_spreadsheet.__exit__ = MagicMock(return_value=None)
        mock_ss.return_value = mock_spreadsheet

        runner._phase_4_apply_formatting()

        mock_load.assert_called_once_with(format_file)
        # Verify apply_formatting was called with merged format_dict
        call_args = mock_spreadsheet.apply_formatting.call_args
        merged_format_dict = call_args[0][2]
        # Merged dict should have both ranges, with format_dict overriding color
        assert merged_format_dict == {
            'A1:B1': {'color': 'blue'},
            'C1:D1': {'size': 12},
        }


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
    """run() executes all 6 phases."""
    ws = MockWorksheetDefinition('TestSheet')

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={},
        worksheets=[ws],
        local_preview=True,
    )

    with (
        patch.object(runner, '_phase_0_run_pre_hooks') as p0,
        patch.object(runner, '_phase_1_validate_structure') as p1,
        patch.object(runner, '_phase_2_generate_data') as p2,
        patch.object(runner, '_phase_3_write_data_and_run_hooks') as p3,
        patch.object(runner, '_phase_4_apply_formatting') as p4,
        patch.object(runner, '_phase_5_log_summary') as p5,
    ):
        runner.run()

        p0.assert_called_once()
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


def test_phase_0_runs_pre_hooks_with_spreadsheet():
    """Phase 0 executes pre-run hooks with Spreadsheet instance."""
    received_spreadsheets = []

    def test_hook(ss):
        received_spreadsheets.append(ss)

    ws = MockWorksheetDefinition('TestSheet')

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws],
        pre_run_hooks=[test_hook],
    )

    with patch('eftoolkit.gsheets.runner.dashboard_runner.Spreadsheet') as mock_ss:
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.__enter__ = MagicMock(return_value=mock_spreadsheet)
        mock_spreadsheet.__exit__ = MagicMock(return_value=None)
        mock_ss.return_value = mock_spreadsheet

        runner._phase_0_run_pre_hooks()

    assert len(received_spreadsheets) == 1
    assert received_spreadsheets[0] == mock_spreadsheet


def test_phase_0_runs_multiple_hooks_in_order():
    """Phase 0 executes multiple pre-run hooks in order."""
    call_order = []

    def hook_a(ss):
        call_order.append('a')

    def hook_b(ss):
        call_order.append('b')

    def hook_c(ss):
        call_order.append('c')

    ws = MockWorksheetDefinition('TestSheet')

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws],
        pre_run_hooks=[hook_a, hook_b, hook_c],
    )

    with patch('eftoolkit.gsheets.runner.dashboard_runner.Spreadsheet') as mock_ss:
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.__enter__ = MagicMock(return_value=mock_spreadsheet)
        mock_spreadsheet.__exit__ = MagicMock(return_value=None)
        mock_ss.return_value = mock_spreadsheet

        runner._phase_0_run_pre_hooks()

    assert call_order == ['a', 'b', 'c']


def test_phase_0_skipped_in_local_preview_mode(caplog):
    """Phase 0 skips pre-run hooks in local_preview mode."""
    hook_called = []

    def test_hook(ss):
        hook_called.append(True)

    ws = MockWorksheetDefinition('TestSheet')

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={},
        worksheets=[ws],
        pre_run_hooks=[test_hook],
        local_preview=True,
    )

    with caplog.at_level(logging.INFO):
        runner._phase_0_run_pre_hooks()

    assert hook_called == []
    assert 'Skipping pre-run hooks' in caplog.text


def test_phase_0_no_op_with_empty_hooks():
    """Phase 0 is a no-op when no pre-run hooks are provided."""
    ws = MockWorksheetDefinition('TestSheet')

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws],
    )

    # Should not raise and should not try to open Spreadsheet
    with patch('eftoolkit.gsheets.runner.dashboard_runner.Spreadsheet') as mock_ss:
        runner._phase_0_run_pre_hooks()
        mock_ss.assert_not_called()


def test_init_stores_pre_run_hooks():
    """DashboardRunner stores pre_run_hooks from init."""

    def hook(ss):
        pass

    ws = MockWorksheetDefinition('TestSheet')

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws],
        pre_run_hooks=[hook],
    )

    assert runner.pre_run_hooks == [hook]


def test_init_defaults_pre_run_hooks_to_empty_list():
    """DashboardRunner defaults pre_run_hooks to empty list."""
    ws = MockWorksheetDefinition('TestSheet')

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws],
    )

    assert runner.pre_run_hooks == []


def test_phase_4_applies_freeze_columns():
    """Phase 4 applies freeze_columns from WorksheetFormatting."""
    formatting = WorksheetFormatting(freeze_columns=2)
    ws = MockWorksheetDefinition('WithFreezeColumns', formatting=formatting)

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws],
    )

    runner._phase_2_generate_data()

    with patch('eftoolkit.gsheets.runner.dashboard_runner.Spreadsheet') as mock_ss:
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.__enter__ = MagicMock(return_value=mock_spreadsheet)
        mock_spreadsheet.__exit__ = MagicMock(return_value=None)
        mock_ss.return_value = mock_spreadsheet

        runner._phase_4_apply_formatting()

        # Verify apply_formatting was called with formatting containing freeze_columns=2
        mock_spreadsheet.apply_formatting.assert_called_once()
        call_args = mock_spreadsheet.apply_formatting.call_args
        assert call_args[0][0] == 'WithFreezeColumns'
        assert call_args[0][1].freeze_columns == 2


def test_phase_4_applies_auto_resize_columns():
    """Phase 4 applies auto_resize_columns from WorksheetFormatting."""
    formatting = WorksheetFormatting(auto_resize_columns=(1, 5))
    ws = MockWorksheetDefinition('WithAutoResize', formatting=formatting)

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws],
    )

    runner._phase_2_generate_data()

    with patch('eftoolkit.gsheets.runner.dashboard_runner.Spreadsheet') as mock_ss:
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.__enter__ = MagicMock(return_value=mock_spreadsheet)
        mock_spreadsheet.__exit__ = MagicMock(return_value=None)
        mock_ss.return_value = mock_spreadsheet

        runner._phase_4_apply_formatting()

        # Verify apply_formatting was called with formatting containing auto_resize_columns
        mock_spreadsheet.apply_formatting.assert_called_once()
        call_args = mock_spreadsheet.apply_formatting.call_args
        assert call_args[0][0] == 'WithAutoResize'
        assert call_args[0][1].auto_resize_columns == (1, 5)


def test_phase_4_applies_merge_ranges():
    """Phase 4 applies merge_ranges from WorksheetFormatting."""
    formatting = WorksheetFormatting(merge_ranges=['A1:C1', 'B5:D5'])
    ws = MockWorksheetDefinition('WithMergeRanges', formatting=formatting)

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws],
    )

    runner._phase_2_generate_data()

    with patch('eftoolkit.gsheets.runner.dashboard_runner.Spreadsheet') as mock_ss:
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.__enter__ = MagicMock(return_value=mock_spreadsheet)
        mock_spreadsheet.__exit__ = MagicMock(return_value=None)
        mock_ss.return_value = mock_spreadsheet

        runner._phase_4_apply_formatting()

        # Verify apply_formatting was called with formatting containing merge_ranges
        mock_spreadsheet.apply_formatting.assert_called_once()
        call_args = mock_spreadsheet.apply_formatting.call_args
        assert call_args[0][0] == 'WithMergeRanges'
        assert call_args[0][1].merge_ranges == ['A1:C1', 'B5:D5']


def test_phase_4_applies_notes():
    """Phase 4 applies notes from WorksheetFormatting."""
    formatting = WorksheetFormatting(notes={'A1': 'Header note', 'B2': 'Data note'})
    ws = MockWorksheetDefinition('WithNotes', formatting=formatting)

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws],
    )

    runner._phase_2_generate_data()

    with patch('eftoolkit.gsheets.runner.dashboard_runner.Spreadsheet') as mock_ss:
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.__enter__ = MagicMock(return_value=mock_spreadsheet)
        mock_spreadsheet.__exit__ = MagicMock(return_value=None)
        mock_ss.return_value = mock_spreadsheet

        runner._phase_4_apply_formatting()

        # Verify apply_formatting was called with formatting containing notes
        mock_spreadsheet.apply_formatting.assert_called_once()
        call_args = mock_spreadsheet.apply_formatting.call_args
        assert call_args[0][0] == 'WithNotes'
        assert call_args[0][1].notes == {'A1': 'Header note', 'B2': 'Data note'}


def test_phase_4_applies_notes_with_cell_location():
    """Phase 4 passes CellLocation keys through to apply_formatting."""
    cell_loc = CellLocation(cell='A1')
    formatting = WorksheetFormatting(
        notes={cell_loc: 'Typed note', 'B2': 'String note'}
    )
    ws = MockWorksheetDefinition('WithTypedNotes', formatting=formatting)

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws],
    )

    runner._phase_2_generate_data()

    with patch('eftoolkit.gsheets.runner.dashboard_runner.Spreadsheet') as mock_ss:
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.__enter__ = MagicMock(return_value=mock_spreadsheet)
        mock_spreadsheet.__exit__ = MagicMock(return_value=None)
        mock_ss.return_value = mock_spreadsheet

        runner._phase_4_apply_formatting()

        # Verify apply_formatting was called with formatting containing notes
        mock_spreadsheet.apply_formatting.assert_called_once()
        call_args = mock_spreadsheet.apply_formatting.call_args
        assert call_args[0][0] == 'WithTypedNotes'
        assert call_args[0][1].notes == {cell_loc: 'Typed note', 'B2': 'String note'}


def test_phase_4_applies_column_widths():
    """Phase 4 applies column_widths from WorksheetFormatting."""
    formatting = WorksheetFormatting(column_widths={'A': 100, 'B': 150})
    ws = MockWorksheetDefinition('WithColumnWidths', formatting=formatting)

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws],
    )

    runner._phase_2_generate_data()

    with patch('eftoolkit.gsheets.runner.dashboard_runner.Spreadsheet') as mock_ss:
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.__enter__ = MagicMock(return_value=mock_spreadsheet)
        mock_spreadsheet.__exit__ = MagicMock(return_value=None)
        mock_ss.return_value = mock_spreadsheet

        runner._phase_4_apply_formatting()

        # Verify apply_formatting was called with formatting containing column_widths
        mock_spreadsheet.apply_formatting.assert_called_once()
        call_args = mock_spreadsheet.apply_formatting.call_args
        assert call_args[0][0] == 'WithColumnWidths'
        assert call_args[0][1].column_widths == {'A': 100, 'B': 150}


def test_phase_4_applies_borders():
    """Phase 4 applies borders from WorksheetFormatting."""
    formatting = WorksheetFormatting(borders={'A1:C10': {'style': 'solid'}})
    ws = MockWorksheetDefinition('WithBorders', formatting=formatting)

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws],
    )

    runner._phase_2_generate_data()

    with patch('eftoolkit.gsheets.runner.dashboard_runner.Spreadsheet') as mock_ss:
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.__enter__ = MagicMock(return_value=mock_spreadsheet)
        mock_spreadsheet.__exit__ = MagicMock(return_value=None)
        mock_ss.return_value = mock_spreadsheet

        runner._phase_4_apply_formatting()

        # Verify apply_formatting was called with formatting containing borders
        mock_spreadsheet.apply_formatting.assert_called_once()
        call_args = mock_spreadsheet.apply_formatting.call_args
        assert call_args[0][0] == 'WithBorders'
        assert call_args[0][1].borders == {'A1:C10': {'style': 'solid'}}


def test_phase_4_applies_conditional_formats():
    """Phase 4 applies conditional_formats from WorksheetFormatting."""
    formatting = WorksheetFormatting(
        conditional_formats=[
            {'range': 'B2:B10', 'type': 'CUSTOM_FORMULA', 'values': ['=B2>100']}
        ]
    )
    ws = MockWorksheetDefinition('WithConditionalFormats', formatting=formatting)

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws],
    )

    runner._phase_2_generate_data()

    with patch('eftoolkit.gsheets.runner.dashboard_runner.Spreadsheet') as mock_ss:
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.__enter__ = MagicMock(return_value=mock_spreadsheet)
        mock_spreadsheet.__exit__ = MagicMock(return_value=None)
        mock_ss.return_value = mock_spreadsheet

        runner._phase_4_apply_formatting()

        # Verify apply_formatting was called with formatting containing conditional_formats
        mock_spreadsheet.apply_formatting.assert_called_once()
        call_args = mock_spreadsheet.apply_formatting.call_args
        assert call_args[0][0] == 'WithConditionalFormats'
        assert call_args[0][1].conditional_formats == [
            {'range': 'B2:B10', 'type': 'CUSTOM_FORMULA', 'values': ['=B2>100']}
        ]


def test_phase_4_applies_data_validations():
    """Phase 4 applies data_validations from WorksheetFormatting."""
    formatting = WorksheetFormatting(
        data_validations=[
            {'range': 'D1:D10', 'type': 'ONE_OF_LIST', 'values': ['A', 'B']}
        ]
    )
    ws = MockWorksheetDefinition('WithDataValidations', formatting=formatting)

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws],
    )

    runner._phase_2_generate_data()

    with patch('eftoolkit.gsheets.runner.dashboard_runner.Spreadsheet') as mock_ss:
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.__enter__ = MagicMock(return_value=mock_spreadsheet)
        mock_spreadsheet.__exit__ = MagicMock(return_value=None)
        mock_ss.return_value = mock_spreadsheet

        runner._phase_4_apply_formatting()

        # Verify apply_formatting was called with formatting containing data_validations
        mock_spreadsheet.apply_formatting.assert_called_once()
        call_args = mock_spreadsheet.apply_formatting.call_args
        assert call_args[0][0] == 'WithDataValidations'
        assert call_args[0][1].data_validations == [
            {'range': 'D1:D10', 'type': 'ONE_OF_LIST', 'values': ['A', 'B']}
        ]


def test_phase_4_applies_all_formatting_options():
    """Phase 4 applies all WorksheetFormatting options together."""
    formatting = WorksheetFormatting(
        freeze_rows=1,
        freeze_columns=1,
        auto_resize_columns=(0, 5),
        merge_ranges=['A1:C1'],
        notes={'A1': 'Note'},
        column_widths={'A': 100},
        borders={'A1:C10': {'style': 'solid'}},
        conditional_formats=[{'range': 'B2:B10', 'type': 'CUSTOM_FORMULA'}],
        data_validations=[{'range': 'D1:D10', 'type': 'ONE_OF_LIST'}],
        format_dict={'A1:B1': {'bold': True}},
    )
    ws = MockWorksheetDefinition('AllOptions', formatting=formatting)

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws],
    )

    runner._phase_2_generate_data()

    with patch('eftoolkit.gsheets.runner.dashboard_runner.Spreadsheet') as mock_ss:
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.__enter__ = MagicMock(return_value=mock_spreadsheet)
        mock_spreadsheet.__exit__ = MagicMock(return_value=None)
        mock_ss.return_value = mock_spreadsheet

        runner._phase_4_apply_formatting()

        # Verify apply_formatting was called with all formatting options
        mock_spreadsheet.apply_formatting.assert_called_once()
        call_args = mock_spreadsheet.apply_formatting.call_args
        assert call_args[0][0] == 'AllOptions'
        fmt = call_args[0][1]
        assert fmt.freeze_rows == 1
        assert fmt.freeze_columns == 1
        assert fmt.auto_resize_columns == (0, 5)
        assert fmt.merge_ranges == ['A1:C1']
        assert fmt.notes == {'A1': 'Note'}
        assert fmt.column_widths == {'A': 100}
        assert fmt.borders == {'A1:C10': {'style': 'solid'}}
        assert fmt.conditional_formats == [
            {'range': 'B2:B10', 'type': 'CUSTOM_FORMULA'}
        ]
        assert fmt.data_validations == [{'range': 'D1:D10', 'type': 'ONE_OF_LIST'}]
        # format_dict is passed through as the third argument
        assert call_args[0][2] == {'A1:B1': {'bold': True}}


# --- Phase 6: Post-run hooks ---


def test_phase_6_runs_post_hooks_with_spreadsheet():
    """Phase 6 executes post-run hooks with Spreadsheet instance."""
    received_spreadsheets = []

    def test_hook(ss):
        received_spreadsheets.append(ss)

    ws = MockWorksheetDefinition('TestSheet')

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws],
        post_run_hooks=[test_hook],
    )

    with patch('eftoolkit.gsheets.runner.dashboard_runner.Spreadsheet') as mock_ss:
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.__enter__ = MagicMock(return_value=mock_spreadsheet)
        mock_spreadsheet.__exit__ = MagicMock(return_value=None)
        mock_ss.return_value = mock_spreadsheet

        runner._phase_6_run_post_hooks()

    assert len(received_spreadsheets) == 1
    assert received_spreadsheets[0] == mock_spreadsheet


def test_phase_6_runs_multiple_hooks_in_order():
    """Phase 6 executes multiple post-run hooks in order."""
    call_order = []

    def hook_a(ss):
        call_order.append('a')

    def hook_b(ss):
        call_order.append('b')

    def hook_c(ss):
        call_order.append('c')

    ws = MockWorksheetDefinition('TestSheet')

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws],
        post_run_hooks=[hook_a, hook_b, hook_c],
    )

    with patch('eftoolkit.gsheets.runner.dashboard_runner.Spreadsheet') as mock_ss:
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.__enter__ = MagicMock(return_value=mock_spreadsheet)
        mock_spreadsheet.__exit__ = MagicMock(return_value=None)
        mock_ss.return_value = mock_spreadsheet

        runner._phase_6_run_post_hooks()

    assert call_order == ['a', 'b', 'c']


def test_phase_6_skipped_in_local_preview_mode(caplog):
    """Phase 6 skips post-run hooks in local_preview mode."""
    hook_called = []

    def test_hook(ss):
        hook_called.append(True)

    ws = MockWorksheetDefinition('TestSheet')

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={},
        worksheets=[ws],
        post_run_hooks=[test_hook],
        local_preview=True,
    )

    with caplog.at_level(logging.INFO):
        runner._phase_6_run_post_hooks()

    assert hook_called == []
    assert 'Skipping post-run hooks' in caplog.text


def test_phase_6_no_op_with_empty_hooks():
    """Phase 6 is a no-op when no post-run hooks are provided."""
    ws = MockWorksheetDefinition('TestSheet')

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws],
    )

    # Should not raise and should not try to open Spreadsheet
    with patch('eftoolkit.gsheets.runner.dashboard_runner.Spreadsheet') as mock_ss:
        runner._phase_6_run_post_hooks()
        mock_ss.assert_not_called()


def test_init_stores_post_run_hooks():
    """DashboardRunner stores post_run_hooks from init."""

    def hook(ss):
        pass

    ws = MockWorksheetDefinition('TestSheet')

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws],
        post_run_hooks=[hook],
    )

    assert runner.post_run_hooks == [hook]


def test_init_defaults_post_run_hooks_to_empty_list():
    """DashboardRunner defaults post_run_hooks to empty list."""
    ws = MockWorksheetDefinition('TestSheet')

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={'type': 'service_account'},
        worksheets=[ws],
    )

    assert runner.post_run_hooks == []


def test_run_executes_phase_6():
    """run() calls _phase_6_run_post_hooks."""
    ws = MockWorksheetDefinition('TestSheet')

    runner = DashboardRunner(
        config={'sheet_name': 'Test'},
        credentials={},
        worksheets=[ws],
        local_preview=True,
    )

    with (
        patch.object(runner, '_phase_0_run_pre_hooks'),
        patch.object(runner, '_phase_1_validate_structure'),
        patch.object(runner, '_phase_2_generate_data'),
        patch.object(runner, '_phase_3_write_data_and_run_hooks'),
        patch.object(runner, '_phase_4_apply_formatting'),
        patch.object(runner, '_phase_5_log_summary'),
        patch.object(runner, '_phase_6_run_post_hooks') as p6,
    ):
        runner.run()

        p6.assert_called_once()
