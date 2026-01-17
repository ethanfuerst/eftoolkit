"""Tests for WorksheetDefinition protocol."""

from pathlib import Path

import pandas as pd

from eftoolkit.gsheets.types import CellLocation, WorksheetAsset, WorksheetDefinition


def test_runtime_checkable():
    """WorksheetDefinition is runtime_checkable."""

    class ValidWorksheet:
        @property
        def name(self) -> str:
            return 'Test'

        def generate(self, config: dict, context: dict) -> list[WorksheetAsset]:
            return [
                WorksheetAsset(
                    df=pd.DataFrame({'a': [1]}),
                    location=CellLocation(cell='A1'),
                )
            ]

        def get_format_overrides(self, context: dict) -> dict:
            return {}

    worksheet = ValidWorksheet()

    assert isinstance(worksheet, WorksheetDefinition)


def test_missing_name_not_instance():
    """Class missing name property is not WorksheetDefinition."""

    class MissingName:
        def generate(self, config: dict, context: dict) -> list[WorksheetAsset]:
            return [
                WorksheetAsset(
                    df=pd.DataFrame({'a': [1]}),
                    location=CellLocation(cell='A1'),
                )
            ]

        def get_format_overrides(self, context: dict) -> dict:
            return {}

    obj = MissingName()

    assert not isinstance(obj, WorksheetDefinition)


def test_missing_generate_not_instance():
    """Class missing generate method is not WorksheetDefinition."""

    class MissingGenerate:
        @property
        def name(self) -> str:
            return 'Test'

        def get_format_overrides(self, context: dict) -> dict:
            return {}

    obj = MissingGenerate()

    assert not isinstance(obj, WorksheetDefinition)


def test_missing_get_format_overrides_not_instance():
    """Class missing get_format_overrides method is not WorksheetDefinition."""

    class MissingOverrides:
        @property
        def name(self) -> str:
            return 'Test'

        def generate(self, config: dict, context: dict) -> list[WorksheetAsset]:
            return [
                WorksheetAsset(
                    df=pd.DataFrame({'a': [1]}),
                    location=CellLocation(cell='A1'),
                )
            ]

    obj = MissingOverrides()

    assert not isinstance(obj, WorksheetDefinition)


def test_valid_implementation_single_asset():
    """A valid WorksheetDefinition with a single WorksheetAsset."""

    class SimpleWorksheet:
        @property
        def name(self) -> str:
            return 'Simple'

        def generate(self, config: dict, context: dict) -> list[WorksheetAsset]:
            df = pd.DataFrame({'value': [1, 2, 3]})
            return [
                WorksheetAsset(
                    df=df,
                    location=CellLocation(cell='B2'),
                    format_config_path=Path('formats/simple.json'),
                )
            ]

        def get_format_overrides(self, context: dict) -> dict:
            return {'header_color': '#4a86e8'}

    worksheet = SimpleWorksheet()
    assets = worksheet.generate(config={}, context={})

    assert worksheet.name == 'Simple'
    assert len(assets) == 1
    assert isinstance(assets[0], WorksheetAsset)
    assert len(assets[0].df) == 3
    assert assets[0].location.cell == 'B2'
    assert worksheet.get_format_overrides({}) == {'header_color': '#4a86e8'}


def test_valid_implementation_multiple_assets():
    """A valid WorksheetDefinition with multiple WorksheetAssets on one worksheet."""

    class RevenueWorksheet:
        @property
        def name(self) -> str:
            return 'Revenue'

        def generate(self, config: dict, context: dict) -> list[WorksheetAsset]:
            summary_df = pd.DataFrame({'month': ['Jan', 'Feb'], 'total': [100, 200]})
            breakdown_df = pd.DataFrame({'category': ['A', 'B'], 'amount': [50, 150]})
            return [
                WorksheetAsset(
                    df=summary_df,
                    location=CellLocation(cell='B2'),
                    format_dict={'header_color': '#4a86e8'},
                ),
                WorksheetAsset(
                    df=breakdown_df,
                    location=CellLocation(cell='B10'),
                    format_config_path=Path('formats/breakdown.json'),
                ),
            ]

        def get_format_overrides(self, context: dict) -> dict:
            return {'currency_format': '$#,##0.00'}

    worksheet = RevenueWorksheet()
    assets = worksheet.generate(config={}, context={})

    assert worksheet.name == 'Revenue'
    assert len(assets) == 2
    assert assets[0].location.cell == 'B2'
    assert assets[0].format_dict == {'header_color': '#4a86e8'}
    assert assets[1].location.cell == 'B10'
    assert assets[1].format_config_path == Path('formats/breakdown.json')
