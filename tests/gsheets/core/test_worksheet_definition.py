"""Tests for WorksheetDefinition protocol."""

import pandas as pd

from eftoolkit.gsheets.runner import (
    CellLocation,
    WorksheetAsset,
    WorksheetDefinition,
    WorksheetFormatting,
)


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

        def get_formatting(self, context: dict) -> WorksheetFormatting | None:
            return None

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

        def get_formatting(self, context: dict) -> WorksheetFormatting | None:
            return None

    obj = MissingName()

    assert not isinstance(obj, WorksheetDefinition)


def test_missing_generate_not_instance():
    """Class missing generate method is not WorksheetDefinition."""

    class MissingGenerate:
        @property
        def name(self) -> str:
            return 'Test'

        def get_formatting(self, context: dict) -> WorksheetFormatting | None:
            return None

    obj = MissingGenerate()

    assert not isinstance(obj, WorksheetDefinition)


def test_missing_get_formatting_not_instance():
    """Class missing get_formatting method is not WorksheetDefinition."""

    class MissingFormatting:
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

    obj = MissingFormatting()

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
                )
            ]

        def get_formatting(self, context: dict) -> WorksheetFormatting | None:
            return WorksheetFormatting(
                freeze_rows=1,
                format_dict={'header_color': '#4a86e8'},
            )

    worksheet = SimpleWorksheet()
    assets = worksheet.generate(config={}, context={})
    formatting = worksheet.get_formatting({})

    assert worksheet.name == 'Simple'
    assert len(assets) == 1
    assert isinstance(assets[0], WorksheetAsset)
    assert len(assets[0].df) == 3
    assert assets[0].location.cell == 'B2'
    assert formatting is not None
    assert formatting.freeze_rows == 1
    assert formatting.format_dict == {'header_color': '#4a86e8'}


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
                ),
                WorksheetAsset(
                    df=breakdown_df,
                    location=CellLocation(cell='B10'),
                ),
            ]

        def get_formatting(self, context: dict) -> WorksheetFormatting | None:
            return WorksheetFormatting(
                freeze_rows=1,
                auto_resize_columns=(0, 5),
                format_dict={'currency_format': '$#,##0.00'},
            )

    worksheet = RevenueWorksheet()
    assets = worksheet.generate(config={}, context={})
    formatting = worksheet.get_formatting({})

    assert worksheet.name == 'Revenue'
    assert len(assets) == 2
    assert assets[0].location.cell == 'B2'
    assert assets[1].location.cell == 'B10'
    assert formatting is not None
    assert formatting.freeze_rows == 1
    assert formatting.auto_resize_columns == (0, 5)


def test_get_formatting_returns_none():
    """WorksheetDefinition can return None from get_formatting()."""

    class NoFormattingWorksheet:
        @property
        def name(self) -> str:
            return 'NoFormatting'

        def generate(self, config: dict, context: dict) -> list[WorksheetAsset]:
            return [
                WorksheetAsset(
                    df=pd.DataFrame({'a': [1]}),
                    location=CellLocation(cell='A1'),
                )
            ]

        def get_formatting(self, context: dict) -> WorksheetFormatting | None:
            return None

    worksheet = NoFormattingWorksheet()
    formatting = worksheet.get_formatting({})

    assert formatting is None
