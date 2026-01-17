"""Tests for WorksheetAsset dataclass."""

from pathlib import Path

import pandas as pd

from eftoolkit.gsheets.types import CellLocation, WorksheetAsset


def test_create_minimal():
    """WorksheetAsset with only required fields."""
    df = pd.DataFrame({'a': [1, 2, 3]})
    location = CellLocation(cell='B4')

    asset = WorksheetAsset(df=df, location=location)

    assert asset.df is df
    assert asset.location == location
    assert asset.format_config_path is None
    assert asset.format_dict is None
    assert asset.post_write_hooks == []


def test_create_with_format_config_path():
    """WorksheetAsset with format_config_path."""
    df = pd.DataFrame({'a': [1]})
    location = CellLocation(cell='A1')
    config_path = Path('formats/summary.json')

    asset = WorksheetAsset(df=df, location=location, format_config_path=config_path)

    assert asset.format_config_path == config_path


def test_create_with_format_dict():
    """WorksheetAsset with inline format_dict."""
    df = pd.DataFrame({'a': [1]})
    location = CellLocation(cell='A1')
    format_dict = {'header_color': '#4a86e8', 'freeze_rows': 1}

    asset = WorksheetAsset(df=df, location=location, format_dict=format_dict)

    assert asset.format_dict == format_dict


def test_create_with_post_write_hooks():
    """WorksheetAsset with post_write_hooks."""
    df = pd.DataFrame({'a': [1]})
    location = CellLocation(cell='A1')
    hooks = [lambda ws: None, lambda ws: None]

    asset = WorksheetAsset(df=df, location=location, post_write_hooks=hooks)

    assert len(asset.post_write_hooks) == 2


def test_post_write_hooks_default_empty_list():
    """Each WorksheetAsset gets its own empty list for post_write_hooks."""
    df = pd.DataFrame({'a': [1]})
    location = CellLocation(cell='A1')

    asset1 = WorksheetAsset(df=df, location=location)
    asset2 = WorksheetAsset(df=df, location=location)

    # Modify one, shouldn't affect the other
    asset1.post_write_hooks.append(lambda ws: None)

    assert len(asset1.post_write_hooks) == 1
    assert len(asset2.post_write_hooks) == 0


def test_create_with_merge_ranges():
    """WorksheetAsset with merge_ranges."""
    df = pd.DataFrame({'a': [1]})
    location = CellLocation(cell='A1')
    merge_ranges = ['B2:F2', 'I2:X2']

    asset = WorksheetAsset(df=df, location=location, merge_ranges=merge_ranges)

    assert asset.merge_ranges == ['B2:F2', 'I2:X2']


def test_create_with_conditional_formats():
    """WorksheetAsset with conditional_formats."""
    df = pd.DataFrame({'a': [1]})
    location = CellLocation(cell='A1')
    conditional_formats = [
        {
            'range': 'X5:X100',
            'type': 'TEXT_EQ',
            'values': ['Yes'],
            'format': {'bold': True},
        }
    ]

    asset = WorksheetAsset(
        df=df, location=location, conditional_formats=conditional_formats
    )

    assert len(asset.conditional_formats) == 1
    assert asset.conditional_formats[0]['range'] == 'X5:X100'


def test_create_with_notes():
    """WorksheetAsset with notes."""
    df = pd.DataFrame({'a': [1]})
    location = CellLocation(cell='A1')
    notes = {'U4': 'Note text here', 'A1': 'Header note'}

    asset = WorksheetAsset(df=df, location=location, notes=notes)

    assert asset.notes == {'U4': 'Note text here', 'A1': 'Header note'}


def test_create_with_column_widths():
    """WorksheetAsset with column_widths."""
    df = pd.DataFrame({'a': [1]})
    location = CellLocation(cell='A1')
    column_widths = {'A': 25, 'J': 284}

    asset = WorksheetAsset(df=df, location=location, column_widths=column_widths)

    assert asset.column_widths == {'A': 25, 'J': 284}


def test_create_with_all_rich_formatting():
    """WorksheetAsset with all rich formatting options."""
    df = pd.DataFrame({'a': [1]})
    location = CellLocation(cell='B4')

    asset = WorksheetAsset(
        df=df,
        location=location,
        merge_ranges=['B2:F2'],
        conditional_formats=[
            {'range': 'B5:B10', 'type': 'NUMBER_GT', 'values': ['100']}
        ],
        notes={'B4': 'Start of data'},
        column_widths={'A': 100, 'B': 200},
    )

    assert asset.merge_ranges == ['B2:F2']
    assert len(asset.conditional_formats) == 1
    assert asset.notes == {'B4': 'Start of data'}
    assert asset.column_widths == {'A': 100, 'B': 200}


def test_rich_formatting_defaults_empty():
    """Rich formatting fields default to empty collections."""
    df = pd.DataFrame({'a': [1]})
    location = CellLocation(cell='A1')

    asset = WorksheetAsset(df=df, location=location)

    assert asset.merge_ranges == []
    assert asset.conditional_formats == []
    assert asset.notes == {}
    assert asset.column_widths == {}


def test_rich_formatting_defaults_not_shared():
    """Each WorksheetAsset gets its own default collections for rich formatting."""
    df = pd.DataFrame({'a': [1]})
    location = CellLocation(cell='A1')

    asset1 = WorksheetAsset(df=df, location=location)
    asset2 = WorksheetAsset(df=df, location=location)

    # Modify asset1's collections
    asset1.merge_ranges.append('A1:B1')
    asset1.notes['A1'] = 'test'
    asset1.column_widths['A'] = 100

    # asset2 should be unaffected
    assert asset2.merge_ranges == []
    assert asset2.notes == {}
    assert asset2.column_widths == {}
