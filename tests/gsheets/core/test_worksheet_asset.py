"""Tests for WorksheetAsset dataclass."""

import pandas as pd

from eftoolkit.gsheets.runner import CellLocation, WorksheetAsset


def test_create_minimal():
    """WorksheetAsset with only required fields."""
    df = pd.DataFrame({'a': [1, 2, 3]})
    location = CellLocation(cell='B4')

    asset = WorksheetAsset(df=df, location=location)

    assert asset.df is df
    assert asset.location == location
    assert asset.post_write_hooks == []


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
