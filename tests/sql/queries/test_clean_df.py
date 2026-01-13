"""Tests for DuckDB _clean_df method."""

import os

import pandas as pd

from eftoolkit.sql import DuckDB


def test_clean_df_replaces_inf_with_none():
    """_clean_df replaces positive infinity with None."""
    db = DuckDB(database=':memory:')

    df = pd.DataFrame({'val': [1.0, float('inf'), 3.0]})

    result = db._clean_df(df)

    assert result['val'].iloc[0] == 1.0
    assert pd.isna(result['val'].iloc[1])
    assert result['val'].iloc[2] == 3.0


def test_clean_df_replaces_negative_inf_with_none():
    """_clean_df replaces negative infinity with None."""
    db = DuckDB(database=':memory:')

    df = pd.DataFrame({'val': [1.0, float('-inf'), 3.0]})

    result = db._clean_df(df)

    assert result['val'].iloc[0] == 1.0
    assert pd.isna(result['val'].iloc[1])
    assert result['val'].iloc[2] == 3.0


def test_clean_df_replaces_nan_with_none():
    """_clean_df replaces NaN with None."""
    db = DuckDB(database=':memory:')

    df = pd.DataFrame({'val': [1.0, float('nan'), 3.0]})

    result = db._clean_df(df)

    assert result['val'].iloc[0] == 1.0
    assert pd.isna(result['val'].iloc[1])
    assert result['val'].iloc[2] == 3.0


def test_clean_df_handles_multiple_special_values():
    """_clean_df handles multiple types of special values."""
    db = DuckDB(database=':memory:')

    df = pd.DataFrame(
        {
            'val': [float('inf'), float('-inf'), float('nan'), 1.0],
        }
    )

    result = db._clean_df(df)

    assert pd.isna(result['val'].iloc[0])
    assert pd.isna(result['val'].iloc[1])
    assert pd.isna(result['val'].iloc[2])
    assert result['val'].iloc[3] == 1.0


def test_get_table_applies_clean_df():
    """get_table applies _clean_df to results."""
    db_path = 'test_clean_df_get_table.db'

    try:
        db = DuckDB(database=db_path)

        df = pd.DataFrame({'val': [1.0, float('inf'), float('nan')]})
        db.create_table_from_df('special_vals', df)

        result = db.get_table('special_vals')

        # All special values should be None
        assert result['val'].iloc[0] == 1.0
        assert pd.isna(result['val'].iloc[1])
        assert pd.isna(result['val'].iloc[2])
    finally:
        if os.path.exists(db_path):
            os.remove(db_path)
