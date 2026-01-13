"""Tests for DuckDB get_table method covering example_usage patterns."""

import os

import pandas as pd

from eftoolkit.sql import DuckDB


def test_get_table_without_where_matches_table_to_df(sample_df):
    """get_table() without WHERE matches boxoffice_drafting table_to_df pattern."""
    db_path = 'test_get_table_no_where.db'

    try:
        # Pattern from example_usage/boxoffice_drafting/query.py:table_to_df
        db = DuckDB(database=db_path)
        db.create_table_from_df('test_table', sample_df)

        result = db.get_table('test_table')

        pd.testing.assert_frame_equal(result, sample_df)
    finally:
        if os.path.exists(db_path):
            os.remove(db_path)


def test_get_table_with_where_matches_filtered_query(sample_df):
    """get_table() with WHERE matches filtered query patterns."""
    db_path = 'test_get_table_with_where.db'

    try:
        # Pattern: SELECT * FROM table WHERE condition
        db = DuckDB(database=db_path)
        db.create_table_from_df('test_table', sample_df)

        result = db.get_table('test_table', where='id > 1')

        assert len(result) == 2
        assert list(result['id']) == [2, 3]
    finally:
        if os.path.exists(db_path):
            os.remove(db_path)


def test_get_table_cleans_special_values():
    """get_table cleans inf/nan like example_usage patterns."""
    db_path = 'test_get_table_special.db'

    try:
        # Pattern from example_usage/boxoffice_drafting/query.py:
        # df = df.replace([float('inf'), float('-inf'), float('nan')], None)
        db = DuckDB(database=db_path)
        df = pd.DataFrame(
            {
                'id': [1, 2, 3],
                'val': [float('inf'), float('-inf'), float('nan')],
            }
        )
        db.create_table_from_df('special_table', df)

        result = db.get_table('special_table')

        assert result['val'].isna().all()
    finally:
        if os.path.exists(db_path):
            os.remove(db_path)
