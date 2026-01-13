"""Tests for DuckDB with persistent database file."""

import os

import pandas as pd

from eftoolkit.sql import DuckDB


def test_full_workflow(sample_df):
    """Test a complete workflow with multiple operations."""
    db_path = 'test_persistent_workflow.db'

    try:
        db = DuckDB(database=db_path)
        db.create_table_from_df('test_table', sample_df)

        result = db.query('SELECT * FROM test_table')
        pd.testing.assert_frame_equal(result, sample_df)

        db.create_table('test_table2', 'SELECT * FROM test_table WHERE id > 1')
        result2 = db.get_table('test_table2')

        assert len(result2) == 2

        df_with_nulls = pd.DataFrame(
            {
                'a': [1, None, float('inf'), float('nan')],
            }
        )
        db.create_table_from_df('null_table', df_with_nulls)
        result3 = db.get_table('null_table')

        assert result3['a'].isna().sum() == 3
    finally:
        if os.path.exists(db_path):
            os.remove(db_path)


def test_get_table_with_where(sample_df):
    """Test get_table with where clause."""
    db_path = 'test_persistent_where.db'

    try:
        db = DuckDB(database=db_path)
        db.create_table_from_df('test_table', sample_df)

        result = db.get_table('test_table', where='id > 1')

        assert len(result) == 2
        assert list(result['id']) == [2, 3]
    finally:
        if os.path.exists(db_path):
            os.remove(db_path)
