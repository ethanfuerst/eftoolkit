"""Tests for DuckDB wrapper."""

import pandas as pd
import pytest


class TestDuckDBPersistent:
    """Test DuckDB operations with a persistent database file."""

    def test_full_workflow(self, persistent_db, sample_df):
        """Test a complete workflow with multiple operations."""
        db = persistent_db
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

    def test_get_table_with_where(self, persistent_db, sample_df):
        """Test get_table with where clause."""
        db = persistent_db
        db.create_table_from_df('test_table', sample_df)

        result = db.get_table('test_table', where='id > 1')
        assert len(result) == 2
        assert list(result['id']) == [2, 3]


class TestDuckDBInMemory:
    """Test individual DuckDB operations with in-memory database."""

    def test_query(self, memory_db):
        """Test query method."""
        result = memory_db.query('SELECT 1 as num')
        assert len(result) == 1
        assert result['num'][0] == 1

    def test_context_manager(self, memory_db):
        """Test context manager support."""
        with memory_db as db:
            result = db.query('SELECT 42 as answer')
            assert result['answer'][0] == 42

    def test_s3_not_configured(self, memory_db, sample_df):
        """Test S3 methods raise when not configured."""
        with pytest.raises(ValueError, match='S3 not configured'):
            memory_db.read_parquet_from_s3('bucket', 'key')

        with pytest.raises(ValueError, match='S3 not configured'):
            memory_db.write_df_to_s3_parquet(sample_df, 'bucket', 'key')


class TestDuckDBImports:
    """Test that imports work correctly."""

    def test_import_from_sql_module(self):
        """Test import from eftoolkit.sql."""
        from eftoolkit.sql import DuckDB

        assert DuckDB is not None

    def test_import_from_root(self):
        """Test import from eftoolkit root."""
        from eftoolkit import DuckDB

        assert DuckDB is not None
