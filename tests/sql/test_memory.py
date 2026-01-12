"""Tests for DuckDB with in-memory database."""

import pytest

from eftoolkit.sql import DuckDB


def test_query():
    """Test query method."""
    db = DuckDB(database=':memory:')

    result = db.query('SELECT 1 as num')

    assert len(result) == 1
    assert result['num'][0] == 1


def test_context_manager():
    """Test context manager support."""
    db = DuckDB(database=':memory:')

    with db as db_ctx:
        result = db_ctx.query('SELECT 42 as answer')
        assert result['answer'][0] == 42


def test_s3_not_configured(sample_df):
    """Test S3 methods raise when not configured."""
    db = DuckDB(database=':memory:')

    with pytest.raises(ValueError, match='S3 not configured'):
        db.read_parquet_from_s3('s3://bucket/key.parquet')

    with pytest.raises(ValueError, match='S3 not configured'):
        db.write_df_to_s3_parquet(sample_df, 's3://bucket/key.parquet')
