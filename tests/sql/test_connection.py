"""Tests for DuckDB connection property and close method."""

import duckdb

from eftoolkit.sql import DuckDB


def test_close_is_noop():
    """close() is a no-op for connection-per-operation model."""
    db = DuckDB(database=':memory:')
    db.close()

    # Should still work after close
    result = db.query('SELECT 1 as num')

    assert result['num'][0] == 1


def test_connection_property_returns_connection():
    """connection property returns a DuckDB connection."""
    db = DuckDB(database=':memory:')
    conn = db.connection

    assert isinstance(conn, duckdb.DuckDBPyConnection)
