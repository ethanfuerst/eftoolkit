"""Tests for DuckDB connection property, close method, and context manager."""

import duckdb

from eftoolkit.sql import DuckDB


def test_close_without_context_manager_is_noop():
    """close() is a no-op when not using context manager."""
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


def test_context_manager_opens_and_closes_connection():
    """Context manager opens connection on entry and closes on exit."""
    db = DuckDB(database=':memory:')

    assert db._active_conn is None

    with db:
        assert db._active_conn is not None
        assert isinstance(db._active_conn, duckdb.DuckDBPyConnection)

    assert db._active_conn is None


def test_context_manager_reuses_connection(tmp_path):
    """All operations within context manager reuse the same connection."""
    db_path = str(tmp_path / 'test.db')

    with DuckDB(database=db_path) as db:
        conn_id = id(db._active_conn)
        db.execute('CREATE TABLE t (x INT)')
        assert id(db._active_conn) == conn_id
        db.execute('INSERT INTO t VALUES (1), (2), (3)')
        assert id(db._active_conn) == conn_id
        result = db.query('SELECT SUM(x) as total FROM t')
        assert id(db._active_conn) == conn_id

    assert result['total'][0] == 6


def test_context_manager_persists_data_in_memory():
    """Data created within context manager persists for the duration."""
    with DuckDB(database=':memory:') as db:
        db.execute('CREATE TABLE nums (val INT)')
        db.execute('INSERT INTO nums VALUES (10), (20)')
        result = db.query('SELECT * FROM nums')

    assert len(result) == 2
    assert list(result['val']) == [10, 20]


def test_close_within_context_manager():
    """close() within context manager closes the active connection."""
    db = DuckDB(database=':memory:')

    with db:
        assert db._active_conn is not None
        db.close()
        assert db._active_conn is None
