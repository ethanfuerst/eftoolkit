"""Tests for DuckDB imports."""


def test_import_from_sql_module():
    """Test import from eftoolkit.sql."""
    from eftoolkit.sql import DuckDB

    assert DuckDB is not None
