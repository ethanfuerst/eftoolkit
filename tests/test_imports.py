"""Test that all package imports work correctly."""


def test_import_duckdb_from_sql():
    """Test: from eftoolkit.sql import DuckDB"""
    from eftoolkit.sql import DuckDB

    assert DuckDB is not None
    assert hasattr(DuckDB, 'query')
    assert hasattr(DuckDB, 'execute')


def test_import_s3filesystem_from_s3():
    """Test: from eftoolkit.s3 import S3FileSystem"""
    from eftoolkit.s3 import S3FileSystem

    assert S3FileSystem is not None
    assert hasattr(S3FileSystem, 'write_df_to_parquet')
    assert hasattr(S3FileSystem, 'read_df_from_parquet')


def test_import_spreadsheet_from_gsheets():
    """Test: from eftoolkit.gsheets import Spreadsheet, Worksheet"""
    from eftoolkit.gsheets import Spreadsheet, Worksheet

    # Spreadsheet manages worksheets
    assert hasattr(Spreadsheet, 'worksheet')
    assert hasattr(Spreadsheet, 'create_worksheet')
    assert hasattr(Spreadsheet, 'delete_worksheet')

    # Worksheet handles read/write operations
    assert hasattr(Worksheet, 'write_dataframe')
    assert hasattr(Worksheet, 'read')
    assert hasattr(Worksheet, 'flush')


def test_import_runner_from_gsheets():
    """Test: from eftoolkit.gsheets.runner import DashboardRunner, etc."""
    from eftoolkit.gsheets.runner import (
        CellLocation,
        DashboardRunner,
        WorksheetAsset,
        WorksheetDefinition,
        WorksheetFormatting,
        WorksheetRegistry,
    )

    assert DashboardRunner is not None
    assert WorksheetRegistry is not None
    assert CellLocation is not None
    assert WorksheetAsset is not None
    assert WorksheetDefinition is not None
    assert WorksheetFormatting is not None


def test_import_setup_logging_from_utils():
    """Test: from eftoolkit.utils import setup_logging"""
    from eftoolkit.utils import setup_logging

    assert setup_logging is not None


def test_import_json_config_from_gsheets_utils():
    """Test: from eftoolkit.gsheets.utils import load_json_config, etc."""
    from eftoolkit.gsheets.utils import load_json_config, remove_comments

    assert load_json_config is not None
    assert remove_comments is not None
