"""Test that all package imports work correctly."""


class TestPackageImports:
    """Test package import structure."""

    def test_import_duckdb_from_sql(self):
        """Test: from eftoolkit.sql import DuckDB"""
        from eftoolkit.sql import DuckDB

        assert DuckDB is not None
        assert hasattr(DuckDB, 'query')
        assert hasattr(DuckDB, 'execute')

    def test_import_s3filesystem_from_s3(self):
        """Test: from eftoolkit.s3 import S3FileSystem"""
        from eftoolkit.s3 import S3FileSystem

        assert S3FileSystem is not None
        assert hasattr(S3FileSystem, 'write_df_to_parquet')
        assert hasattr(S3FileSystem, 'read_df_from_parquet')

    def test_import_spreadsheet_from_gsheets(self):
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

    def test_import_all_from_root(self):
        """Test importing all main classes from package root."""
        from eftoolkit import DuckDB, S3FileSystem, Spreadsheet, Worksheet

        assert DuckDB is not None
        assert S3FileSystem is not None
        assert Spreadsheet is not None
        assert Worksheet is not None

    def test_package_version(self):
        """Test package has version."""
        import eftoolkit

        assert hasattr(eftoolkit, '__version__')
        assert eftoolkit.__version__ == '0.0.1'
