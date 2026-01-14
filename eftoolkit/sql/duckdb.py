"""DuckDB wrapper with S3 integration."""

from contextlib import contextmanager
from typing import TYPE_CHECKING, Optional

import duckdb
import pandas as pd

if TYPE_CHECKING:
    from eftoolkit.s3 import S3FileSystem


class DuckDB:
    """Thin wrapper around duckdb.DuckDBPyConnection with S3 integration.

    Inherits all native DuckDB methods (query, execute, sql, fetchone, fetchall, etc.)
    via delegation to the underlying connection.

    S3 operations use eftoolkit.s3.S3FileSystem internally.
    """

    def __init__(
        self,
        database: str = ':memory:',
        *,
        s3: Optional['S3FileSystem'] = None,
        s3_region: str | None = None,
        s3_access_key_id: str | None = None,
        s3_secret_access_key: str | None = None,
        s3_endpoint: str | None = None,
        s3_url_style: str | None = None,
    ):
        """Initialize DuckDB with optional S3 integration.

        Args:
            database: Path to the database file or ':memory:' for in-memory database
            s3: Existing S3FileSystem instance to use for S3 operations
            s3_region: AWS region for S3 access (creates S3FileSystem internally)
            s3_access_key_id: AWS access key ID for S3 access
            s3_secret_access_key: AWS secret access key for S3 access
            s3_endpoint: Custom S3 endpoint
            s3_url_style: S3 URL style ('path' or 'vhost')
        """
        self.database = database
        self._s3 = s3
        self.s3_region = s3_region
        self.s3_access_key_id = s3_access_key_id
        self.s3_secret_access_key = s3_secret_access_key
        self.s3_endpoint = s3_endpoint
        self.s3_url_style = s3_url_style
        self._active_conn: duckdb.DuckDBPyConnection | None = None

        # Create S3FileSystem from credentials if provided and no s3 instance given
        if self._s3 is None and s3_access_key_id and s3_secret_access_key:
            from eftoolkit.s3 import S3FileSystem

            self._s3 = S3FileSystem(
                access_key_id=s3_access_key_id,
                secret_access_key=s3_secret_access_key,
                region=s3_region,
                endpoint=s3_endpoint,
            )

    @property
    def s3(self) -> Optional['S3FileSystem']:
        """S3FileSystem instance used for S3 operations, or None if not configured."""
        return self._s3

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """Underlying DuckDB connection (for direct access to native API)."""
        return self._get_connection().__enter__()

    def _setup_s3(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Configure S3 credentials on the connection."""
        if self.s3_access_key_id and self.s3_secret_access_key:
            conn.execute('INSTALL httpfs;')
            conn.execute('LOAD httpfs;')

            if self.s3_url_style:
                conn.execute(f"SET s3_url_style='{self.s3_url_style}';")

            endpoint_clause = (
                f", ENDPOINT '{self.s3_endpoint}'" if self.s3_endpoint else ''
            )
            conn.execute(f"""
                CREATE SECRET IF NOT EXISTS (
                    TYPE S3,
                    KEY_ID '{self.s3_access_key_id}',
                    SECRET '{self.s3_secret_access_key}',
                    REGION '{self.s3_region}'
                    {endpoint_clause}
                );
            """)

    @contextmanager
    def _get_connection(self):
        """Get a configured database connection.

        If used within a context manager (with statement), reuses the active
        connection. Otherwise, creates a new connection for each operation.
        """
        if self._active_conn is not None:
            # Reuse active connection from context manager
            yield self._active_conn
        else:
            # Create a new connection for this operation
            conn = duckdb.connect(database=self.database)
            self._setup_s3(conn)
            try:
                yield conn
            finally:
                conn.close()

    def _clean_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Replace inf/nan values with None."""
        return df.replace([float('inf'), float('-inf'), float('nan')], None)

    def query(self, sql: str) -> pd.DataFrame:
        """Execute SQL and return DataFrame.

        Args:
            sql: SQL query to execute.

        Returns:
            DataFrame containing the query results.

        Example:
            >>> db = DuckDB()
            >>> df = db.query("SELECT 1 as id, 'Alice' as name")
            >>> print(df)
               id   name
            0   1  Alice
        """
        with self._get_connection() as conn:
            return conn.query(sql).fetchdf()

    def execute(self, sql: str, *args: object, **kwargs: object) -> None:
        """Execute SQL without returning results.

        This method can be used for any DuckDB SQL command, including:
        - DDL statements (CREATE, DROP, ALTER)
        - DML statements (INSERT, UPDATE, DELETE)
        - DuckDB COPY commands for S3 writes (e.g., COPY ... TO 's3://...')

        Args:
            sql: SQL statement to execute.
            *args: Positional arguments passed to duckdb execute.
            **kwargs: Keyword arguments passed to duckdb execute.
        """
        with self._get_connection() as conn:
            conn.execute(sql, *args, **kwargs)

    def get_table(self, table_name: str, where: str | None = None) -> pd.DataFrame:
        """SELECT * FROM table with optional WHERE clause.

        Automatically cleans inf/nan values to None.

        Args:
            table_name: Name of the table to query.
            where: Optional WHERE clause (without 'WHERE' keyword).

        Returns:
            DataFrame with table contents.

        Example:
            >>> db = DuckDB()
            >>> db.create_table('users', "SELECT 1 as id, 'Alice' as name")
            >>> df = db.get_table('users')
            >>> filtered = db.get_table('users', where="id = 1")
        """
        where_clause = f' WHERE {where}' if where else ''
        df = self.query(f'SELECT * FROM {table_name}{where_clause}')
        return self._clean_df(df)

    def create_table(self, table_name: str, sql: str) -> None:
        """CREATE OR REPLACE TABLE from SQL.

        Args:
            table_name: Name for the new table.
            sql: SQL SELECT statement to define table contents.

        Example:
            >>> db = DuckDB()
            >>> db.create_table('active_users', "SELECT * FROM users WHERE active = true")
        """
        self.execute(f'CREATE OR REPLACE TABLE {table_name} AS ({sql})')

    def create_table_from_df(self, table_name: str, df: pd.DataFrame) -> None:
        """CREATE OR REPLACE TABLE from DataFrame.

        Args:
            table_name: Name for the new table.
            df: DataFrame to store as a table.

        Example:
            >>> import pandas as pd
            >>> db = DuckDB()
            >>> df = pd.DataFrame({'id': [1, 2], 'name': ['Alice', 'Bob']})
            >>> db.create_table_from_df('users', df)
        """
        with self._get_connection() as conn:
            conn.register('temp_df', df)
            conn.execute(
                f'CREATE OR REPLACE TABLE {table_name} AS (SELECT * FROM temp_df)'
            )

    def read_parquet_from_s3(self, s3_uri: str) -> pd.DataFrame:
        """Read parquet from S3.

        Args:
            s3_uri: S3 URI (e.g., 's3://bucket/path/file.parquet')

        Returns:
            DataFrame with parquet contents

        Raises:
            ValueError: If S3 is not configured
        """
        if self._s3 is None:
            raise ValueError(
                'S3 not configured. Pass s3= or S3 credentials to __init__'
            )
        return self._s3.read_df_from_parquet(s3_uri)

    def write_df_to_s3_parquet(self, df: pd.DataFrame, s3_uri: str) -> None:
        """Write DataFrame to S3 as parquet.

        Args:
            df: DataFrame to write
            s3_uri: S3 URI (e.g., 's3://bucket/path/file.parquet')

        Raises:
            ValueError: If S3 is not configured
        """
        if self._s3 is None:
            raise ValueError(
                'S3 not configured. Pass s3= or S3 credentials to __init__'
            )
        self._s3.write_df_to_parquet(df, s3_uri)

    def __enter__(self) -> 'DuckDB':
        """Context manager entry - opens a persistent connection.

        When used as a context manager, all operations within the block
        reuse the same connection for better performance.

        Example:
            >>> with DuckDB() as db:
            ...     db.execute("CREATE TABLE t (x INT)")
            ...     db.execute("INSERT INTO t VALUES (1)")
            ...     result = db.query("SELECT * FROM t")
        """
        self._active_conn = duckdb.connect(database=self.database)
        self._setup_s3(self._active_conn)
        return self

    def __exit__(self, *_args) -> None:
        """Context manager exit - closes the persistent connection."""
        if self._active_conn is not None:
            self._active_conn.close()
            self._active_conn = None

    def close(self) -> None:
        """Close the active connection if one exists."""
        if self._active_conn is not None:
            self._active_conn.close()
            self._active_conn = None
