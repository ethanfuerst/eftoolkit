#!/usr/bin/env python3
"""Basic DuckDB usage examples.

This example demonstrates common DuckDB operations using eftoolkit's DuckDB wrapper.
The wrapper provides a thin layer over DuckDB with optional S3 integration.

Run with: uv run python examples/basic_duckdb.py
"""

import os

import pandas as pd

from eftoolkit.sql import DuckDB


def main():
    """Demonstrate basic DuckDB operations."""
    # Use a file-based database so tables persist across operations
    # (in-memory databases lose data between connections)
    db = DuckDB(database='example_demo.duckdb')

    # --- Create tables from SQL ---
    print('=== Creating tables from SQL ===')

    # create_table() runs CREATE OR REPLACE TABLE ... AS (sql)
    db.create_table(
        'users',
        """
        SELECT * FROM (VALUES
            (1, 'Alice', 'alice@example.com'),
            (2, 'Bob', 'bob@example.com'),
            (3, 'Charlie', 'charlie@example.com')
        ) AS t(id, name, email)
        """,
    )
    print('Created users table')

    db.create_table(
        'orders',
        """
        SELECT * FROM (VALUES
            (101, 1, 99.99, '2024-01-15'),
            (102, 2, 149.50, '2024-01-16'),
            (103, 1, 75.00, '2024-01-17'),
            (104, 3, 200.00, '2024-01-18')
        ) AS t(order_id, user_id, amount, order_date)
        """,
    )
    print('Created orders table')

    # --- Create table from DataFrame ---
    print('\n=== Creating table from DataFrame ===')

    products_df = pd.DataFrame(
        {
            'product_id': [1, 2, 3],
            'name': ['Widget', 'Gadget', 'Gizmo'],
            'price': [29.99, 49.99, 19.99],
        }
    )
    db.create_table_from_df('products', products_df)
    print('Created products table from DataFrame')

    # --- Query data ---
    print('\n=== Querying data ===')

    # query() returns a DataFrame
    result = db.query("""
        SELECT u.name, SUM(o.amount) as total_spent
        FROM users u
        JOIN orders o ON u.id = o.user_id
        GROUP BY u.name
        ORDER BY total_spent DESC
    """)
    print('User spending:')
    print(result.to_string(index=False))

    # --- Get table with optional WHERE clause ---
    print('\n=== Using get_table() ===')

    # Get all users
    all_users = db.get_table('users')
    print(f'All users ({len(all_users)} rows):')
    print(all_users.to_string(index=False))

    # Get users with WHERE clause
    filtered_users = db.get_table('users', where="name LIKE 'A%'")
    print(f"\nUsers starting with 'A' ({len(filtered_users)} rows):")
    print(filtered_users.to_string(index=False))

    # --- Execute arbitrary SQL ---
    print('\n=== Using execute() for DDL/DML ===')

    # execute() is for statements that don't return data
    db.execute('CREATE INDEX IF NOT EXISTS idx_user_id ON orders(user_id)')
    print('Created index')

    db.execute("INSERT INTO users VALUES (4, 'Diana', 'diana@example.com')")
    print('Inserted new user')

    # Verify the insert
    new_users = db.get_table('users', where='id = 4')
    print(f'New user: {new_users.to_dict("records")[0]}')

    # --- Cleanup ---
    print('\n=== Cleanup ===')

    # Clean up the demo database file
    os.remove('example_demo.duckdb')
    print('Cleaned up example_demo.duckdb')

    print('\n=== Done! ===')


if __name__ == '__main__':
    main()
