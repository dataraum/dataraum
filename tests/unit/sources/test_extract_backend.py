"""Tests for `extract_backend` against DuckDB's built-in sqlite extension.

Sqlite is a real backend (not a mock) so these tests exercise the
actual INSTALL/LOAD/ATTACH/CREATE TABLE/DETACH pipeline end-to-end.
The mssql-specific behavior is covered in the integration smoke test
in Phase 7.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import duckdb
import pytest

from dataraum.sources.backends import (
    BACKEND_ATTACH_TYPES,
    BACKEND_EXTENSIONS,
    extract_backend,
)
from dataraum.sources.db_recipe import RecipeTable


@pytest.fixture
def sqlite_source(tmp_path: Path) -> str:
    """A small sqlite database with two tables and a foreign key."""
    db_path = tmp_path / "source.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            region TEXT
        );
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            total REAL NOT NULL,
            order_date TEXT,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        );
        INSERT INTO customers VALUES
            (1, 'Acme Corp', 'EMEA'),
            (2, 'Globex',    'APAC'),
            (3, 'Initech',   'AMER');
        INSERT INTO orders VALUES
            (101, 1, 100.50,  '2024-01-15'),
            (102, 1, 250.00,  '2024-02-01'),
            (103, 2,  75.25,  '2024-02-10'),
            (104, 3, 999.99,  '2024-03-01');
        """
    )
    conn.commit()
    conn.close()
    return str(db_path)


@pytest.fixture
def duckdb_conn():
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


class TestRegistry:
    def test_supported_backends_include_mssql(self):
        assert "mssql" in BACKEND_EXTENSIONS
        assert BACKEND_EXTENSIONS["mssql"] == "mssql"
        assert BACKEND_ATTACH_TYPES["mssql"] == "MSSQL"

    def test_all_four_backends_registered(self):
        for b in ("mssql", "postgres", "mysql", "sqlite"):
            assert b in BACKEND_EXTENSIONS, b
            assert b in BACKEND_ATTACH_TYPES, b


class TestExtractBackendHappyPath:
    def test_materializes_single_query(self, sqlite_source, duckdb_conn):
        result = extract_backend(
            backend="sqlite",
            url=sqlite_source,
            queries=[
                RecipeTable(name="customers", sql="SELECT customer_id, name, region FROM customers")
            ],
            duckdb_conn=duckdb_conn,
        )
        assert result.success, result.error
        payload = result.unwrap()
        assert len(payload.tables) == 1
        t = payload.tables[0]
        assert t.name == "customers"
        assert t.duckdb_table == "raw_customers"
        assert t.row_count == 3
        col_names = [c[0] for c in t.columns]
        assert col_names == ["customer_id", "name", "region"]

    def test_materializes_multiple_queries(self, sqlite_source, duckdb_conn):
        result = extract_backend(
            backend="sqlite",
            url=sqlite_source,
            queries=[
                RecipeTable(name="customers", sql="SELECT * FROM customers"),
                RecipeTable(name="orders", sql="SELECT * FROM orders"),
            ],
            duckdb_conn=duckdb_conn,
        )
        assert result.success, result.error
        names = [t.name for t in result.unwrap().tables]
        assert names == ["customers", "orders"]
        rows_by_name = {t.name: t.row_count for t in result.unwrap().tables}
        assert rows_by_name == {"customers": 3, "orders": 4}

    def test_creates_real_duckdb_tables(self, sqlite_source, duckdb_conn):
        extract_backend(
            backend="sqlite",
            url=sqlite_source,
            queries=[RecipeTable(name="customers", sql="SELECT * FROM customers")],
            duckdb_conn=duckdb_conn,
        )
        # After extraction, the DuckDB connection should hold the raw table.
        actual_rows = duckdb_conn.execute("SELECT count(*) FROM raw_customers").fetchone()
        assert actual_rows[0] == 3

    def test_user_sql_with_where_clause(self, sqlite_source, duckdb_conn):
        result = extract_backend(
            backend="sqlite",
            url=sqlite_source,
            queries=[
                RecipeTable(
                    name="big_orders",
                    sql="SELECT order_id, total FROM orders WHERE total > 100",
                )
            ],
            duckdb_conn=duckdb_conn,
        )
        assert result.success, result.error
        assert result.unwrap().tables[0].row_count == 3  # 100.50, 250.00, 999.99

    def test_user_sql_with_join(self, sqlite_source, duckdb_conn):
        result = extract_backend(
            backend="sqlite",
            url=sqlite_source,
            queries=[
                RecipeTable(
                    name="orders_with_customer",
                    sql=(
                        "SELECT o.order_id, o.total, c.name AS customer_name "
                        "FROM orders o JOIN customers c "
                        "ON c.customer_id = o.customer_id"
                    ),
                )
            ],
            duckdb_conn=duckdb_conn,
        )
        assert result.success, result.error
        rows = duckdb_conn.execute(
            "SELECT customer_name FROM raw_orders_with_customer ORDER BY order_id"
        ).fetchall()
        assert [r[0] for r in rows] == ["Acme Corp", "Acme Corp", "Globex", "Initech"]

    def test_zero_rows_is_warning_not_error(self, sqlite_source, duckdb_conn):
        result = extract_backend(
            backend="sqlite",
            url=sqlite_source,
            queries=[RecipeTable(name="empty", sql="SELECT * FROM customers WHERE 1=0")],
            duckdb_conn=duckdb_conn,
        )
        assert result.success, result.error
        payload = result.unwrap()
        assert payload.tables[0].row_count == 0
        assert any("0 rows" in w for w in payload.warnings)


class TestExtractBackendFailures:
    def test_unsupported_backend(self, duckdb_conn):
        result = extract_backend(
            backend="oracle",
            url="x",
            queries=[RecipeTable(name="t", sql="SELECT 1")],
            duckdb_conn=duckdb_conn,
        )
        assert not result.success
        assert "oracle" in result.error.lower()

    def test_empty_queries_rejected(self, sqlite_source, duckdb_conn):
        result = extract_backend(
            backend="sqlite",
            url=sqlite_source,
            queries=[],
            duckdb_conn=duckdb_conn,
        )
        assert not result.success
        assert "at least one query" in result.error.lower()

    def test_bad_sql_fails_loud_with_table_name(self, sqlite_source, duckdb_conn):
        result = extract_backend(
            backend="sqlite",
            url=sqlite_source,
            queries=[
                RecipeTable(
                    name="bad_table",
                    sql="SELECT nonexistent_column FROM customers",
                )
            ],
            duckdb_conn=duckdb_conn,
        )
        assert not result.success
        assert "bad_table" in result.error
        assert "SELECT failed" in result.error

    def test_missing_source_db_fails_loud(self, tmp_path, duckdb_conn):
        result = extract_backend(
            backend="sqlite",
            url=str(tmp_path / "does_not_exist.db"),
            queries=[RecipeTable(name="t", sql="SELECT 1")],
            duckdb_conn=duckdb_conn,
        )
        assert not result.success
        # Either ATTACH fails (preferred) or CREATE TABLE fails — both acceptable
        # as long as the message surfaces the problem.
        assert (
            "ATTACH" in result.error
            or "SELECT failed" in result.error
            or "not exist" in result.error.lower()
        )


class TestExtractBackendCleanup:
    def test_connection_left_clean_after_success(self, sqlite_source, duckdb_conn):
        extract_backend(
            backend="sqlite",
            url=sqlite_source,
            queries=[RecipeTable(name="customers", sql="SELECT * FROM customers")],
            duckdb_conn=duckdb_conn,
        )
        # After extraction, the alias must be detached — another ATTACH with
        # the same alias should succeed.
        duckdb_conn.execute(f"ATTACH '{sqlite_source}' AS src (TYPE SQLITE, READ_ONLY)")
        duckdb_conn.execute("DETACH src")

    def test_connection_left_clean_after_sql_failure(self, sqlite_source, duckdb_conn):
        result = extract_backend(
            backend="sqlite",
            url=sqlite_source,
            queries=[
                RecipeTable(name="bad", sql="SELECT * FROM no_such_table"),
            ],
            duckdb_conn=duckdb_conn,
        )
        assert not result.success
        # The alias must be detached even on failure.
        duckdb_conn.execute(f"ATTACH '{sqlite_source}' AS src (TYPE SQLITE, READ_ONLY)")
        duckdb_conn.execute("DETACH src")

    def test_default_catalog_restored_after_extraction(self, sqlite_source, duckdb_conn):
        extract_backend(
            backend="sqlite",
            url=sqlite_source,
            queries=[RecipeTable(name="customers", sql="SELECT * FROM customers")],
            duckdb_conn=duckdb_conn,
        )
        # After extraction, the default catalog must be memory again so
        # subsequent queries against memory.main.* resolve correctly.
        result = duckdb_conn.execute("SELECT current_database()").fetchone()
        assert result[0] == "memory"
