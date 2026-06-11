"""Tests for `extract_backend` against DuckDB's built-in sqlite extension.

Sqlite is a real backend (not a mock) so these tests exercise the
actual INSTALL/LOAD/ATTACH/CREATE TABLE/DETACH pipeline end-to-end.
The mssql-specific behavior is covered in the integration smoke test
in Phase 7.

Post-DAT-504 the extraction targets ``lake.raw`` FQNs (the same convergent
write the csv/json/parquet loaders do). These unit tests keep that cheap with
an in-memory ``ATTACH ':memory:' AS lake`` + ``CREATE SCHEMA lake.raw`` —
the real DuckLake catalog is exercised in
``tests/integration/sources/test_db_recipe_lake.py``.
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


def _attach_lake(conn: duckdb.DuckDBPyConnection) -> None:
    """Stand in for the production lake catalog on a unit-test connection."""
    conn.execute("ATTACH ':memory:' AS lake")
    conn.execute("CREATE SCHEMA lake.raw")
    conn.execute("CREATE SCHEMA lake.typed")


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
    _attach_lake(conn)
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

    def test_creates_real_duckdb_tables_in_lake_raw(self, sqlite_source, duckdb_conn):
        extract_backend(
            backend="sqlite",
            url=sqlite_source,
            queries=[RecipeTable(name="customers", sql="SELECT * FROM customers")],
            duckdb_conn=duckdb_conn,
        )
        # After extraction, the raw table lives in lake.raw (DAT-504).
        actual_rows = duckdb_conn.execute(
            'SELECT count(*) FROM lake.raw."raw_customers"'
        ).fetchone()
        assert actual_rows[0] == 3

    def test_no_stray_table_outside_lake_raw(self, sqlite_source, duckdb_conn):
        """The DAT-504 live bug: tables used to land in ``<catalog>.main`` while
        metadata registered layer="raw" → ``lake.raw``. Pin that NOTHING lands
        outside the raw schema."""
        extract_backend(
            backend="sqlite",
            url=sqlite_source,
            queries=[RecipeTable(name="customers", sql="SELECT * FROM customers")],
            duckdb_conn=duckdb_conn,
        )
        strays = duckdb_conn.execute(
            "SELECT database_name, schema_name, table_name FROM duckdb_tables() "
            "WHERE NOT (database_name = 'lake' AND schema_name = 'raw')"
        ).fetchall()
        assert strays == []

    def test_second_execution_with_leftovers_converges(self, sqlite_source, duckdb_conn):
        """CREATE OR REPLACE: a redelivery overwrites the leftover raw table
        instead of colliding (lake convergence rule)."""
        for _ in range(2):
            result = extract_backend(
                backend="sqlite",
                url=sqlite_source,
                queries=[RecipeTable(name="customers", sql="SELECT * FROM customers")],
                duckdb_conn=duckdb_conn,
            )
            assert result.success, result.error
            assert result.unwrap().tables[0].row_count == 3
        count = duckdb_conn.execute(
            "SELECT count(*) FROM duckdb_tables() "
            "WHERE database_name = 'lake' AND schema_name = 'raw' "
            "AND table_name = 'raw_customers'"
        ).fetchone()
        assert count[0] == 1

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
            'SELECT customer_name FROM lake.raw."raw_orders_with_customer" ORDER BY order_id'
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
        result = duckdb_conn.execute("SELECT current_catalog()").fetchone()
        assert result[0] == "memory"

    def test_use_restore_keeps_catalog_and_schema_pair(self, sqlite_source, duckdb_conn):
        """The third DAT-504 bug: the old ``USE {catalog}`` restore stranded the
        connection at ``<catalog>.main``. Production sits at ``lake.typed`` —
        the restore must bring back the snapshotted (catalog, schema) PAIR."""
        duckdb_conn.execute("USE lake.typed")
        result = extract_backend(
            backend="sqlite",
            url=sqlite_source,
            queries=[RecipeTable(name="customers", sql="SELECT * FROM customers")],
            duckdb_conn=duckdb_conn,
        )
        assert result.success, result.error
        pair = duckdb_conn.execute("SELECT current_catalog(), current_schema()").fetchone()
        assert pair == ("lake", "typed")

    def test_use_restore_keeps_pair_after_sql_failure(self, sqlite_source, duckdb_conn):
        duckdb_conn.execute("USE lake.typed")
        result = extract_backend(
            backend="sqlite",
            url=sqlite_source,
            queries=[RecipeTable(name="bad", sql="SELECT * FROM no_such_table")],
            duckdb_conn=duckdb_conn,
        )
        assert not result.success
        pair = duckdb_conn.execute("SELECT current_catalog(), current_schema()").fetchone()
        assert pair == ("lake", "typed")


class TestExtensionCachePreBake:
    """The image pre-bakes extensions (worker.Dockerfile): extract_backend must
    honor DUCKDB_EXTENSION_DIRECTORY (cache lookup at the baked path) and
    DUCKLAKE_SKIP_INSTALL (no network INSTALL) — the same contract as
    bootstrap_lake / apply_s3_secret in server/storage.py.
    """

    def test_install_lands_in_configured_extension_directory(
        self, sqlite_source, duckdb_conn, tmp_path, monkeypatch
    ):
        """With DUCKDB_EXTENSION_DIRECTORY set, the INSTALL writes the cache there."""
        ext_dir = tmp_path / "ext-cache"
        monkeypatch.setenv("DUCKDB_EXTENSION_DIRECTORY", str(ext_dir))
        result = extract_backend(
            backend="sqlite",
            url=sqlite_source,
            queries=[RecipeTable(name="customers", sql="SELECT * FROM customers")],
            duckdb_conn=duckdb_conn,
        )
        assert result.success, result.error
        assert list(ext_dir.rglob("sqlite_scanner.duckdb_extension"))

    def test_skip_install_loads_from_pre_baked_cache(self, sqlite_source, tmp_path, monkeypatch):
        """The container contract: extensions baked at build time, INSTALL skipped.

        Bake sqlite into a cache dir (stand-in for the image build step), then
        run extract_backend with DUCKLAKE_SKIP_INSTALL=1 — LOAD must resolve
        the baked file and the extraction must work end-to-end.
        """
        ext_dir = tmp_path / "ext-cache"
        bake = duckdb.connect(":memory:")
        bake.execute(f"SET extension_directory = '{ext_dir}'")
        bake.execute("INSTALL sqlite")
        bake.close()

        monkeypatch.setenv("DUCKDB_EXTENSION_DIRECTORY", str(ext_dir))
        monkeypatch.setenv("DUCKLAKE_SKIP_INSTALL", "1")
        conn = duckdb.connect(":memory:")
        _attach_lake(conn)
        try:
            result = extract_backend(
                backend="sqlite",
                url=sqlite_source,
                queries=[RecipeTable(name="customers", sql="SELECT * FROM customers")],
                duckdb_conn=conn,
            )
            assert result.success, result.error
            assert result.unwrap().tables[0].row_count == 3
        finally:
            conn.close()


class TestExtractBackendFileBackedConnection:
    """A file-backed connection (its default catalog named after the file) must
    behave exactly like :memory:: extraction lands in ``lake.raw`` regardless of
    the connection's own catalog, and the USE-restore brings back the file
    catalog afterwards."""

    def test_extracts_into_lake_raw_not_file_catalog(self, sqlite_source, tmp_path):
        db_path = tmp_path / "session_data.duckdb"
        conn = duckdb.connect(str(db_path))
        _attach_lake(conn)
        try:
            result = extract_backend(
                backend="sqlite",
                url=sqlite_source,
                queries=[RecipeTable(name="customers", sql="SELECT * FROM customers")],
                duckdb_conn=conn,
                raw_prefix="aw_",
            )
            assert result.success, result.error
            assert result.value is not None
            assert len(result.value.tables) == 1
            assert result.value.tables[0].duckdb_table == "aw_customers"

            # The materialized table lives in lake.raw, NOT in the file catalog.
            count = conn.execute('SELECT count(*) FROM lake.raw."aw_customers"').fetchone()
            assert count is not None
            assert count[0] >= 1
            file_catalog = conn.execute("SELECT current_catalog()").fetchone()[0]
            strays = conn.execute(
                "SELECT table_name FROM duckdb_tables() WHERE database_name = ?",
                [file_catalog],
            ).fetchall()
            assert strays == []
        finally:
            conn.close()

    def test_default_catalog_restored_after_extraction_file_backed(self, sqlite_source, tmp_path):
        """The finally-block USE must restore the file-backed catalog, not 'memory'."""
        db_path = tmp_path / "session_data.duckdb"
        conn = duckdb.connect(str(db_path))
        _attach_lake(conn)
        try:
            expected_catalog = conn.execute("SELECT current_catalog()").fetchone()[0]
            extract_backend(
                backend="sqlite",
                url=sqlite_source,
                queries=[RecipeTable(name="customers", sql="SELECT * FROM customers")],
                duckdb_conn=conn,
            )
            assert conn.execute("SELECT current_catalog()").fetchone()[0] == expected_catalog
        finally:
            conn.close()
