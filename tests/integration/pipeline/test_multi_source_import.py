"""Integration tests for multi-source import.

These tests exercise the real import code path (DuckDB + SQLAlchemy)
but only run the import phase — no LLM calls, ~1-2s total.

Coverage:
  - Individual source types: CSV, Parquet, SQLite-as-database
  - Combinations: CSV+Parquet, CSV+Parquet+SQLite (all types in one import)
  - Column limits across combined sources
  - Name collision avoidance
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch
from uuid import uuid4

import duckdb as duckdb_pkg
import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from dataraum.core.credentials import ResolvedCredential
from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases.import_phase import ImportPhase
from dataraum.storage import Column, Source, Table

if TYPE_CHECKING:
    import duckdb


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def two_csv_sources(tmp_path: Path) -> tuple[Path, Path]:
    """Create two CSV files in separate directories simulating two sources."""
    dir_a = tmp_path / "bookings"
    dir_a.mkdir()
    (dir_a / "orders.csv").write_text("order_id,customer,amount\n1,Alice,100\n2,Bob,200\n")

    dir_b = tmp_path / "products"
    dir_b.mkdir()
    (dir_b / "catalog.csv").write_text(
        "product_id,name,price\n10,Widget,9.99\n20,Gadget,19.99\n30,Doohickey,4.99\n"
    )

    return dir_a / "orders.csv", dir_b / "catalog.csv"


@pytest.fixture
def parquet_source(tmp_path: Path) -> Path:
    """Create a Parquet file with typed data (integers, floats, strings)."""
    pq_path = tmp_path / "metrics.parquet"
    conn = duckdb_pkg.connect(":memory:")
    conn.execute(
        f"""
        COPY (
            SELECT 1 AS metric_id, 'revenue' AS metric_name, 1500.50 AS value
            UNION ALL SELECT 2, 'cost', 800.25
            UNION ALL SELECT 3, 'profit', 700.25
        ) TO '{pq_path}' (FORMAT PARQUET)
        """
    )
    conn.close()
    return pq_path


@pytest.fixture
def sqlite_source(tmp_path: Path) -> Path:
    """Create a SQLite database with test tables."""
    db_path = tmp_path / "warehouse.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE customers (id INTEGER, name TEXT, email TEXT, active INTEGER)")
    conn.execute(
        "INSERT INTO customers VALUES (1, 'Alice', 'alice@example.com', 1), "
        "(2, 'Bob', 'bob@example.com', 1), "
        "(3, 'Charlie', 'charlie@example.com', 0)"
    )
    conn.execute("CREATE TABLE regions (region_id INTEGER, region_name TEXT)")
    conn.execute("INSERT INTO regions VALUES (1, 'North'), (2, 'South')")
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def many_column_csv(tmp_path: Path) -> Path:
    """Create a CSV with many columns for limit testing."""
    n_cols = 20
    header = ",".join(f"col_{i}" for i in range(n_cols))
    row = ",".join(str(i) for i in range(n_cols))
    csv_path = tmp_path / "wide.csv"
    csv_path.write_text(f"{header}\n{row}\n")
    return csv_path


def _mock_credential_chain(url: str) -> MagicMock:
    """Create a mock CredentialChain that resolves to the given URL."""
    chain = MagicMock()
    chain.resolve.return_value = ResolvedCredential(url=url, source="test")
    return chain


# ---------------------------------------------------------------------------
# Multi-source import tests
# ---------------------------------------------------------------------------


class TestMultiSourceImport:
    """Integration tests for loading multiple registered sources."""

    def test_two_file_sources_loaded(
        self,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        two_csv_sources: tuple[Path, Path],
    ):
        """Two CSV file sources produce prefixed tables."""
        orders_csv, catalog_csv = two_csv_sources
        phase = ImportPhase()
        source_id = str(uuid4())

        registered = [
            {"name": "bookings", "source_type": "csv", "path": str(orders_csv)},
            {"name": "products", "source_type": "csv", "path": str(catalog_csv)},
        ]

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={"registered_sources": registered},
        )

        result = phase.run(ctx)
        session.commit()

        assert result.status == PhaseStatus.COMPLETED, f"Failed: {result.error}"
        assert len(result.outputs["raw_tables"]) == 2

        # Verify tables have prefixed names
        tables = (
            session.execute(select(Table).where(Table.source_id == source_id, Table.layer == "raw"))
            .scalars()
            .all()
        )
        table_names = {t.table_name for t in tables}
        assert "bookings__orders" in table_names
        assert "products__catalog" in table_names

        # Verify DuckDB has the renamed tables
        duckdb_tables = {row[0] for row in duckdb_conn.execute("SHOW TABLES").fetchall()}
        assert "bookings__orders" in duckdb_tables
        assert "products__catalog" in duckdb_tables

        # Verify data is intact
        rows = duckdb_conn.execute('SELECT count(*) FROM "bookings__orders"').fetchone()
        assert rows is not None
        assert rows[0] == 2

        rows = duckdb_conn.execute('SELECT count(*) FROM "products__catalog"').fetchone()
        assert rows is not None
        assert rows[0] == 3

    def test_columns_created_for_all_sources(
        self,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        two_csv_sources: tuple[Path, Path],
    ):
        """Column records are created for every table across sources."""
        orders_csv, catalog_csv = two_csv_sources
        phase = ImportPhase()
        source_id = str(uuid4())

        registered = [
            {"name": "bookings", "source_type": "csv", "path": str(orders_csv)},
            {"name": "products", "source_type": "csv", "path": str(catalog_csv)},
        ]

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={"registered_sources": registered},
        )

        result = phase.run(ctx)
        session.commit()
        assert result.status == PhaseStatus.COMPLETED

        # 3 columns from orders + 3 columns from catalog = 6 total
        total_cols = session.execute(
            select(func.count(Column.column_id)).join(Table).where(Table.source_id == source_id)
        ).scalar_one()
        assert total_cols == 6

    def test_source_record_created_as_multi_source(
        self,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        two_csv_sources: tuple[Path, Path],
    ):
        """A single Source record is created with type 'multi_source'."""
        orders_csv, catalog_csv = two_csv_sources
        phase = ImportPhase()
        source_id = str(uuid4())

        registered = [
            {"name": "bookings", "source_type": "csv", "path": str(orders_csv)},
            {"name": "products", "source_type": "csv", "path": str(catalog_csv)},
        ]

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={"registered_sources": registered},
        )

        result = phase.run(ctx)
        session.commit()
        assert result.status == PhaseStatus.COMPLETED

        source = session.get(Source, source_id)
        assert source is not None
        assert source.source_type == "multi_source"
        assert source.connection_config is not None
        assert set(source.connection_config["sources"]) == {"bookings", "products"}

    def test_partial_failure_still_loads_good_sources(
        self,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        two_csv_sources: tuple[Path, Path],
    ):
        """If one source fails, the other is still loaded (with warning)."""
        _, catalog_csv = two_csv_sources
        phase = ImportPhase()
        source_id = str(uuid4())

        registered = [
            {"name": "bad", "source_type": "csv", "path": "/nonexistent/file.csv"},
            {"name": "products", "source_type": "csv", "path": str(catalog_csv)},
        ]

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={"registered_sources": registered},
        )

        result = phase.run(ctx)
        session.commit()

        assert result.status == PhaseStatus.COMPLETED
        assert len(result.outputs["raw_tables"]) == 1
        assert any("bad" in w for w in result.warnings)

    def test_all_sources_fail_returns_failure(
        self,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ):
        """When every source fails, the phase fails."""
        phase = ImportPhase()
        source_id = str(uuid4())

        registered = [
            {"name": "bad1", "source_type": "csv", "path": "/nonexistent/a.csv"},
            {"name": "bad2", "source_type": "csv", "path": "/nonexistent/b.csv"},
        ]

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={"registered_sources": registered},
        )

        result = phase.run(ctx)

        assert result.status == PhaseStatus.FAILED
        assert "No tables were loaded" in (result.error or "")

    def test_legacy_single_path_still_works(
        self,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        two_csv_sources: tuple[Path, Path],
    ):
        """source_path config still works (legacy mode)."""
        orders_csv, _ = two_csv_sources
        phase = ImportPhase()
        source_id = str(uuid4())

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={"source_path": str(orders_csv)},
        )

        result = phase.run(ctx)
        session.commit()

        assert result.status == PhaseStatus.COMPLETED
        tables = (
            session.execute(select(Table).where(Table.source_id == source_id, Table.layer == "raw"))
            .scalars()
            .all()
        )
        # No prefix — legacy mode uses the file stem directly
        assert tables[0].table_name == "orders"


# ---------------------------------------------------------------------------
# Column limit tests (real DB)
# ---------------------------------------------------------------------------


class TestColumnLimitIntegration:
    """Integration tests for column limit enforcement with real databases."""

    def test_under_limit_succeeds(
        self,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        many_column_csv: Path,
    ):
        """Import succeeds when column count is under the limit."""
        phase = ImportPhase()
        source_id = str(uuid4())

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={"source_path": str(many_column_csv)},
        )

        # 20 columns, default limit is 500
        result = phase.run(ctx)
        session.commit()

        assert result.status == PhaseStatus.COMPLETED

    def test_over_limit_fails(
        self,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        many_column_csv: Path,
    ):
        """Import fails with clear message when column limit exceeded."""
        phase = ImportPhase()
        source_id = str(uuid4())

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={"source_path": str(many_column_csv)},
        )

        # Set limit to 5, our CSV has 20 columns
        with patch(
            "dataraum.pipeline.phases.import_phase.load_pipeline_config",
            return_value={"limits": {"max_columns": 5}},
        ):
            result = phase.run(ctx)

        assert result.status == PhaseStatus.FAILED
        assert "Column limit exceeded" in (result.error or "")
        assert "20 > 5" in (result.error or "")

    def test_limit_enforced_across_multi_source(
        self,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        two_csv_sources: tuple[Path, Path],
    ):
        """Column limit counts columns across ALL sources, not per-source."""
        orders_csv, catalog_csv = two_csv_sources
        phase = ImportPhase()
        source_id = str(uuid4())

        registered = [
            {"name": "bookings", "source_type": "csv", "path": str(orders_csv)},
            {"name": "products", "source_type": "csv", "path": str(catalog_csv)},
        ]

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={"registered_sources": registered},
        )

        # 3 + 3 = 6 columns total, set limit to 4
        with patch(
            "dataraum.pipeline.phases.import_phase.load_pipeline_config",
            return_value={"limits": {"max_columns": 4}},
        ):
            result = phase.run(ctx)

        assert result.status == PhaseStatus.FAILED
        assert "Column limit exceeded" in (result.error or "")
        assert "6 > 4" in (result.error or "")


# ---------------------------------------------------------------------------
# Individual source type tests
# ---------------------------------------------------------------------------


class TestParquetSource:
    """Parquet file source loads with correct types and prefixed names."""

    def test_parquet_file_loaded(
        self,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        parquet_source: Path,
    ):
        """Single Parquet file loads with native types preserved."""
        phase = ImportPhase()
        source_id = str(uuid4())

        registered = [
            {"name": "analytics", "source_type": "parquet", "path": str(parquet_source)},
        ]

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={"registered_sources": registered},
        )

        result = phase.run(ctx)
        session.commit()

        assert result.status == PhaseStatus.COMPLETED, f"Failed: {result.error}"
        assert len(result.outputs["raw_tables"]) == 1

        # Verify prefixed table name
        tables = (
            session.execute(select(Table).where(Table.source_id == source_id, Table.layer == "raw"))
            .scalars()
            .all()
        )
        assert len(tables) == 1
        assert tables[0].table_name == "analytics__metrics"

        # Verify data accessible in DuckDB
        rows = duckdb_conn.execute('SELECT count(*) FROM "analytics__metrics"').fetchone()
        assert rows is not None
        assert rows[0] == 3

        # Verify columns have native types (not VARCHAR like CSV)
        columns = (
            session.execute(select(Column).where(Column.table_id == tables[0].table_id))
            .scalars()
            .all()
        )
        col_types = {c.column_name: c.raw_type for c in columns}
        assert col_types["metric_id"] != "VARCHAR"  # Should be INTEGER/BIGINT
        assert col_types["value"] != "VARCHAR"  # Should be DOUBLE/FLOAT


class TestSQLiteSource:
    """SQLite database source loads via DuckDB ATTACH."""

    def test_sqlite_source_loaded(
        self,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        sqlite_source: Path,
    ):
        """SQLite database tables imported with prefixed names."""
        # DuckDB needs the sqlite extension loaded
        duckdb_conn.execute("INSTALL sqlite; LOAD sqlite;")

        phase = ImportPhase()
        source_id = str(uuid4())

        registered = [
            {
                "name": "warehouse",
                "source_type": "sqlite",
                "backend": "sqlite",
                "credential_ref": "warehouse",
                "tables": ["customers", "regions"],
            },
        ]

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={"registered_sources": registered},
        )

        mock_chain = _mock_credential_chain(str(sqlite_source))
        with patch(
            "dataraum.core.credentials.CredentialChain",
            return_value=mock_chain,
        ):
            result = phase.run(ctx)
        session.commit()

        assert result.status == PhaseStatus.COMPLETED, f"Failed: {result.error}"
        assert len(result.outputs["raw_tables"]) == 2

        # Verify prefixed table names
        tables = (
            session.execute(select(Table).where(Table.source_id == source_id, Table.layer == "raw"))
            .scalars()
            .all()
        )
        table_names = {t.table_name for t in tables}
        assert table_names == {"warehouse__customers", "warehouse__regions"}

        # Verify data accessible in DuckDB
        rows = duckdb_conn.execute('SELECT count(*) FROM "warehouse__customers"').fetchone()
        assert rows is not None
        assert rows[0] == 3

        rows = duckdb_conn.execute('SELECT count(*) FROM "warehouse__regions"').fetchone()
        assert rows is not None
        assert rows[0] == 2

        # Verify Column records created
        for table in tables:
            cols = (
                session.execute(select(Column).where(Column.table_id == table.table_id))
                .scalars()
                .all()
            )
            assert len(cols) > 0

    def test_sqlite_table_filter(
        self,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        sqlite_source: Path,
    ):
        """Only specified tables are imported when filter is set."""
        duckdb_conn.execute("INSTALL sqlite; LOAD sqlite;")

        phase = ImportPhase()
        source_id = str(uuid4())

        registered = [
            {
                "name": "warehouse",
                "source_type": "sqlite",
                "backend": "sqlite",
                "credential_ref": "warehouse",
                "tables": ["customers"],  # Only customers, not regions
            },
        ]

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={"registered_sources": registered},
        )

        mock_chain = _mock_credential_chain(str(sqlite_source))
        with patch(
            "dataraum.core.credentials.CredentialChain",
            return_value=mock_chain,
        ):
            result = phase.run(ctx)
        session.commit()

        assert result.status == PhaseStatus.COMPLETED
        assert len(result.outputs["raw_tables"]) == 1

        tables = (
            session.execute(select(Table).where(Table.source_id == source_id, Table.layer == "raw"))
            .scalars()
            .all()
        )
        assert len(tables) == 1
        assert tables[0].table_name == "warehouse__customers"


# ---------------------------------------------------------------------------
# Combination tests (multiple source types in one import)
# ---------------------------------------------------------------------------


class TestSourceCombinations:
    """Multiple source types loaded together in a single import pass."""

    def test_csv_plus_parquet(
        self,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        two_csv_sources: tuple[Path, Path],
        parquet_source: Path,
    ):
        """CSV and Parquet sources combined in one multi-source import."""
        orders_csv, _ = two_csv_sources
        phase = ImportPhase()
        source_id = str(uuid4())

        registered = [
            {"name": "bookings", "source_type": "csv", "path": str(orders_csv)},
            {"name": "analytics", "source_type": "parquet", "path": str(parquet_source)},
        ]

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={"registered_sources": registered},
        )

        result = phase.run(ctx)
        session.commit()

        assert result.status == PhaseStatus.COMPLETED, f"Failed: {result.error}"
        assert len(result.outputs["raw_tables"]) == 2

        tables = (
            session.execute(select(Table).where(Table.source_id == source_id, Table.layer == "raw"))
            .scalars()
            .all()
        )
        table_names = {t.table_name for t in tables}
        assert "bookings__orders" in table_names
        assert "analytics__metrics" in table_names

    def test_all_types_combined(
        self,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        two_csv_sources: tuple[Path, Path],
        parquet_source: Path,
        sqlite_source: Path,
    ):
        """CSV + Parquet + SQLite all in one import — the crown jewel test."""
        duckdb_conn.execute("INSTALL sqlite; LOAD sqlite;")

        orders_csv, catalog_csv = two_csv_sources
        phase = ImportPhase()
        source_id = str(uuid4())

        registered = [
            {"name": "bookings", "source_type": "csv", "path": str(orders_csv)},
            {"name": "products", "source_type": "csv", "path": str(catalog_csv)},
            {"name": "analytics", "source_type": "parquet", "path": str(parquet_source)},
            {
                "name": "warehouse",
                "source_type": "sqlite",
                "backend": "sqlite",
                "credential_ref": "warehouse",
                "tables": ["customers", "regions"],
            },
        ]

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={"registered_sources": registered},
        )

        mock_chain = _mock_credential_chain(str(sqlite_source))
        with patch(
            "dataraum.core.credentials.CredentialChain",
            return_value=mock_chain,
        ):
            result = phase.run(ctx)
        session.commit()

        assert result.status == PhaseStatus.COMPLETED, f"Failed: {result.error}"

        # 2 CSV + 1 Parquet + 2 SQLite tables = 5 total
        assert len(result.outputs["raw_tables"]) == 5

        tables = (
            session.execute(select(Table).where(Table.source_id == source_id, Table.layer == "raw"))
            .scalars()
            .all()
        )
        table_names = {t.table_name for t in tables}
        assert table_names == {
            "bookings__orders",
            "products__catalog",
            "analytics__metrics",
            "warehouse__customers",
            "warehouse__regions",
        }

        # All tables share the same source_id
        source_ids = {t.source_id for t in tables}
        assert source_ids == {source_id}

        # All tables have columns
        for table in tables:
            col_count = session.execute(
                select(func.count(Column.column_id)).where(Column.table_id == table.table_id)
            ).scalar_one()
            assert col_count > 0, f"Table {table.table_name} has no columns"

        # Verify total column count:
        # orders(3) + catalog(3) + metrics(3) + customers(4) + regions(2) = 15
        total_cols = session.execute(
            select(func.count(Column.column_id)).join(Table).where(Table.source_id == source_id)
        ).scalar_one()
        assert total_cols == 15

        # All 5 DuckDB tables queryable
        for name in table_names:
            count = duckdb_conn.execute(f'SELECT count(*) FROM "{name}"').fetchone()
            assert count is not None
            assert count[0] > 0, f"DuckDB table {name} is empty"

    def test_name_collision_avoided(
        self,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        tmp_path: Path,
    ):
        """Two sources with identically-named files get distinct table names."""
        # Both sources have a file called "data.csv" but with different content
        dir_a = tmp_path / "source_a"
        dir_a.mkdir()
        (dir_a / "data.csv").write_text("id,x\n1,a\n2,b\n")

        dir_b = tmp_path / "source_b"
        dir_b.mkdir()
        (dir_b / "data.csv").write_text("id,y,z\n1,c,d\n")

        phase = ImportPhase()
        source_id = str(uuid4())

        registered = [
            {"name": "alpha", "source_type": "csv", "path": str(dir_a / "data.csv")},
            {"name": "beta", "source_type": "csv", "path": str(dir_b / "data.csv")},
        ]

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={"registered_sources": registered},
        )

        result = phase.run(ctx)
        session.commit()

        assert result.status == PhaseStatus.COMPLETED, f"Failed: {result.error}"
        assert len(result.outputs["raw_tables"]) == 2

        tables = (
            session.execute(select(Table).where(Table.source_id == source_id, Table.layer == "raw"))
            .scalars()
            .all()
        )
        table_names = {t.table_name for t in tables}
        # Prefix prevents collision
        assert table_names == {"alpha__data", "beta__data"}

        # Different row counts prove they're distinct tables
        alpha_count = duckdb_conn.execute('SELECT count(*) FROM "alpha__data"').fetchone()
        beta_count = duckdb_conn.execute('SELECT count(*) FROM "beta__data"').fetchone()
        assert alpha_count is not None and alpha_count[0] == 2
        assert beta_count is not None and beta_count[0] == 1
