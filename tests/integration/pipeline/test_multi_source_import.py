"""Integration tests for multi-source import.

These tests exercise the real import code path (DuckDB + SQLAlchemy)
but only run the import phase — no LLM calls, ~1s total.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import patch
from uuid import uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

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
    (dir_a / "orders.csv").write_text(
        "order_id,customer,amount\n1,Alice,100\n2,Bob,200\n"
    )

    dir_b = tmp_path / "products"
    dir_b.mkdir()
    (dir_b / "catalog.csv").write_text(
        "product_id,name,price\n10,Widget,9.99\n20,Gadget,19.99\n30,Doohickey,4.99\n"
    )

    return dir_a / "orders.csv", dir_b / "catalog.csv"


@pytest.fixture
def many_column_csv(tmp_path: Path) -> Path:
    """Create a CSV with many columns for limit testing."""
    n_cols = 20
    header = ",".join(f"col_{i}" for i in range(n_cols))
    row = ",".join(str(i) for i in range(n_cols))
    csv_path = tmp_path / "wide.csv"
    csv_path.write_text(f"{header}\n{row}\n")
    return csv_path


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
        tables = session.execute(
            select(Table).where(Table.source_id == source_id, Table.layer == "raw")
        ).scalars().all()
        table_names = {t.table_name for t in tables}
        assert "bookings__orders" in table_names
        assert "products__catalog" in table_names

        # Verify DuckDB has the renamed tables
        duckdb_tables = {
            row[0] for row in duckdb_conn.execute("SHOW TABLES").fetchall()
        }
        assert "bookings__orders" in duckdb_tables
        assert "products__catalog" in duckdb_tables

        # Verify data is intact
        rows = duckdb_conn.execute(
            'SELECT count(*) FROM "bookings__orders"'
        ).fetchone()
        assert rows is not None
        assert rows[0] == 2

        rows = duckdb_conn.execute(
            'SELECT count(*) FROM "products__catalog"'
        ).fetchone()
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
            select(func.count(Column.column_id))
            .join(Table)
            .where(Table.source_id == source_id)
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
        tables = session.execute(
            select(Table).where(Table.source_id == source_id, Table.layer == "raw")
        ).scalars().all()
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
