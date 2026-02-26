"""Multi-source pipeline integration tests.

Layer 3: Run multi-source data through import → typing → statistics
to verify downstream phases correctly process tables from multiple sources.

These tests use the PipelineTestHarness with no LLM calls.
Runtime: ~2-5s per test.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import patch

import duckdb as duckdb_pkg
import pytest
from sqlalchemy import func, select

from dataraum.analysis.statistics.db_models import StatisticalProfile
from dataraum.core.credentials import ResolvedCredential
from dataraum.pipeline.base import PhaseStatus
from dataraum.storage import Column, Table

if TYPE_CHECKING:
    from tests.integration.conftest import PipelineTestHarness


# ---------------------------------------------------------------------------
# Fixtures: multi-source data with variety of types
# ---------------------------------------------------------------------------


@pytest.fixture
def multi_source_csv_dir(tmp_path: Path) -> dict[str, Path]:
    """CSV files with varied column types for typing/statistics coverage."""
    # Orders: integers, strings, floats
    orders_csv = tmp_path / "orders.csv"
    orders_csv.write_text(
        "order_id,customer_name,amount,quantity\n"
        "1,Alice,100.50,2\n"
        "2,Bob,200.75,5\n"
        "3,Charlie,50.25,1\n"
        "4,Diana,300.00,3\n"
        "5,Eve,150.60,4\n"
    )

    # Events: dates, categories, booleans
    events_csv = tmp_path / "events.csv"
    events_csv.write_text(
        "event_id,event_date,category,is_active\n"
        "1,2024-01-15,launch,true\n"
        "2,2024-02-20,update,true\n"
        "3,2024-03-10,bugfix,false\n"
        "4,2024-04-05,launch,true\n"
    )

    return {"orders": orders_csv, "events": events_csv}


@pytest.fixture
def multi_source_parquet(tmp_path: Path) -> Path:
    """Parquet file with native numeric types."""
    pq_path = tmp_path / "metrics.parquet"
    conn = duckdb_pkg.connect(":memory:")
    conn.execute(
        f"""
        COPY (
            SELECT
                1 AS metric_id, 'revenue' AS name, 1500.50 AS value, 10 AS count
            UNION ALL SELECT 2, 'cost', 800.25, 20
            UNION ALL SELECT 3, 'profit', 700.25, 30
            UNION ALL SELECT 4, 'margin', 0.47, 40
            UNION ALL SELECT 5, 'growth', 1.15, 50
        ) TO '{pq_path}' (FORMAT PARQUET)
        """
    )
    conn.close()
    return pq_path


@pytest.fixture
def multi_source_sqlite(tmp_path: Path) -> Path:
    """SQLite database with integer and text columns."""
    db_path = tmp_path / "warehouse.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE regions (region_id INTEGER, region_name TEXT, country TEXT)")
    conn.execute(
        "INSERT INTO regions VALUES "
        "(1, 'North America', 'US'), "
        "(2, 'Europe', 'DE'), "
        "(3, 'Asia Pacific', 'JP')"
    )
    conn.commit()
    conn.close()
    return db_path


# ---------------------------------------------------------------------------
# Tests: multi-source data through import → typing → statistics
# ---------------------------------------------------------------------------


class TestMultiSourcePipeline:
    """Run multi-source data through multiple pipeline phases."""

    def _run_import(
        self,
        harness: PipelineTestHarness,
        registered_sources: list[dict],
        *,
        mock_db_url: str | None = None,
    ) -> None:
        """Run import phase with registered sources."""
        config: dict = {"registered_sources": registered_sources}

        if mock_db_url:
            from unittest.mock import MagicMock

            mock_chain = MagicMock()
            mock_chain.resolve.return_value = ResolvedCredential(url=mock_db_url, source="test")
            with patch(
                "dataraum.core.credentials.CredentialChain",
                return_value=mock_chain,
            ):
                result = harness.run_phase("import", config=config)
        else:
            result = harness.run_phase("import", config=config)

        assert result.status == PhaseStatus.COMPLETED, f"Import failed: {result.error}"

    def _run_typing(self, harness: PipelineTestHarness) -> None:
        """Run typing phase."""
        result = harness.run_phase("typing")
        assert result.status == PhaseStatus.COMPLETED, f"Typing failed: {result.error}"

    def _run_statistics(self, harness: PipelineTestHarness) -> None:
        """Run statistics phase."""
        result = harness.run_phase("statistics")
        assert result.status == PhaseStatus.COMPLETED, f"Statistics failed: {result.error}"

    def test_csv_sources_through_statistics(
        self,
        harness: PipelineTestHarness,
        multi_source_csv_dir: dict[str, Path],
    ):
        """Two CSV sources → typing → statistics: all tables get typed and profiled."""
        registered = [
            {
                "name": "sales",
                "source_type": "csv",
                "path": str(multi_source_csv_dir["orders"]),
            },
            {
                "name": "tracking",
                "source_type": "csv",
                "path": str(multi_source_csv_dir["events"]),
            },
        ]

        self._run_import(harness, registered)
        self._run_typing(harness)
        self._run_statistics(harness)

        with harness.session_factory() as session:
            # Verify raw tables exist with prefixed names
            raw_tables = (
                session.execute(
                    select(Table).where(
                        Table.source_id == harness.source_id, Table.layer == "raw"
                    )
                )
                .scalars()
                .all()
            )
            raw_names = {t.table_name for t in raw_tables}
            assert raw_names == {"sales__orders", "tracking__events"}

            # Verify typed tables exist (created by typing phase)
            typed_tables = (
                session.execute(
                    select(Table).where(
                        Table.source_id == harness.source_id, Table.layer == "typed"
                    )
                )
                .scalars()
                .all()
            )
            assert len(typed_tables) == 2
            typed_names = {t.table_name for t in typed_tables}
            assert typed_names == {"sales__orders", "tracking__events"}

            # Verify statistics were computed for all typed columns
            for typed_table in typed_tables:
                col_count = session.execute(
                    select(func.count(Column.column_id)).where(
                        Column.table_id == typed_table.table_id
                    )
                ).scalar_one()
                assert col_count > 0, f"No columns for typed table {typed_table.table_name}"

                stat_count = session.execute(
                    select(func.count(StatisticalProfile.profile_id))
                    .join(Column)
                    .where(Column.table_id == typed_table.table_id)
                ).scalar_one()
                assert stat_count > 0, (
                    f"No statistics for typed table {typed_table.table_name}"
                )

    def test_csv_plus_parquet_through_statistics(
        self,
        harness: PipelineTestHarness,
        multi_source_csv_dir: dict[str, Path],
        multi_source_parquet: Path,
    ):
        """CSV + Parquet sources produce typed tables with statistics."""
        registered = [
            {
                "name": "sales",
                "source_type": "csv",
                "path": str(multi_source_csv_dir["orders"]),
            },
            {
                "name": "analytics",
                "source_type": "parquet",
                "path": str(multi_source_parquet),
            },
        ]

        self._run_import(harness, registered)
        self._run_typing(harness)
        self._run_statistics(harness)

        with harness.session_factory() as session:
            typed_tables = (
                session.execute(
                    select(Table).where(
                        Table.source_id == harness.source_id, Table.layer == "typed"
                    )
                )
                .scalars()
                .all()
            )
            typed_names = {t.table_name for t in typed_tables}
            assert "sales__orders" in typed_names
            assert "analytics__metrics" in typed_names

            # Verify Parquet table columns exist with expected names
            analytics_table = next(
                t for t in typed_tables if t.table_name == "analytics__metrics"
            )
            cols = (
                session.execute(
                    select(Column).where(Column.table_id == analytics_table.table_id)
                )
                .scalars()
                .all()
            )
            assert {c.column_name for c in cols} == {"metric_id", "name", "value", "count"}

            # Statistics exist for all typed tables
            for t in typed_tables:
                stat_count = session.execute(
                    select(func.count(StatisticalProfile.profile_id))
                    .join(Column)
                    .where(Column.table_id == t.table_id)
                ).scalar_one()
                assert stat_count > 0, f"No statistics for {t.table_name}"

    def test_all_source_types_through_statistics(
        self,
        harness: PipelineTestHarness,
        multi_source_csv_dir: dict[str, Path],
        multi_source_parquet: Path,
        multi_source_sqlite: Path,
    ):
        """CSV + Parquet + SQLite all loaded and analyzed through statistics."""
        harness.duckdb_conn.execute("INSTALL sqlite; LOAD sqlite;")

        registered = [
            {
                "name": "sales",
                "source_type": "csv",
                "path": str(multi_source_csv_dir["orders"]),
            },
            {
                "name": "analytics",
                "source_type": "parquet",
                "path": str(multi_source_parquet),
            },
            {
                "name": "warehouse",
                "source_type": "sqlite",
                "backend": "sqlite",
                "credential_ref": "warehouse",
                "tables": ["regions"],
            },
        ]

        self._run_import(harness, registered, mock_db_url=str(multi_source_sqlite))
        self._run_typing(harness)
        self._run_statistics(harness)

        with harness.session_factory() as session:
            # All 3 sources produced typed tables
            typed_tables = (
                session.execute(
                    select(Table).where(
                        Table.source_id == harness.source_id, Table.layer == "typed"
                    )
                )
                .scalars()
                .all()
            )
            typed_names = {t.table_name for t in typed_tables}
            assert typed_names == {
                "sales__orders",
                "analytics__metrics",
                "warehouse__regions",
            }

            # Total columns across all typed tables
            total_cols = session.execute(
                select(func.count(Column.column_id))
                .join(Table)
                .where(
                    Table.source_id == harness.source_id,
                    Table.layer == "typed",
                )
            ).scalar_one()
            # orders(4) + metrics(4) + regions(3) = 11
            assert total_cols == 11

            # Statistics exist for every typed table
            for t in typed_tables:
                stat_count = session.execute(
                    select(func.count(StatisticalProfile.profile_id))
                    .join(Column)
                    .where(Column.table_id == t.table_id)
                ).scalar_one()
                assert stat_count > 0, f"No statistics for {t.table_name}"

            # DuckDB typed tables are queryable
            for name in typed_names:
                count = harness.duckdb_conn.execute(
                    f'SELECT count(*) FROM "typed_{name}"'
                ).fetchone()
                assert count is not None and count[0] > 0, (
                    f"DuckDB typed table typed_{name} is empty or missing"
                )

    def test_data_integrity_across_phases(
        self,
        harness: PipelineTestHarness,
        multi_source_csv_dir: dict[str, Path],
    ):
        """Row counts are preserved from raw → typed tables."""
        registered = [
            {
                "name": "sales",
                "source_type": "csv",
                "path": str(multi_source_csv_dir["orders"]),
            },
            {
                "name": "tracking",
                "source_type": "csv",
                "path": str(multi_source_csv_dir["events"]),
            },
        ]

        self._run_import(harness, registered)
        self._run_typing(harness)

        with harness.session_factory() as session:
            raw_tables = (
                session.execute(
                    select(Table).where(
                        Table.source_id == harness.source_id, Table.layer == "raw"
                    )
                )
                .scalars()
                .all()
            )
            typed_tables = (
                session.execute(
                    select(Table).where(
                        Table.source_id == harness.source_id, Table.layer == "typed"
                    )
                )
                .scalars()
                .all()
            )

            raw_counts = {t.table_name: t.row_count for t in raw_tables}
            typed_counts = {t.table_name: t.row_count for t in typed_tables}

            # Row counts match between raw and typed
            for name in raw_counts:
                assert name in typed_counts, f"Typed table missing for {name}"
                assert raw_counts[name] == typed_counts[name], (
                    f"Row count mismatch for {name}: raw={raw_counts[name]}, "
                    f"typed={typed_counts[name]}"
                )

            # Verify actual DuckDB row counts match metadata
            for t in typed_tables:
                actual = harness.duckdb_conn.execute(
                    f'SELECT count(*) FROM "{t.duckdb_path}"'
                ).fetchone()
                assert actual is not None
                assert actual[0] == t.row_count, (
                    f"DuckDB row count mismatch for {t.table_name}: "
                    f"duckdb={actual[0]}, metadata={t.row_count}"
                )
