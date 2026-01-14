"""Tests for statistical quality phase."""

from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.pipeline.base import PhaseContext, PhaseStatus
from dataraum_context.pipeline.phases import StatisticalQualityPhase
from dataraum_context.storage import Column, Source, Table

if TYPE_CHECKING:
    import duckdb


class TestStatisticalQualityPhase:
    """Tests for StatisticalQualityPhase."""

    def test_phase_properties(self):
        phase = StatisticalQualityPhase()
        assert phase.name == "statistical_quality"
        assert phase.description == "Benford's Law and outlier detection"
        assert phase.dependencies == ["statistics"]
        assert phase.outputs == ["quality_metrics"]

    @pytest.mark.asyncio
    async def test_skip_when_no_typed_tables(
        self, async_session: AsyncSession, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test skip when no typed tables exist."""
        phase = StatisticalQualityPhase()
        source_id = str(uuid4())

        ctx = PhaseContext(
            session=async_session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={},
        )

        skip_reason = await phase.should_skip(ctx)
        assert skip_reason is not None
        assert "No typed tables" in skip_reason

    @pytest.mark.asyncio
    async def test_skip_when_no_numeric_columns(
        self, async_session: AsyncSession, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test skip when typed tables have no numeric columns."""
        phase = StatisticalQualityPhase()
        source_id = str(uuid4())

        # Create a source with a typed table containing only VARCHAR columns
        source = Source(
            source_id=source_id,
            name="test_source",
            source_type="csv",
        )
        async_session.add(source)

        table = Table(
            table_id=str(uuid4()),
            source_id=source_id,
            table_name="test_table",
            layer="typed",
            duckdb_path="typed_test_table",
            row_count=10,
        )
        async_session.add(table)

        # Add only VARCHAR columns
        col = Column(
            column_id=str(uuid4()),
            table_id=table.table_id,
            column_name="name",
            column_position=0,
            raw_type="VARCHAR",
            resolved_type="VARCHAR",
        )
        async_session.add(col)
        await async_session.commit()

        ctx = PhaseContext(
            session=async_session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={},
        )

        skip_reason = await phase.should_skip(ctx)
        assert skip_reason is not None
        assert "No numeric columns" in skip_reason

    @pytest.mark.asyncio
    async def test_fails_when_no_typed_tables(
        self, async_session: AsyncSession, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test failure when run without typed tables."""
        phase = StatisticalQualityPhase()
        source_id = str(uuid4())

        ctx = PhaseContext(
            session=async_session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={},
        )

        result = await phase.run(ctx)

        assert result.status == PhaseStatus.FAILED
        assert "No typed tables" in (result.error or "")

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_unassessed_columns(
        self, async_session: AsyncSession, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """Test returns empty results when tables have no unassessed numeric columns."""
        phase = StatisticalQualityPhase()
        source_id = str(uuid4())

        # Create a typed table with VARCHAR-only columns
        source = Source(
            source_id=source_id,
            name="test_source",
            source_type="csv",
        )
        async_session.add(source)

        table = Table(
            table_id=str(uuid4()),
            source_id=source_id,
            table_name="test_table",
            layer="typed",
            duckdb_path="typed_test_table",
            row_count=10,
        )
        async_session.add(table)

        # Add only non-numeric column
        col = Column(
            column_id=str(uuid4()),
            table_id=table.table_id,
            column_name="category",
            column_position=0,
            raw_type="VARCHAR",
            resolved_type="VARCHAR",
        )
        async_session.add(col)
        await async_session.commit()

        ctx = PhaseContext(
            session=async_session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={},
        )

        result = await phase.run(ctx)

        # Should succeed with empty results (no numeric columns to assess)
        assert result.status == PhaseStatus.COMPLETED
        assert result.outputs["quality_metrics"] == []
        assert result.records_processed == 0
