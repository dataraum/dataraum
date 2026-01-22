"""Temporal phase implementation.

Analyzes temporal columns for:
- Granularity detection (daily, weekly, monthly, etc.)
- Completeness and gap analysis
- Seasonality patterns
- Trend detection
- Change point detection
- Staleness assessment
"""

from __future__ import annotations

from sqlalchemy import func, select

from dataraum.analysis.temporal import TemporalColumnProfile, profile_temporal
from dataraum.core.logging import get_logger
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.storage import Column, Table

logger = get_logger(__name__)


class TemporalPhase(BasePhase):
    """Temporal profiling phase.

    Analyzes temporal columns for patterns, trends, and anomalies.
    """

    @property
    def name(self) -> str:
        return "temporal"

    @property
    def description(self) -> str:
        return "Temporal pattern and trend analysis"

    @property
    def dependencies(self) -> list[str]:
        # Temporal only needs typed tables, can run in parallel with statistics
        return ["typing"]

    @property
    def outputs(self) -> list[str]:
        return ["temporal_profiles"]

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if no temporal columns or all already profiled."""

        # Get typed tables
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return f"No typed tables found for source {ctx.source_id}"

        logger.info(f"Temporal: Found {len(typed_tables)} typed tables")

        # Check for temporal columns
        temporal_types = ["DATE", "TIMESTAMP", "TIMESTAMPTZ"]
        typed_table_ids = [t.table_id for t in typed_tables]

        # First, get all columns to see what types exist
        all_columns_stmt = select(Column).where(Column.table_id.in_(typed_table_ids))
        all_columns = (ctx.session.execute(all_columns_stmt)).scalars().all()
        type_counts: dict[str, int] = {}
        for col in all_columns:
            t = col.resolved_type or "NULL"
            type_counts[t] = type_counts.get(t, 0) + 1
        logger.info(f"Temporal: Column types in typed tables: {type_counts}")

        temporal_columns_stmt = select(Column).where(
            Column.table_id.in_(typed_table_ids),
            Column.resolved_type.in_(temporal_types),
        )
        temporal_columns = (ctx.session.execute(temporal_columns_stmt)).scalars().all()

        if not temporal_columns:
            return f"No temporal columns found (types: {temporal_types}, available: {list(type_counts.keys())})"

        logger.info(f"Temporal: Found {len(temporal_columns)} temporal columns")

        # Check existing profiles
        existing_count = (
            ctx.session.execute(select(func.count(TemporalColumnProfile.profile_id)))
        ).scalar() or 0

        if existing_count >= len(temporal_columns):
            return f"All {existing_count} temporal columns already profiled"

        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Run temporal profiling on typed tables."""
        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return PhaseResult.failed("No typed tables found. Run typing phase first.")

        # Check for temporal columns
        temporal_types = ["DATE", "TIMESTAMP", "TIMESTAMPTZ"]
        typed_table_ids = [t.table_id for t in typed_tables]
        temporal_columns_stmt = select(Column).where(
            Column.table_id.in_(typed_table_ids),
            Column.resolved_type.in_(temporal_types),
        )
        temporal_columns = (ctx.session.execute(temporal_columns_stmt)).scalars().all()

        if not temporal_columns:
            return PhaseResult.success(
                outputs={"temporal_profiles": [], "message": "No temporal columns found"},
                records_processed=0,
                records_created=0,
            )

        # Profile each table
        profiled_tables = []
        total_profiles = 0
        seasonality_count = 0
        trend_count = 0
        stale_count = 0
        warnings = []

        for typed_table in typed_tables:
            profile_result = profile_temporal(
                table_id=typed_table.table_id,
                duckdb_conn=ctx.duckdb_conn,
                session=ctx.session,
            )

            if not profile_result.success:
                warnings.append(
                    f"Failed to profile {typed_table.table_name}: {profile_result.error}"
                )
                continue

            result_data = profile_result.unwrap()
            num_profiles = len(result_data.column_profiles)

            if num_profiles > 0:
                profiled_tables.append(typed_table.table_name)
                total_profiles += num_profiles

                # Count findings
                for profile in result_data.column_profiles:
                    if profile.seasonality and profile.seasonality.has_seasonality:
                        seasonality_count += 1
                    if profile.trend and profile.trend.has_trend:
                        trend_count += 1
                    if profile.update_frequency and profile.update_frequency.is_stale:
                        stale_count += 1

        return PhaseResult.success(
            outputs={
                "temporal_profiles": profiled_tables,
                "total_profiles": total_profiles,
                "with_seasonality": seasonality_count,
                "with_trend": trend_count,
                "stale_columns": stale_count,
            },
            records_processed=len(temporal_columns),
            records_created=total_profiles,
            warnings=warnings if warnings else None,
        )
