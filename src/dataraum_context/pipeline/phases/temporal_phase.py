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

from dataraum_context.analysis.temporal import TemporalColumnProfile, profile_temporal
from dataraum_context.pipeline.base import PhaseContext, PhaseResult
from dataraum_context.pipeline.phases.base import BasePhase
from dataraum_context.storage import Column, Table


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

    async def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if no temporal columns or all already profiled."""
        # Get typed tables
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = await ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return "No typed tables found"

        # Check for temporal columns
        temporal_types = ["DATE", "TIMESTAMP", "TIMESTAMPTZ"]
        typed_table_ids = [t.table_id for t in typed_tables]
        temporal_columns_stmt = select(Column).where(
            Column.table_id.in_(typed_table_ids),
            Column.resolved_type.in_(temporal_types),
        )
        temporal_columns = (await ctx.session.execute(temporal_columns_stmt)).scalars().all()

        if not temporal_columns:
            return "No temporal columns found"

        # Check existing profiles
        existing_count = (
            await ctx.session.execute(select(func.count(TemporalColumnProfile.profile_id)))
        ).scalar() or 0

        if existing_count >= len(temporal_columns):
            return f"All {existing_count} temporal columns already profiled"

        return None

    async def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Run temporal profiling on typed tables."""
        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = await ctx.session.execute(stmt)
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
        temporal_columns = (await ctx.session.execute(temporal_columns_stmt)).scalars().all()

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
            profile_result = await profile_temporal(
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
