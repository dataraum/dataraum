"""Statistics phase implementation.

Computes statistical profiles for typed tables:
- Basic counts (total, null, distinct, cardinality)
- String stats (min/max/avg length)
- Top values (frequency analysis)
- Numeric stats (min, max, mean, stddev, skewness, kurtosis, cv)
- Percentiles
- Histograms
"""

from __future__ import annotations

from sqlalchemy import select

from dataraum_context.analysis.statistics import profile_statistics
from dataraum_context.analysis.statistics.db_models import StatisticalProfile
from dataraum_context.pipeline.base import PhaseContext, PhaseResult
from dataraum_context.pipeline.phases.base import BasePhase
from dataraum_context.storage import Column, Table


class StatisticsPhase(BasePhase):
    """Statistics profiling phase.

    Computes statistical profiles for all typed tables.
    Profiles include basic counts, string/numeric stats, histograms, and top values.
    """

    @property
    def name(self) -> str:
        return "statistics"

    @property
    def description(self) -> str:
        return "Statistical profiling of typed tables"

    @property
    def dependencies(self) -> list[str]:
        return ["typing"]

    @property
    def outputs(self) -> list[str]:
        return ["statistical_profiles"]

    async def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if all typed tables already have profiles."""
        # Get typed tables
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = await ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return "No typed tables found"

        # Check which tables already have profiles
        typed_table_ids = [t.table_id for t in typed_tables]
        columns_stmt = select(Column).where(Column.table_id.in_(typed_table_ids))
        columns = (await ctx.session.execute(columns_stmt)).scalars().all()

        profiled_stmt = (
            select(StatisticalProfile.column_id)
            .where(StatisticalProfile.layer == "typed")
            .distinct()
        )
        profiled_column_ids = set((await ctx.session.execute(profiled_stmt)).scalars().all())

        # Check if any table has unprofiled columns
        for tt in typed_tables:
            table_columns = [c for c in columns if c.table_id == tt.table_id]
            table_column_ids = {c.column_id for c in table_columns}
            if table_column_ids - profiled_column_ids:
                return None  # At least one table needs profiling

        return "All typed tables already profiled"

    async def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Run statistical profiling on typed tables."""
        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = await ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return PhaseResult.failed("No typed tables found. Run typing phase first.")

        # Get all columns for typed tables
        typed_table_ids = [t.table_id for t in typed_tables]
        columns_stmt = select(Column).where(Column.table_id.in_(typed_table_ids))
        all_columns = (await ctx.session.execute(columns_stmt)).scalars().all()

        # Get already profiled columns
        profiled_stmt = (
            select(StatisticalProfile.column_id)
            .where(StatisticalProfile.layer == "typed")
            .distinct()
        )
        profiled_column_ids = set((await ctx.session.execute(profiled_stmt)).scalars().all())

        # Find tables that need profiling
        unprofiled_tables = []
        for tt in typed_tables:
            table_columns = [c for c in all_columns if c.table_id == tt.table_id]
            table_column_ids = {c.column_id for c in table_columns}
            if table_column_ids - profiled_column_ids:
                unprofiled_tables.append(tt)

        if not unprofiled_tables:
            # All already profiled - return success with existing profiles
            return PhaseResult.success(
                outputs={"statistical_profiles": [t.table_name for t in typed_tables]},
                records_processed=0,
                records_created=0,
            )

        # Profile each table
        profiled_tables = []
        total_profiles_created = 0
        total_columns_processed = 0
        warnings = []

        for typed_table in unprofiled_tables:
            stats_result = await profile_statistics(
                table_id=typed_table.table_id,
                duckdb_conn=ctx.duckdb_conn,
                session=ctx.session,
            )

            if not stats_result.success:
                warnings.append(f"Failed to profile {typed_table.table_name}: {stats_result.error}")
                continue

            profile_result = stats_result.unwrap()
            profiled_tables.append(typed_table.table_name)
            total_profiles_created += len(profile_result.column_profiles)
            total_columns_processed += len(profile_result.column_profiles)

        if not profiled_tables and warnings:
            return PhaseResult.failed(f"All tables failed profiling: {'; '.join(warnings)}")

        return PhaseResult.success(
            outputs={"statistical_profiles": profiled_tables},
            records_processed=total_columns_processed,
            records_created=total_profiles_created,
            warnings=warnings if warnings else None,
        )
