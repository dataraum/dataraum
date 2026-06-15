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

from types import ModuleType

from dataraum.analysis.statistics import profile_statistics
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase


@analysis_phase
class StatisticsPhase(BasePhase):
    """Statistics profiling phase.

    Computes statistical profiles for all typed tables.
    Profiles include basic counts, string/numeric stats, histograms, and top values.
    """

    @property
    def name(self) -> str:
        return "statistics"

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.analysis.statistics import db_models

        return [db_models]

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip only when there are genuinely no typed tables to profile.

        Structural early-out only (DAT-413): a re-run mints a fresh ``run_id``
        and must always re-profile under it, so the old "profiles already exist
        → skip" bail is gone. ``profile_statistics`` is a pure insert stamping
        ``run_id`` on each ``StatisticalProfile``, so a new run's profiles coexist
        with prior runs'; the promoted head names which run is current.
        """
        typed_tables = self._typed_tables(ctx)

        if not typed_tables:
            return "No typed tables found"

        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Run statistical profiling on the scoped typed tables."""
        typed_tables = self._typed_tables(ctx)

        if not typed_tables:
            return PhaseResult.failed("No typed tables found. Run typing phase first.")

        # Re-derive every scoped typed table under this run (DAT-413): no
        # "already profiled" filter — a re-run re-profiles, stamping its own
        # ``run_id``, and prior runs' profiles are left intact.
        profiled_tables = []
        total_profiles_created = 0
        total_columns_processed = 0
        warnings = []

        for typed_table in typed_tables:
            stats_result = profile_statistics(
                table_id=typed_table.table_id,
                duckdb_conn=ctx.duckdb_conn,
                session=ctx.session,
                config=ctx.config,
                run_id=ctx.run_id,
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
            summary=f"{total_profiles_created} profiles across {len(profiled_tables)} tables",
        )
