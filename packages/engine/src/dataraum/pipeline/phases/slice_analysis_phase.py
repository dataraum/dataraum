"""Slice analysis phase implementation.

Materializes slice tables for downstream per-(slice, period) sums (DAT-491):
- Creates slice tables in DuckDB from SliceDefinitions
- Registers slice tables in metadata database

The per-slice statistical profiling / quality pass was cut (DAT-518): its
``ColumnStatistics``/quality rows (keyed by slice column_ids) had no reader.
"""

from __future__ import annotations

from sqlalchemy import select

from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.slicing.slice_runner import register_slice_tables
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase


@analysis_phase
class SliceAnalysisPhase(BasePhase):
    """Materialize + register slice tables for downstream period sums.

    Creates slice tables in DuckDB from SliceDefinition SQL templates and
    registers them in the metadata database.

    Requires: slicing phase.
    """

    @property
    def name(self) -> str:
        return "slice_analysis"

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip on genuine preconditions only — this run's slice definitions.

        No "slice tables already exist" arm (DAT-502): physical slice tables
        are NOT run-versioned, so their presence says nothing about THIS run —
        a teach re-run (fresh ``run_id``) found the prior run's slice tables and
        silently skipped its fresh analyses (the DAT-448 bug class). A re-run
        always re-executes: slice DDL is ``CREATE OR REPLACE`` (idempotent) and
        the analysis writers are run-scoped.
        """
        # Source-free: the session's selected typed tables (DAT-403).
        typed_tables = self._typed_tables(ctx)

        if not typed_tables:
            return "No typed tables found"

        table_ids = [t.table_id for t in typed_tables]

        # Check for THIS run's slice definitions (run-versioned, DAT-448)
        slice_stmt = select(SliceDefinition).where(
            SliceDefinition.table_id.in_(table_ids),
            SliceDefinition.run_id == ctx.run_id,
        )
        slice_result = ctx.session.execute(slice_stmt)
        slice_defs = slice_result.scalars().all()

        if not slice_defs:
            return "No slice definitions found"

        total_slices = sum(len(sd.distinct_values or []) for sd in slice_defs)
        if total_slices == 0:
            return "No slice values defined"

        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Run slice analysis."""
        # Source-free: the session's selected typed tables (DAT-403).
        typed_tables = self._typed_tables(ctx)

        if not typed_tables:
            return PhaseResult.failed("No typed tables found. Run typing phase first.")

        table_ids = [t.table_id for t in typed_tables]

        # Get THIS run's slice definitions (run-versioned, DAT-448)
        slice_stmt = (
            select(SliceDefinition)
            .where(
                SliceDefinition.table_id.in_(table_ids),
                SliceDefinition.run_id == ctx.run_id,
            )
            .order_by(SliceDefinition.slice_priority)
        )
        slice_result = ctx.session.execute(slice_stmt)
        slice_defs = list(slice_result.scalars().all())

        if not slice_defs:
            return PhaseResult.success(
                outputs={
                    "slices_registered": 0,
                    "message": "No slice definitions found",
                },
                records_processed=0,
                records_created=0,
            )

        # Execute slice SQL templates to create slice tables in DuckDB.
        # Each sql_template contains CREATE statements for ALL values in that slice.
        # The templates already reference the slicing view as their source
        # (rewritten by slicing_view_phase after view creation).
        slices_created = 0
        errors: list[str] = []

        for slice_def in slice_defs:
            if not slice_def.sql_template:
                continue

            try:
                ctx.duckdb_conn.execute(slice_def.sql_template)
                slices_created += len(slice_def.distinct_values or [])
            except Exception as e:
                errors.append(f"Failed to create slices for {slice_def.column_id}: {e}")

        # Register slice tables in metadata
        register_result = register_slice_tables(
            session=ctx.session,
            duckdb_conn=ctx.duckdb_conn,
            slice_definitions=slice_defs,
        )

        if not register_result.success:
            return PhaseResult.failed(register_result.error or "Failed to register slice tables")

        slice_infos = register_result.unwrap()

        if not slice_infos:
            return PhaseResult.success(
                outputs={
                    "slices_registered": 0,
                    "slices_created": slices_created,
                    "message": "No slice tables found in DuckDB",
                },
                records_processed=len(slice_defs),
                records_created=0,
            )

        return PhaseResult.success(
            outputs={
                "slices_registered": len(slice_infos),
                "slices_created": slices_created,
                "errors": errors if errors else None,
            },
            records_processed=len(slice_defs),
            records_created=len(slice_infos),
            summary=f"{len(slice_infos)} slices registered",
        )
