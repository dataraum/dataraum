"""Slice analysis phase implementation.

Executes slice SQL templates and runs analysis on the resulting slice tables:
- Creates slice tables in DuckDB from SliceDefinitions
- Registers slice tables in metadata database
- Runs statistics and quality analysis on each slice
- Copies semantic annotations from parent tables
"""

from __future__ import annotations

from sqlalchemy import select

from dataraum_context.analysis.slicing.db_models import SliceDefinition
from dataraum_context.analysis.slicing.slice_runner import (
    register_slice_tables,
    run_analysis_on_slices,
)
from dataraum_context.pipeline.base import PhaseContext, PhaseResult
from dataraum_context.pipeline.phases.base import BasePhase
from dataraum_context.storage import Table


class SliceAnalysisPhase(BasePhase):
    """Execute slice SQL and analyze resulting slice tables.

    Creates slice tables in DuckDB based on SliceDefinition SQL templates,
    registers them in the metadata database, and runs statistical
    analysis on each slice.

    Requires: slicing phase.
    """

    @property
    def name(self) -> str:
        return "slice_analysis"

    @property
    def description(self) -> str:
        return "Execute slice SQL and analyze slice tables"

    @property
    def dependencies(self) -> list[str]:
        return ["slicing"]

    @property
    def outputs(self) -> list[str]:
        return ["slice_profiles"]

    @property
    def is_llm_phase(self) -> bool:
        return False

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if no slice definitions exist or all slices already analyzed."""
        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return "No typed tables found"

        table_ids = [t.table_id for t in typed_tables]

        # Check for slice definitions
        slice_stmt = select(SliceDefinition).where(SliceDefinition.table_id.in_(table_ids))
        slice_result = ctx.session.execute(slice_stmt)
        slice_defs = slice_result.scalars().all()

        if not slice_defs:
            return "No slice definitions found"

        # Check if slice tables already exist
        total_slices = sum(len(sd.distinct_values or []) for sd in slice_defs)
        if total_slices == 0:
            return "No slice values defined"

        # Check existing slice tables
        existing_stmt = select(Table).where(
            Table.layer == "slice", Table.source_id == ctx.source_id
        )
        existing_result = ctx.session.execute(existing_stmt)
        existing_slices = len(list(existing_result.scalars().all()))

        if existing_slices >= total_slices:
            return "All slices already analyzed"

        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Run slice analysis."""
        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return PhaseResult.failed("No typed tables found. Run typing phase first.")

        table_ids = [t.table_id for t in typed_tables]

        # Get slice definitions
        slice_stmt = (
            select(SliceDefinition)
            .where(SliceDefinition.table_id.in_(table_ids))
            .order_by(SliceDefinition.slice_priority)
        )
        slice_result = ctx.session.execute(slice_stmt)
        slice_defs = list(slice_result.scalars().all())

        if not slice_defs:
            return PhaseResult.success(
                outputs={
                    "slice_profiles": 0,
                    "message": "No slice definitions found",
                },
                records_processed=0,
                records_created=0,
            )

        # Execute slice SQL templates to create slice tables in DuckDB
        slices_created = 0
        errors: list[str] = []

        for slice_def in slice_defs:
            if not slice_def.sql_template:
                continue

            for value in slice_def.distinct_values or []:
                # Substitute the value into the SQL template
                # The template should have a placeholder like {value} or similar
                try:
                    sql = slice_def.sql_template.format(value=value)
                    ctx.duckdb_conn.execute(sql)
                    slices_created += 1
                except Exception as e:
                    errors.append(f"Failed to create slice for {value}: {e}")

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
                    "slice_profiles": 0,
                    "slices_created": slices_created,
                    "message": "No slice tables found in DuckDB",
                },
                records_processed=len(slice_defs),
                records_created=0,
            )

        # Run analysis on slice tables
        # Note: semantic_agent is None since we copy annotations, not re-analyze
        analysis_result = run_analysis_on_slices(
            session=ctx.session,
            duckdb_conn=ctx.duckdb_conn,
            slice_infos=slice_infos,
            semantic_agent=None,
            run_statistics=True,
            run_quality=True,
            run_semantic=False,  # Skip semantic - it needs an agent
        )

        errors.extend(analysis_result.errors)

        return PhaseResult.success(
            outputs={
                "slice_profiles": analysis_result.statistics_computed,
                "slices_registered": analysis_result.slices_registered,
                "slices_analyzed": analysis_result.slices_analyzed,
                "quality_assessed": analysis_result.quality_assessed,
                "errors": errors if errors else None,
            },
            records_processed=len(slice_defs),
            records_created=analysis_result.slices_registered,
        )
