"""Typing phase - infer and resolve column types.

This phase:
1. Infers type candidates for all VARCHAR columns using pattern matching
2. Creates typed tables with proper data types
3. Creates quarantine tables for rows with type cast failures
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from dataraum_context.analysis.typing import infer_type_candidates, resolve_types
from dataraum_context.pipeline.base import PhaseContext, PhaseResult
from dataraum_context.pipeline.phases.base import BasePhase
from dataraum_context.storage import Table


class TypingPhase(BasePhase):
    """Typing phase - type inference and resolution.

    Takes raw VARCHAR tables and creates typed tables with proper data types.
    Uses pattern matching and TRY_CAST validation to infer types.

    Configuration (in ctx.config):
        min_confidence: Minimum confidence for automatic type selection (default: 0.85)

    Inputs (from previous phases):
        import.raw_tables: List of raw table IDs to process

    Outputs:
        typed_tables: List of typed table IDs
        type_decisions: Dict mapping column_id to resolved type
    """

    @property
    def name(self) -> str:
        return "typing"

    @property
    def description(self) -> str:
        return "Type inference and resolution"

    @property
    def dependencies(self) -> list[str]:
        return ["import"]

    @property
    def outputs(self) -> list[str]:
        return ["typed_tables", "type_decisions"]

    async def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if typed tables already exist for all raw tables."""
        raw_table_ids = ctx.get_output("import", "raw_tables", [])
        if not raw_table_ids:
            return "No raw tables to process"

        # Check if all raw tables have corresponding typed tables
        for table_id in raw_table_ids:
            raw_table = await ctx.session.get(Table, table_id)
            if not raw_table:
                continue

            # Check if typed table exists
            stmt = select(Table).where(
                Table.source_id == raw_table.source_id,
                Table.table_name == raw_table.table_name,
                Table.layer == "typed",
            )
            result = await ctx.session.execute(stmt)
            typed_table = result.scalar_one_or_none()

            if not typed_table:
                return None  # At least one table needs typing

        return "All tables already typed"

    async def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Run type inference and resolution.

        Args:
            ctx: Phase context

        Returns:
            PhaseResult with typed_tables and type_decisions
        """
        # Get raw tables from import phase
        raw_table_ids = ctx.get_output("import", "raw_tables", [])
        if not raw_table_ids:
            # Try to get from table_ids in context
            raw_table_ids = ctx.table_ids

        if not raw_table_ids:
            return PhaseResult.failed("No raw tables to process")

        min_confidence = ctx.config.get("min_confidence", 0.85)

        typed_tables: list[str] = []
        type_decisions: dict[str, str] = {}
        warnings: list[str] = []
        total_rows_processed = 0
        total_typed_created = 0

        for table_id in raw_table_ids:
            # Load table with columns
            stmt = (
                select(Table).where(Table.table_id == table_id).options(selectinload(Table.columns))
            )
            result = await ctx.session.execute(stmt)
            table = result.scalar_one_or_none()

            if not table:
                warnings.append(f"Table not found: {table_id}")
                continue

            if table.layer != "raw":
                warnings.append(f"Table {table.table_name} is not a raw table")
                continue

            # Phase 1: Infer type candidates
            inference_result = await infer_type_candidates(
                table=table,
                duckdb_conn=ctx.duckdb_conn,
                session=ctx.session,
            )

            if not inference_result.success:
                warnings.append(
                    f"Type inference failed for {table.table_name}: {inference_result.error}"
                )
                continue

            # Phase 2: Resolve types (create typed + quarantine tables)
            resolution_result = await resolve_types(
                table_id=table_id,
                duckdb_conn=ctx.duckdb_conn,
                session=ctx.session,
                min_confidence=min_confidence,
            )

            if not resolution_result.success:
                warnings.append(
                    f"Type resolution failed for {table.table_name}: {resolution_result.error}"
                )
                continue

            resolution = resolution_result.unwrap()

            # Find the typed table ID
            stmt = select(Table).where(
                Table.source_id == table.source_id,
                Table.table_name == table.table_name,
                Table.layer == "typed",
            )
            typed_result = await ctx.session.execute(stmt)
            typed_table = typed_result.scalar_one_or_none()

            if typed_table:
                typed_tables.append(typed_table.table_id)

            # Record type decisions
            for col_result in resolution.column_results:
                type_decisions[col_result.column_id] = col_result.target_type.value

            total_rows_processed += resolution.total_rows
            total_typed_created += 1

            # Log quarantine info if any rows were quarantined
            if resolution.quarantined_rows > 0:
                pct = (
                    (resolution.quarantined_rows / resolution.total_rows * 100)
                    if resolution.total_rows > 0
                    else 0
                )
                warnings.append(
                    f"{table.table_name}: {resolution.quarantined_rows} rows ({pct:.1f}%) quarantined"
                )

        if not typed_tables:
            return PhaseResult.failed("No tables were successfully typed")

        return PhaseResult.success(
            outputs={
                "typed_tables": typed_tables,
                "type_decisions": type_decisions,
            },
            records_processed=total_rows_processed,
            records_created=total_typed_created,
            warnings=warnings,
        )
