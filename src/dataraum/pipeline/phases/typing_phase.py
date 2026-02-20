"""Typing phase - infer and resolve column types.

This phase:
1. Infers type candidates for all VARCHAR columns using pattern matching
2. Creates typed tables with proper data types
3. Creates quarantine tables for rows with type cast failures

For strongly-typed sources (e.g., Parquet), type inference is skipped and
the source types are trusted directly.
"""

from __future__ import annotations

from types import ModuleType
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from dataraum.analysis.typing import infer_type_candidates, resolve_types
from dataraum.analysis.typing.patterns import load_typing_config
from dataraum.core.logging import get_logger
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.storage import Column, Table

logger = get_logger(__name__)


@analysis_phase
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

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.analysis.typing import db_models

        return [db_models]

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if typed tables already exist for all raw tables."""
        raw_table_ids = ctx.get_output("import", "raw_tables", [])
        if not raw_table_ids:
            return "No raw tables to process"

        # Check if all raw tables have corresponding typed tables
        for table_id in raw_table_ids:
            raw_table = ctx.session.get(Table, table_id)
            if not raw_table:
                continue

            # Check if typed table exists
            stmt = select(Table).where(
                Table.source_id == raw_table.source_id,
                Table.table_name == raw_table.table_name,
                Table.layer == "typed",
            )
            result = ctx.session.execute(stmt)
            typed_table = result.scalar_one_or_none()

            if not typed_table:
                return None  # At least one table needs typing

        return "All tables already typed"

    def _is_strongly_typed(self, table: Table) -> bool:
        """Check if a table comes from a strongly-typed source (e.g., Parquet).

        A table is strongly typed if any of its columns have a non-VARCHAR raw_type,
        meaning the source already provided type information.
        """
        for col in table.columns:
            if col.raw_type and col.raw_type != "VARCHAR":
                return True
        return False

    def _promote_strongly_typed(
        self,
        table: Table,
        ctx: PhaseContext,
    ) -> tuple[str, dict[str, str]]:
        """Create typed table for a strongly-typed source by copying the raw table.

        No type inference or TRY_CAST needed - source types are trusted.

        Args:
            table: Raw table with non-VARCHAR types
            ctx: Phase context

        Returns:
            Tuple of (typed_table_id, column type decisions)
        """
        raw_table_name = table.duckdb_path or f"raw_{table.table_name}"
        typed_table_name = f"typed_{table.table_name}"

        # Create typed table as direct copy (types already correct)
        ctx.duckdb_conn.execute(
            f'CREATE TABLE "{typed_table_name}" AS SELECT * FROM "{raw_table_name}"'
        )

        # Get row count
        row_count_result = ctx.duckdb_conn.execute(
            f'SELECT COUNT(*) FROM "{typed_table_name}"'
        ).fetchone()
        row_count = row_count_result[0] if row_count_result else 0

        # Create typed Table record
        typed_table_id = str(uuid4())
        typed_table = Table(
            table_id=typed_table_id,
            source_id=table.source_id,
            table_name=table.table_name,
            layer="typed",
            duckdb_path=typed_table_name,
            row_count=row_count,
        )
        ctx.session.add(typed_table)

        # Create Column records for the typed table, using raw_type as resolved_type
        type_decisions: dict[str, str] = {}
        for col in table.columns:
            resolved_type = col.raw_type or "VARCHAR"
            new_col_id = str(uuid4())
            typed_col = Column(
                column_id=new_col_id,
                table_id=typed_table_id,
                column_name=col.column_name,
                original_name=col.original_name,
                column_position=col.column_position,
                raw_type=col.raw_type,
                resolved_type=resolved_type,
            )
            ctx.session.add(typed_col)
            type_decisions[new_col_id] = resolved_type

        logger.info(
            "strongly_typed_promoted",
            table=table.table_name,
            columns=len(table.columns),
            rows=row_count,
        )

        return typed_table_id, type_decisions

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Run type inference and resolution.

        For strongly-typed sources (Parquet), skips inference and promotes
        types directly. For untyped sources (CSV), runs full inference pipeline.

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

        typing_config = load_typing_config(ctx.config)
        min_confidence = typing_config["min_confidence"]

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
            result = ctx.session.execute(stmt)
            table = result.scalar_one_or_none()

            if not table:
                warnings.append(f"Table not found: {table_id}")
                continue

            if table.layer != "raw":
                warnings.append(f"Table {table.table_name} is not a raw table")
                continue

            # Check if source is strongly typed (e.g., Parquet)
            if self._is_strongly_typed(table):
                typed_table_id, decisions = self._promote_strongly_typed(table, ctx)
                typed_tables.append(typed_table_id)
                type_decisions.update(decisions)
                total_rows_processed += table.row_count or 0
                total_typed_created += 1
                continue

            # Untyped source: run full inference pipeline
            # Phase 1: Infer type candidates
            inference_result = infer_type_candidates(
                table=table,
                duckdb_conn=ctx.duckdb_conn,
                session=ctx.session,
            )

            if not inference_result.success:
                warnings.append(
                    f"Type inference failed for {table.table_name}: {inference_result.error}"
                )
                continue

            # Flush type candidates so resolve_types can query them via selectinload
            # This is necessary because selectinload queries the DB, not the session cache
            ctx.session.flush()

            # Phase 2: Resolve types (create typed + quarantine tables)
            resolution_result = resolve_types(
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

            # Use the typed table ID directly from the result (no query needed)
            typed_tables.append(resolution.typed_table_id)

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
