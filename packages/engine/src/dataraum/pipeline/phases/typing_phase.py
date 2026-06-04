"""Typing phase - infer and resolve column types.

This phase:
1. Infers type candidates for all VARCHAR columns using pattern matching
2. Creates typed tables with proper data types
3. Creates quarantine tables for rows with type cast failures

For strongly-typed sources (e.g., Parquet), type inference is skipped and
the source types are trusted directly.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import ModuleType
from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from dataraum.analysis.typing import infer_type_candidates, resolve_types
from dataraum.analysis.typing.patterns import load_typing_config
from dataraum.core.logging import get_logger
from dataraum.investigation.queries import link_session_tables
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.storage import Table
from dataraum.storage.upsert import upsert

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = get_logger(__name__)


@analysis_phase
class TypingPhase(BasePhase):
    """Typing phase - type inference and resolution.

    Takes raw VARCHAR tables and creates typed tables with proper data types.
    Uses pattern matching and TRY_CAST validation to infer types.

    Configuration (in ctx.config):
        min_confidence: Minimum confidence for automatic type selection (default: 0.85)
    """

    @property
    def name(self) -> str:
        return "typing"

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.analysis.typing import db_models

        return [db_models]

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip only when there are genuinely no raw tables to type.

        Structural early-out only (DAT-413): a re-run mints a fresh ``run_id``
        and must always re-derive typing under it, so the old "typed counterpart
        already exists → skip" bail is gone. ``_run`` is idempotent on a re-type
        — it ``CREATE OR REPLACE``s the typed/quarantine DuckDB tables and reuses
        the stable typed ``Table``/``Column`` ids (DAT-373), re-deriving
        ``TypeDecision``/``TypeCandidate`` rows under the new ``run_id`` (the
        delete-before-insert is scoped to ``ctx.run_id`` so the prior run's rows
        survive). The promoted head names which run is current.

        When ``ctx.table_ids`` is set (the per-table fan-out scope) only those raw
        tables are considered — source-agnostic (DAT-422), via the same resolver
        ``_run`` uses.
        """
        if not self._resolve_target_table_ids(ctx):
            if ctx.table_ids:
                return "No raw tables match the requested table_ids filter"
            return "No raw tables to process"

        return None

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
        # Post-DAT-341: Table.duckdb_path is the bare ``<source>__<table>`` form
        # under the workspace-stable layer schemas. Strongly-typed copy reads
        # from lake.raw and writes to lake.typed with the same bare name.
        from dataraum.core.duckdb_naming import schema_for_layer
        from dataraum.server.storage import LAKE_CATALOG_ALIAS

        if not table.duckdb_path:
            raise RuntimeError(
                f"Raw table {table.table_id} has no duckdb_path — loader did not register it"
            )
        bare = table.duckdb_path
        raw_target = f'{LAKE_CATALOG_ALIAS}.{schema_for_layer("raw")}."{bare}"'
        typed_target = f'{LAKE_CATALOG_ALIAS}.{schema_for_layer("typed")}."{bare}"'

        # Emit → store → execute (DAT-414): the strongly-typed copy is a plain
        # ``CREATE OR REPLACE … AS SELECT *``. Capture the DDL string so it is
        # versioned like the untyped path's recipes (stored below, once the typed
        # Table id is reconciled); the executed SQL is unchanged.
        typed_sql = f"CREATE OR REPLACE TABLE {typed_target} AS SELECT * FROM {raw_target}"
        ctx.duckdb_conn.execute(typed_sql)

        # Get row count
        row_count_result = ctx.duckdb_conn.execute(
            f"SELECT COUNT(*) FROM {typed_target}"
        ).fetchone()
        row_count = row_count_result[0] if row_count_result else 0

        # Reconcile the typed Table + Column rows (DAT-373 Option A): reuse the
        # existing rows for this (source, table_name, "typed") so a re-type keeps
        # the typed Table id + Column ids stable. The reconcile helpers live in
        # resolve_types alongside the untyped path so both share one identity rule.
        from dataraum.analysis.typing.resolution import (
            reconcile_typed_columns,
            reconcile_typed_table,
        )

        ctx.session.flush()
        typed_table = reconcile_typed_table(ctx.session, table, "typed", bare, row_count)
        ctx.session.flush()

        # Persist the versioned materialization recipe (DAT-414) for the
        # strongly-typed copy. Only a ``typed`` artifact exists here (no cast can
        # fail → no quarantine table). Keyed on the stable typed Table id, stamped
        # with the run, reading the raw layer.
        from dataraum.analysis.typing.recipe import store_recipe

        store_recipe(
            ctx.session,
            session_id=ctx.require_session_id(),
            table_id=typed_table.table_id,
            layer="typed",
            run_id=ctx.run_id,
            target_fqn=typed_target,
            ddl=typed_sql,
            depends_on=[raw_target],
        )

        desired = [
            (
                col.column_name,
                col.original_name,
                col.column_position,
                col.raw_type,
                col.raw_type or "VARCHAR",
            )
            for col in table.columns
        ]
        column_map = reconcile_typed_columns(ctx.session, typed_table, desired)
        ctx.session.flush()

        # Stamp a TypeDecision per typed column. Strongly-typed columns are
        # "trusted source" decisions. ``TypeDecision`` is one-per-column-per-run,
        # so upsert on ``(column_id, run_id)``: idempotent under a Temporal
        # at-least-once retry (same run_id, no delete needed) while a NEW run's
        # rows still coexist with prior runs'; the promoted head names which run
        # is current.
        from dataraum.analysis.typing.db_models import TypeDecision

        decided_at = datetime.now(UTC)
        type_decisions: dict[str, str] = {}
        td_rows: list[dict[str, Any]] = []
        for col in table.columns:
            resolved = col.raw_type or "VARCHAR"
            typed_col_id = column_map[col.column_name]
            # PK omitted so the model's Python-side default applies.
            td_rows.append(
                {
                    "session_id": ctx.require_session_id(),
                    "column_id": typed_col_id,
                    "run_id": ctx.run_id,
                    "decided_type": resolved,
                    "decision_source": "automatic",
                    "decided_at": decided_at,
                    "decision_reason": "strongly-typed source (types trusted)",
                }
            )
            type_decisions[typed_col_id] = resolved
        upsert(ctx.session, TypeDecision, td_rows, index_elements=["column_id", "run_id"])

        logger.debug(
            "strongly_typed_promoted",
            table=table.table_name,
            columns=len(table.columns),
            rows=row_count,
        )

        return typed_table.table_id, type_decisions

    def _resolve_target_table_ids(self, ctx: PhaseContext) -> list[str]:
        """Resolve which raw table_ids to type for this run — source-agnostic (DAT-422).

        Prefer the explicit per-table scope: the per-table fan-out (DAT-370) hands
        each child its one raw table id, and that id is the run's unit of work
        regardless of which (per-object, DAT-422) source it belongs to — so a run
        whose raw tables span multiple per-object sources types each correctly.
        Filtered to the ``raw`` layer; ids that aren't raw tables are dropped. Only
        when no ids are passed (a legacy single-source caller, e.g. a teach replay
        that scopes the whole source) fall back to "all raw tables of the bound
        ``ctx.source_id``".
        """
        if ctx.table_ids:
            stmt = select(Table.table_id).where(
                Table.table_id.in_(ctx.table_ids),
                Table.layer == "raw",
            )
            return [row[0] for row in ctx.session.execute(stmt)]

        stmt = select(Table.table_id).where(
            Table.source_id == ctx.source_id,
            Table.layer == "raw",
        )
        return [row[0] for row in ctx.session.execute(stmt)]

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Run type inference and resolution.

        For strongly-typed sources (Parquet), skips inference and promotes
        types directly. For untyped sources (CSV), runs full inference pipeline.

        Args:
            ctx: Phase context

        Returns:
            PhaseResult with typed_tables and type_decisions
        """
        raw_table_ids = self._resolve_target_table_ids(ctx)

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
            table_stmt = (
                select(Table).where(Table.table_id == table_id).options(selectinload(Table.columns))
            )
            result = ctx.session.execute(table_stmt)
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
                session_id=ctx.require_session_id(),
                run_id=ctx.run_id,
            )

            if not inference_result.success:
                warnings.append(
                    f"Type inference failed for {table.table_name}: {inference_result.error}"
                )
                continue

            # Apply unit overrides before resolution
            _apply_unit_overrides(ctx.session, ctx.config, table)

            # Flush type candidates so resolve_types can query them via selectinload
            # This is necessary because selectinload queries the DB, not the session cache
            ctx.session.flush()

            # Phase 2: Resolve types (create typed + quarantine tables)
            resolution_result = resolve_types(
                table_id=table_id,
                duckdb_conn=ctx.duckdb_conn,
                session=ctx.session,
                min_confidence=min_confidence,
                session_id=ctx.require_session_id(),
                run_id=ctx.run_id,
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

        # Link the run's session to the tables it just typed (DAT-407) so the
        # session's source is derivable without a stored ``source_id``. Written
        # here — a side-effect of typed-table creation, same transaction — rather
        # than as a separate workflow activity. Idempotent across teach re-types.
        link_session_tables(ctx.session, ctx.require_session_id(), typed_tables)

        return PhaseResult.success(
            outputs={
                "typed_tables": typed_tables,
                "type_decisions": type_decisions,
            },
            records_processed=total_rows_processed,
            records_created=total_typed_created,
            warnings=warnings,
            summary=f"{len(typed_tables)} tables typed, {len(type_decisions)} type decisions",
        )


def _apply_unit_overrides(
    session: Session,
    config: dict,  # type: ignore[type-arg]
    table: Table,
) -> None:
    """Patch TypeCandidate.detected_unit from config overrides.

    Reads ``overrides.units`` from typing config. Keys are
    ``"table.column"``; values contain ``{unit: "USD"}``.
    """
    from dataraum.analysis.typing.db_models import TypeCandidate

    overrides = config.get("overrides", {})
    if not isinstance(overrides, dict):
        return
    units = overrides.get("units", {})
    if not isinstance(units, dict) or not units:
        return

    for col in table.columns:
        col_ref = f"{table.table_name}.{col.column_name}"
        entry = units.get(col_ref)
        if not isinstance(entry, dict):
            continue
        unit = entry.get("unit")
        if not unit:
            continue

        # Patch the best type candidate for this column
        tc = session.execute(
            select(TypeCandidate)
            .where(TypeCandidate.column_id == col.column_id)
            .order_by(TypeCandidate.confidence.desc())
            .limit(1)
        ).scalar_one_or_none()
        if tc is not None:
            tc.detected_unit = unit
            tc.unit_confidence = 1.0
            logger.info("unit_override_applied", column=col_ref, unit=unit)
