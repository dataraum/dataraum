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
from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy import delete, select
from sqlalchemy.orm import selectinload

from dataraum.analysis.typing import infer_type_candidates, resolve_types
from dataraum.analysis.typing.patterns import load_typing_config
from dataraum.core.logging import get_logger
from dataraum.investigation.queries import link_session_tables
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.storage import Column, Table

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

    def replay_cleanup(self, ctx: PhaseContext, table_ids: list[str]) -> None:
        """Clear typing's OWN state in place for raw tables in ``table_ids`` (DAT-373).

        Triggered when a teach (e.g. ``type_pattern``) requires re-typing
        one or more raw tables. Scoped to ``table_ids`` (raw table ids from
        ``replay.raw_table_ids``) so parallel siblings in a fan-out keep
        their typed state.

        Owner-scoped, in-place (DAT-373 Option A). The typed/quarantine
        ``Table`` and ``Column`` rows are **kept** — ``_run`` reconciles them
        in place on the re-type, keeping their ids stable so other stages'
        per-Column rows (begin_session / frame-ground findings, etc.) stay
        attached. The cascade-drop of the whole typed ``Table`` is reserved
        for ``import`` / source teardown, NOT a re-type.

        Clears only typing-owned state:
            1. ``TypeCandidate`` / ``TypeDecision`` rows for the raw AND typed
               columns in scope. The raw rows are typing's inference audit;
               the typed rows are typing's copies (``resolve_types`` writes
               one ``TypeDecision`` per typed column, guarded by
               ``uq_column_type_decision``). Both are deleted so the re-run's
               fresh inserts get a clean slate.
            2. The DuckDB ``lake.typed.<bare>`` / ``lake.quarantine.<bare>``
               tables. ``CREATE OR REPLACE`` on the next run rebuilds them;
               we drop them explicitly to keep ``DROP TABLE IF EXISTS``
               idempotent against schema drift between teach iterations.

        It does NOT touch ``StatisticalProfile`` / ``SemanticAnnotation`` /
        temporal / quality / eligibility rows — those belong to their phases,
        which own their own ``replay_cleanup`` and run after typing on the
        replay.

        Empty ``table_ids`` means source-wide — re-types every raw table.
        """
        from dataraum.analysis.typing.db_models import TypeCandidate, TypeDecision
        from dataraum.core.duckdb_naming import schema_for_layer
        from dataraum.server.storage import LAKE_CATALOG_ALIAS

        raw_stmt = select(Table).where(Table.source_id == ctx.source_id, Table.layer == "raw")
        if table_ids:
            raw_stmt = raw_stmt.where(Table.table_id.in_(table_ids))
        raw_tables = list(ctx.session.execute(raw_stmt).scalars())
        if not raw_tables:
            return

        # 1: clear TypeCandidate/TypeDecision for the raw columns AND the typed
        # columns sharing the raw table_name. Typed/quarantine columns are
        # reached by joining the typed/quarantine Table rows (same table_name).
        names = [t.table_name for t in raw_tables]
        owned_table_ids = list(
            ctx.session.execute(
                select(Table.table_id).where(
                    Table.source_id == ctx.source_id,
                    Table.table_name.in_(names),
                    Table.layer.in_(["raw", "typed", "quarantine"]),
                )
            ).scalars()
        )
        col_ids = list(
            ctx.session.execute(
                select(Column.column_id).where(Column.table_id.in_(owned_table_ids))
            ).scalars()
        )
        if col_ids:
            ctx.session.execute(delete(TypeCandidate).where(TypeCandidate.column_id.in_(col_ids)))
            ctx.session.execute(delete(TypeDecision).where(TypeDecision.column_id.in_(col_ids)))
            ctx.session.flush()

        # 2: drop the DuckDB typed/quarantine tables for these bare names.
        # The raw row carries the canonical bare name (``<source>__<table>``),
        # and typed/quarantine share it (post-DAT-341). The Postgres typed +
        # quarantine Table/Column rows are intentionally left in place.
        for raw in raw_tables:
            if not raw.duckdb_path:
                continue
            for layer in ("typed", "quarantine"):
                fqn = f'{LAKE_CATALOG_ALIAS}.{schema_for_layer(layer)}."{raw.duckdb_path}"'
                ctx.duckdb_conn.execute(f"DROP TABLE IF EXISTS {fqn}")

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if the targeted raw tables are already typed.

        When ``ctx.table_ids`` is set (per-table teach replay), only the
        requested raw tables are considered — so a targeted untyped table
        still runs even if its sibling tables are already typed.

        A typed table counts as "done" only if its columns still carry the
        ``TypeDecision`` rows ``_run`` writes (DAT-373). The typed ``Table``
        row now survives a re-type replay (stable identity), so its mere
        presence is no longer the signal: ``replay_cleanup`` clears the typed
        columns' ``TypeDecision`` in place, and an empty/decisionless typed
        table re-types. This is the Postgres signal ``replay_cleanup`` deletes,
        keeping skip and cleanup in lock-step.
        """
        from dataraum.analysis.typing.db_models import TypeDecision

        stmt = select(Table).where(
            Table.source_id == ctx.source_id,
            Table.layer == "raw",
        )
        raw_tables = list(ctx.session.execute(stmt).scalars())
        if not raw_tables:
            return "No raw tables to process"

        if ctx.table_ids:
            requested = set(ctx.table_ids)
            raw_tables = [t for t in raw_tables if t.table_id in requested]
            if not raw_tables:
                return "No raw tables match the requested table_ids filter"

        # A targeted raw table needs typing if it has no typed counterpart OR
        # that counterpart's columns have lost their TypeDecisions (post-cleanup).
        for raw_table in raw_tables:
            typed_table = ctx.session.execute(
                select(Table).where(
                    Table.source_id == ctx.source_id,
                    Table.table_name == raw_table.table_name,
                    Table.layer == "typed",
                )
            ).scalar_one_or_none()
            if not typed_table:
                return None  # At least one table needs typing

            has_decision = ctx.session.execute(
                select(TypeDecision.decision_id)
                .join(Column, Column.column_id == TypeDecision.column_id)
                .where(Column.table_id == typed_table.table_id)
                .limit(1)
            ).first()
            if has_decision is None:
                return None  # Typed row exists but was cleaned for a re-type

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

        # Create typed table as direct copy (types already correct)
        ctx.duckdb_conn.execute(
            f"CREATE OR REPLACE TABLE {typed_target} AS SELECT * FROM {raw_target}"
        )

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
        # "trusted source" decisions; writing them keeps the typed-table skip
        # signal uniform with the untyped path (``should_skip`` treats a typed
        # table with no TypeDecisions as needing a re-type — DAT-373). On a
        # re-type the stable column ids are reused, so clear prior copies first
        # to respect ``uq_column_type_decision``.
        from dataraum.analysis.typing.db_models import TypeDecision

        typed_col_ids = list(column_map.values())
        if typed_col_ids:
            ctx.session.execute(
                delete(TypeDecision).where(TypeDecision.column_id.in_(typed_col_ids))
            )
            ctx.session.flush()

        type_decisions: dict[str, str] = {}
        for col in table.columns:
            resolved = col.raw_type or "VARCHAR"
            typed_col_id = column_map[col.column_name]
            ctx.session.add(
                TypeDecision(
                    decision_id=str(uuid4()),
                    session_id=ctx.require_session_id(),
                    column_id=typed_col_id,
                    run_id=ctx.run_id,
                    decided_type=resolved,
                    decision_source="automatic",
                    decided_at=datetime.now(UTC),
                    decision_reason="strongly-typed source (types trusted)",
                )
            )
            type_decisions[typed_col_id] = resolved

        logger.debug(
            "strongly_typed_promoted",
            table=table.table_name,
            columns=len(table.columns),
            rows=row_count,
        )

        return typed_table.table_id, type_decisions

    def _resolve_target_table_ids(self, ctx: PhaseContext) -> list[str]:
        """Resolve which raw table_ids to type for this run.

        Resolution order:

        1. Query the raw tables registered under ``ctx.source_id``.
        2. If none are found, fall back to ``ctx.table_ids`` verbatim — some
           callers carry the ids in context without a source-scoped raw row.
        3. If raw tables exist *and* ``ctx.table_ids`` is set, intersect:
           type only the requested subset. This is the per-table teach-replay
           path — re-type one table without touching its siblings.

        An empty ``ctx.table_ids`` means "all raw tables" (backward compatible).
        Requested ids that are not raw tables of this source are dropped.
        """
        stmt = select(Table.table_id).where(
            Table.source_id == ctx.source_id,
            Table.layer == "raw",
        )
        raw_table_ids = [row[0] for row in ctx.session.execute(stmt)]

        if not raw_table_ids:
            # No source-scoped raw rows: preserve the pre-existing fallback of
            # trusting caller-provided ids verbatim (unreachable from the
            # scheduler today, which never sets table_ids without raw rows).
            return list(ctx.table_ids)

        if ctx.table_ids:
            requested = set(ctx.table_ids)
            return [tid for tid in raw_table_ids if tid in requested]

        return raw_table_ids

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
