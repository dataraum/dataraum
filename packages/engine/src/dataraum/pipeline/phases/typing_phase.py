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
from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy import delete, select
from sqlalchemy.orm import selectinload

from dataraum.analysis.typing import infer_type_candidates, resolve_types
from dataraum.analysis.typing.patterns import load_typing_config
from dataraum.core.logging import get_logger
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.cleanup import exec_delete
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
    def duckdb_layers(self) -> list[str]:
        return ["typed", "quarantine"]

    def cleanup(
        self,
        session: Session,
        source_id: str,
        table_ids: list[str],
        column_ids: list[str],
    ) -> int:
        # NOTE: source-scoped — deletes typed/quarantine for the WHOLE source,
        # ignoring any per-table filter. `_run`/`should_skip` honor
        # `ctx.table_ids`, but cleanup does not: the cleanup() path
        # has no table-filter param. A per-table replay that runs cleanup first
        # would clobber sibling tables' typed rows. TODO(DAT-344): thread a
        # table filter through cleanup when the per-table replay path lands.
        from dataraum.analysis.typing.db_models import TypeCandidate, TypeDecision

        count = 0
        # Delete TypeCandidate and TypeDecision for raw-layer columns
        raw_table_ids = list(
            session.execute(
                select(Table.table_id).where(Table.source_id == source_id, Table.layer == "raw")
            )
            .scalars()
            .all()
        )
        if raw_table_ids:
            raw_col_ids = list(
                session.execute(select(Column.column_id).where(Column.table_id.in_(raw_table_ids)))
                .scalars()
                .all()
            )
            if raw_col_ids:
                count += exec_delete(
                    session,
                    delete(TypeCandidate).where(TypeCandidate.column_id.in_(raw_col_ids)),
                )
                count += exec_delete(
                    session,
                    delete(TypeDecision).where(TypeDecision.column_id.in_(raw_col_ids)),
                )
        # Delete typed and quarantine layer Tables (CASCADE deletes Columns and children)
        count += exec_delete(
            session,
            delete(Table).where(
                Table.source_id == source_id,
                Table.layer.in_(["typed", "quarantine"]),
            ),
        )
        return count

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.analysis.typing import db_models

        return [db_models]

    def replay_cleanup(self, ctx: PhaseContext, table_ids: list[str]) -> None:
        """Drop the typed/quarantine state for raw tables in ``table_ids`` (DAT-343).

        Triggered when a teach (e.g. ``type_pattern``) requires re-typing
        one or more raw tables. Scoped to ``table_ids`` (raw table ids from
        ``replay.raw_table_ids``) so parallel siblings in a fan-out keep
        their typed state.

        Drops, in order:
            1. ``TypeCandidate`` / ``TypeDecision`` rows for the raw columns
               in scope — these are FK'd to the raw Column (not the typed
               Column), so the cascade through the typed Table delete below
               doesn't reach them; we delete them explicitly so re-inference
               gets a clean slate.
            2. The matching typed ``Table`` row (looked up by name in the
               ``typed`` layer). Cascade deletes its Columns and every
               per-column derived row (statistical profiles, quality
               metrics, semantic annotations, temporal profiles,
               relationships).
            3. Any leftover quarantine ``Table`` row by the same name.
            4. The DuckDB ``lake.typed.<bare>`` and ``lake.quarantine.<bare>``
               tables. ``CREATE OR REPLACE`` on the next run would overwrite
               them, but we drop them explicitly to keep ``DROP TABLE IF
               EXISTS`` idempotent against schema drift between teach
               iterations.

        Empty ``table_ids`` means source-wide — re-types every raw table.
        """
        from dataraum.analysis.typing.db_models import TypeCandidate, TypeDecision
        from dataraum.core.duckdb_naming import schema_for_layer
        from dataraum.server.storage import LAKE_CATALOG_ALIAS

        raw_stmt = select(Table).where(
            Table.source_id == ctx.source_id, Table.layer == "raw"
        )
        if table_ids:
            raw_stmt = raw_stmt.where(Table.table_id.in_(table_ids))
        raw_tables = list(ctx.session.execute(raw_stmt).scalars())
        if not raw_tables:
            return

        # 1: drop TypeCandidate/TypeDecision for the raw columns in scope.
        raw_col_ids = list(
            ctx.session.execute(
                select(Column.column_id).where(
                    Column.table_id.in_([t.table_id for t in raw_tables])
                )
            ).scalars()
        )
        if raw_col_ids:
            ctx.session.execute(
                delete(TypeCandidate).where(TypeCandidate.column_id.in_(raw_col_ids))
            )
            ctx.session.execute(
                delete(TypeDecision).where(TypeDecision.column_id.in_(raw_col_ids))
            )

        # 2 + 3: drop typed/quarantine Tables sharing the raw table_name —
        # typed and quarantine each have at most one row per name per source,
        # so we match by (source_id, table_name, layer).
        names = [t.table_name for t in raw_tables]
        ctx.session.execute(
            delete(Table).where(
                Table.source_id == ctx.source_id,
                Table.table_name.in_(names),
                Table.layer.in_(["typed", "quarantine"]),
            )
        )
        ctx.session.flush()

        # 4: drop the DuckDB typed/quarantine tables for these bare names.
        # The raw row carries the canonical bare name (``<source>__<table>``),
        # and typed/quarantine share it (post-DAT-341).
        for raw in raw_tables:
            if not raw.duckdb_path:
                continue
            for layer in ("typed", "quarantine"):
                fqn = f'{LAKE_CATALOG_ALIAS}.{schema_for_layer(layer)}."{raw.duckdb_path}"'
                ctx.duckdb_conn.execute(f"DROP TABLE IF EXISTS {fqn}")

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if typed tables already exist for the targeted raw tables.

        When ``ctx.table_ids`` is set (per-table teach replay), only the
        requested raw tables are considered — so a targeted untyped table
        still runs even if its sibling tables are already typed.
        """
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

        # Check if all (targeted) raw tables have corresponding typed tables
        for raw_table in raw_tables:
            typed_stmt = select(Table).where(
                Table.source_id == ctx.source_id,
                Table.table_name == raw_table.table_name,
                Table.layer == "typed",
            )
            typed_table = ctx.session.execute(typed_stmt).scalar_one_or_none()
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

        # Create typed Table record
        typed_table_id = str(uuid4())
        typed_table = Table(
            table_id=typed_table_id,
            source_id=table.source_id,
            table_name=table.table_name,
            layer="typed",
            duckdb_path=bare,
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

        logger.debug(
            "strongly_typed_promoted",
            table=table.table_name,
            columns=len(table.columns),
            rows=row_count,
        )

        return typed_table_id, type_decisions

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
