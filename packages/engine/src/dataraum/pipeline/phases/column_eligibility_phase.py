"""Column eligibility phase implementation.

Thin wrapper — business logic lives in analysis/eligibility/evaluator.py.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import ModuleType
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from dataraum.analysis.eligibility.config import load_eligibility_config
from dataraum.analysis.eligibility.db_models import ColumnEligibilityRecord
from dataraum.analysis.eligibility.evaluator import (
    evaluate_rules,
    extract_metrics,
    quarantine_and_drop_columns,
)
from dataraum.analysis.statistics.db_models import StatisticalProfile
from dataraum.analysis.typing.db_models import MaterializationRecipe
from dataraum.core.logging import get_logger
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases._column_cleanup import delete_column_dependents
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.storage import Column, Table
from dataraum.storage.upsert import upsert

if TYPE_CHECKING:
    pass

logger = get_logger(__name__)


@analysis_phase
class ColumnEligibilityPhase(BasePhase):
    """Column eligibility evaluation phase.

    Evaluates each column against configurable quality thresholds.
    Ineligible columns are dropped from typed tables and their data
    is preserved in quarantine tables for potential recovery.

    Requires: statistics phase (for null_ratio, distinct_count, cardinality_ratio)
    """

    @property
    def name(self) -> str:
        return "column_eligibility"

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.analysis.eligibility import db_models

        return [db_models]

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip only when there are genuinely no typed columns to evaluate.

        Structural early-out only (DAT-413): a re-run mints a fresh ``run_id``
        and must always re-evaluate eligibility under it, so the old "all columns
        already have records → skip" bail is gone. ``_run`` stamps ``run_id`` on
        each ``ColumnEligibilityRecord`` and only treats THIS run's records as
        already-evaluated, so a new run re-derives while prior runs' records stay
        intact; the promoted head names which run is current.
        """
        typed_tables = self._typed_tables(ctx)

        if not typed_tables:
            return "No typed tables found"

        table_ids = [t.table_id for t in typed_tables]

        cols_stmt = select(Column).where(Column.table_id.in_(table_ids))
        columns = ctx.session.execute(cols_stmt).scalars().all()

        if not columns:
            return "No columns found"

        return None

    def _typed_recipe_ddl(self, ctx: PhaseContext, table: Table) -> str:
        """The run's stored typed ``MaterializationRecipe`` DDL for ``table``.

        Grain ``(typed table_id, "typed", ctx.run_id)`` — written by both typing
        paths (resolution + the strongly-typed copy) before eligibility runs.
        The row is READ-only here: the convergent drop sequence (DAT-504)
        depends on it reproducing the FULL-column typed table, so it is never
        overwritten with post-drop DDL. Absence is a bug upstream — fail loud,
        no fallback.
        """
        # A None run_id (non-run callers, mirrors the profiles query above)
        # compiles to ``run_id IS NULL``.
        recipe = ctx.session.execute(
            select(MaterializationRecipe).where(
                MaterializationRecipe.table_id == table.table_id,
                MaterializationRecipe.layer == "typed",
                MaterializationRecipe.run_id == ctx.run_id,
            )
        ).scalar_one_or_none()
        if recipe is None:
            raise RuntimeError(
                f"No typed materialization recipe for table {table.table_name} "
                f"({table.table_id}) at run {ctx.run_id} — typing stores it before "
                "eligibility runs; cannot drop columns convergently without it."
            )
        return recipe.ddl

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Run column eligibility evaluation."""
        config = load_eligibility_config(ctx.config)

        typed_tables = self._typed_tables(ctx)

        if not typed_tables:
            return PhaseResult.failed("No typed tables found. Run typing phase first.")

        table_ids = [t.table_id for t in typed_tables]
        table_map = {t.table_id: t for t in typed_tables}

        # Get columns with statistical profiles
        cols_stmt = select(Column).where(Column.table_id.in_(table_ids))
        all_columns = ctx.session.execute(cols_stmt).scalars().all()

        # Filter to THIS run's profiles (DAT-413): the statistics phase stamps
        # ``run_id`` on every StatisticalProfile it writes, and eligibility reads
        # its own run's upstream output. No-op under one run; the head pointer is
        # not consulted yet (Phase 3).
        profiles_stmt = select(StatisticalProfile).where(
            StatisticalProfile.column_id.in_([c.column_id for c in all_columns]),
            StatisticalProfile.layer == "typed",
            StatisticalProfile.run_id == ctx.run_id,
        )
        profiles = ctx.session.execute(profiles_stmt).scalars().all()
        profile_map = {p.column_id: p for p in profiles}

        # Track results
        counts = {"ELIGIBLE": 0, "WARN": 0, "INELIGIBLE": 0}
        columns_to_drop: dict[str, list[tuple[Column, str]]] = {}
        warnings: list[str] = []
        evaluated_at = datetime.now(UTC)
        rows: list[dict[str, Any]] = []

        # Evaluate every column under THIS run and upsert (DAT-413). The old
        # "already has a record for this run → skip" guard is gone: a Temporal
        # at-least-once retry re-runs this whole activity against committed rows,
        # so we re-derive and upsert on ``(column_id, run_id)`` instead of either
        # duplicating (which would make scalar_one_or_none() raise) or skipping.
        for column in all_columns:
            table = table_map.get(column.table_id)
            if not table:
                continue

            profile = profile_map.get(column.column_id)
            metrics = extract_metrics(profile)
            status, rule_id, reason = evaluate_rules(config, metrics, column.column_name)

            # PK omitted so the model's Python-side default applies.
            rows.append(
                {
                    "column_id": column.column_id,
                    "table_id": table.table_id,
                    # The column's source is its table's source — resolved
                    # relationally off the row (DAT-422/426: the identity is
                    # source-free, and a run can span multiple per-object sources,
                    # so there is no single run source to record).
                    "source_id": table.source_id,
                    "run_id": ctx.require_run_id(),
                    "column_name": column.column_name,
                    "table_name": table.table_name,
                    "resolved_type": column.resolved_type,
                    "status": status,
                    "triggered_rule": rule_id,
                    "reason": reason,
                    "metrics_snapshot": metrics,
                    "config_version": config.version,
                    "evaluated_at": evaluated_at,
                }
            )
            counts[status] += 1

            if status == "INELIGIBLE":
                if table.table_id not in columns_to_drop:
                    columns_to_drop[table.table_id] = []
                columns_to_drop[table.table_id].append((column, reason or "Ineligible"))

            logger.debug(
                "column_eligibility_evaluated",
                column=column.column_name,
                table=table.table_name,
                status=status,
                rule=rule_id,
            )

        # Upsert on ``(column_id, run_id)`` so a retry refreshes rows in place.
        upsert(ctx.session, ColumnEligibilityRecord, rows, index_elements=["column_id", "run_id"])

        # Drop ineligible columns from the lake (DAT-504 convergent sequence):
        # rebuild the typed table from the run's stored recipe, snapshot the
        # quarantined columns in one shot, then drop them. A lake failure
        # propagates and fails the activity (rollback per 9d262fde) — no
        # warning downgrade; warnings remain only for the structural
        # no-duckdb_path case below.
        for table_id, columns_data in columns_to_drop.items():
            table = table_map[table_id]
            if not table.duckdb_path:
                warnings.append(f"Table {table.table_name} has no DuckDB path, cannot drop columns")
                continue

            quarantine_and_drop_columns(
                ctx.duckdb_conn,
                table.duckdb_path,
                columns_data,
                typed_recipe_ddl=self._typed_recipe_ddl(ctx, table),
            )

            # A dropped column is junk — its dependent run-stamped metadata is
            # meaningless and must go with it. The FK children of ``columns`` no
            # longer ``ON DELETE CASCADE`` (DAT-506 torn-window cut), so the
            # deliberate column drop deletes them explicitly here rather than
            # relying on a DB cascade.
            delete_column_dependents(ctx, [c.column_id for c, _ in columns_data])
            for column, _ in columns_data:
                ctx.session.delete(column)

            logger.debug(
                "columns_dropped",
                table=table.table_name,
                dropped_count=len(columns_data),
                columns=[c.column_name for c, _ in columns_data],
            )

        return PhaseResult.success(
            outputs={
                "eligible": counts["ELIGIBLE"],
                "warned": counts["WARN"],
                "dropped": counts["INELIGIBLE"],
            },
            records_processed=sum(counts.values()),
            records_created=sum(counts.values()),
            warnings=warnings if warnings else None,
            summary=f"{counts['ELIGIBLE']} eligible, {counts['WARN']} warned, {counts['INELIGIBLE']} dropped",
        )
