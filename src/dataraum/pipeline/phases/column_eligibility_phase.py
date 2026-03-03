"""Column eligibility phase implementation.

Thin orchestrator — business logic lives in analysis/eligibility/evaluator.py.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import ModuleType
from uuid import uuid4

from sqlalchemy import select

from dataraum.analysis.eligibility.config import load_eligibility_config
from dataraum.analysis.eligibility.db_models import ColumnEligibilityRecord
from dataraum.analysis.eligibility.evaluator import (
    evaluate_rules,
    extract_metrics,
    is_likely_key,
    quarantine_and_drop_columns,
)
from dataraum.analysis.statistics.db_models import StatisticalProfile
from dataraum.core.logging import get_logger
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.storage import Column, Table

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
    def description(self) -> str:
        return "Column eligibility evaluation"

    @property
    def dependencies(self) -> list[str]:
        return ["statistics"]

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.analysis.eligibility import db_models

        return [db_models]

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if all columns already have eligibility records."""
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        typed_tables = ctx.session.execute(stmt).scalars().all()

        if not typed_tables:
            return "No typed tables found"

        table_ids = [t.table_id for t in typed_tables]

        cols_stmt = select(Column).where(Column.table_id.in_(table_ids))
        columns = ctx.session.execute(cols_stmt).scalars().all()

        if not columns:
            return "No columns found"

        column_ids = [c.column_id for c in columns]

        existing_stmt = select(ColumnEligibilityRecord.column_id).where(
            ColumnEligibilityRecord.column_id.in_(column_ids)
        )
        existing = set(ctx.session.execute(existing_stmt).scalars().all())

        if len(existing) >= len(column_ids):
            return "All columns already have eligibility records"

        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Run column eligibility evaluation."""
        config = load_eligibility_config(ctx.config)

        # Get typed tables
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        typed_tables = ctx.session.execute(stmt).scalars().all()

        if not typed_tables:
            return PhaseResult.failed("No typed tables found. Run typing phase first.")

        table_ids = [t.table_id for t in typed_tables]
        table_map = {t.table_id: t for t in typed_tables}

        # Get columns with statistical profiles
        cols_stmt = select(Column).where(Column.table_id.in_(table_ids))
        all_columns = ctx.session.execute(cols_stmt).scalars().all()

        profiles_stmt = select(StatisticalProfile).where(
            StatisticalProfile.column_id.in_([c.column_id for c in all_columns]),
            StatisticalProfile.layer == "typed",
        )
        profiles = ctx.session.execute(profiles_stmt).scalars().all()
        profile_map = {p.column_id: p for p in profiles}

        # Check for existing eligibility records
        existing_stmt = select(ColumnEligibilityRecord.column_id).where(
            ColumnEligibilityRecord.column_id.in_([c.column_id for c in all_columns])
        )
        existing_column_ids = set(ctx.session.execute(existing_stmt).scalars().all())

        # Track results
        counts = {"ELIGIBLE": 0, "WARN": 0, "INELIGIBLE": 0}
        columns_to_drop: dict[str, list[tuple[Column, str]]] = {}
        warnings: list[str] = []

        # Evaluate each column
        for column in all_columns:
            if column.column_id in existing_column_ids:
                continue

            table = table_map.get(column.table_id)
            if not table:
                continue

            profile = profile_map.get(column.column_id)
            metrics = extract_metrics(profile)
            status, rule_id, reason = evaluate_rules(config, metrics, column.column_name)

            # Check if critical column (likely key)
            if status == "INELIGIBLE" and is_likely_key(column.column_name, config.key_patterns):
                return PhaseResult.failed(
                    f"Critical column '{column.column_name}' in table '{table.table_name}' "
                    f"is ineligible: {reason}. Cannot proceed with unusable key column."
                )

            record = ColumnEligibilityRecord(
                eligibility_id=str(uuid4()),
                column_id=column.column_id,
                table_id=table.table_id,
                source_id=ctx.source_id,
                column_name=column.column_name,
                table_name=table.table_name,
                resolved_type=column.resolved_type,
                status=status,
                triggered_rule=rule_id,
                reason=reason,
                metrics_snapshot=metrics,
                config_version=config.version,
                evaluated_at=datetime.now(UTC),
            )
            ctx.session.add(record)
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

        # Drop ineligible columns from DuckDB tables
        for table_id, columns_data in columns_to_drop.items():
            table = table_map[table_id]
            if not table.duckdb_path:
                warnings.append(f"Table {table.table_name} has no DuckDB path, cannot drop columns")
                continue

            try:
                quarantine_and_drop_columns(
                    ctx.duckdb_conn,
                    table.duckdb_path,
                    columns_data,
                )

                for column, _ in columns_data:
                    ctx.session.delete(column)

                logger.debug(
                    "columns_dropped",
                    table=table.table_name,
                    dropped_count=len(columns_data),
                    columns=[c.column_name for c, _ in columns_data],
                )

            except Exception as e:
                warnings.append(f"Failed to drop columns from {table.table_name}: {e}")
                logger.warning(
                    "column_drop_failed",
                    table=table.table_name,
                    error=str(e),
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
        )
