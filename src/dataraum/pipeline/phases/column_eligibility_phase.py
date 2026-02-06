"""Column eligibility phase implementation.

Evaluates columns against configurable quality thresholds and
drops ineligible columns from typed tables (moving data to quarantine).
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import structlog
from sqlalchemy import select

from dataraum.analysis.eligibility.config import (
    EligibilityConfig,
    load_eligibility_config,
)
from dataraum.analysis.eligibility.db_models import ColumnEligibilityRecord
from dataraum.analysis.statistics.db_models import StatisticalProfile
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.storage import Column, Table

logger = structlog.get_logger(__name__)


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
    def outputs(self) -> list[str]:
        return ["eligible", "warned", "dropped"]

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if all columns already have eligibility records."""
        # Get typed tables
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        typed_tables = ctx.session.execute(stmt).scalars().all()

        if not typed_tables:
            return "No typed tables found"

        table_ids = [t.table_id for t in typed_tables]

        # Get columns
        cols_stmt = select(Column).where(Column.table_id.in_(table_ids))
        columns = ctx.session.execute(cols_stmt).scalars().all()

        if not columns:
            return "No columns found"

        column_ids = [c.column_id for c in columns]

        # Check for existing eligibility records
        existing_stmt = select(ColumnEligibilityRecord.column_id).where(
            ColumnEligibilityRecord.column_id.in_(column_ids)
        )
        existing = set(ctx.session.execute(existing_stmt).scalars().all())

        if len(existing) >= len(column_ids):
            return "All columns already have eligibility records"

        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Run column eligibility evaluation."""
        # Load config
        config = load_eligibility_config()

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

        # Get statistical profiles (using typed layer)
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
        columns_to_drop: dict[str, list[tuple[Column, str]]] = {}  # table_id -> [(column, reason)]
        warnings: list[str] = []

        # Evaluate each column
        for column in all_columns:
            # Skip if already evaluated
            if column.column_id in existing_column_ids:
                continue

            table = table_map.get(column.table_id)
            if not table:
                continue

            # Get metrics from statistical profile
            profile = profile_map.get(column.column_id)
            metrics = _extract_metrics(profile)

            # Evaluate rules
            status, rule_id, reason = _evaluate_rules(config, metrics, column.column_name)

            # Check if critical column (likely key)
            if status == "INELIGIBLE" and _is_likely_key(column.column_name, config.key_patterns):
                return PhaseResult.failed(
                    f"Critical column '{column.column_name}' in table '{table.table_name}' "
                    f"is ineligible: {reason}. Cannot proceed with unusable key column."
                )

            # Create eligibility record (denormalized - survives column deletion)
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

            # Track columns to drop
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

        # Flush eligibility records before deleting columns
        # (records no longer have FK to columns, so order matters for clarity)
        ctx.session.flush()

        # Drop ineligible columns from DuckDB tables
        for table_id, columns_data in columns_to_drop.items():
            table = table_map[table_id]
            if not table.duckdb_path:
                warnings.append(f"Table {table.table_name} has no DuckDB path, cannot drop columns")
                continue

            try:
                _quarantine_and_drop_columns(
                    ctx.duckdb_conn,
                    table.duckdb_path,
                    columns_data,
                )

                # Delete Column rows (cascades to StatisticalProfile, TypeCandidate, etc.)
                for column, _ in columns_data:
                    ctx.session.delete(column)

                logger.info(
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


def _extract_metrics(profile: StatisticalProfile | None) -> dict[str, Any]:
    """Extract metrics from statistical profile for rule evaluation."""
    if profile is None:
        return {
            "null_ratio": None,
            "distinct_count": None,
            "cardinality_ratio": None,
            "total_count": None,
        }

    return {
        "null_ratio": profile.null_ratio,
        "distinct_count": profile.distinct_count,
        "cardinality_ratio": profile.cardinality_ratio,
        "total_count": profile.total_count,
    }


def _evaluate_rules(
    config: EligibilityConfig,
    metrics: dict[str, Any],
    column_name: str,
) -> tuple[str, str | None, str | None]:
    """Evaluate eligibility rules against column metrics.

    Returns:
        Tuple of (status, rule_id, reason)
    """
    # Build evaluation context
    eval_context = {
        # Metrics
        "null_ratio": metrics.get("null_ratio"),
        "distinct_count": metrics.get("distinct_count"),
        "cardinality_ratio": metrics.get("cardinality_ratio"),
        "total_count": metrics.get("total_count"),
        # Thresholds
        "max_null_ratio": config.thresholds.max_null_ratio,
        "eliminate_single_value": config.thresholds.eliminate_single_value,
        "warn_null_ratio": config.thresholds.warn_null_ratio,
    }

    # Handle None values in conditions
    if eval_context["null_ratio"] is None:
        # Can't evaluate without null_ratio - mark as eligible with warning
        return ("ELIGIBLE", None, None)

    for rule in config.rules:
        try:
            if _evaluate_condition(rule.condition, eval_context):
                reason = _format_reason(rule.reason, metrics)
                return (rule.status, rule.id, reason)
        except Exception as e:
            logger.warning(
                "rule_evaluation_error",
                rule_id=rule.id,
                column=column_name,
                error=str(e),
            )

    return (config.default_status, None, None)


def _evaluate_condition(condition: str, context: dict[str, Any]) -> bool:
    """Safely evaluate a condition expression."""
    # Simple evaluation - replace variable names with values
    # Only allow safe operations
    try:
        # Handle None values - if any metric is None, condition is False
        for key, value in context.items():
            if value is None and key in condition:
                return False

        # Replace variable names
        expr = condition
        for key, value in context.items():
            if isinstance(value, bool):
                expr = expr.replace(key, str(value))
            elif isinstance(value, (int, float)):
                expr = expr.replace(key, str(value))

        # Evaluate (only allow comparison and boolean operators)
        # This is safe because we control the input format
        result = eval(expr, {"__builtins__": {}}, {})  # noqa: S307
        return bool(result)
    except Exception:
        return False


def _format_reason(template: str, metrics: dict[str, Any]) -> str:
    """Format reason template with actual values."""
    try:
        return template.format(**metrics)
    except (KeyError, ValueError):
        return template


def _is_likely_key(column_name: str, patterns: list[str]) -> bool:
    """Check if column name matches key patterns."""
    for pattern in patterns:
        if re.search(pattern, column_name, re.IGNORECASE):
            return True
    return False


def _quarantine_and_drop_columns(
    conn: Any,  # DuckDB connection
    typed_table: str,
    columns_data: list[tuple[Column, str]],
) -> None:
    """Move column data to quarantine and drop from typed table.

    Args:
        conn: DuckDB connection
        typed_table: Name of the typed table (e.g., "typed_orders")
        columns_data: List of (Column, reason) tuples to drop
    """
    base_name = typed_table.replace("typed_", "")
    quarantine_table = f"quarantine_columns_{base_name}"

    # Create quarantine table if not exists
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS "{quarantine_table}" (
            _row_id INTEGER,
            _column_name VARCHAR,
            _value VARCHAR,
            _quarantine_reason VARCHAR,
            _quarantined_at TIMESTAMP
        )
    """)

    # For each column, insert data then drop
    for column, reason in columns_data:
        # Escape reason for SQL
        escaped_reason = reason.replace("'", "''") if reason else "Unknown"

        # Insert column data into quarantine
        conn.execute(f"""
            INSERT INTO "{quarantine_table}"
            SELECT
                ROW_NUMBER() OVER () as _row_id,
                '{column.column_name}' as _column_name,
                CAST("{column.column_name}" AS VARCHAR) as _value,
                '{escaped_reason}' as _quarantine_reason,
                CURRENT_TIMESTAMP as _quarantined_at
            FROM "{typed_table}"
        """)

        # Drop column from typed table
        conn.execute(f"""
            ALTER TABLE "{typed_table}" DROP COLUMN "{column.column_name}"
        """)
