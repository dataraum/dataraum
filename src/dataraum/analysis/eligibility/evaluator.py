"""Column eligibility evaluation logic.

Evaluates columns against configurable quality thresholds.
Pure logic — no pipeline/phase dependencies.
"""

from __future__ import annotations

import re
from typing import Any

from dataraum.analysis.eligibility.config import EligibilityConfig
from dataraum.analysis.statistics.db_models import StatisticalProfile
from dataraum.core.logging import get_logger

logger = get_logger(__name__)


def extract_metrics(profile: StatisticalProfile | None) -> dict[str, Any]:
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


def evaluate_rules(
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
            if evaluate_condition(rule.condition, eval_context):
                reason = format_reason(rule.reason, metrics)
                return (rule.status, rule.id, reason)
        except Exception as e:
            logger.warning(
                "rule_evaluation_error",
                rule_id=rule.id,
                column=column_name,
                error=str(e),
            )

    return (config.default_status, None, None)


def evaluate_condition(condition: str, context: dict[str, Any]) -> bool:
    """Safely evaluate a condition expression."""
    try:
        # Handle None values - if any metric is None, condition is False
        for key, value in context.items():
            if value is None and key in condition:
                return False

        # Replace variable names (longest keys first to avoid substring collisions,
        # e.g. "null_ratio" must not corrupt "max_null_ratio" or "warn_null_ratio")
        expr = condition
        for key in sorted(context, key=len, reverse=True):
            value = context[key]
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


def format_reason(template: str, metrics: dict[str, Any]) -> str:
    """Format reason template with actual values."""
    try:
        return template.format(**metrics)
    except (KeyError, ValueError):
        return template


def is_likely_key(column_name: str, patterns: list[str]) -> bool:
    """Check if column name matches key patterns."""
    for pattern in patterns:
        if re.search(pattern, column_name, re.IGNORECASE):
            return True
    return False


def quarantine_and_drop_columns(
    conn: Any,  # DuckDB connection
    typed_table: str,
    columns_data: list[tuple[Any, str]],
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
