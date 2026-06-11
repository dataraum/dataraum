"""Column eligibility evaluation logic.

Evaluates columns against configurable quality thresholds.
Pure logic — no pipeline/phase dependencies.
"""

from __future__ import annotations

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
        "warn_single_value": config.thresholds.warn_single_value,
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
    except KeyError, ValueError:
        return template


def quarantine_and_drop_columns(
    conn: Any,  # DuckDB connection
    typed_table: str,
    columns_data: list[tuple[Any, str]],
    *,
    typed_recipe_ddl: str,
) -> None:
    """Quarantine ineligible columns and drop them from the typed table (DAT-504).

    Convergent under Temporal at-least-once redelivery — every lake write is an
    idempotent overwrite under a run-stable name:

    1. Re-execute the run's stored typed ``MaterializationRecipe`` DDL, restoring
       the full-column typed table regardless of what a prior partial attempt
       left behind. The recipe row is READ-only — it must keep reproducing the
       full-column table for exactly this convergence to work.
    2. ``CREATE OR REPLACE`` the companion quarantine table in
       ``lake.quarantine`` in one shot (no append — a re-run replaces, never
       duplicates).
    3. ``ALTER TABLE … DROP COLUMN`` each quarantined column — guaranteed
       present after the rebuild, so the drop can't error on a re-run.

    Any failure propagates to the caller — a lake failure fails the phase
    (no swallowed-warning downgrade).

    Args:
        conn: DuckDB connection with the ``lake`` catalog attached.
        typed_table: Bare name of the typed table (e.g., ``"source__orders"``
            — the ``<source>__<table>`` form stored on ``Table.duckdb_path``
            post-DAT-341).
        columns_data: List of (Column, reason) tuples to drop.
        typed_recipe_ddl: The run's stored typed-layer materialization DDL
            (``CREATE OR REPLACE TABLE lake.typed.… AS SELECT …``), re-executed
            verbatim to restore the full-column table.
    """
    # Lazy imports, mirroring the loaders: avoid pulling the DuckLake bootstrap
    # surface into module-load.
    from dataraum.core.duckdb_naming import schema_for_layer
    from dataraum.server.storage import LAKE_CATALOG_ALIAS

    typed_fqn = f'{LAKE_CATALOG_ALIAS}.{schema_for_layer("typed")}."{typed_table}"'
    # Run-stable companion name in the quarantine layer schema — same bare base
    # as the typed table so layer-aware cleanup locates it. The typing phase's
    # cast-failure quarantine already owns ``lake.quarantine."<bare>"``, so the
    # column-quarantine keeps its distinct ``quarantine_columns_`` prefix.
    quarantine_fqn = (
        f'{LAKE_CATALOG_ALIAS}.{schema_for_layer("quarantine")}."quarantine_columns_{typed_table}"'
    )

    # 1. Restore the full-column typed table from the run's recipe.
    conn.execute(typed_recipe_ddl)

    # 2. One-shot quarantine snapshot: one SELECT per quarantined column,
    # unioned, replacing the table as a whole.
    selects = []
    for column, reason in columns_data:
        escaped_reason = reason.replace("'", "''") if reason else "Unknown"
        # Column names can legitimately contain quotes (CSV headers, MSSQL) —
        # escape per context: '' inside the single-quoted literal, "" inside
        # the double-quoted identifiers.
        escaped_col = column.column_name.replace("'", "''")
        escaped_col_ident = column.column_name.replace('"', '""')
        selects.append(f"""
            SELECT
                ROW_NUMBER() OVER () as _row_id,
                '{escaped_col}' as _column_name,
                CAST("{escaped_col_ident}" AS VARCHAR) as _value,
                '{escaped_reason}' as _quarantine_reason,
                CURRENT_TIMESTAMP as _quarantined_at
            FROM {typed_fqn}
        """)
    conn.execute(f"CREATE OR REPLACE TABLE {quarantine_fqn} AS {' UNION ALL '.join(selects)}")

    # 3. Drop the quarantined columns — present for sure after step 1.
    for column, _reason in columns_data:
        escaped_col_ident = column.column_name.replace('"', '""')
        conn.execute(f'ALTER TABLE {typed_fqn} DROP COLUMN "{escaped_col_ident}"')
