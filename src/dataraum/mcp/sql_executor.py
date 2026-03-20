"""Direct SQL execution with quality metadata and snippet integration.

Core logic for the run_sql MCP tool. Converts caller-provided SQL (raw or
structured steps) into execute_sql_steps() calls, enriches results with
per-column quality metadata, and integrates with the snippet library.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from dataraum.query.execution import ExecutionResult, SQLStep, execute_sql_steps

if TYPE_CHECKING:
    import duckdb
    from sqlalchemy.orm import Session

_log = logging.getLogger(__name__)

# Hard ceiling on row limit to prevent enormous responses.
MAX_ROW_LIMIT = 10_000
DEFAULT_ROW_LIMIT = 100


def run_sql(
    cursor: duckdb.DuckDBPyConnection,
    *,
    steps: list[dict[str, Any]] | None = None,
    sql: str | None = None,
    limit: int = DEFAULT_ROW_LIMIT,
) -> dict[str, Any]:
    """Execute SQL and return results as a structured dict.

    Args:
        cursor: DuckDB connection.
        steps: Structured SQL steps (list of dicts with step_id, sql, description,
            optional column_mappings).
        sql: Raw SQL convenience mode. Mutually exclusive with steps.
        limit: Max rows to return. Capped at MAX_ROW_LIMIT.

    Returns:
        Dict with columns, rows, row_count, truncated, steps_executed.
        On error, returns dict with "error" key.
    """
    # --- Validate input ---
    if steps is not None and sql is not None:
        return {"error": "Provide either 'steps' or 'sql', not both."}
    if steps is None and sql is None:
        return {"error": "Provide either 'steps' or 'sql'."}

    # Clamp limit
    effective_limit = min(max(1, limit), MAX_ROW_LIMIT)

    # --- Build SQLStep list + final_sql ---
    sql_steps: list[SQLStep]
    final_sql: str

    if sql is not None:
        # Convenience mode: wrap raw SQL as single step
        sql_steps = [SQLStep(step_id="query", sql=sql, description="Raw SQL query")]
        final_sql = "SELECT * FROM query"
    else:
        assert steps is not None
        sql_steps = [
            SQLStep(
                step_id=s["step_id"],
                sql=s["sql"],
                description=s.get("description", ""),
            )
            for s in steps
        ]
        # Final SQL selects from the last step
        last_step_id = sql_steps[-1].step_id
        final_sql = f"SELECT * FROM {last_step_id}"

    # --- Execute ---
    result = execute_sql_steps(
        steps=sql_steps,
        final_sql=final_sql,
        duckdb_conn=cursor,
        repair_fn=None,
        return_table=True,
    )

    if not result.success or not result.value:
        return {"error": str(result.error)}

    exec_result: ExecutionResult = result.value
    columns = exec_result.columns or []
    all_rows = exec_result.rows or []
    total_rows = len(all_rows)
    truncated = total_rows > effective_limit
    sliced_rows = all_rows[:effective_limit]

    # Convert to list-of-dicts
    rows_as_dicts = [dict(zip(columns, row)) for row in sliced_rows]

    from dataraum.mcp.formatters import format_run_sql_result

    return format_run_sql_result(
        columns=columns,
        rows=rows_as_dicts,
        step_results=exec_result.step_results,
        limit=effective_limit,
        total_rows=total_rows,
    )
