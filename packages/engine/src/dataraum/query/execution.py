"""Shared SQL step execution logic.

Used by the GraphAgent. DAT-616: the engine now MIRRORS the cockpit answer agent —
steps + final_sql are folded into ONE standalone CTE (``compose_standalone``, the Python
mirror of the cockpit ``composeStandalone``) and that single statement is the deterministic
executable (validated == executed, no temp-view state). The per-step scalars the metric
verifier's support gate needs are still fetched (steps are standalone SELECTs by prompt
contract), so the cheap floor survives; the composed CTE is the executable artifact the
metric carries alongside its snippet list.

Usage:
    result = execute_sql_steps(
        steps=steps,
        final_sql=final_sql,
        duckdb_conn=conn,
        repair_fn=my_repair_function,
    )
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result

if TYPE_CHECKING:
    import duckdb

logger = get_logger(__name__)

_LEADING_WITH = re.compile(r"^\s*with\s+", re.IGNORECASE)


def compose_standalone(steps: list[SQLStep], final_sql: str) -> str:
    """Fold ``{steps, final_sql}`` into ONE standalone statement.

    Each step becomes a CTE; ``final_sql`` references them by name. Python mirror of the
    cockpit ``composeStandalone`` (``run-steps.ts``) — the EXACT statement that executes,
    so there is no temp-view-vs-result divergence. No steps → ``final_sql`` verbatim.
    A ``final_sql`` that brings its OWN leading ``WITH`` has its CTEs merged into the one
    ``WITH`` (never the invalid ``WITH … WITH …``).
    """
    final = final_sql.strip().rstrip(";").strip()
    if not steps:
        return final
    ctes = ",\n".join(f"{s.step_id} AS (\n{s.sql}\n)" for s in steps)
    if _LEADING_WITH.match(final):
        rest = _LEADING_WITH.sub("", final, count=1)
        return f"WITH {ctes},\n{rest}"
    return f"WITH {ctes}\n{final}"


@dataclass
class SQLStep:
    """A single SQL step to execute."""

    step_id: str
    sql: str
    description: str


@dataclass
class StepExecutionResult:
    """Result of executing a single step."""

    step_id: str
    sql_executed: str
    value: Any = None
    repair_attempts: int = 0
    original_sql: str | None = None  # Pre-repair SQL when repair_attempts > 0


@dataclass
class ExecutionResult:
    """Result of executing all steps + final SQL."""

    step_results: list[StepExecutionResult]
    columns: list[str] | None = None
    rows: list[tuple[Any, ...]] | None = None
    total_count: int | None = None
    final_value: Any = None
    # DAT-616: the single self-contained statement actually executed (steps as CTEs +
    # final_sql) — the metric's executable artifact, alongside its per-step snippet list.
    composed_sql: str | None = None


# Type alias for the repair function signature
RepairFn = Callable[[str, str, str], Result[str]]


def execute_sql_steps(
    steps: list[SQLStep],
    final_sql: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    *,
    max_repair_attempts: int = 2,
    repair_fn: RepairFn | None = None,
    return_table: bool = False,
    display_limit: int | None = None,
) -> Result[ExecutionResult]:
    """Execute SQL steps as temp views and run final SQL.

    The GraphAgent's execution pattern:
    1. Create temp view for each step
    2. Execute final_sql that references the views
    3. Optionally repair SQL on failure

    Args:
        steps: Ordered list of SQL steps to execute
        final_sql: SQL that combines step results into final output
        duckdb_conn: DuckDB connection/cursor
        max_repair_attempts: Max repair retries per step (default 2)
        repair_fn: Optional function(failed_sql, error_msg, description) -> Result[repaired_sql]
        return_table: If True, return columns+rows from final SQL. If False, return scalar value.
        display_limit: If set and return_table is True, push LIMIT to DuckDB
            and compute total_count via COUNT(*). Avoids loading unbounded
            results into Python memory.

    Returns:
        Result with ExecutionResult on success
    """
    step_results: list[StepExecutionResult] = []

    # Fetch each step's scalar for the verifier's support gate. A step MAY reference an
    # earlier step (the old temp-view model allowed it; formula steps with depends_on do),
    # so each step is probed inside a CTE context of the prior (already-validated) steps —
    # `WITH <prior…>, <this step> SELECT * FROM <this step>` — not in raw isolation. No temp
    # views; the (possibly repaired) SQL then composes into the single executable below.
    validated: list[SQLStep] = []
    for step in steps:
        step_result = _execute_step(
            step=step,
            prior_steps=validated,
            duckdb_conn=duckdb_conn,
            max_repair_attempts=max_repair_attempts,
            repair_fn=repair_fn,
        )
        if not step_result.success or not step_result.value:
            return Result.fail(step_result.error or f"Step '{step.step_id}' failed")
        sr = step_result.value
        step_results.append(sr)
        validated.append(SQLStep(step_id=sr.step_id, sql=sr.sql_executed, description=""))

    # Compose the single standalone statement (steps as CTEs + final_sql), using each
    # step's executed (post-repair) SQL — the deterministic executable, no temp views.
    composed_sql = compose_standalone(validated, final_sql)

    # Execute the composed statement ONCE.
    final_result = _execute_final(
        final_sql=composed_sql,
        duckdb_conn=duckdb_conn,
        max_repair_attempts=max_repair_attempts,
        repair_fn=repair_fn,
        return_table=return_table,
        display_limit=display_limit,
    )
    # A clean execution returns its value faithfully — including a genuine 0 and a
    # NULL (no-support) result. Degeneracy is judged downstream by the metric
    # verifier (graphs.verifier), NOT conflated into a truthiness test here: the
    # old `not final_result.value` rejected a real 0 and gave an empty-support
    # result the generic reason "Final SQL failed". Only a thrown SQL error (after
    # repair is exhausted) fails the run now. (DAT-616)
    if not final_result.success:
        return Result.fail(final_result.error or "Final SQL failed")

    execution_result = ExecutionResult(step_results=step_results, composed_sql=composed_sql)

    if return_table:
        # Table mode: _execute_final returns (columns, rows, total_count) on success
        # (an empty table is an empty rows list, never None).
        if final_result.value is None:
            return Result.fail("Final SQL returned no result in table mode")
        columns, rows, total_count = final_result.value
        execution_result.columns = columns
        execution_result.rows = rows
        execution_result.total_count = total_count
    else:
        execution_result.final_value = final_result.value

    return Result.ok(execution_result)


def _execute_step(
    step: SQLStep,
    prior_steps: list[SQLStep],
    duckdb_conn: duckdb.DuckDBPyConnection,
    max_repair_attempts: int,
    repair_fn: RepairFn | None,
) -> Result[StepExecutionResult]:
    """Fetch a single step's scalar with retry/repair logic.

    DAT-616: no temp view. The step is probed inside a CTE context of the prior
    (already-validated) steps — ``WITH <prior…>, <step> SELECT * FROM <step> LIMIT 1`` —
    so a step that references an earlier step still resolves (the old temp-view model
    allowed this; formula steps depend on it). Takes the first column of the first row,
    matching the prior `SELECT * FROM <view>` semantics. Repair targets the STEP's SQL,
    not the composed probe.
    """
    original_sql = step.sql
    current_sql = step.sql
    last_error: str | None = None

    for attempt in range(max_repair_attempts + 1):
        try:
            probe = compose_standalone(
                [*prior_steps, SQLStep(step.step_id, current_sql, "")],
                f'SELECT * FROM "{step.step_id}" LIMIT 1',
            )
            result = duckdb_conn.execute(probe).fetchone()
            value = result[0] if result else None

            return Result.ok(
                StepExecutionResult(
                    step_id=step.step_id,
                    sql_executed=current_sql,
                    value=value,
                    repair_attempts=attempt,
                    original_sql=original_sql if attempt > 0 else None,
                )
            )
        except Exception as e:
            last_error = str(e)
            if attempt < max_repair_attempts and repair_fn:
                logger.info(
                    "step_failed_attempting_repair",
                    step_id=step.step_id,
                    attempt=attempt + 1,
                    error=str(e),
                )
                repair_result = repair_fn(current_sql, last_error, step.description)
                if repair_result.success and repair_result.value:
                    current_sql = repair_result.value
                    logger.info("step_repaired", step_id=step.step_id)
                else:
                    return Result.fail(
                        f"Step '{step.step_id}' failed and repair failed: {last_error}"
                    )
            elif attempt >= max_repair_attempts:
                return Result.fail(
                    f"Step '{step.step_id}' failed after {attempt + 1} attempts: {last_error}"
                )
            else:
                # No repair function, fail immediately
                return Result.fail(f"Step '{step.step_id}' failed: {last_error}")

    return Result.fail(f"Step '{step.step_id}' failed: {last_error}")


def _execute_final(
    final_sql: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    max_repair_attempts: int,
    repair_fn: RepairFn | None,
    return_table: bool,
    display_limit: int | None = None,
) -> Result[Any]:
    """Execute the final SQL with retry/repair logic.

    When display_limit is set and return_table is True, the LIMIT is pushed
    to DuckDB (not applied as a Python slice) and total_count is computed
    via COUNT(*) on the original SQL. Returns (columns, rows, total_count).
    """
    current_sql = final_sql
    last_error: str | None = None

    for attempt in range(max_repair_attempts + 1):
        try:
            if return_table and display_limit is not None:
                # Push LIMIT to DuckDB — avoids loading unbounded results
                limited_sql = f"SELECT * FROM ({current_sql}) AS _dr_limited LIMIT {display_limit}"
                result = duckdb_conn.execute(limited_sql)
                columns = [desc[0] for desc in result.description]
                rows = result.fetchall()
                # Get total count from original (unlimited) SQL
                count_row = duckdb_conn.execute(
                    f"SELECT COUNT(*) FROM ({current_sql}) AS _dr_count"
                ).fetchone()
                total_count = count_row[0] if count_row else len(rows)
                return Result.ok((columns, rows, total_count))
            elif return_table:
                result = duckdb_conn.execute(current_sql)
                columns = [desc[0] for desc in result.description]
                rows = result.fetchall()
                return Result.ok((columns, rows, len(rows)))
            else:
                result = duckdb_conn.execute(current_sql)
                row = result.fetchone()
                return Result.ok(row[0] if row else None)
        except Exception as e:
            last_error = str(e)
            if attempt < max_repair_attempts and repair_fn:
                logger.info("final_sql_failed_attempting_repair", attempt=attempt + 1, error=str(e))
                repair_result = repair_fn(
                    current_sql, last_error, "Combine step results into final answer"
                )
                if repair_result.success and repair_result.value:
                    current_sql = repair_result.value
                    logger.info("Repaired final SQL")
                else:
                    return Result.fail(f"Final SQL failed and repair failed: {last_error}")
            elif attempt >= max_repair_attempts:
                return Result.fail(f"Final SQL failed after {attempt + 1} attempts: {last_error}")
            else:
                return Result.fail(f"Final SQL failed: {last_error}")

    return Result.fail(f"Final SQL failed: {last_error}")
