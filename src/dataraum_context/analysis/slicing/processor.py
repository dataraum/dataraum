"""Slicing analysis processor.

Orchestrates slicing analysis using the SlicingAgent and stores results.
"""

from datetime import UTC, datetime
from typing import Any

import duckdb
from sqlalchemy.orm import Session

from dataraum_context.analysis.slicing.agent import SlicingAgent
from dataraum_context.analysis.slicing.db_models import SliceDefinition, SlicingAnalysisRun
from dataraum_context.analysis.slicing.models import SlicingAnalysisResult
from dataraum_context.analysis.slicing.utils import load_slicing_context
from dataraum_context.core.models.base import Result


def analyze_slices(
    session: Session,
    agent: SlicingAgent,
    table_ids: list[str],
    duckdb_conn: duckdb.DuckDBPyConnection | None = None,
    execute_slices: bool = False,
) -> Result[SlicingAnalysisResult]:
    """Run slicing analysis on tables.

    Steps:
    1. Load context from previous analysis phases
    2. Call slicing agent for LLM analysis
    3. Store slice definitions in database
    4. Optionally execute SQL to create slice tables

    Args:
        session: Database session
        agent: Slicing agent for LLM analysis
        table_ids: List of table IDs to analyze
        duckdb_conn: DuckDB connection (required if execute_slices=True)
        execute_slices: Whether to execute SQL and create slice tables

    Returns:
        Result containing SlicingAnalysisResult
    """
    started_at = datetime.now(UTC)

    # Create run record
    run = SlicingAnalysisRun(
        table_ids=table_ids,
        started_at=started_at,
        status="running",
    )
    session.add(run)
    session.flush()

    try:
        # Load context from previous phases
        context_data = load_slicing_context(session, table_ids)

        # Update run with context stats
        run.tables_analyzed = len(context_data.get("tables", []))
        run.columns_considered = sum(
            len(t.get("columns", [])) for t in context_data.get("tables", [])
        )

        # Call slicing agent
        llm_result = agent.analyze(
            session=session,
            table_ids=table_ids,
            context_data=context_data,
        )

        if not llm_result.success:
            run.status = "failed"
            run.error_message = llm_result.error
            run.completed_at = datetime.now(UTC)
            run.duration_seconds = (run.completed_at - started_at).total_seconds()
            return Result.fail(llm_result.error or "Slicing analysis failed")

        result = llm_result.unwrap()

        # Store slice definitions
        for rec in result.recommendations:
            slice_def = SliceDefinition(
                table_id=rec.table_id,
                column_id=rec.column_id,
                slice_priority=rec.slice_priority,
                slice_type="categorical",
                distinct_values=rec.distinct_values,
                value_count=rec.value_count,
                reasoning=rec.reasoning,
                business_context=rec.business_context,
                confidence=rec.confidence,
                sql_template=rec.sql_template,
                detection_source="llm",
            )
            session.add(slice_def)

        # Update run record
        run.recommendations_count = len(result.recommendations)
        run.slices_generated = len(result.slice_queries)
        run.status = "completed"
        run.completed_at = datetime.now(UTC)
        run.duration_seconds = (run.completed_at - started_at).total_seconds()

        # Optionally execute slice SQL
        if execute_slices and duckdb_conn:
            _execution_results = _execute_slice_queries(duckdb_conn, result.slice_queries)
            # Could add execution results to response if needed

        return Result.ok(result)

    except Exception as e:
        run.status = "failed"
        run.error_message = str(e)
        run.completed_at = datetime.now(UTC)
        run.duration_seconds = (run.completed_at - started_at).total_seconds()
        return Result.fail(f"Slicing analysis failed: {e}")


def _execute_slice_queries(
    duckdb_conn: duckdb.DuckDBPyConnection,
    slice_queries: list[Any],
) -> dict[str, Any]:
    """Execute slice SQL queries to create tables.

    Args:
        duckdb_conn: DuckDB connection
        slice_queries: List of SliceSQL objects

    Returns:
        Dict with execution results
    """
    results: dict[str, list[Any]] = {
        "success": [],
        "failed": [],
    }

    for slice_sql in slice_queries:
        try:
            # Drop table if exists first
            drop_sql = f"DROP TABLE IF EXISTS {slice_sql.table_name}"
            duckdb_conn.execute(drop_sql)

            # Create slice table
            duckdb_conn.execute(slice_sql.sql_query)
            results["success"].append(slice_sql.table_name)
        except Exception as e:
            results["failed"].append({"table_name": slice_sql.table_name, "error": str(e)})

    return results


def execute_slices_from_definitions(
    duckdb_conn: duckdb.DuckDBPyConnection,
    slice_definitions: list[SliceDefinition],
    source_table: str,
) -> dict[str, Any]:
    """Execute slices from stored definitions.

    This function can be called separately to create slice tables
    from previously stored slice definitions.

    Args:
        duckdb_conn: DuckDB connection
        slice_definitions: List of SliceDefinition records
        source_table: Source table name to slice from

    Returns:
        Dict with execution results
    """
    results: dict[str, list[Any]] = {
        "success": [],
        "failed": [],
    }

    for slice_def in slice_definitions:
        if not slice_def.distinct_values:
            continue

        column_name = slice_def.column.column_name if slice_def.column else "unknown"

        for value in slice_def.distinct_values:
            # Sanitize for table name
            import re

            safe_value = re.sub(r"[^a-zA-Z0-9]", "_", str(value))
            safe_value = re.sub(r"_+", "_", safe_value).strip("_").lower()
            safe_column = re.sub(r"[^a-zA-Z0-9]", "_", column_name)
            safe_column = re.sub(r"_+", "_", safe_column).strip("_").lower()

            table_name = f"slice_{safe_column}_{safe_value}"
            quoted_column = f'"{column_name}"'
            escaped_value = str(value).replace("'", "''")

            try:
                # Drop if exists
                duckdb_conn.execute(f"DROP TABLE IF EXISTS {table_name}")

                # Create slice
                sql = f"""CREATE TABLE {table_name} AS
SELECT * FROM {source_table}
WHERE {quoted_column} = '{escaped_value}'"""
                duckdb_conn.execute(sql)
                results["success"].append(table_name)
            except Exception as e:
                results["failed"].append({"table_name": table_name, "error": str(e)})

    return results


__all__ = [
    "analyze_slices",
    "execute_slices_from_definitions",
]
