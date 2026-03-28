"""Formatters for JSON-structured MCP tool output.

These produce Python dicts that the server layer serializes to JSON.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dataraum.query.models import QueryResult


_QUERY_DISPLAY_ROWS = 50


def format_query_result(result: QueryResult) -> dict[str, Any]:
    """Format query result as structured dict."""
    output: dict[str, Any] = {
        "confidence": {
            "label": result.confidence_level.label,
            "emoji": result.confidence_level.emoji,
        },
        "answer": result.answer,
    }

    if result.contract:
        output["contract"] = result.contract

    if result.data and result.columns:
        total = len(result.data)
        displayed = result.data[:_QUERY_DISPLAY_ROWS]
        truncated = total > _QUERY_DISPLAY_ROWS
        data_block: dict[str, Any] = {
            "columns": result.columns,
            "row_count": total,
            "rows_returned": len(displayed),
            "rows": displayed,
            "truncated": truncated,
        }
        if truncated:
            data_block["hint"] = (
                f"{len(displayed)} of {total} rows returned. "
                "Use run_sql with the SQL above for more rows, "
                "or add export_format for the full dataset."
            )
        output["data"] = data_block

    if result.execution_steps:
        output["execution_steps"] = [
            {
                "step_id": step.step_id,
                "description": step.description,
                "sql": step.sql,
                "from_snippet": bool(step.snippet_id),
            }
            for step in result.execution_steps
        ]

    if result.sql:
        output["sql"] = result.sql

    if result.risk_assessment:
        output["risk_assessment"] = result.risk_assessment
    elif result.assumptions:
        output["assumptions"] = [
            {"assumption": a.assumption, "basis": a.basis.value} for a in result.assumptions
        ]

    return output


def format_run_sql_result(
    columns: list[str],
    rows: list[dict[str, Any]],
    *,
    limit: int,
    total_rows: int,
    step_info: list[dict[str, Any]] | None = None,
    step_results: list[Any] | None = None,
    column_quality: dict[str, Any] | None = None,
    quality_caveat: str | None = None,
    snippet_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Format run_sql result as structured dict.

    Args:
        columns: Output column names.
        rows: Result rows as list of dicts.
        limit: Applied row limit.
        total_rows: Total rows before truncation.
        step_info: Per-step execution info (with snippet status). Preferred.
        step_results: StepExecutionResult list — fallback when step_info is absent.
        column_quality: Per-column quality metadata.
        quality_caveat: Warning when quality data is incomplete.
        snippet_summary: Snippet reuse/save summary.
    """
    if step_info is not None:
        steps_executed = step_info
    elif step_results is not None:
        steps_executed = [{"step_id": sr.step_id, "sql": sr.sql_executed} for sr in step_results]
    else:
        steps_executed = []

    truncated = total_rows > limit
    result: dict[str, Any] = {
        "columns": columns,
        "row_count": total_rows,
        "rows_returned": len(rows),
        "rows": rows,
        "truncated": truncated,
        "steps_executed": steps_executed,
    }
    if truncated:
        result["hint"] = (
            f"Showing {len(rows)} of {total_rows} rows. "
            f"Increase limit (max 10000) or use export_format for the full dataset."
        )
    # Surface quality warnings prominently for columns grade C or worse
    warnings: list[str] = []
    if quality_caveat:
        warnings.append(quality_caveat)
    if column_quality is not None:
        for col_name, meta in column_quality.items():
            if not isinstance(meta, dict):
                continue
            grade = meta.get("quality_grade")
            source = meta.get("source_column", col_name)
            readiness = meta.get("readiness")
            if grade and grade >= "C":
                score = meta.get("quality_score", "")
                score_str = f" ({score})" if score else ""
                msg = f"{col_name} ({source}): Grade {grade}{score_str}"
                if readiness == "investigate":
                    msg += " — investigate before using in aggregations"
                elif readiness == "blocked":
                    msg += " — quality too low for reliable analysis"
                warnings.append(msg)
    if warnings:
        result["warnings"] = warnings
    if column_quality is not None:
        result["column_quality"] = column_quality
    if snippet_summary is not None:
        result["snippet_summary"] = snippet_summary
    return result
