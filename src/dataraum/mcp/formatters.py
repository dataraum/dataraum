"""Formatters for JSON-structured MCP tool output.

These produce Python dicts that the server layer serializes to JSON.
Replaces the previous markdown formatters for better LLM parseability.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dataraum.entropy.contracts import ContractEvaluation, ContractProfile
    from dataraum.pipeline.runner import RunResult
    from dataraum.query.models import QueryResult


def format_pipeline_result(result: RunResult) -> dict[str, Any]:
    """Format pipeline run result as structured dict."""
    failed = result.get_failed_phases()
    return {
        "status": "complete" if result.success else "failed",
        "output_dir": str(result.output_dir) if result.output_dir else None,
        "duration_seconds": round(result.duration_seconds, 1),
        "phases": {
            "completed": result.phases_completed,
            "failed": result.phases_failed,
            "skipped": result.phases_skipped,
        },
        "failures": [{"phase": p.phase_name, "error": p.error} for p in failed],
        "next_steps": (
            [
                "Use get_context for full schema and relationships",
                "Use get_quality for entropy, contracts, and resolution actions",
                "Use query to ask questions about the data",
            ]
            if result.success
            else [
                "Check the failures above and fix the data issues",
                "Re-run analyze after fixing",
            ]
        ),
    }


def format_entropy_summary(
    source_name: str,
    overall_readiness: str,
    avg_entropy_score: float,
    interpretations: Sequence[Any],
    table_filter: str | None = None,
    dimension_scores: dict[str, float] | None = None,
) -> dict[str, Any]:
    """Format entropy summary as structured dict."""
    dim_breakdown = None
    if dimension_scores:
        dim_breakdown = {
            dim: round(score, 3)
            for dim, score in sorted(dimension_scores.items(), key=lambda x: -x[1])
            if score > 0
        }

    interp_list = []
    for interp in interpretations:
        entry: dict[str, Any] = {
            "table": interp.table_name,
            "column": interp.column_name,
        }
        if interp.explanation:
            entry["explanation"] = interp.explanation
        interp_list.append(entry)

    result: dict[str, Any] = {
        "source_name": source_name,
        "overall_status": overall_readiness.upper(),
        "entropy_score": round(avg_entropy_score, 3),
        "columns_analyzed": len(interpretations),
    }
    if table_filter:
        result["table_filter"] = table_filter
    if dim_breakdown:
        result["dimension_breakdown"] = dim_breakdown
    if interp_list:
        result["interpretations"] = interp_list
    return result


def format_contract_evaluation(
    evaluation: ContractEvaluation,
    profile: ContractProfile,
) -> dict[str, Any]:
    """Format contract evaluation as structured dict."""
    from dataraum.entropy.config import get_dimension_label

    dimensions = []
    for dim, threshold in profile.dimension_thresholds.items():
        score = evaluation.dimension_scores.get(dim, 0.0)
        dimensions.append(
            {
                "dimension": dim,
                "label": get_dimension_label(dim),
                "score": round(score, 2),
                "threshold": threshold,
                "passing": score <= threshold,
            }
        )

    result: dict[str, Any] = {
        "contract": profile.name,
        "display_name": profile.display_name,
        "description": profile.description,
        "status": "PASS" if evaluation.is_compliant else "FAIL",
        "confidence": evaluation.confidence_level.label,
        "overall_score": round(evaluation.overall_score, 2),
        "threshold": profile.overall_threshold,
        "dimensions": dimensions,
    }

    if evaluation.violations:
        result["violations"] = [
            {"severity": v.severity, "details": v.details} for v in evaluation.violations
        ]
    if evaluation.warnings:
        result["warnings"] = [{"details": w.details} for w in evaluation.warnings]

    if not evaluation.is_compliant and evaluation.worst_dimension:
        result["path_to_compliance"] = {
            "focus_dimension": evaluation.worst_dimension,
            "worst_score": round(evaluation.worst_dimension_score, 2),
            "estimated_effort": evaluation.estimated_effort_to_comply,
        }

    return result


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
        output["data"] = {
            "columns": result.columns,
            "row_count": len(result.data),
            "rows": result.data[:50],
        }

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


def format_actions_report(
    source_name: str,
    actions: list[dict[str, Any]],
    priority_filter: str | None = None,
    table_filter: str | None = None,
) -> dict[str, Any]:
    """Format resolution actions report as structured dict."""
    formatted_actions = []
    for action in actions:
        entry: dict[str, Any] = {
            "action": action["action"],
            "priority_score": round(action.get("priority_score", 0), 3),
            "effort": action.get("effort", "medium"),
        }
        if action.get("description"):
            entry["description"] = action["description"]

        network_impact = action.get("network_impact", 0.0)
        if network_impact > 0:
            net: dict[str, Any] = {
                "delta": round(network_impact, 3),
                "columns_affected": action.get("network_columns", 0),
            }
            column_deltas = action.get("column_deltas", {})
            if column_deltas:
                sorted_deltas = sorted(column_deltas.items(), key=lambda x: -x[1])
                net["top_columns"] = {col: round(d, 3) for col, d in sorted_deltas[:5]}
            entry["network_impact"] = net

        affected = action.get("affected_columns", [])
        if affected:
            entry["affected_columns"] = affected

        params = action.get("parameters", {})
        if params and isinstance(params, dict):
            entry["parameters"] = params

        if action.get("expected_impact"):
            entry["expected_impact"] = action["expected_impact"]

        fixes = action.get("fixes_violations", [])
        if fixes:
            entry["fixes_violations"] = fixes

        formatted_actions.append(entry)

    quick_wins = [
        {
            "action": a["action"],
            "priority_score": round(a.get("priority_score", 0), 3),
            "description": a.get("description", ""),
        }
        for a in actions
        if a.get("effort") == "low" and a.get("priority_score", 0) > 0.5
    ][:3]

    result: dict[str, Any] = {
        "source_name": source_name,
        "total_actions": len(actions),
        "actions": formatted_actions,
    }

    filters: dict[str, str] = {}
    if priority_filter:
        filters["priority"] = priority_filter
    if table_filter:
        filters["table"] = table_filter
    if filters:
        result["filters"] = filters

    if quick_wins:
        result["quick_wins"] = quick_wins

    return result


def format_quality_report(sections: dict[str, Any]) -> dict[str, Any]:
    """Combine quality sections into a unified report dict."""
    result: dict[str, Any] = {}
    for name in ("entropy", "contract", "actions"):
        content = sections.get(name)
        if content:
            result[name] = content

    if not result:
        result["message"] = "No quality data available. Run the pipeline first."

    return result


def format_export_result(
    output_path: str, fmt: str, row_count: int, sidecar_path: str
) -> dict[str, Any]:
    """Format export result as structured dict."""
    return {
        "file": output_path,
        "format": fmt,
        "rows": row_count,
        "metadata_sidecar": sidecar_path,
    }


def format_zone_status(
    zone_name: str,
    gate_label: str,
    gate_phase: str,
    violations: list[dict[str, Any]],
    passing: list[dict[str, Any]],
    skipped_detectors: list[dict[str, str]],
    contract_name: str | None = None,
) -> dict[str, Any]:
    """Format zone status as structured dict."""
    if violations:
        next_steps = [
            "Review triage guidance and pick an action for each violation",
            (
                "Call apply_fix(fixes=[{action: ..., target: "
                '"column:table.col", parameters: {...}, reason: "..."}])'
            ),
            "Call continue_pipeline to advance to the next zone after fixing",
        ]
    elif skipped_detectors:
        next_steps = ["All measured dimensions passing — use continue_pipeline to advance"]
    else:
        next_steps = ["All dimensions passing — pipeline is clean"]

    return {
        "zone": zone_name,
        "gate": gate_label,
        "gate_phase": gate_phase,
        "contract": contract_name,
        "summary": {
            "violations": len(violations),
            "passing": len(passing),
        },
        "violations": violations,
        "passing": passing,
        "skipped_detectors": skipped_detectors,
        "next_steps": next_steps,
    }


def format_run_sql_result(
    columns: list[str],
    rows: list[dict[str, Any]],
    step_results: list[Any],
    *,
    limit: int,
    total_rows: int,
    column_quality: dict[str, Any] | None = None,
    quality_caveat: str | None = None,
) -> dict[str, Any]:
    """Format run_sql result as structured dict.

    Args:
        columns: Output column names.
        rows: Result rows as list of dicts.
        step_results: StepExecutionResult list from execute_sql_steps.
        limit: Applied row limit.
        total_rows: Total rows before truncation.
        column_quality: Per-column quality metadata (Phase 2b).
        quality_caveat: Warning when quality data is incomplete.
    """
    result: dict[str, Any] = {
        "columns": columns,
        "row_count": len(rows),
        "rows": rows,
        "truncated": total_rows > limit,
        "steps_executed": [
            {"step_id": sr.step_id, "sql": sr.sql_executed} for sr in step_results
        ],
    }
    if column_quality is not None:
        result["column_quality"] = column_quality
    if quality_caveat:
        result["quality_caveat"] = quality_caveat
    return result
