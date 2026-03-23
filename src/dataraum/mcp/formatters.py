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
        result["top_blocker"] = get_dimension_label(evaluation.worst_dimension)
        result["path_to_compliance"] = {
            "focus_dimension": evaluation.worst_dimension,
            "worst_score": round(evaluation.worst_dimension_score, 2),
            "estimated_effort": evaluation.estimated_effort_to_comply,
        }

    return result


def format_query_result(
    result: QueryResult, persisted_decisions: list[str] | None = None
) -> dict[str, Any]:
    """Format query result as structured dict per spec.

    Produces:
    - answer: { summary, data, sql }
    - decisions_made: auto-applied high-confidence assumptions (single-line strings)
    - open_questions: medium-confidence ambiguous decisions (max 3)
    - confidence: { level, factors }
    """
    from dataraum.graphs.models import AssumptionBasis

    # Split assumptions into auto-applied vs open questions
    decisions_made: list[str] = []
    open_questions: list[dict[str, Any]] = []

    for assumption in result.assumptions:
        if assumption.confidence >= 0.8 or assumption.basis == AssumptionBasis.USER_SPECIFIED:
            # High confidence or user-specified: auto-applied, surface as decision
            decisions_made.append(assumption.assumption)
        elif assumption.confidence >= 0.5:
            # Medium confidence: surface as open question
            open_questions.append(
                {
                    "issue": assumption.assumption,
                    "options": ["Keep as-is", "Provide clarification"],
                    "impact": assumption.assumption,
                }
            )
        # Low confidence (<0.5) assumptions are suppressed

    # Cap open questions at 3, highest confidence first
    open_questions = sorted(
        open_questions,
        key=lambda q: next(
            (a.confidence for a in result.assumptions if a.assumption == q["issue"]),
            0.0,
        ),
        reverse=True,
    )[:3]

    # Also extract decisions from validation_notes (high-signal decisions from LLM)
    for note in result.validation_notes:
        if note and note not in decisions_made:
            decisions_made.append(note)

    # Prepend persisted user_decision fixes (from prior apply_fix calls)
    if persisted_decisions:
        decisions_made = [
            d for d in persisted_decisions if d not in decisions_made
        ] + decisions_made

    # Build answer block
    answer: dict[str, Any] = {}
    if result.answer:
        answer["summary"] = result.answer
    if result.data and result.columns:
        answer["data"] = result.data[:50]
    if result.sql:
        answer["sql"] = result.sql

    # Confidence block: level + factors from risk_assessment or entropy
    confidence_label = (
        result.confidence_level.label.lower() if result.confidence_level else "medium"
    )
    # Map label to spec level
    level_map = {
        "high": "high",
        "✅ high": "high",
        "medium": "medium",
        "⚠️ medium": "medium",
        "low": "low",
        "❌ low": "low",
    }
    confidence_level = level_map.get(confidence_label, "medium")

    confidence_factors: list[str] = []
    if result.risk_assessment:
        # Extract concise factors from risk_assessment (which may be a long string)
        lines = [ln.strip() for ln in result.risk_assessment.splitlines() if ln.strip()]
        confidence_factors = lines[:3]
    elif result.entropy_score is not None:
        confidence_factors.append(f"Entropy score: {round(result.entropy_score, 3)}")

    confidence: dict[str, Any] = {"level": confidence_level}
    if confidence_factors:
        confidence["factors"] = confidence_factors

    output: dict[str, Any] = {"answer": answer}

    if decisions_made:
        output["decisions_made"] = decisions_made

    if open_questions:
        output["open_questions"] = open_questions

    output["confidence"] = confidence

    if result.contract:
        output["contract"] = result.contract

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

    # Add top-level overall block derived from entropy section
    if "entropy" in sections:
        e = sections["entropy"]
        overall: dict[str, Any] = {"status": e.get("overall_status", "UNKNOWN")}
        if "entropy_score" in e:
            overall["entropy_score"] = e["entropy_score"]
        result["overall"] = overall

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
        "next_steps": next_steps,
    }


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
    assumptions_applied: list[str] | None = None,
    sql_warnings: list[str] | None = None,
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
        assumptions_applied: General facts about the data auto-applied (e.g. units).
        sql_warnings: Heuristic warnings about the submitted SQL (non-blocking).
    """
    if step_info is not None:
        steps_executed = step_info
    elif step_results is not None:
        steps_executed = [{"step_id": sr.step_id, "sql": sr.sql_executed} for sr in step_results]
    else:
        steps_executed = []

    result: dict[str, Any] = {
        "columns": columns,
        "row_count": len(rows),
        "rows": rows,
        "truncated": total_rows > limit,
        "steps_executed": steps_executed,
    }

    if assumptions_applied:
        result["assumptions_applied"] = assumptions_applied

    # Surface quality warnings prominently for columns grade C or worse
    warnings: list[str] = []
    if sql_warnings:
        warnings.extend(sql_warnings)
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
