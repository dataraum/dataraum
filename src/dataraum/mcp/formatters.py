"""Formatters for LLM-optimized output.

These format data for consumption by LLMs via MCP tools.
Focus on clarity and structured information.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dataraum.entropy.contracts import ContractEvaluation, ContractProfile
    from dataraum.pipeline.runner import RunResult
    from dataraum.query.models import QueryResult


def format_context_for_llm(source_name: str, context_text: str) -> str:
    """Format context document for LLM consumption."""
    return f"""# Data Context: {source_name}

{context_text}

---
Use this context to understand the data schema, relationships, and quality characteristics
when answering questions or generating queries.
"""


def format_entropy_summary(
    source_name: str,
    snapshot: Any,
    interpretations: Sequence[Any],
    table_filter: str | None = None,
    dimension_scores: dict[str, float] | None = None,
) -> str:
    """Format entropy summary for LLM consumption.

    Args:
        source_name: Name of the data source
        snapshot: EntropySnapshotRecord with overall stats
        interpretations: List of EntropyInterpretationRecord
        table_filter: Optional table filter applied
        dimension_scores: Optional dict of dimension path -> avg score across columns
    """
    lines = [f"# Entropy Summary: {source_name}"]

    if table_filter:
        lines.append(f"Filtered to table: {table_filter}")

    lines.append("")
    lines.append(f"## Overall Status: {snapshot.overall_readiness.upper()}")
    lines.append(f"Entropy Score: {snapshot.avg_entropy_score:.3f}")
    lines.append("")

    # Dimension breakdown
    if dimension_scores:
        lines.append("## Dimension Breakdown")
        # Sort by score descending to highlight worst dimensions first
        sorted_dims = sorted(dimension_scores.items(), key=lambda x: -x[1])
        for dim, score in sorted_dims:
            if score > 0:
                lines.append(f"- {dim}: {score:.3f}")
        lines.append("")

    lines.append("## Issue Summary")
    lines.append(f"- Total columns analyzed: {len(interpretations)}")
    lines.append("")

    # Column interpretations (grouped by table)
    if interpretations:
        lines.append("## Column Interpretations")
        current_table = None
        for interp in interpretations:
            if interp.table_name != current_table:
                current_table = interp.table_name
                lines.append(f"\n**{current_table}**")
            if interp.explanation:
                # Extract first sentence. Naive split(".") breaks on
                # decimal numbers (e.g. "score is 0.39"). Split only
                # on period followed by space+uppercase.
                parts = re.split(r"(?<=[.!?])\s+(?=[A-Z])", interp.explanation, maxsplit=1)
                lines.append(f"- {interp.column_name}: {parts[0]}")
            else:
                lines.append(f"- {interp.column_name}")

    return "\n".join(lines)


def format_contract_evaluation(
    evaluation: ContractEvaluation,
    profile: ContractProfile,
) -> str:
    """Format contract evaluation for LLM consumption."""
    lines = [f"# Contract Evaluation: {profile.display_name}"]
    lines.append("")

    # Status
    status = "PASS" if evaluation.is_compliant else "FAIL"
    lines.append(f"## Status: {status} ({evaluation.confidence_level.label})")
    lines.append(
        f"Overall Score: {evaluation.overall_score:.2f} (threshold: {profile.overall_threshold})"
    )
    lines.append("")

    # Description
    lines.append(f"**Description:** {profile.description}")
    lines.append("")

    # Dimension scores
    from dataraum.entropy.config import get_dimension_label

    lines.append("## Dimension Scores")
    for dim, threshold in profile.dimension_thresholds.items():
        score = evaluation.dimension_scores.get(dim, 0.0)
        status_icon = "✓" if score <= threshold else "✗"
        lines.append(f"- {get_dimension_label(dim)}: {score:.2f} / {threshold:.2f} [{status_icon}]")
    lines.append("")

    # Violations
    if evaluation.violations:
        lines.append("## Violations")
        for v in evaluation.violations:
            lines.append(f"- **[{v.severity}]** {v.details}")
        lines.append("")

    # Warnings
    if evaluation.warnings:
        lines.append("## Warnings")
        for w in evaluation.warnings:
            lines.append(f"- {w.details}")
        lines.append("")

    # Path to compliance
    if not evaluation.is_compliant and evaluation.worst_dimension:
        lines.append("## Path to Compliance")
        lines.append(
            f"Focus on: {evaluation.worst_dimension} (score: {evaluation.worst_dimension_score:.2f})"
        )
        lines.append(f"Estimated effort: {evaluation.estimated_effort_to_comply}")

    return "\n".join(lines)


def format_query_result(result: QueryResult) -> str:
    """Format query result for LLM consumption."""
    lines = ["# Query Result"]
    lines.append("")

    # Confidence
    lines.append(f"## Confidence: {result.confidence_level.label} {result.confidence_level.emoji}")
    if result.contract:
        lines.append(f"Contract: {result.contract}")
    lines.append("")

    # Answer
    lines.append("## Answer")
    lines.append(result.answer)
    lines.append("")

    # Data
    if result.data and result.columns:
        lines.append("## Data")
        lines.append(f"Columns: {', '.join(result.columns)}")
        lines.append(f"Rows: {len(result.data)}")
        lines.append("")

        # Show first few rows as markdown table
        if len(result.data) <= 20:
            # Header
            lines.append("| " + " | ".join(result.columns) + " |")
            lines.append("| " + " | ".join(["---"] * len(result.columns)) + " |")
            # Rows
            for row in result.data[:20]:
                values = [str(row.get(c, ""))[:100] for c in result.columns]
                lines.append("| " + " | ".join(values) + " |")
        else:
            lines.append(f"(Showing first 20 of {len(result.data)} rows)")
            lines.append("| " + " | ".join(result.columns) + " |")
            lines.append("| " + " | ".join(["---"] * len(result.columns)) + " |")
            for row in result.data[:20]:
                values = [str(row.get(c, ""))[:100] for c in result.columns]
                lines.append("| " + " | ".join(values) + " |")
        lines.append("")

    # Execution graph (steps + final SQL)
    if result.execution_steps:
        lines.append("## Execution Graph")
        for i, step in enumerate(result.execution_steps, 1):
            source = " (from snippet knowledge base)" if step.snippet_id else ""
            lines.append(f"### Step {i}: {step.step_id}")
            lines.append(f"{step.description}{source}")
            lines.append("```sql")
            lines.append(step.sql)
            lines.append("```")
            lines.append("")
        if result.sql:
            lines.append("### Final SQL")
            lines.append(
                "Combines "
                + ", ".join(f"`{s.step_id}`" for s in result.execution_steps)
                + " into the result."
            )
            lines.append("```sql")
            lines.append(result.sql)
            lines.append("```")
            lines.append("")
    elif result.sql:
        lines.append("## Generated SQL")
        lines.append("```sql")
        lines.append(result.sql)
        lines.append("```")
        lines.append("")

    # Risk assessment (replaces separate assumptions section when present)
    if result.risk_assessment:
        lines.append("## Risk Assessment")
        lines.append(result.risk_assessment)
    elif result.assumptions:
        lines.append("## Assumptions")
        for a in result.assumptions:
            lines.append(f"- {a.assumption} ({a.basis.value})")

    return "\n".join(lines)


def format_actions_report(
    source_name: str,
    actions: list[dict[str, Any]],
    priority_filter: str | None = None,
    table_filter: str | None = None,
) -> str:
    """Format resolution actions report for LLM consumption.

    Args:
        source_name: Name of the data source
        actions: List of merged action dictionaries
        priority_filter: Optional filter applied
        table_filter: Optional table filter applied

    Returns:
        Formatted markdown report
    """
    lines = [f"# Resolution Actions Report: {source_name}"]
    lines.append("")

    # Filters applied
    filters = []
    if priority_filter:
        filters.append(f"priority={priority_filter}")
    if table_filter:
        filters.append(f"table={table_filter}")
    if filters:
        lines.append(f"*Filters: {', '.join(filters)}*")
        lines.append("")

    if not actions:
        lines.append("No resolution actions found matching the criteria.")
        return "\n".join(lines)

    # Summary
    lines.append("## Summary")
    lines.append(f"- **{len(actions)}** resolution actions ranked by combined score")
    lines.append("- Score combines: detector reduction + column breadth + network causal impact")

    if actions:
        top = actions[0]
        cols = len(top.get("affected_columns", []))
        lines.append(f"- Top action: **{top['action']}** ({cols} columns)")
    lines.append("")

    # Ranked actions (single list, score-ordered)
    lines.append("## Actions (ranked by impact)")
    lines.append("")

    for i, action in enumerate(actions, 1):
        lines.append(f"### {i}. {action['action']}")

        # Score and effort
        score = action.get("priority_score", 0)
        effort = action.get("effort", "medium")
        lines.append(f"**Score:** {score:.3f} | **Effort:** {effort}")
        lines.append("")

        # Description
        if action.get("description"):
            lines.append(f"**Description:** {action['description']}")
            lines.append("")

        # Network causal impact with per-column breakdown
        network_impact = action.get("network_impact", 0.0)
        network_cols = action.get("network_columns", 0)
        column_deltas = action.get("column_deltas", {})
        if network_impact > 0:
            lines.append(
                f"**Network Impact:** {network_impact:.3f} causal delta "
                f"across {network_cols} columns"
            )
            if column_deltas:
                # Show top columns by delta
                sorted_deltas = sorted(column_deltas.items(), key=lambda x: -x[1])
                for col, delta in sorted_deltas[:5]:
                    lines.append(f"  - {col}: {delta:.3f}")
                if len(sorted_deltas) > 5:
                    lines.append(f"  - ... and {len(sorted_deltas) - 5} more")
            lines.append("")

        # Affected columns
        affected = action.get("affected_columns", [])
        if affected:
            lines.append(f"**Affected Columns ({len(affected)}):**")
            for col in affected[:10]:
                lines.append(f"- {col}")
            if len(affected) > 10:
                lines.append(f"- ... and {len(affected) - 10} more")
            lines.append("")

        # Parameters
        params = action.get("parameters", {})
        if params and isinstance(params, dict):
            lines.append("**Parameters:**")
            for k, v in params.items():
                lines.append(f"- {k}: {v}")
            lines.append("")

        # Expected impact
        if action.get("expected_impact"):
            lines.append(f"**Expected Impact:** {action['expected_impact']}")
            lines.append("")

        # Contract violations this fixes
        fixes = action.get("fixes_violations", [])
        if fixes:
            lines.append(f"**Fixes Contract Violations:** {', '.join(fixes)}")
            lines.append("")

        lines.append("---")
        lines.append("")

    # Quick wins: low effort + high score
    quick_wins = [
        a for a in actions if a.get("effort") == "low" and a.get("priority_score", 0) > 0.5
    ]
    if quick_wins:
        lines.append("## Quick Wins (High Score, Low Effort)")
        for a in quick_wins[:3]:
            lines.append(
                f"- **{a['action']}** (score={a.get('priority_score', 0):.3f}): "
                f"{a.get('description', '')[:200]}"
            )
        lines.append("")

    return "\n".join(lines)


def format_quality_report(sections: dict[str, str]) -> str:
    """Combine quality sections into a unified report.

    Args:
        sections: Dict mapping section name ('entropy', 'contract', 'actions')
            to their pre-formatted markdown content.
    """
    lines = ["# Data Quality Report", ""]

    for name in ("entropy", "contract", "actions"):
        content = sections.get(name)
        if content:
            lines.append(content)
            lines.append("")

    if len(sections) == 0:
        lines.append("No quality data available. Run the pipeline first.")

    return "\n".join(lines)


def format_export_result(output_path: str, fmt: str, row_count: int, sidecar_path: str) -> str:
    """Format export result for LLM consumption."""
    lines = ["# Export Complete"]
    lines.append("")
    lines.append(f"**File:** `{output_path}`")
    lines.append(f"**Format:** {fmt}")
    lines.append(f"**Rows:** {row_count}")
    lines.append(f"**Metadata:** `{sidecar_path}`")
    lines.append("")
    lines.append(
        "The metadata sidecar contains provenance information: "
        "source SQL, entropy score, assumptions, and confidence level."
    )
    return "\n".join(lines)


def format_zone_status(
    zone_name: str,
    gate_label: str,
    gate_phase: str,
    violations: list[dict[str, Any]],
    passing: list[dict[str, Any]],
    skipped_detectors: list[dict[str, str]],
    contract_name: str | None = None,
) -> str:
    """Format zone status for LLM consumption.

    Args:
        zone_name: Human-readable zone name (e.g. "foundation")
        gate_label: Gate label (e.g. "Gate 1")
        gate_phase: Phase name (e.g. "quality_review")
        violations: List of violation dicts with dimension_path, detector_id, score, threshold, affected_targets, executable_actions, triage_guidance, interpretation, accepted_targets
        passing: List of passing dimension dicts with dimension_path, detector_id, score, threshold
        skipped_detectors: List of dicts with detector_id and reason
        contract_name: Contract used for evaluation
    """
    lines = [f"# Zone Status: {zone_name} ({gate_label}: {gate_phase})"]
    if contract_name:
        lines.append(f"Contract: {contract_name}")
    lines.append("")

    n_violations = len(violations)
    n_passing = len(passing)
    lines.append(f"## Status: {n_violations} violations, {n_passing} dimensions passing")
    lines.append("")

    # Violations
    if violations:
        lines.append("## Violations")
        for v in violations:
            dim = v["dimension_path"]
            det = v["detector_id"]
            lines.append("")
            lines.append(f"### {det} — `{dim}`")
            lines.append(f"Score: **{v['score']:.3f}** (threshold: {v['threshold']:.2f})")

            # Affected targets
            targets = v.get("affected_targets", [])
            accepted = v.get("accepted_targets", [])
            if targets:
                lines.append(f"Affected: {', '.join(targets[:15])}")
            if accepted:
                lines.append(f"Already accepted: {', '.join(accepted)}")

            # Triage guidance
            triage = v.get("triage_guidance", "")
            if triage:
                lines.append("")
                lines.append(f"**Triage:** {triage.strip()}")

            # Interpretation context (LLM + Bayesian network analysis)
            interp = v.get("interpretation")
            if interp:
                lines.append("")
                lines.append(f"**Interpretation:** {interp['explanation']}")
                res_actions = interp.get("resolution_actions", [])
                if res_actions:
                    for ra in res_actions[:3]:
                        lines.append(
                            f"- {ra.get('action', '?')}: {ra.get('description', '')} "
                            f"(effort: {ra.get('effort', '?')})"
                        )

            # Executable actions
            actions = v.get("executable_actions", [])
            if actions:
                lines.append("")
                lines.append("**Available fix actions:**")
                for a in actions:
                    lines.append(f"- **{a['action']}** ({a.get('routing', '?')})")
                    if a.get("guidance"):
                        # First line of guidance only
                        first_line = a["guidance"].strip().split("\n")[0]
                        lines.append(f"  {first_line}")
                    fields = a.get("fields", {})
                    if fields:
                        for fname, fdef in fields.items():
                            req = "required" if fdef.get("required") else "optional"
                            ftype = fdef.get("type", "string")
                            desc = fdef.get("description", "")
                            line = f"  - `{fname}` ({ftype}, {req}): {desc}"
                            if fdef.get("enum_values"):
                                line += f" [{', '.join(fdef['enum_values'])}]"
                            lines.append(line)
        lines.append("")

    # Passing dimensions
    if passing:
        lines.append("## Passing Dimensions")
        lines.append("")
        lines.append("| Detector | Score | Threshold |")
        lines.append("|----------|-------|-----------|")
        for p in passing:
            lines.append(f"| {p['detector_id']} | {p['score']:.3f} | {p['threshold']:.2f} |")
        lines.append("")

    # Skipped detectors
    if skipped_detectors:
        lines.append("## Skipped (need later zone analyses)")
        for s in skipped_detectors:
            lines.append(f"- {s['detector_id']}: {s['reason']}")
        lines.append("")

    # Next steps guidance
    lines.append("## Next Steps")
    if violations:
        lines.append(
            "- Review triage guidance above and pick an action for each violation"
        )
        lines.append(
            '- Call `apply_fix(fixes=[{action: "...", target: "column:table.col", '
            'parameters: {...}, reason: "..."}])` to apply fixes'
        )
        lines.append("- Call `continue_pipeline` to advance to the next zone after fixing")
    elif skipped_detectors:
        lines.append("- All measured dimensions passing — use `continue_pipeline` to advance")
    else:
        lines.append("- All dimensions passing — pipeline is clean")

    return "\n".join(lines)


def format_pipeline_result(result: RunResult) -> str:
    """Format pipeline run result for LLM consumption.

    Args:
        result: RunResult from pipeline execution

    Returns:
        Formatted markdown summary
    """
    lines = ["# Pipeline Analysis Complete"]
    lines.append("")

    # Source info
    if result.output_dir:
        lines.append(f"**Output:** `{result.output_dir}`")

    # Duration
    minutes = int(result.duration_seconds // 60)
    seconds = result.duration_seconds % 60
    if minutes > 0:
        lines.append(f"**Duration:** {minutes}m {seconds:.1f}s")
    else:
        lines.append(f"**Duration:** {seconds:.1f}s")
    lines.append("")

    # Phase summary
    lines.append("## Phases")
    lines.append(
        f"- Completed: {result.phases_completed}"
        f" | Failed: {result.phases_failed}"
        f" | Skipped: {result.phases_skipped}"
    )
    lines.append("")

    # Failed phases detail
    failed = result.get_failed_phases()
    if failed:
        lines.append("## Failures")
        for phase in failed:
            lines.append(f"- **{phase.phase_name}**: {phase.error}")
        lines.append("")

    # Next steps
    lines.append("## Next Steps")
    if result.success:
        lines.append("- Use `get_context` for full schema and relationships")
        lines.append("- Use `get_quality` for entropy, contracts, and resolution actions")
        lines.append("- Use `query` to ask questions about the data")
    else:
        lines.append("- Check the failures above and fix the data issues")
        lines.append("- Re-run `analyze` after fixing")

    return "\n".join(lines)
