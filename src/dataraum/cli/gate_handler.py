"""CLI gate handler — functions for resolving EXIT_CHECK events.

Renders post-verification results during the pipeline run and resolves
EXIT_CHECK events based on gate mode. In PAUSE mode, provides an
interactive fix UI with DocumentAgent config mode Q&A.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from dataraum.entropy.gate import match_threshold
from dataraum.pipeline.events import PipelineEvent
from dataraum.pipeline.fixes import FixInput
from dataraum.pipeline.runner import GateMode
from dataraum.pipeline.scheduler import Resolution, ResolutionAction

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def handle_exit_check(
    console: Console,
    event: PipelineEvent,
    gate_mode: GateMode,
    contract_thresholds: dict[str, float] | None = None,
    session: Session | None = None,
    source_id: str | None = None,
) -> Resolution:
    """Resolve an EXIT_CHECK event based on gate mode.

    Args:
        console: Rich console for output.
        event: The EXIT_CHECK event with violations.
        gate_mode: How to handle the check.
        contract_thresholds: Dimension thresholds from the contract.
        session: DB session (required for PAUSE mode).
        source_id: Source ID (required for PAUSE mode).

    Returns:
        Resolution telling the scheduler what to do.
    """
    match gate_mode:
        case GateMode.SKIP:
            # Skip mode: suppress inline panel, violations appear in final summary
            return Resolution(action=ResolutionAction.DEFER)

        case GateMode.FAIL:
            # Fail mode: show violations so user understands why pipeline aborts
            render_violations(
                console,
                event.violations,
                event.column_details,
                all_scores=event.scores or None,
                contract_thresholds=contract_thresholds,
                phase_name=event.phase,
            )
            console.print("  [red]Aborting — gate mode is fail[/red]")
            return Resolution(action=ResolutionAction.ABORT)

        case GateMode.PAUSE:
            return _handle_pause(
                console, event, contract_thresholds, session, source_id,
            )

        case _:
            return Resolution(action=ResolutionAction.DEFER)


def render_gate_scores(
    console: Console,
    scores: dict[str, float],
    contract_thresholds: dict[str, float] | None = None,
    phase_name: str | None = None,
) -> None:
    """Render all gate measurement scores as a Rich panel.

    Shows passing and failing dimensions with contract status.

    Args:
        console: Rich console for output.
        scores: All measured entropy scores at this gate.
        contract_thresholds: Contract thresholds keyed by dimension path.
        phase_name: Name of the gate phase.
    """
    if not scores:
        return

    thresholds = contract_thresholds or {}

    table = Table(show_header=True, box=None, padding=(0, 2))
    table.add_column("Dimension", style="bold")
    table.add_column("Score", justify="right")
    table.add_column("Threshold", justify="right", style="dim")
    table.add_column("Status")
    table.add_column("Bar")

    violations = 0
    passing = 0

    for dim_path, score in sorted(scores.items()):
        threshold = match_threshold(dim_path, thresholds)
        filled = round(score * 10)
        bar = "\u2593" * filled + "\u2591" * (10 - filled)

        if threshold is not None:
            if score > threshold:
                violations += 1
                gap = score - threshold
                color = "red"
                status = f"[red]+{gap:.2f}[/red]"
            else:
                passing += 1
                headroom = threshold - score
                color = "yellow" if headroom < 0.1 else "green"
                status = f"[{color}]\u2212{headroom:.2f}[/{color}]"
            thresh_str = f"{threshold:.2f}"
        else:
            color = "dim"
            status = "[dim]no contract[/dim]"
            thresh_str = ""

        table.add_row(
            f"[{color}]{dim_path}[/{color}]",
            f"[{color}]{score:.2f}[/{color}]",
            thresh_str,
            status,
            bar,
        )

    title = "Gate Measurement"
    if phase_name:
        title += f": {phase_name}"
    summary = f"{violations} violation{'s' if violations != 1 else ''}, {passing} passing"
    border = "red" if violations > 0 else "green"

    console.print()
    console.print(
        Panel(
            table,
            title=title,
            subtitle=summary,
            title_align="left",
            subtitle_align="right",
            border_style=border,
            padding=(1, 2),
        )
    )


def render_violations(
    console: Console,
    violations: dict[str, tuple[float, float]],
    column_details: dict[str, dict[str, float]] | None = None,
    all_scores: dict[str, float] | None = None,
    contract_thresholds: dict[str, float] | None = None,
    phase_name: str | None = None,
) -> None:
    """Render post-verification violations as a Rich panel.

    Args:
        console: Rich console for output.
        violations: dimension_path -> (score, threshold).
        column_details: dimension_path -> {target -> score}. Optional.
        all_scores: All measured entropy scores (for distance-to-green display).
        contract_thresholds: Contract thresholds keyed by dimension path.
        phase_name: Name of the phase that triggered the check.
    """
    if not violations:
        return

    table = Table(show_header=True, box=None, padding=(0, 2))
    table.add_column("Dimension", style="bold")
    table.add_column("Score", justify="right")
    table.add_column("Threshold", justify="right", style="dim")
    table.add_column("Gap", justify="right")
    table.add_column("Bar")

    # Sort violations by gap size (score - threshold), descending
    sorted_violations = sorted(
        violations.items(),
        key=lambda item: item[1][0] - item[1][1],
        reverse=True,
    )

    for dim_path, (score, threshold) in sorted_violations:
        filled = round(score * 10)
        bar = "\u2593" * filled + "\u2591" * (10 - filled)
        score_style = "red" if score > threshold else "green"
        gap = score - threshold
        table.add_row(
            dim_path,
            f"[{score_style}]{score:.2f}[/{score_style}]",
            f"{threshold:.2f}",
            f"[red]+{gap:.2f}[/red]",
            bar,
        )

        # Show top-3 worst columns if column_details available
        if column_details:
            col_scores = column_details.get(dim_path, {})
            worst = sorted(col_scores.items(), key=lambda x: x[1], reverse=True)[:3]
            for target, col_score in worst:
                table.add_row(
                    f"  [dim]{target}[/dim]",
                    f"[dim]{col_score:.2f}[/dim]",
                    "",
                    "",
                    "",
                )

    # Show passing dimensions with headroom (distance to green)
    if all_scores and contract_thresholds:
        passing_rows: list[tuple[str, float, float, float]] = []
        for dim_path, score in sorted(all_scores.items()):
            if dim_path in violations:
                continue
            matched = match_threshold(dim_path, contract_thresholds)
            if matched is not None:
                threshold = matched
                headroom = threshold - score
                passing_rows.append((dim_path, score, threshold, headroom))

        if passing_rows:
            # Sort by headroom ascending (closest to flipping first)
            passing_rows.sort(key=lambda r: r[3])
            table.add_row("", "", "", "", "")  # spacer
            for dim_path, score, threshold, headroom in passing_rows:
                filled = round(score * 10)
                bar = "\u2593" * filled + "\u2591" * (10 - filled)
                color = "yellow" if headroom < 0.1 else "green"
                table.add_row(
                    f"[{color}]{dim_path}[/{color}]",
                    f"[{color}]{score:.2f}[/{color}]",
                    f"{threshold:.2f}",
                    f"[{color}]\u2212{headroom:.2f}[/{color}]",
                    bar,
                )

    title = "Post-Verification"
    if phase_name:
        title += f": {phase_name}"
    console.print()
    console.print(
        Panel(
            table,
            title=title,
            title_align="left",
            border_style="yellow",
            padding=(1, 2),
        )
    )


# ---------------------------------------------------------------------------
# PAUSE mode
# ---------------------------------------------------------------------------


def _handle_pause(
    console: Console,
    event: PipelineEvent,
    contract_thresholds: dict[str, float] | None,
    session: Session | None,
    source_id: str | None,
) -> Resolution:
    """Handle EXIT_CHECK in PAUSE mode — interactive fix UI.

    Shows violations, presents available fix actions, and runs an
    agent-driven Q&A flow to produce a fix resolution.
    """
    # Show violations panel
    render_violations(
        console,
        event.violations,
        event.column_details,
        all_scores=event.scores or None,
        contract_thresholds=contract_thresholds,
        phase_name=event.phase,
    )

    # Collect all available fixes across violating dimensions
    actions = _collect_fix_actions(event)

    if not actions:
        console.print("  [dim]No fix actions available. Deferring.[/dim]")
        return Resolution(action=ResolutionAction.DEFER)

    # Display action menu
    console.print()
    console.print("[bold]Available fixes:[/bold]")
    for i, action in enumerate(actions, 1):
        console.print(
            f"  \\[{i}] {action['action_name']} "
            f"[dim]({action['phase_name']} phase)[/dim] "
            f"— {action['dimension']}"
        )
    console.print()
    console.print("  \\[d] Defer — continue pipeline, address later")
    console.print("  \\[a] Abort — stop pipeline")
    console.print()

    for attempt in range(3):
        choice = console.input("[bold]Select: [/bold]").strip().lower()

        if choice == "d":
            return Resolution(action=ResolutionAction.DEFER)
        if choice == "a":
            return Resolution(action=ResolutionAction.ABORT)

        try:
            idx = int(choice) - 1
            if 0 <= idx < len(actions):
                break
            remaining = 2 - attempt
            console.print(f"[yellow]Invalid choice ({remaining} attempts left).[/yellow]")
        except ValueError:
            remaining = 2 - attempt
            console.print(f"[yellow]Invalid choice ({remaining} attempts left).[/yellow]")
    else:
        console.print("[yellow]No valid input, deferring.[/yellow]")
        return Resolution(action=ResolutionAction.DEFER)

    selected = actions[idx]

    if session is None or source_id is None:
        console.print("[red]Session not available for PAUSE mode. Deferring.[/red]")
        return Resolution(action=ResolutionAction.DEFER)

    return _run_fix_flow(console, session, source_id, selected, event)


def _collect_fix_actions(
    event: PipelineEvent,
) -> list[dict[str, str]]:
    """Collect and deduplicate fix actions from the EXIT_CHECK event."""
    actions: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for dim_path, dim_actions in event.available_fixes.items():
        for action_info in dim_actions:
            name = action_info["action_name"]
            key = (name, dim_path)
            if key not in seen:
                seen.add(key)
                entry: dict[str, str] = {
                    "action_name": name,
                    "phase_name": action_info["phase_name"],
                    "dimension": dim_path,
                }
                if "guidance" in action_info:
                    entry["guidance"] = action_info["guidance"]
                if "fields" in action_info:
                    entry["fields"] = action_info["fields"]
                actions.append(entry)

    return actions


def _run_fix_flow(
    console: Console,
    session: Session,
    source_id: str,
    action_info: dict[str, str],
    event: PipelineEvent,
) -> Resolution:
    """Run the agent-driven Q&A flow for a selected fix action.

    Returns a FIX resolution on success, DEFER on cancellation or error.
    """
    from dataraum.cli.commands.fix import _create_document_agent

    context = build_gate_context(session, source_id, action_info, event)

    try:
        agent = _create_document_agent()
    except Exception as e:
        console.print(f"[red]Failed to initialize LLM: {e}[/red]")
        return Resolution(action=ResolutionAction.DEFER)

    # Generate questions
    console.print("\n[dim]Generating questions...[/dim]")
    q_result = agent.generate_config_questions(context)
    if not q_result.success:
        console.print(f"[red]Error: {q_result.error}[/red]")
        return Resolution(action=ResolutionAction.DEFER)

    questions = q_result.unwrap()
    if questions.context_summary:
        console.print(f"\n[dim]{questions.context_summary}[/dim]\n")

    # Collect answers
    answers_parts: list[str] = []
    for q in questions.questions:
        console.print(f"[bold]{q.question}[/bold]")
        if q.question_type == "multiple_choice" and q.choices:
            for j, choice_text in enumerate(q.choices, 1):
                console.print(f"  {j}. {choice_text}")
        answer = console.input("[green]> [/green]").strip()

        # Resolve multiple choice number to text
        if q.question_type == "multiple_choice" and q.choices:
            try:
                ci = int(answer) - 1
                if 0 <= ci < len(q.choices):
                    answer = q.choices[ci]
            except ValueError:
                pass

        answers_parts.append(f"Q: {q.question}\nA: {answer}")
        console.print()

    user_answers = "\n\n".join(answers_parts)

    # Interpret answers — enforce parameter structure via tool schema
    console.print("[dim]Interpreting answers...[/dim]")
    from dataraum.entropy.detectors.base import get_default_registry

    dim_path = action_info["dimension"]
    registry = get_default_registry()
    schema = registry.get_fix_schema(action_info["action_name"], dim_path)
    param_schema = schema.parameter_json_schema() if schema else None
    interp_result = agent.interpret_config_answers(
        context, user_answers, parameter_schema=param_schema
    )
    if not interp_result.success:
        console.print(f"[red]Error: {interp_result.error}[/red]")
        return Resolution(action=ResolutionAction.DEFER)

    interp = interp_result.unwrap()

    # Validate parameters against schema (belt-and-suspenders)
    action_name = interp.config_action or action_info["action_name"]
    if action_name != action_info["action_name"]:
        schema = registry.get_fix_schema(action_name, dim_path)
    if schema:
        errors = schema.validate_payload(interp.parameters)
        if errors:
            console.print(f"[red]LLM returned invalid parameters: {'; '.join(errors)}[/red]")
            console.print("[yellow]Deferring — try again or fix manually.[/yellow]")
            return Resolution(action=ResolutionAction.DEFER)

    console.print(f"\n[bold]Interpretation:[/bold] {interp.summary}")
    console.print(f"[dim]{interp.interpretation}[/dim]")
    console.print(f"[dim]Confidence: {interp.confidence}[/dim]\n")

    confirm = console.input("[bold]Apply fix? (y/n): [/bold]").strip().lower()
    if confirm != "y":
        console.print(f"[yellow]Fix cancelled, deferring.[/yellow] [dim](input was {confirm!r})[/dim]")
        return Resolution(action=ResolutionAction.DEFER)

    # Thread entropy evidence into FixInput for audit trail
    dim_path = action_info["dimension"]
    evidence: dict[str, Any] = {}
    score, threshold = event.violations.get(dim_path, (0.0, 0.0))
    evidence["score"] = score
    evidence["threshold"] = threshold
    col_scores = event.column_details.get(dim_path, {})
    if col_scores:
        evidence["column_scores"] = col_scores

    fix_input = FixInput(
        action_name=interp.config_action or action_info["action_name"],
        dimension=dim_path,
        parameters=dict(interp.parameters),
        interpretation=interp.interpretation,
        affected_columns=interp.affected_columns,
        entropy_evidence=evidence,
    )

    return Resolution(action=ResolutionAction.FIX, fix_inputs=[fix_input])


# ---------------------------------------------------------------------------
# Gate context builder (lightweight — works at Gate 1)
# ---------------------------------------------------------------------------


def build_gate_context(
    session: Session,
    source_id: str,
    action_info: dict[str, str],
    event: PipelineEvent,
) -> str:
    """Build context for DocumentAgent config mode at a quality gate.

    Lighter than _build_agent_context() in fix.py — works at Gate 1
    where EntropyInterpretationRecord doesn't yet exist. Uses detector
    evidence from the event, data profiles, and semantic annotations.

    Args:
        session: Database session.
        source_id: Source ID for querying metadata.
        action_info: Selected action with action_name, phase_name, dimension.
        event: The EXIT_CHECK event with scores and column details.

    Returns:
        Structured context string for the LLM prompt.
    """
    dim_path = action_info["dimension"]
    score, threshold = event.violations.get(dim_path, (0.0, 0.0))
    affected_targets = _get_affected_targets(dim_path, event)

    sections: list[str] = []

    # Section 1: Action details
    action_lines = [
        "<action_details>",
        f"Action: {action_info['action_name']}",
        f"Description: Fix for {dim_path} contract violation",
        f"Priority: high (score: {score:.2f})",
        f"Affected columns: {', '.join(affected_targets)}",
    ]
    if "guidance" in action_info:
        action_lines.append(f"Guidance: {action_info['guidance']}")
    if "fields" in action_info:
        action_lines.append("Expected parameters:")
        for line in action_info["fields"].splitlines():
            action_lines.append(f"  {line}")
    action_lines.append("</action_details>")
    sections.append("\n".join(action_lines))

    # Section 2: Entropy evidence
    evidence_lines = [
        "<entropy_evidence>",
        f"Detector: {dim_path.split('.')[-1]}",
        f"Score: {score:.2f}",
        f"Threshold: {threshold:.2f}",
    ]
    col_scores = event.column_details.get(dim_path, {})
    if col_scores:
        worst = sorted(col_scores.items(), key=lambda x: x[1], reverse=True)[:5]
        evidence_lines.append("Worst targets:")
        for target, col_score in worst:
            evidence_lines.append(f"  {target}: {col_score:.2f}")
    evidence_lines.append("</entropy_evidence>")
    sections.append("\n".join(evidence_lines))

    # Section 3: Data profile from DB
    data_section = _build_data_profile(session, source_id, affected_targets)
    if data_section:
        sections.append(data_section)

    return "\n\n".join(sections)


def _get_affected_targets(
    dim_path: str, event: PipelineEvent,
) -> list[str]:
    """Extract column/table targets that actually violate the contract.

    Only returns targets whose individual score exceeds the contract
    threshold. Columns that scored but are below threshold (e.g.
    aligned temporal columns at 0.1) are healthy and not actionable.
    """
    col_scores = event.column_details.get(dim_path, {})
    if not col_scores:
        return []

    _, threshold = event.violations.get(dim_path, (0.0, 0.0))

    return [
        t
        for t, s in sorted(col_scores.items(), key=lambda x: x[1], reverse=True)
        if s > threshold
    ]


def _build_data_profile(
    session: Session, source_id: str, targets: list[str],
) -> str:
    """Build data profile section from DB for affected targets.

    Targets are strings like "column:table.col" or "table:table".
    """
    from sqlalchemy import select

    from dataraum.analysis.semantic.db_models import SemanticAnnotation
    from dataraum.analysis.statistics.db_models import StatisticalProfile
    from dataraum.storage import Column, Table

    lines = ["<data_profile>"]
    has_data = False

    for target in targets[:10]:  # Cap to avoid huge context
        # Parse target format: "column:table.col" or "table:table"
        if ":" in target:
            scope, ref = target.split(":", 1)
        else:
            scope, ref = "column", target

        parts = ref.split(".", 1)
        table_name = parts[0]
        column_name = parts[1] if len(parts) > 1 else None

        if scope == "column" and column_name:
            stmt = (
                select(Column, Table, StatisticalProfile)
                .join(Table, Column.table_id == Table.table_id)
                .outerjoin(StatisticalProfile, Column.column_id == StatisticalProfile.column_id)
                .where(Table.source_id == source_id, Table.layer == "typed", Table.table_name == table_name, Column.column_name == column_name)
                .limit(1)
            )
            row = session.execute(stmt).first()
            if row:
                col, tbl, profile = row
                has_data = True
                lines.append(f"\n  Column: {table_name}.{column_name}")
                lines.append(f"    Table rows: {tbl.row_count or 'unknown'}")
                if col.resolved_type:
                    lines.append(f"    Current type: {col.resolved_type}")
                if profile:
                    lines.append(f"    Distinct values: {profile.distinct_count or 'unknown'}")
                    lines.append(f"    Null ratio: {profile.null_ratio or 0:.1%}")
                    pdata: dict[str, Any] = profile.profile_data or {}
                    top_vals = pdata.get("top_values", [])
                    if top_vals:
                        samples = [str(v.get("value", "")) for v in top_vals[:5]]
                        lines.append(f"    Sample values: {', '.join(samples)}")
                    numeric = pdata.get("numeric_stats")
                    if numeric:
                        lines.append(
                            f"    Range: {numeric.get('min', '?')} to {numeric.get('max', '?')}"
                            f", mean: {numeric.get('mean', '?')}"
                        )

                # Semantic annotation
                sem_stmt = select(SemanticAnnotation).where(
                    SemanticAnnotation.column_id == col.column_id
                )
                sem = session.execute(sem_stmt).scalars().first()
                if sem:
                    lines.append(f"    Semantic role: {sem.semantic_role or 'unknown'}")
                    if sem.business_name:
                        lines.append(f"    Business name: {sem.business_name}")
                    if sem.business_concept:
                        lines.append(f"    Business concept: {sem.business_concept}")

    lines.append("</data_profile>")
    return "\n".join(lines) if has_data else ""
