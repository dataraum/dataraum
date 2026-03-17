"""CLI gate handler — functions for resolving EXIT_CHECK events.

Renders post-verification results during the pipeline run and resolves
EXIT_CHECK events based on gate mode. In PAUSE mode, provides an
interactive fix UI with batch action plans via DocumentAgent.
"""

from __future__ import annotations

from dataclasses import dataclass
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
                column_evidence=event.column_evidence,
            )
            console.print("  [red]Aborting — gate mode is fail[/red]")
            return Resolution(action=ResolutionAction.ABORT)

        case GateMode.PAUSE:
            return _handle_pause(
                console,
                event,
                contract_thresholds,
                session,
                source_id,
            )

        case _:
            return Resolution(action=ResolutionAction.DEFER)


def render_gate_scores(
    console: Console,
    scores: dict[str, float],
    contract_thresholds: dict[str, float] | None = None,
    phase_name: str | None = None,
    column_details: dict[str, dict[str, float]] | None = None,
    column_evidence: dict[str, dict[str, dict[str, Any]]] | None = None,
) -> None:
    """Render all gate measurement scores as a Rich panel.

    Shows passing, failing, and accepted dimensions with contract status.
    Accepted dimensions are above threshold but all violating targets have
    been acknowledged — the gate doesn't block on them.

    Args:
        console: Rich console for output.
        scores: All measured entropy scores at this gate.
        contract_thresholds: Contract thresholds keyed by dimension path.
        phase_name: Name of the gate phase.
        column_details: Per-column scores for accepted detection.
        column_evidence: Per-column evidence with accepted flags.
    """
    if not scores:
        return

    thresholds = contract_thresholds or {}
    col_details = column_details or {}
    col_evidence = column_evidence or {}

    table = Table(show_header=True, box=None, padding=(0, 2))
    table.add_column("Dimension", style="bold")
    table.add_column("Score", justify="right")
    table.add_column("Threshold", justify="right", style="dim")
    table.add_column("Status")
    table.add_column("Bar")

    violations = 0
    passing = 0
    accepted_count = 0

    for dim_path, score in sorted(scores.items()):
        threshold = match_threshold(dim_path, thresholds)
        filled = round(score * 10)
        bar = "\u2593" * filled + "\u2591" * (10 - filled)

        if threshold is not None:
            if score > threshold:
                # Check if all above-threshold targets are accepted
                if _is_dimension_accepted(dim_path, threshold, col_details, col_evidence):
                    accepted_count += 1
                    color = "cyan"
                    status = "[cyan]ACCEPTED[/cyan]"
                else:
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
    parts = [f"{violations} violation{'s' if violations != 1 else ''}"]
    parts.append(f"{passing} passing")
    if accepted_count:
        parts.append(f"{accepted_count} accepted")
    summary = ", ".join(parts)
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


def _is_dimension_accepted(
    dim_path: str,
    threshold: float,
    column_details: dict[str, dict[str, float]],
    column_evidence: dict[str, dict[str, dict[str, Any]]],
) -> bool:
    """Check if a dimension's above-threshold targets are all accepted.

    Returns True if there are above-threshold targets and every one of
    them has ``evidence.accepted=True``. This mirrors the contract
    overrule logic in ``assess_contracts``.
    """
    col_scores = column_details.get(dim_path, {})
    if not col_scores:
        return False

    dim_ev = column_evidence.get(dim_path, {})
    above_threshold = [t for t, s in col_scores.items() if s > threshold]
    if not above_threshold:
        return False

    return all(dim_ev.get(t, {}).get("accepted", False) for t in above_threshold)


def render_violations(
    console: Console,
    violations: dict[str, tuple[float, float]],
    column_details: dict[str, dict[str, float]] | None = None,
    all_scores: dict[str, float] | None = None,
    contract_thresholds: dict[str, float] | None = None,
    phase_name: str | None = None,
    column_evidence: dict[str, dict[str, dict[str, Any]]] | None = None,
) -> None:
    """Render post-verification violations as a Rich panel.

    Args:
        console: Rich console for output.
        violations: dimension_path -> (score, threshold).
        column_details: dimension_path -> {target -> score}. Optional.
        all_scores: All measured entropy scores (for distance-to-green display).
        contract_thresholds: Contract thresholds keyed by dimension path.
        phase_name: Name of the phase that triggered the check.
        column_evidence: Per-column evidence for accepted detection.
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

    # Show passing and accepted dimensions
    col_det = column_details or {}
    col_ev = column_evidence or {}

    if all_scores and contract_thresholds:
        passing_rows: list[tuple[str, float, float, float, bool]] = []
        for dim_path, score in sorted(all_scores.items()):
            if dim_path in violations:
                continue
            matched = match_threshold(dim_path, contract_thresholds)
            if matched is not None:
                threshold = matched
                headroom = threshold - score
                accepted = _is_dimension_accepted(dim_path, threshold, col_det, col_ev)
                passing_rows.append((dim_path, score, threshold, headroom, accepted))

        if passing_rows:
            # Sort: accepted first (by score desc), then passing by headroom asc
            passing_rows.sort(key=lambda r: (not r[4], r[3]))
            table.add_row("", "", "", "", "")  # spacer
            for dim_path, score, threshold, headroom, accepted in passing_rows:
                filled = round(score * 10)
                bar = "\u2593" * filled + "\u2591" * (10 - filled)
                if accepted:
                    table.add_row(
                        f"[cyan]{dim_path}[/cyan]",
                        f"[cyan]{score:.2f}[/cyan]",
                        f"{threshold:.2f}",
                        "[cyan]ACCEPTED[/cyan]",
                        bar,
                    )
                else:
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
        column_evidence=event.column_evidence,
    )

    # Collect available fixes grouped by dimension
    groups = _collect_fix_groups(event)

    if not groups:
        console.print("  [dim]No fix actions available. Deferring.[/dim]")
        return Resolution(action=ResolutionAction.DEFER)

    # Display dimension menu
    console.print()
    console.print("[bold]Available fixes:[/bold]")
    for i, group in enumerate(groups, 1):
        action_names = ", ".join(a["action_name"] for a in group.actions)
        console.print(
            f"  \\[{i}] [bold]{group.label}[/bold] "
            f"[dim]({group.dimension}, score {group.score:.2f})[/dim]"
        )
        console.print(
            f"      {len(group.actions)} action{'s' if len(group.actions) != 1 else ''}: {action_names}"
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
            if 0 <= idx < len(groups):
                break
            remaining = 2 - attempt
            console.print(f"[yellow]Invalid choice ({remaining} attempts left).[/yellow]")
        except ValueError:
            remaining = 2 - attempt
            console.print(f"[yellow]Invalid choice ({remaining} attempts left).[/yellow]")
    else:
        console.print("[yellow]No valid input, deferring.[/yellow]")
        return Resolution(action=ResolutionAction.DEFER)

    selected_group = groups[idx]

    if session is None or source_id is None:
        console.print("[red]Session not available for PAUSE mode. Deferring.[/red]")
        return Resolution(action=ResolutionAction.DEFER)

    return _run_fix_flow(console, session, source_id, selected_group, event)


@dataclass
class _DimensionFixGroup:
    """All available fix actions for a single violating dimension."""

    dimension: str
    score: float
    threshold: float
    actions: list[dict[str, str]]

    @property
    def label(self) -> str:
        """Human-readable label: last segment of dimension path."""
        return self.dimension.rsplit(".", 1)[-1]


def _collect_fix_groups(
    event: PipelineEvent,
) -> list[_DimensionFixGroup]:
    """Group available fix actions by dimension.

    Instead of flattening all actions into a single list, groups them so the
    user picks a dimension to fix and the LLM sees all available actions for
    that dimension and triages to the best one.
    """
    groups: list[_DimensionFixGroup] = []

    for dim_path, dim_actions in event.available_fixes.items():
        score, threshold = event.violations.get(dim_path, (0.0, 0.0))
        actions: list[dict[str, str]] = []
        seen: set[str] = set()

        for action_info in dim_actions:
            name = action_info["action_name"]
            if name in seen:
                continue
            seen.add(name)
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

        if actions:
            groups.append(
                _DimensionFixGroup(
                    dimension=dim_path,
                    score=score,
                    threshold=threshold,
                    actions=actions,
                )
            )

    # Sort by gap descending (worst violation first)
    groups.sort(key=lambda g: g.score - g.threshold, reverse=True)
    return groups


def _run_fix_flow(
    console: Console,
    session: Session,
    source_id: str,
    group: _DimensionFixGroup,
    event: PipelineEvent,
) -> Resolution:
    """Run the agent-driven fix flow for a dimension's fix actions.

    Uses a batch action plan: the LLM proposes one action per violating
    target, the user reviews and confirms, producing FixInputs in one round.
    Works for both single-target and multi-target dimensions.

    Returns a FIX resolution on success, DEFER on cancellation or error.
    """
    from dataraum.cli.commands.fix import _create_document_agent

    context = build_gate_context(session, source_id, group, event)

    try:
        agent = _create_document_agent()
    except Exception as e:
        console.print(f"[red]Failed to initialize LLM: {e}[/red]")
        return Resolution(action=ResolutionAction.DEFER)
    console.print("\n[dim]Generating action plan...[/dim]")
    plan_result = agent.generate_batch_plan(context)
    if not plan_result.success:
        console.print(f"[red]Error: {plan_result.error}[/red]")
        return Resolution(action=ResolutionAction.DEFER)

    plan = plan_result.unwrap()
    if not plan.items:
        console.print("[yellow]No actions proposed. Deferring.[/yellow]")
        return Resolution(action=ResolutionAction.DEFER)

    # Display plan as Rich table
    console.print(f"\n[bold]{plan.summary}[/bold]\n")
    plan_table = Table(show_header=True, box=None, padding=(0, 2))
    plan_table.add_column("Target", style="bold")
    plan_table.add_column("Action")
    plan_table.add_column("Reason", style="dim")

    for item in plan.items:
        # Show score inline if available
        col_scores = event.column_details.get(group.dimension, {})
        score = col_scores.get(item.target)
        target_label = item.target
        if score is not None:
            target_label += f" ({score:.2f})"
        plan_table.add_row(target_label, item.recommended_action, item.reason)

    console.print(plan_table)
    console.print()

    # Confirm plan
    confirm = console.input("[bold]Apply plan? (y/n): [/bold]").strip().lower()
    if confirm != "y":
        console.print(
            f"[yellow]Plan cancelled, deferring.[/yellow] [dim](input was {confirm!r})[/dim]"
        )
        return Resolution(action=ResolutionAction.DEFER)

    # Collect follow-up answers (shared parameters like "reason")
    follow_up_answers: dict[str, str] = {}
    for q in plan.follow_up_questions:
        console.print(f"\n[bold]{q.question}[/bold]")
        if q.question_type == "multiple_choice" and q.choices:
            for j, choice_text in enumerate(q.choices, 1):
                console.print(f"  {j}. {choice_text}")
        answer = console.input("[green]> [/green]").strip()
        if q.question_type == "multiple_choice" and q.choices:
            try:
                ci = int(answer) - 1
                if 0 <= ci < len(q.choices):
                    answer = q.choices[ci]
            except ValueError:
                pass
        # Store answer keyed by question text (used to thread into parameters)
        follow_up_answers[q.question] = answer

    # Build FixInputs from plan items
    dim_path = group.dimension
    evidence: dict[str, Any] = {
        "score": group.score,
        "threshold": group.threshold,
    }
    col_scores = event.column_details.get(dim_path, {})
    if col_scores:
        evidence["column_scores"] = col_scores

    # Pre-fill parameters from detector evidence (deterministic, no LLM needed)
    target_evidence = event.column_evidence.get(dim_path, {})

    fix_inputs: list[FixInput] = []
    for item in plan.items:
        params = dict(item.parameters)

        # Enrich from component evidence — detector already knows columns,
        # pattern_type, etc. so the user shouldn't be asked for them.
        ev = target_evidence.get(item.target, {})
        for key in ("pattern_type", "columns", "description", "confidence"):
            if key in ev and key not in params:
                params[key] = ev[key]
        if "business_rule_hypothesis" in ev and "description" not in params:
            params["description"] = ev["business_rule_hypothesis"]

        # Extract table from target reference (e.g. "table:journal_lines")
        if item.target.startswith("table:") and "table" not in params:
            params["table"] = item.target.split(":", 1)[1]

        # Thread follow-up answers into parameters as "reason"
        # (accept_finding's only user-provided parameter)
        if follow_up_answers and "reason" not in params:
            # Use first follow-up answer as reason
            first_answer = next(iter(follow_up_answers.values()), None)
            if first_answer:
                params["reason"] = first_answer

        fix_inputs.append(
            FixInput(
                action_name=item.recommended_action,
                dimension=dim_path,
                parameters=params,
                affected_columns=[_target_to_column_ref(item.target)],
                entropy_evidence=evidence,
            )
        )

    console.print(f"\n[dim]Applying {len(fix_inputs)} fixes...[/dim]")
    return Resolution(action=ResolutionAction.FIX, fix_inputs=fix_inputs)


def _target_to_column_ref(target: str) -> str:
    """Convert a target string to a column reference.

    "column:payments.invoice_id" -> "payments.invoice_id"
    "table:payments" -> "payments"
    """
    if ":" in target:
        return target.split(":", 1)[1]
    return target


# ---------------------------------------------------------------------------
# Gate context builder (lightweight — works at Gate 1)
# ---------------------------------------------------------------------------


def build_gate_context(
    session: Session,
    source_id: str,
    group: _DimensionFixGroup,
    event: PipelineEvent,
) -> str:
    """Build context for DocumentAgent config mode at a quality gate.

    Lighter than _build_agent_context() in fix.py — works at Gate 1
    where EntropyInterpretationRecord doesn't yet exist. Uses detector
    evidence from the event, data profiles, and semantic annotations.

    Includes ALL available fix actions for the dimension so the LLM can
    triage to the best one.

    Args:
        session: Database session.
        source_id: Source ID for querying metadata.
        group: Dimension fix group with all available actions.
        event: The EXIT_CHECK event with scores and column details.

    Returns:
        Structured context string for the LLM prompt.
    """
    dim_path = group.dimension
    score = group.score
    threshold = group.threshold
    affected_targets = _get_affected_targets(dim_path, event)

    sections: list[str] = []

    # Section 1: Available actions (all of them — LLM picks the best one)
    action_lines = [
        "<available_actions>",
        f"Dimension: {dim_path}",
        f"Score: {score:.2f} (threshold: {threshold:.2f})",
        f"Affected columns: {', '.join(affected_targets)}",
        "",
        "Choose the BEST action for each violating target.",
        "Prefer corrective actions (recalculate, override, add pattern) over accept_finding.",
        "",
    ]
    for i, action in enumerate(group.actions, 1):
        action_lines.append(f"--- Action {i}: {action['action_name']} ---")
        action_lines.append(f"Phase: {action['phase_name']}")
        if "guidance" in action:
            action_lines.append(f"Guidance: {action['guidance']}")
        if "fields" in action:
            action_lines.append("Expected parameters:")
            for line in action["fields"].splitlines():
                action_lines.append(f"  {line}")
        action_lines.append("")
    action_lines.append("</available_actions>")
    sections.append("\n".join(action_lines))

    # Section 1b: Detector-specific triage guidance (if available)
    from dataraum.entropy.detectors.base import get_default_registry

    registry = get_default_registry()
    detector_by_path = {d.dimension_path: d for d in registry.get_all_detectors()}
    detector = detector_by_path.get(dim_path)
    if detector and detector.triage_guidance:
        sections.append(f"<triage_guidance>\n{detector.triage_guidance}\n</triage_guidance>")

    # Section 2: Entropy evidence with per-target component breakdown
    # Merge column, table, and view details — detectors may be any scope
    all_target_scores: dict[str, float] = {}
    all_target_scores.update(event.column_details.get(dim_path, {}))
    all_target_scores.update(getattr(event, "table_details", {}).get(dim_path, {}))
    all_target_scores.update(getattr(event, "view_details", {}).get(dim_path, {}))

    evidence_lines = [
        "<entropy_evidence>",
        f"Detector: {dim_path.split('.')[-1]}",
        f"Score: {score:.2f}",
        f"Threshold: {threshold:.2f}",
    ]
    # Per-target component evidence from gate result (if available)
    target_evidence = getattr(event, "column_evidence", {}).get(dim_path, {})
    if all_target_scores:
        evidence_lines.append("")
        evidence_lines.append("Per-target breakdown:")
        for target, target_score in sorted(all_target_scores.items(), key=lambda x: -x[1]):
            label = "VIOLATING" if target_score > threshold else "passing"
            line = f"  {target}: {target_score:.2f} ({label})"
            ev = target_evidence.get(target, {})
            if ev:
                components = []
                for k, v in sorted(ev.items()):
                    if k == "accepted":
                        if v:
                            components.append("ACCEPTED")
                    else:
                        components.append(f"{k}={v}")
                if components:
                    line += f" [{', '.join(components)}]"
            evidence_lines.append(line)
    evidence_lines.append("</entropy_evidence>")
    sections.append("\n".join(evidence_lines))

    # Section 3: Data profile from DB
    data_section = _build_data_profile(session, source_id, affected_targets)
    if data_section:
        sections.append(data_section)

    return "\n\n".join(sections)


def _get_affected_targets(
    dim_path: str,
    event: PipelineEvent,
) -> list[str]:
    """Extract column/table/view targets that actually violate the contract.

    Checks column_details, table_details, and view_details — detectors
    may be column-scoped, table-scoped, or view-scoped.
    """
    all_scores: dict[str, float] = {}
    all_scores.update(event.column_details.get(dim_path, {}))
    all_scores.update(getattr(event, "table_details", {}).get(dim_path, {}))
    all_scores.update(getattr(event, "view_details", {}).get(dim_path, {}))
    if not all_scores:
        return []

    _, threshold = event.violations.get(dim_path, (0.0, 0.0))

    return [
        t for t, s in sorted(all_scores.items(), key=lambda x: x[1], reverse=True) if s > threshold
    ]


def _build_data_profile(
    session: Session,
    source_id: str,
    targets: list[str],
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
                .where(
                    Table.source_id == source_id,
                    Table.layer == "typed",
                    Table.table_name == table_name,
                    Column.column_name == column_name,
                )
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
