"""CLI gate handler — functions for resolving EXIT_CHECK events.

Renders post-verification results during the pipeline run and resolves
EXIT_CHECK events based on gate mode.
"""

from __future__ import annotations

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from dataraum.pipeline.events import PipelineEvent
from dataraum.pipeline.runner import GateMode
from dataraum.pipeline.scheduler import Resolution, ResolutionAction


def handle_exit_check(
    console: Console,
    event: PipelineEvent,
    gate_mode: GateMode,
    contract_thresholds: dict[str, float] | None = None,
) -> Resolution:
    """Resolve an EXIT_CHECK event based on gate mode.

    Args:
        console: Rich console for output.
        event: The EXIT_CHECK event with violations.
        gate_mode: How to handle the check.
        contract_thresholds: Dimension thresholds from the contract.

    Returns:
        Resolution telling the scheduler what to do.
    """
    render_violations(
        console,
        event.violations,
        event.column_details,
        all_scores=event.scores or None,
        contract_thresholds=contract_thresholds,
        phase_name=event.phase,
    )

    match gate_mode:
        case GateMode.SKIP:
            return Resolution(action=ResolutionAction.DEFER)

        case GateMode.FAIL:
            console.print("  [red]Aborting — gate mode is fail[/red]")
            return Resolution(action=ResolutionAction.ABORT)

        case _:
            return Resolution(action=ResolutionAction.DEFER)


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
            matched = _match_threshold(dim_path, contract_thresholds)
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


def _match_threshold(
    dimension_path: str, thresholds: dict[str, float]
) -> float | None:
    """Match a dimension path to a threshold using prefix matching."""
    parts = dimension_path.split(".")
    for i in range(len(parts), 0, -1):
        prefix = ".".join(parts[:i])
        if prefix in thresholds:
            return thresholds[prefix]
    return None
