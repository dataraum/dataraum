"""CLI gate handler — functions for resolving EXIT_CHECK events.

Presents violations to the user and collects resolution decisions.
"""

from __future__ import annotations

from rich.console import Console

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
    match gate_mode:
        case GateMode.SKIP:
            _print_violations_summary(console, event, "yellow", "deferred")
            return Resolution(action=ResolutionAction.DEFER)

        case GateMode.FAIL:
            _print_violations_summary(console, event, "red", "aborting")
            return Resolution(action=ResolutionAction.ABORT)

        case _:
            return Resolution(action=ResolutionAction.DEFER)


def _print_violations_summary(
    console: Console,
    event: PipelineEvent,
    color: str,
    action: str,
) -> None:
    """Print a concise summary of exit-check violations."""
    n = len(event.violations)
    dims = ", ".join(
        f"{dim} ({score:.2f} > {thresh:.2f})"
        for dim, (score, thresh) in sorted(event.violations.items())
    )
    console.print(
        f"  [{color}]~[/{color}] Exit check after [bold]{event.phase}[/bold]: "
        f"{n} violation{'s' if n != 1 else ''} {action}"
    )
    if dims:
        console.print(f"    [dim]{dims}[/dim]")
