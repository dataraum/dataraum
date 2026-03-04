"""CLI gate handler — functions for resolving EXIT_CHECK events.

Presents violations to the user and collects resolution decisions.
No class needed since the generator protocol handles state.
"""

from __future__ import annotations

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table

from dataraum.entropy.fix_executor import ActionRegistry, FixRequest
from dataraum.pipeline.events import PipelineEvent
from dataraum.pipeline.runner import GateMode
from dataraum.pipeline.scheduler import Resolution, ResolutionAction


def handle_exit_check(
    console: Console,
    event: PipelineEvent,
    gate_mode: GateMode,
    action_registry: ActionRegistry | None = None,
) -> Resolution:
    """Resolve an EXIT_CHECK event based on gate mode.

    Args:
        console: Rich console for output.
        event: The EXIT_CHECK event with violations.
        gate_mode: How to handle the check.
        action_registry: Available fix actions (needed for PAUSE).

    Returns:
        Resolution telling the scheduler what to do.
    """
    match gate_mode:
        case GateMode.SKIP:
            console.print("  [yellow]~[/yellow] Exit check: violations deferred")
            return Resolution(action=ResolutionAction.DEFER)

        case GateMode.FAIL:
            console.print("  [red]~[/red] Exit check: violations found, aborting")
            return Resolution(action=ResolutionAction.ABORT)

        case GateMode.PAUSE:
            return _interactive_resolution(console, event, action_registry)

        case _:
            return Resolution(action=ResolutionAction.DEFER)


def _interactive_resolution(
    console: Console,
    event: PipelineEvent,
    action_registry: ActionRegistry | None = None,
) -> Resolution:
    """Handle PAUSE mode — interactive prompt for user resolution.

    Args:
        console: Rich console for output.
        event: The EXIT_CHECK event with violations.
        action_registry: Available fix actions.

    Returns:
        Resolution based on user's choice.
    """
    try:
        _render_violations(console, event.violations, event.column_details)

        # Build fix options from registry
        fix_options: list[tuple[int, str, str, str]] = []  # (idx, action_type, target, label)
        option_idx = 1

        if action_registry:
            for dim_path in event.violations:
                for action_def in action_registry.list_actions():
                    if dim_path in action_def.improves_dimensions:
                        label = f"Fix: {action_def.action_type} (improves {dim_path})"
                        fix_options.append((option_idx, action_def.action_type, "", label))
                        option_idx += 1

        # Show options
        console.print()
        for idx, _, _, label in fix_options:
            console.print(f"  [bold cyan][{idx}][/bold cyan] {label}")
        console.print(f"  [bold cyan][{option_idx}][/bold cyan] Defer all (continue pipeline)")
        console.print(f"  [bold cyan][{option_idx + 1}][/bold cyan] Abort pipeline")
        console.print()

        choice = Prompt.ask("  Choice", console=console)
        choice = choice.strip()

        if not choice or choice == str(option_idx):
            return Resolution(action=ResolutionAction.DEFER)

        if choice == str(option_idx + 1):
            return Resolution(action=ResolutionAction.ABORT)

        # Check for fix option
        choice_int = int(choice)
        matching = [opt for opt in fix_options if opt[0] == choice_int]
        if matching:
            _, action_type, target, _ = matching[0]
            # Prompt for target if not set
            if not target:
                target = Prompt.ask("  Target (e.g. column:table.col)", console=console)
            # Prompt for action parameters
            params: dict[str, str] = {}
            if action_registry:
                resolved_def = action_registry.get(action_type)
                if resolved_def:
                    for param_name, param_desc in resolved_def.parameters_schema.items():
                        value = Prompt.ask(f"  {param_desc}", console=console)
                        params[param_name] = value

            fix_request = FixRequest(
                action_type=action_type,
                target=target,
                parameters=params,
            )
            return Resolution(action=ResolutionAction.FIX, fixes=[fix_request])

        # Invalid choice — defer
        return Resolution(action=ResolutionAction.DEFER)

    except (KeyboardInterrupt, EOFError):
        console.print("\n  [dim]Interrupted — deferring[/dim]")
        return Resolution(action=ResolutionAction.DEFER)
    except (ValueError, IndexError):
        return Resolution(action=ResolutionAction.DEFER)


def _render_violations(
    console: Console,
    violations: dict[str, tuple[float, float]],
    column_details: dict[str, dict[str, float]] | None = None,
) -> None:
    """Render violations as a Rich panel with table.

    Args:
        console: Rich console for output.
        violations: dimension_path -> (score, threshold).
        column_details: dimension_path -> {target -> score}. Optional.
    """
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

    console.print()
    console.print(
        Panel(
            table,
            title="EXIT CHECK: Contract Violations",
            title_align="left",
            border_style="yellow",
            padding=(1, 2),
        )
    )
