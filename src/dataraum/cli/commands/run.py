"""Run pipeline command."""

from __future__ import annotations

import sys
import warnings
from collections.abc import Generator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console
from rich.status import Status

from dataraum.cli.common import console, setup_logging
from dataraum.cli.gate_handler import handle_exit_check, render_fix_result
from dataraum.entropy.fix_executor import ActionRegistry
from dataraum.pipeline.events import EventType, PipelineEvent
from dataraum.pipeline.runner import GateMode
from dataraum.pipeline.scheduler import PipelineResult, Resolution
from dataraum.pipeline.setup import setup_pipeline


def run(
    source: Annotated[
        Path | None,
        typer.Argument(
            help="Path to CSV file or directory. When omitted, uses registered sources.",
        ),
    ] = None,
    output: Annotated[
        Path,
        typer.Option(
            "--output",
            "-o",
            help="Output directory for database files",
        ),
    ] = Path("./pipeline_output"),
    name: Annotated[
        str | None,
        typer.Option(
            "--name",
            "-n",
            help="Name for the data source (default: derived from path)",
        ),
    ] = None,
    phase: Annotated[
        str | None,
        typer.Option(
            "--phase",
            "-p",
            help="Run only this phase and its dependencies",
        ),
    ] = None,
    gate_mode: Annotated[
        str,
        typer.Option(
            "--gate-mode",
            "-g",
            help="How to handle entropy gates: skip (default), pause (interactive), fail",
        ),
    ] = "skip",
    contract: Annotated[
        str | None,
        typer.Option(
            "--contract",
            help="Target contract name for gate evaluation",
        ),
    ] = None,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            "-f",
            help="Force re-run of target phase, deleting previous results",
        ),
    ] = False,
    quiet: Annotated[
        bool,
        typer.Option(
            "--quiet",
            "-q",
            help="Suppress progress output",
        ),
    ] = False,
    verbose: Annotated[
        int,
        typer.Option(
            "--verbose",
            "-v",
            count=True,
            help="Increase logging verbosity (-v=INFO, -vv=DEBUG)",
        ),
    ] = 0,
    log_format: Annotated[
        str,
        typer.Option(
            "--log-format",
            help="Log output format (console or json)",
        ),
    ] = "console",
) -> None:
    """Run the pipeline on CSV data.

    Examples:

        dataraum run /path/to/csv/directory

        dataraum run /path/to/file.csv --output ./my_output

        dataraum run --output ./output  # Uses registered sources

        dataraum run /path/to/data --phase semantic  # Run up to semantic phase

        dataraum run /path/to/data -v         # Show INFO level structlog

        dataraum run /path/to/data -vv        # Show DEBUG level logs

        dataraum run /path/to/data --log-format json  # JSON logs for cloud
    """
    setup_logging(verbosity=verbose, log_format=log_format)

    # Validate --gate-mode
    try:
        resolved_gate_mode = GateMode(gate_mode)
    except ValueError:
        console.print(
            f"[red]Error: Invalid gate mode: {gate_mode}. Use: skip, pause, fail[/red]"
        )
        raise typer.Exit(1) from None

    # Validate --force flag
    if force and not phase:
        console.print("[red]Error: --force requires --phase[/red]")
        raise typer.Exit(1)
    if force and phase == "import":
        console.print("[red]Error: --force is not supported for the import phase[/red]")
        raise typer.Exit(1)

    # Validate source path if provided
    source_path: Path | None = None
    if source is not None:
        resolved = source.resolve()
        if not resolved.exists():
            console.print(f"[red]Error: Source path does not exist: {source}[/red]")
            raise typer.Exit(1)
        source_path = resolved

    # TTY detection for interactive features
    is_interactive = sys.stdin.isatty() and not quiet

    # Warn if pause requested in non-interactive mode
    if resolved_gate_mode == GateMode.PAUSE and not is_interactive:
        console.print(
            "[yellow]Warning: --gate-mode pause requires an interactive terminal. "
            "Falling back to skip.[/yellow]"
        )
        resolved_gate_mode = GateMode.SKIP

    # Interactive contract selection (if TTY and no --contract specified)
    if is_interactive and contract is None:
        from dataraum.entropy.contracts import list_contracts

        available = list_contracts()
        if available:
            contract_names = [c["name"] for c in available]
            console.print()
            console.print("[bold]Available contracts:[/bold]")
            for i, c in enumerate(available, 1):
                console.print(f"  [{i}] {c['display_name']} — {c['description']}")
            console.print("  [0] None (skip contract evaluation)")
            console.print()
            try:
                from rich.prompt import Prompt

                choice = Prompt.ask(
                    "  Select contract",
                    default="0",
                    console=console,
                )
                choice = choice.strip()
                if choice.isdigit():
                    idx = int(choice)
                    if 1 <= idx <= len(contract_names):
                        contract = contract_names[idx - 1]
                        console.print(f"  Using contract: [bold]{contract}[/bold]")
            except (KeyboardInterrupt, EOFError):
                pass

    # Setup pipeline using shared logic
    setup = setup_pipeline(
        source_path=source_path,
        output_dir=output,
        source_name=name,
        target_phase=phase,
        force_phase=force,
        contract=contract,
    )

    gen = setup.scheduler.run()

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=RuntimeWarning, module="numpy")
        warnings.filterwarnings("ignore", category=RuntimeWarning, module="scipy")

        result = _drive_pipeline(
            gen=gen,
            console=console,
            gate_mode=resolved_gate_mode,
            action_registry=setup.action_registry,
            quiet=quiet,
        )

    setup.session.commit()
    raise typer.Exit(0 if result.success else 1)


@dataclass
class _RunStats:
    """Accumulated stats from phase events during a pipeline run."""

    total_processed: int = 0
    total_created: int = 0
    total_duration: float = 0.0
    all_warnings: list[tuple[str, str]] = field(
        default_factory=list
    )  # (phase, warning)
    phase_details: list[tuple[str, int, int, float]] = field(
        default_factory=list
    )  # (name, processed, created, duration)
    phase_outputs: dict[str, dict[str, Any]] = field(
        default_factory=dict
    )  # phase_name -> outputs


def _drive_pipeline(
    gen: Generator[PipelineEvent, Resolution | None, PipelineResult],
    console: Console,
    gate_mode: GateMode,
    action_registry: ActionRegistry | None = None,
    quiet: bool = False,
) -> PipelineResult:
    """Drive the scheduler generator, rendering events to the terminal.

    Args:
        gen: The scheduler generator.
        console: Rich console for output.
        gate_mode: How to handle EXIT_CHECK events.
        action_registry: Available fix actions.
        quiet: Suppress progress output.

    Returns:
        PipelineResult from the generator.
    """
    result: PipelineResult | None = None
    status: Status | None = None
    stats = _RunStats()

    if not quiet:
        status = Status("Starting pipeline...", console=console, spinner="dots")
        status.start()

    try:
        event = next(gen)
        while True:
            match event.event_type:
                case EventType.PIPELINE_STARTED:
                    if status:
                        status.update(f"Pipeline started ({event.total} phases)")

                case EventType.PHASE_STARTED:
                    if status:
                        status.update(f"[bold]{event.phase}[/bold]...")

                case EventType.PHASE_COMPLETED:
                    if status:
                        status.stop()
                    if not quiet:
                        _print_phase_completed(console, event)
                    # Accumulate stats
                    stats.total_processed += event.records_processed
                    stats.total_created += event.records_created
                    stats.total_duration += event.duration_seconds
                    for w in event.warnings:
                        stats.all_warnings.append((event.phase, w))
                    if event.records_processed or event.records_created:
                        stats.phase_details.append((
                            event.phase,
                            event.records_processed,
                            event.records_created,
                            event.duration_seconds,
                        ))
                    if event.outputs:
                        stats.phase_outputs[event.phase] = event.outputs
                    if status:
                        status.start()

                case EventType.PHASE_FAILED:
                    if status:
                        status.stop()
                    if not quiet:
                        console.print(
                            f"  [red]\u2717[/red] {event.phase}: {event.error}"
                        )
                    if status:
                        status.start()

                case EventType.PHASE_SKIPPED:
                    if status:
                        status.stop()
                    if not quiet:
                        console.print(
                            f"  [yellow]\u25cb[/yellow] {event.phase}: {event.message}"
                        )
                    if status:
                        status.start()

                case EventType.POST_VERIFICATION:
                    if not quiet and event.scores:
                        _print_post_verification(console, event)

                case EventType.FIX_APPLIED:
                    if status:
                        status.stop()
                    if not quiet:
                        render_fix_result(console, event)
                    if status:
                        status.start()

                case EventType.EXIT_CHECK:
                    if status:
                        status.stop()
                    resolution = handle_exit_check(
                        console, event, gate_mode, action_registry
                    )
                    event = gen.send(resolution)
                    if status:
                        status.start()
                    continue

                case EventType.PIPELINE_COMPLETED:
                    pass  # Handled after loop

            event = next(gen)

    except StopIteration as e:
        result = e.value
    finally:
        if status:
            status.stop()

    if result is None:
        # Should not happen, but be defensive
        result = PipelineResult(
            success=False,
            phases_completed=[],
            phases_failed=[],
            phases_skipped=[],
            final_scores={},
            deferred_issues=[],
            error="Generator ended without returning a result",
        )

    if not quiet:
        _print_summary(console, result, stats)

    return result


def _print_phase_completed(console: Console, event: PipelineEvent) -> None:
    """Print phase completion with metadata."""
    parts = [f"  [green]\u2713[/green] {event.phase} ({event.duration_seconds:.1f}s)"]

    # Add record counts if present
    details: list[str] = []
    if event.records_processed:
        details.append(f"{event.records_processed:,} processed")
    if event.records_created:
        details.append(f"{event.records_created:,} created")
    if details:
        parts[0] += f" [dim]\u2014 {', '.join(details)}[/dim]"

    console.print(parts[0])

    # Show phase summary if present
    if event.summary:
        console.print(f"    [dim]{event.summary}[/dim]")

    # Show warnings inline
    for warning in event.warnings:
        console.print(f"    [yellow]! {warning}[/yellow]")


def _print_post_verification(console: Console, event: PipelineEvent) -> None:
    """Print post-verification entropy scores."""
    for dim, score in sorted(event.scores.items()):
        if score < 0.2:
            color = "green"
        elif score < 0.5:
            color = "yellow"
        else:
            color = "red"
        console.print(f"    [{color}]\u25c6[/{color}] {dim}: [{color}]{score:.2f}[/{color}]")


def _print_summary(
    console: Console, result: PipelineResult, stats: _RunStats
) -> None:
    """Print post-run summary with phase breakdown and data overview.

    Args:
        console: Rich console for output.
        result: The pipeline result.
        stats: Accumulated stats from phase events.
    """
    console.print()
    console.print("[bold]Pipeline Results[/bold]")
    console.print("=" * 60)

    # Overview line
    completed = len(result.phases_completed)
    failed = len(result.phases_failed)
    skipped = len(result.phases_skipped)
    console.print(
        f"  Phases: [green]{completed} completed[/green]"
        f"{f', [red]{failed} failed[/red]' if failed else ''}"
        f"{f', [yellow]{skipped} skipped[/yellow]' if skipped else ''}"
        f"  [dim]({stats.total_duration:.1f}s total)[/dim]"
    )

    # Data throughput
    if stats.total_processed or stats.total_created:
        console.print(
            f"  Records: {stats.total_processed:,} processed, "
            f"{stats.total_created:,} created"
        )

    # Per-phase breakdown (only phases that touched data)
    if stats.phase_details:
        console.print()
        console.print("[bold]Phase Breakdown[/bold]")
        console.print("-" * 60)
        for name, processed, created, duration in stats.phase_details:
            parts = []
            if processed:
                parts.append(f"{processed:,} in")
            if created:
                parts.append(f"{created:,} out")
            detail = ", ".join(parts)
            console.print(
                f"  {name:<24} {duration:>5.1f}s  [dim]{detail}[/dim]"
            )

    # Discoveries (phase outputs detail)
    if stats.phase_outputs:
        from dataraum.pipeline.base import PhaseResult

        console.print()
        console.print("[bold]Discoveries[/bold]")
        console.print("-" * 60)
        for phase_name, outputs in stats.phase_outputs.items():
            detail = PhaseResult(
                status=PhaseResult.success().status, outputs=outputs
            ).detail()
            if detail:
                console.print(f"  [bold]{phase_name}[/bold]")
                console.print(detail)

    # Warnings
    if stats.all_warnings:
        console.print()
        console.print(f"[yellow]Warnings ({len(stats.all_warnings)}):[/yellow]")
        for phase, warning in stats.all_warnings:
            console.print(f"  [yellow]![/yellow] {phase}: {warning}")

    # Final entropy scores
    if result.final_scores:
        console.print()
        console.print("[bold]Entropy State[/bold]")
        console.print("-" * 60)
        for dim, score in sorted(result.final_scores.items()):
            label = dim[:28].ljust(28)
            filled = round(score * 10)
            bar = "\u2588" * filled + "\u2591" * (10 - filled)
            if score < 0.2:
                color = "green"
            elif score < 0.5:
                color = "yellow"
            else:
                color = "red"
            console.print(f"  {label} [{color}]{score:.3f}[/{color}]  {bar}")

    # Deferred issues
    if result.deferred_issues:
        console.print()
        console.print(f"[yellow]Deferred issues: {len(result.deferred_issues)}[/yellow]")
        for issue in result.deferred_issues:
            console.print(
                f"  - {issue.dimension_path}: "
                f"{issue.score:.2f} > {issue.threshold:.2f} "
                f"(from {issue.producing_phase})"
            )

    # Error
    if result.error:
        console.print()
        console.print(f"[red]Error: {result.error}[/red]")

    # Overall status
    console.print()
    if result.success:
        console.print("[green]Pipeline completed successfully[/green]")
    else:
        console.print("[red]Pipeline completed with failures[/red]")
    console.print()
