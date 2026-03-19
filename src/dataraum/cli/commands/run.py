"""Run pipeline command."""

from __future__ import annotations

import sys
import warnings
from collections.abc import Generator
from dataclasses import dataclass, field
from pathlib import Path
from time import monotonic
from typing import Annotated, Any

import typer
from rich.console import Console
from rich.live import Live
from rich.text import Text

from dataraum.cli.common import console, setup_logging
from dataraum.cli.gate_handler import handle_exit_check_interactive
from dataraum.core.logging import LogBuffer, activate_console, deactivate_console
from dataraum.entropy.gate import match_threshold
from dataraum.pipeline.events import EventType, PipelineEvent
from dataraum.pipeline.scheduler import PipelineResult, Resolution, ResolutionAction
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
            except KeyboardInterrupt, EOFError:
                pass

    # Default to aggregation_safe if no contract selected
    if contract is None:
        contract = "aggregation_safe"
        if not quiet:
            console.print(
                f"[dim]No contract specified, using {contract} (override with --contract)[/dim]"
            )

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

        result, stats = _drive_pipeline(
            gen=gen,
            console=console,
            interactive=False,
            quiet=quiet,
            contract_name=setup.contract_name,
            contract_thresholds=setup.contract_thresholds,
            session=setup.session,
            source_id=setup.source_id,
        )

    # Update PipelineRun status (all phase data already committed incrementally)
    from dataraum.pipeline.db_models import PipelineRun

    run_record = setup.session.get(PipelineRun, setup.run_id)
    if run_record:
        run_record.status = "completed" if result.success else "failed"
        setup.session.commit()

    if not quiet:
        _print_summary(
            console,
            result,
            stats,
            contract,
            setup.contract_thresholds,
            output_dir=output,
        )

    raise typer.Exit(0 if result.success else 1)


@dataclass
class _RunStats:
    """Accumulated stats from phase events during a pipeline run."""

    total_duration: float = 0.0
    all_warnings: list[tuple[str, str]] = field(default_factory=list)  # (phase, warning)
    phase_errors: list[tuple[str, str]] = field(default_factory=list)  # (phase, error)
    # First-seen score per dimension (before any phase changed it)
    first_scores: dict[str, float] = field(default_factory=dict)


_BRAILLE_FRAMES = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"


@dataclass
class _PhaseTracker:
    """Live-renderable tracker for running pipeline phases.

    Implements ``__rich__()`` so Rich ``Live`` can render it directly.
    Each call updates the spinner frame and elapsed times.
    """

    _running: dict[str, float] = field(default_factory=dict)
    _frame: int = 0

    def start(self, phase: str) -> None:
        """Mark *phase* as running."""
        self._running[phase] = monotonic()

    def stop(self, phase: str) -> None:
        """Remove *phase* from the running set."""
        self._running.pop(phase, None)

    def __rich__(self) -> Text:
        if not self._running:
            return Text("")
        self._frame += 1
        char = _BRAILLE_FRAMES[self._frame % len(_BRAILLE_FRAMES)]
        now = monotonic()
        lines: list[str] = []
        for phase in sorted(self._running):
            elapsed = now - self._running[phase]
            lines.append(f"  {char} {phase} ({elapsed:.0f}s)")
        return Text("\n".join(lines))


def _drive_pipeline(
    gen: Generator[PipelineEvent, Resolution | None, PipelineResult],
    console: Console,
    interactive: bool = False,
    quiet: bool = False,
    contract_name: str | None = None,
    contract_thresholds: dict[str, float] | None = None,
    session: Any = None,
    source_id: str | None = None,
) -> tuple[PipelineResult, _RunStats]:
    """Drive the scheduler generator, rendering events to the terminal.

    Args:
        gen: The scheduler generator.
        console: Rich console for output.
        interactive: When True, pause at EXIT_CHECK for interactive fix UI.
            When False, auto-defer all gate violations.
        quiet: Suppress progress output.
        contract_name: Name of the target contract (for summary display).
        contract_thresholds: Dimension thresholds from the contract.
        session: DB session (required for interactive mode).
        source_id: Source ID (required for interactive mode).

    Returns:
        Tuple of (PipelineResult, _RunStats) from the generator.
    """
    result: PipelineResult | None = None
    stats = _RunStats()
    tracker = _PhaseTracker()
    log_buffer = LogBuffer(console=console)
    live: Live | None = None

    if not quiet:
        live = Live(tracker, console=console, transient=True)
        live.start()
        activate_console(console, log_buffer=log_buffer)

    try:
        event = next(gen)
        while True:
            match event.event_type:
                case EventType.PIPELINE_STARTED:
                    if not quiet:
                        contract_info = f", contract: {contract_name}" if contract_name else ""
                        console.print(f"Pipeline started ({event.total} phases{contract_info})")

                case EventType.PHASE_STARTED:
                    tracker.start(event.phase)

                case EventType.PHASE_COMPLETED:
                    tracker.stop(event.phase)
                    if not quiet:
                        _print_phase_completed(console, event)
                    stats.total_duration += event.duration_seconds
                    for w in event.warnings:
                        stats.all_warnings.append((event.phase, w))

                case EventType.PHASE_FAILED:
                    tracker.stop(event.phase)
                    if not quiet:
                        console.print(f"  [red]\u2717[/red] {event.phase}: {event.error}")
                    stats.phase_errors.append((event.phase, event.error or "unknown error"))

                case EventType.PHASE_SKIPPED:
                    if not quiet:
                        console.print(f"  [yellow]\u25cb[/yellow] {event.phase}: {event.message}")

                case EventType.POST_VERIFICATION:
                    if event.scores:
                        for dim in event.scores:
                            if dim not in stats.first_scores:
                                stats.first_scores[dim] = event.scores[dim]
                        if not quiet:
                            from dataraum.cli.gate_handler import render_gate_scores

                            render_gate_scores(
                                console,
                                event.scores,
                                contract_thresholds=contract_thresholds,
                                phase_name=event.phase,
                                column_details=event.column_details,
                                accepted_targets=event.accepted_targets,
                            )
                    if event.skipped_detectors and not quiet:
                        for sd in event.skipped_detectors:
                            console.print(f"  [dim]~ {sd['detector_id']}: {sd['reason']}[/dim]")

                case EventType.EXIT_CHECK:
                    if interactive:
                        if live:
                            deactivate_console()
                            live.stop()
                        resolution = handle_exit_check_interactive(
                            console,
                            event,
                            contract_thresholds=contract_thresholds,
                            session=session,
                            source_id=source_id,
                        )
                        event = gen.send(resolution)
                        if live:
                            live.start()
                            activate_console(console, log_buffer=log_buffer)
                    else:
                        event = gen.send(Resolution(action=ResolutionAction.DEFER))
                    continue

                case EventType.PIPELINE_COMPLETED:
                    pass  # Handled after loop

            event = next(gen)

    except StopIteration as e:
        result = e.value
    finally:
        deactivate_console()
        if live:
            live.stop()

    if result is None:
        # Should not happen, but be defensive
        result = PipelineResult(
            success=False,
            phases_completed=[],
            phases_failed=[],
            phases_skipped=[],
            phases_blocked=[],
            final_scores={},
            deferred_issues=[],
            error="Generator ended without returning a result",
        )

    return result, stats


def _print_phase_completed(console: Console, event: PipelineEvent) -> None:
    """Print phase completion with summary."""
    line = f"  [green]\u2713[/green] {event.phase} ({event.duration_seconds:.1f}s)"
    if event.summary:
        line += f" [dim]\u2014 {event.summary}[/dim]"
    console.print(line)

    # Show warnings inline
    for warning in event.warnings:
        console.print(f"    [yellow]! {warning}[/yellow]")


def _print_summary(
    console: Console,
    result: PipelineResult,
    stats: _RunStats,
    contract_name: str | None = None,
    contract_thresholds: dict[str, float] | None = None,
    output_dir: Path | None = None,
) -> None:
    """Print post-run summary with phase breakdown and data overview.

    Args:
        console: Rich console for output.
        result: The pipeline result.
        stats: Accumulated stats from phase events.
        contract_name: Name of the evaluated contract.
        contract_thresholds: Dimension thresholds from the contract.
        output_dir: Pipeline output directory (for fix command hint).
    """
    thresholds = contract_thresholds or {}

    console.print()
    console.print("[bold]Pipeline Results[/bold]")
    console.print("=" * 60)

    # Overview line
    completed = len(result.phases_completed)
    failed = len(result.phases_failed)
    skipped = len(result.phases_skipped)
    blocked = len(result.phases_blocked)
    console.print(
        f"  Phases: [green]{completed} completed[/green]"
        f"{f', [red]{failed} failed[/red]' if failed else ''}"
        f"{f', [yellow]{skipped} skipped[/yellow]' if skipped else ''}"
        f"{f', [yellow]{blocked} blocked[/yellow]' if blocked else ''}"
        f"  [dim]({stats.total_duration:.1f}s total)[/dim]"
    )

    # Errors
    if stats.phase_errors:
        console.print()
        console.print(f"[red]Errors ({len(stats.phase_errors)}):[/red]")
        for phase, error in stats.phase_errors:
            console.print(f"  [red]\u2717[/red] {phase}: {error}")

    # Blocked phases
    if result.phases_blocked:
        console.print()
        console.print(
            f"[yellow]Blocked ({len(result.phases_blocked)}):[/yellow] "
            f"[dim]{', '.join(result.phases_blocked)}[/dim]"
        )

    # Warnings
    if stats.all_warnings:
        console.print()
        console.print(f"[yellow]Warnings ({len(stats.all_warnings)}):[/yellow]")
        for phase, warning in stats.all_warnings:
            console.print(f"  [yellow]![/yellow] {phase}: {warning}")

    # Final entropy scores with contract evaluation
    if result.final_scores or thresholds:
        console.print()
        header = "[bold]Entropy State[/bold]"
        if contract_name:
            header += f" [dim]\\[{contract_name}][/dim]"
        console.print(header)
        console.print("-" * 60)

        passing = 0
        evaluated = 0
        not_measured = 0

        for dim, score in sorted(result.final_scores.items()):
            label = dim[:34].ljust(34)
            filled = round(score * 10)
            bar = "\u2588" * filled + "\u2591" * (10 - filled)
            if score < 0.2:
                color = "green"
            elif score < 0.5:
                color = "yellow"
            else:
                color = "red"

            # Delta from first measurement
            prev = stats.first_scores.get(dim)
            delta_str = ""
            if prev is not None:
                delta = score - prev
                if delta < -0.005:
                    delta_str = f" [green]\u2193{abs(delta):.2f}[/green]"
                elif delta > 0.005:
                    delta_str = f" [red]\u2191{delta:.2f}[/red]"

            threshold = match_threshold(dim, thresholds)
            if threshold is not None:
                evaluated += 1
                if score <= threshold:
                    passing += 1
                    mark = f"[green]\u2713[/green] [dim](<= {threshold:.2f})[/dim]"
                else:
                    gap = score - threshold
                    mark = f"[red]\u2717[/red] [dim](<= {threshold:.2f}, gap: {gap:.2f})[/dim]"
                console.print(f"  {label} [{color}]{score:.3f}[/{color}]  {bar}  {mark}{delta_str}")
            else:
                console.print(f"  {label} [{color}]{score:.3f}[/{color}]  {bar}{delta_str}")

        # Show contracted dimensions that were never measured.
        # A threshold like "structural.types" is considered measured if any
        # score key starts with it (e.g. "structural.types.type_fidelity").
        measured_dims = set(result.final_scores.keys())
        for dim in sorted(thresholds):
            has_measurement = dim in measured_dims or any(
                k.startswith(dim + ".") for k in measured_dims
            )
            if not has_measurement:
                not_measured += 1
                label = dim[:34].ljust(34)
                console.print(f"  {label} [dim]  ---   not measured[/dim]")

        total_contracted = evaluated + not_measured
        if total_contracted > 0:
            compliance_color = "green" if passing == total_contracted else "yellow"
            parts = f"{passing}/{total_contracted} dimensions passing"
            if not_measured:
                parts += f", {not_measured} not measured"
                compliance_color = "yellow"
            pct = round(100 * passing / total_contracted)
            console.print()
            console.print(f"  [{compliance_color}]Contract: {parts} ({pct}%)[/{compliance_color}]")

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

    # Fix command hint (when there are deferred issues or high-entropy dimensions)
    has_high_entropy = any(s >= 0.5 for s in result.final_scores.values())
    if result.deferred_issues or has_high_entropy:
        out = output_dir
        console.print()
        console.print("[dim]To improve data quality, document domain knowledge:[/dim]")
        console.print(f"[dim]  dataraum fix {out}[/dim]")
    console.print()
