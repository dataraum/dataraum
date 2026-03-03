"""Run pipeline command."""

from __future__ import annotations

import sys
import warnings
from pathlib import Path
from typing import Annotated, Any

import typer

from dataraum.cli.common import console, setup_logging


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
            help="How to handle entropy gates: skip (default), pause (interactive), fail, auto_fix",
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

    from dataraum.pipeline.runner import GateMode, RunConfig
    from dataraum.pipeline.runner import run as run_pipeline

    # Validate --gate-mode
    try:
        resolved_gate_mode = GateMode(gate_mode)
    except ValueError:
        console.print(
            f"[red]Error: Invalid gate mode: {gate_mode}. Use: skip, pause, fail, auto_fix[/red]"
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

    # Default gate_mode: TTY → pause, non-TTY → skip
    if gate_mode == "skip" and is_interactive:
        # User didn't explicitly set gate_mode; use pause for interactive sessions
        pass  # Keep as skip unless user wants interactive — opt-in for now

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

    # Wire gate handler for interactive mode
    gate_handler = None
    if resolved_gate_mode == GateMode.PAUSE and is_interactive:
        from dataraum.cli.gate_handler import InteractiveCLIHandler

        gate_handler = InteractiveCLIHandler(console=console)

    # Event-driven live display for interactive mode
    event_callback = None
    _live_ctx = None
    if is_interactive:
        from rich.live import Live
        from rich.text import Text

        from dataraum.pipeline.events import EventType, PipelineEvent

        # Mutable live state
        _live_state: dict[str, Any] = {
            "step": 0,
            "total": 0,
            "running": [],
            "latest_scores": {},
            "latest_gate": "",
        }

        def _render_live() -> Text:
            s = _live_state
            step = s["step"]
            total = s["total"]
            running = s["running"]
            lines = []

            # Main status line
            running_str = ", ".join(running) if running else "waiting..."
            lines.append(f"Pipeline [{step}/{total}]  Running: {running_str}")

            # Latest entropy scores
            if s["latest_scores"]:
                scores_str = ", ".join(f"{d}={v:.3f}" for d, v in s["latest_scores"].items())
                lines.append(f"  entropy: {scores_str}")

            # Latest gate info
            if s["latest_gate"]:
                lines.append(f"  gate: {s['latest_gate']}")

            return Text("\n".join(lines))

        def _event_handler(event: PipelineEvent) -> None:
            s = _live_state
            s["step"] = event.step
            s["total"] = event.total

            if event.event_type == EventType.PHASE_STARTED:
                s["running"] = event.parallel_phases
            elif event.event_type == EventType.PHASE_COMPLETED:
                # Remove completed phase from running list
                if event.phase in s["running"]:
                    s["running"] = [p for p in s["running"] if p != event.phase]
            elif event.event_type == EventType.POST_VERIFICATION:
                s["latest_scores"] = event.scores
            elif event.event_type == EventType.GATE_EVALUATED:
                if event.gate_status == "passed":
                    s["latest_gate"] = f"{event.phase} — passed"
                elif event.gate_status == "skipped":
                    s["latest_gate"] = f"{event.phase} — skipped (violations)"
            elif event.event_type == EventType.GATE_BLOCKED:
                s["latest_gate"] = f"{event.phase} — BLOCKED"
            elif event.event_type == EventType.PIPELINE_COMPLETED:
                s["running"] = []

            if _live_ctx is not None:
                try:
                    _live_ctx.update(_render_live())
                except Exception:
                    pass

        event_callback = _event_handler

    config = RunConfig(
        source_path=source_path,
        output_dir=output,
        source_name=name,
        target_phase=phase,
        force_phase=force,
        gate_mode=resolved_gate_mode,
        contract=contract,
        gate_handler=gate_handler,
        event_callback=event_callback,
    )

    # Run pipeline — with live display if interactive
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=RuntimeWarning, module="numpy")
        warnings.filterwarnings("ignore", category=RuntimeWarning, module="scipy")

        if is_interactive and event_callback is not None:
            from rich.live import Live

            with Live(console=console, refresh_per_second=4, transient=True) as live:
                _live_ctx = live
                if gate_handler is not None:
                    gate_handler.set_live(live)
                result = run_pipeline(config)
                _live_ctx = None
        else:
            result = run_pipeline(config)
    run_result = result.unwrap()

    # Print user-facing output
    if not quiet:
        console.print("\n[bold]Pipeline Run[/bold]")
        console.print("=" * 60)
        console.print(f"Source: {config.source_path or '(registered sources)'}")
        console.print(f"Output: {config.output_dir}")
        console.print(f"Source ID: {run_result.source_id}")

        if config.target_phase:
            console.print(f"Target Phase: {config.target_phase}")

        # Build gate info lookup from gate_events
        gate_info: dict[str, dict[str, Any]] = {}
        for ge in run_result.gate_events:
            phase_name_ge = ge.get("phase", "")
            if phase_name_ge:
                gate_info[phase_name_ge] = ge

        # Show per-phase results
        if run_result.phases:
            console.print()
            console.print("[bold]Phase Results[/bold]")
            console.print("-" * 60)
            for phase_result in run_result.phases:
                status_icon = {
                    "completed": "[green]✓[/green]",
                    "failed": "[red]✗[/red]",
                    "skipped": "[yellow]○[/yellow]",
                    "gate_blocked": "[yellow]⊘[/yellow]",
                }.get(phase_result.status, "?")
                duration_str = (
                    f" ({phase_result.duration_seconds:.1f}s)"
                    if phase_result.duration_seconds > 0
                    else ""
                )
                console.print(
                    f"  {status_icon} {phase_result.phase_name}: "
                    f"{phase_result.status}{duration_str}"
                )
                if phase_result.error and phase_result.status != "gate_blocked":
                    console.print(f"      [red]Error: {phase_result.error}[/red]")

                # Show gate info for this phase
                gi = gate_info.get(phase_result.phase_name)
                if gi:
                    gs = gi.get("gate_status", "")
                    violations = gi.get("violations", {})
                    if gs == "passed":
                        console.print("      [dim]gate: passed[/dim]")
                    elif gs in ("blocked", "skipped"):
                        parts = []
                        for dim, v in violations.items():
                            cur = v.get("current", 0)
                            thr = v.get("threshold", 0)
                            if cur < 0:
                                parts.append(
                                    f"{dim} [red]✗[/red] (not yet measured, needs {thr:.2f})"
                                )
                            else:
                                parts.append(f"{dim} [red]✗[/red] ({cur:.2f} > {thr:.2f})")
                        if parts:
                            console.print(f"      [yellow]gate: {', '.join(parts)}[/yellow]")

                # Show entropy scores produced by this phase
                if phase_result.post_verification_scores:
                    scores_str = ", ".join(
                        f"{d}={s:.3f}" for d, s in phase_result.post_verification_scores.items()
                    )
                    console.print(f"      [dim]entropy: {scores_str}[/dim]")

        # Entropy State
        if run_result.final_entropy_scores:
            console.print()
            console.print("[bold]Entropy State[/bold]")
            console.print("-" * 60)
            for dim, score in sorted(run_result.final_entropy_scores.items()):
                # Truncate/pad dimension name for alignment
                label = dim[:22].ljust(22)
                # Build bar: 10 chars, filled proportionally
                filled = round(score * 10)
                bar = "\u2588" * filled + "\u2591" * (10 - filled)
                # Color based on severity
                if score < 0.2:
                    color = "green"
                elif score < 0.5:
                    color = "yellow"
                else:
                    color = "red"
                console.print(f"  {label} [{color}]{score:.3f}[/{color}]  {bar}")

        # Gate Summary
        if run_result.gate_events:
            total_gates = sum(
                1 for g in run_result.gate_events if g.get("event_type") == "gate_evaluated"
            )
            passed_gates = sum(
                1
                for g in run_result.gate_events
                if g.get("event_type") == "gate_evaluated" and g.get("gate_status") == "passed"
            )
            blocked_gates = sum(
                1
                for g in run_result.gate_events
                if g.get("event_type") in ("gate_evaluated", "gate_blocked")
                and g.get("gate_status") in ("blocked", "skipped")
            )
            console.print()
            console.print(
                f"[bold]Gate Summary:[/bold] {total_gates + blocked_gates} evaluated, "
                f"{passed_gates} passed, {blocked_gates} blocked/skipped"
            )

        # Summary
        console.print()
        console.print("[bold]Summary[/bold]")
        console.print("-" * 60)
        console.print(f"  [green]Completed:[/green] {run_result.phases_completed}")
        console.print(f"  [red]Failed:[/red] {run_result.phases_failed}")
        console.print(f"  [yellow]Skipped:[/yellow] {run_result.phases_skipped}")
        console.print(f"  Duration: {run_result.duration_seconds:.2f}s")

        # Data metrics
        if run_result.total_tables_processed > 0 or run_result.total_rows_processed > 0:
            console.print()
            console.print("[bold]Data Metrics[/bold]")
            console.print("-" * 60)
            console.print(f"  Tables: {run_result.total_tables_processed}")
            console.print(f"  Rows: {run_result.total_rows_processed:,}")

        # Slowest phases (show top 5 if more than 3 phases ran)
        slowest = run_result.get_slowest_phases(5)
        if len(slowest) > 3:
            console.print()
            console.print("[bold]Slowest Phases[/bold]")
            console.print("-" * 60)
            for phase_name, duration in slowest:
                pct = (
                    (duration / run_result.duration_seconds * 100)
                    if run_result.duration_seconds > 0
                    else 0
                )
                console.print(f"  {phase_name}: {duration:.1f}s ({pct:.0f}%)")

        # Bottleneck operations (if any timings recorded)
        bottlenecks = run_result.get_bottleneck_operations(5)
        if bottlenecks:
            console.print()
            console.print("[bold]Bottleneck Operations[/bold]")
            console.print("-" * 60)
            for phase_name, op_name, duration in bottlenecks:
                console.print(f"  {phase_name}/{op_name}: {duration:.1f}s")

        # Output files
        if run_result.output_dir:
            console.print()
            console.print("[bold]Output files:[/bold]")
            console.print(f"  Metadata: {run_result.output_dir / 'metadata.db'}")
            console.print(f"  Data: {run_result.output_dir / 'data.duckdb'}")

        # Overall error (exception during setup)
        if run_result.error:
            console.print()
            console.print(f"[red]Error: {run_result.error}[/red]")

        # Warnings from phase failures
        if result.warnings:
            console.print()
            console.print("[yellow]Warnings:[/yellow]")
            for warning in result.warnings:
                console.print(f"  - {warning}")

        console.print()

    raise typer.Exit(0 if run_result.success else 1)
