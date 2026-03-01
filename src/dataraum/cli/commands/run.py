"""Run pipeline command."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

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

        dataraum run /path/to/data -v         # Show INFO level logs

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
        console.print(f"[red]Error: Invalid gate mode: {gate_mode}. Use: skip, pause, fail, auto_fix[/red]")
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

    config = RunConfig(
        source_path=source_path,
        output_dir=output,
        source_name=name,
        target_phase=phase,
        force_phase=force,
        gate_mode=resolved_gate_mode,
        contract=contract,
    )

    # Run pipeline - always returns Result.ok with RunResult
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
                if phase_result.error:
                    console.print(f"      [red]Error: {phase_result.error}[/red]")

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
