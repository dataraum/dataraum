"""Fix command — run pipeline interactively to resolve data quality issues."""

from __future__ import annotations

import sys
import warnings
from pathlib import Path
from typing import Annotated

import typer

from dataraum.cli.common import console, setup_logging


def fix(
    source: Annotated[
        Path | None,
        typer.Argument(
            help="Path to CSV file or directory. When omitted, uses registered sources.",
        ),
    ] = None,
    output_dir: Annotated[
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
    contract: Annotated[
        str | None,
        typer.Option("--contract", help="Target contract name for gate evaluation"),
    ] = None,
    verbose: Annotated[
        int,
        typer.Option("--verbose", "-v", count=True, help="Increase logging verbosity"),
    ] = 0,
    log_format: Annotated[
        str,
        typer.Option("--log-format", help="Log output format (console or json)"),
    ] = "console",
) -> None:
    """Run pipeline interactively, pausing at quality gates to review and fix issues.

    Can be used for a fresh run or to re-run on existing pipeline output.
    Only affected phases re-run when metadata already exists.

    Examples:

        dataraum fix /path/to/data

        dataraum fix ./pipeline_output --contract aggregation_safe

        dataraum fix  # re-run on default output with registered sources
    """
    setup_logging(verbosity=verbose, log_format=log_format)

    # Validate source path if provided
    source_path: Path | None = None
    if source is not None:
        resolved = source.resolve()
        if not resolved.exists():
            console.print(f"[red]Error: Source path does not exist: {source}[/red]")
            raise typer.Exit(1)
        source_path = resolved

    # TTY check
    if not sys.stdin.isatty():
        console.print("[red]Error: fix command requires an interactive terminal[/red]")
        raise typer.Exit(1)

    # Interactive contract selection (same as run command)
    if contract is None:
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

                choice = Prompt.ask("  Select contract", default="0", console=console)
                choice = choice.strip()
                if choice.isdigit():
                    idx = int(choice)
                    if 1 <= idx <= len(contract_names):
                        contract = contract_names[idx - 1]
                        console.print(f"  Using contract: [bold]{contract}[/bold]")
            except (KeyboardInterrupt, EOFError):
                pass

    if contract is None:
        contract = "aggregation_safe"
        console.print(
            f"[dim]No contract specified, using {contract} (override with --contract)[/dim]"
        )

    # Setup pipeline
    from dataraum.pipeline.setup import setup_pipeline

    setup = setup_pipeline(
        source_path=source_path,
        output_dir=output_dir,
        source_name=name,
        contract=contract,
    )

    gen = setup.scheduler.run()

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=RuntimeWarning, module="numpy")
        warnings.filterwarnings("ignore", category=RuntimeWarning, module="scipy")

        from dataraum.cli.commands.run import _drive_pipeline, _print_summary

        result, stats = _drive_pipeline(
            gen=gen,
            console=console,
            interactive=True,
            contract_name=setup.contract_name,
            contract_thresholds=setup.contract_thresholds,
            session=setup.session,
            source_id=setup.source_id,
        )

    # Update PipelineRun status
    from dataraum.pipeline.db_models import PipelineRun

    run_record = setup.session.get(PipelineRun, setup.run_id)
    if run_record:
        run_record.status = "completed" if result.success else "failed"
        setup.session.commit()

    _print_summary(
        console,
        result,
        stats,
        contract,
        setup.contract_thresholds,
        output_dir=output_dir,
    )

    raise typer.Exit(0 if result.success else 1)
