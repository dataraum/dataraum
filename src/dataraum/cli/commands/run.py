"""Run pipeline command."""

from __future__ import annotations

import sys
import warnings
from collections.abc import Generator
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.status import Status

from dataraum.cli.common import console, setup_logging
from dataraum.cli.gate_handler import handle_exit_check
from dataraum.entropy.fix_executor import ActionRegistry
from dataraum.pipeline.events import EventType, PipelineEvent
from dataraum.pipeline.runner import GateMode
from dataraum.pipeline.scheduler import PipelineResult, Resolution


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

    # Setup pipeline and drive it
    gen, action_registry = _setup_pipeline(
        source_path=source_path,
        output_dir=output,
        source_name=name,
        target_phase=phase,
        force_phase=force,
        contract=contract,
    )

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=RuntimeWarning, module="numpy")
        warnings.filterwarnings("ignore", category=RuntimeWarning, module="scipy")

        result = _drive_pipeline(
            gen=gen,
            console=console,
            gate_mode=resolved_gate_mode,
            action_registry=action_registry,
            quiet=quiet,
        )

    raise typer.Exit(0 if result.success else 1)


def _setup_pipeline(
    *,
    source_path: Path | None,
    output_dir: Path,
    source_name: str | None,
    target_phase: str | None,
    force_phase: bool,
    contract: str | None,
) -> tuple[
    Generator[PipelineEvent, Resolution | None, PipelineResult],
    ActionRegistry | None,
]:
    """Create PipelineScheduler and return its generator.

    Returns:
        Tuple of (generator, action_registry).
    """
    from typing import Any
    from uuid import uuid4

    from sqlalchemy import select

    from dataraum.core.config import load_phase_config, load_pipeline_config
    from dataraum.core.connections import ConnectionConfig, ConnectionManager
    from dataraum.entropy.fix_executor import (
        FixExecutor,
        get_default_action_registry,
    )
    from dataraum.pipeline.base import Phase
    from dataraum.pipeline.db_models import PipelineRun
    from dataraum.pipeline.registry import get_all_dependencies, get_registry
    from dataraum.pipeline.scheduler import PipelineScheduler
    from dataraum.storage import Source

    # 1. Initialize storage
    output_dir.mkdir(parents=True, exist_ok=True)
    conn_config = ConnectionConfig.for_directory(output_dir)
    manager = ConnectionManager(conn_config)
    manager.initialize()

    session = manager.get_session()
    duckdb_conn = manager._duckdb_conn  # noqa: SLF001

    # 2. Resolve source_id
    source_id: str
    if source_path is not None:
        resolved_name = source_name or source_path.stem
        existing = session.execute(
            select(Source).where(Source.name == resolved_name)
        ).scalar_one_or_none()
        source_id = existing.source_id if existing else str(uuid4())
    else:
        existing = session.execute(
            select(Source).where(Source.name == "multi_source")
        ).scalar_one_or_none()
        source_id = existing.source_id if existing else str(uuid4())

    # 3. Load pipeline and phase configs
    pipeline_yaml_config = load_pipeline_config()
    active_phase_names = pipeline_yaml_config.get("phases", [])
    phase_configs = {name: load_phase_config(name) for name in active_phase_names}

    # Build runtime config
    runtime_config: dict[str, Any]
    if source_path is not None:
        runtime_config = {
            "source_path": str(source_path),
            "source_name": source_name or source_path.stem,
        }
    else:
        runtime_config = {
            "source_name": "multi_source",
        }

    # 4. Load phases from registry
    registry = get_registry()
    phases: dict[str, Phase] = {name: cls() for name, cls in registry.items()}

    # 5. Filter phases if --phase set
    if target_phase:
        deps = get_all_dependencies(target_phase)
        keep = deps | {target_phase}
        phases = {n: p for n, p in phases.items() if n in keep}

    # 6. Create PipelineRun record
    run_id = str(uuid4())
    run_record = PipelineRun(
        run_id=run_id,
        source_id=source_id,
        status="running",
        config={"target_phase": target_phase, "force_phase": force_phase},
    )
    session.add(run_record)
    session.flush()

    # 6b. Force-clean target phase before scheduling
    if force_phase and target_phase:
        from dataraum.pipeline.cleanup import cleanup_phase

        assert duckdb_conn is not None
        cleanup_phase(target_phase, source_id, session, duckdb_conn)
        session.flush()

    # 7. Load contract thresholds
    thresholds: dict[str, float] = {}
    if contract:
        from dataraum.entropy.contracts import get_contract

        contract_obj = get_contract(contract)
        if contract_obj:
            thresholds = contract_obj.dimension_thresholds

    # 8. Create fix executor
    action_registry = get_default_action_registry()
    fix_executor = FixExecutor(action_registry)

    # 9. Create scheduler and return generator
    scheduler = PipelineScheduler(
        phases=phases,
        source_id=source_id,
        run_id=run_id,
        session=session,
        duckdb_conn=duckdb_conn,
        contract_thresholds=thresholds,
        fix_executor=fix_executor,
        phase_configs=phase_configs,
        runtime_config=runtime_config,
    )

    return scheduler.run(), action_registry


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
                        console.print(
                            f"  [green]\u2713[/green] {event.phase}"
                            f" ({event.duration_seconds:.1f}s)"
                        )
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
                    pass  # Silent unless --verbose

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
        _print_summary(console, result)

    return result


def _print_summary(console: Console, result: PipelineResult) -> None:
    """Print post-run summary.

    Args:
        console: Rich console for output.
        result: The pipeline result.
    """
    console.print()
    console.print("[bold]Pipeline Results[/bold]")
    console.print("=" * 60)

    # Counts
    console.print(f"  [green]Completed:[/green] {len(result.phases_completed)}")
    console.print(f"  [red]Failed:[/red] {len(result.phases_failed)}")
    console.print(f"  [yellow]Skipped:[/yellow] {len(result.phases_skipped)}")

    # Final entropy scores
    if result.final_scores:
        console.print()
        console.print("[bold]Entropy State[/bold]")
        console.print("-" * 60)
        for dim, score in sorted(result.final_scores.items()):
            label = dim[:22].ljust(22)
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
