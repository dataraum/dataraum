"""CLI for dataraum pipeline.

Provides commands for running, inspecting, and monitoring the pipeline.

Usage:
    dataraum run /path/to/data
    dataraum run /path/to/data --phase import
    dataraum status ./pipeline_output
    dataraum inspect ./pipeline_output

Environment:
    Loads .env file from current directory if present.
    Set ANTHROPIC_API_KEY for LLM phases.

Logging:
    -v / --verbose: Show INFO level logs
    -vv: Show DEBUG level logs
    --log-format json: Output logs as JSON (for cloud/production)
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any

import typer

if TYPE_CHECKING:
    from dataraum.entropy.contracts import ContractEvaluation, ContractProfile
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table as RichTable

from dataraum.core.logging import configure_logging

# Load .env file from current directory (for API keys, etc.)
load_dotenv()


def setup_logging(verbosity: int = 0, log_format: str = "console") -> None:
    """Configure structured logging based on verbosity level.

    Args:
        verbosity: 0=WARNING, 1=INFO, 2+=DEBUG
        log_format: "console" for development, "json" for production/cloud
    """
    if verbosity >= 2:
        level = "DEBUG"
    elif verbosity >= 1:
        level = "INFO"
    else:
        level = "WARNING"

    configure_logging(
        log_level=level,
        log_format=log_format,
        show_timestamps=verbosity >= 1,
        color=log_format == "console",
    )


app = typer.Typer(
    name="dataraum",
    help="DataRaum Context Engine - extract rich metadata from data sources.",
    no_args_is_help=True,
)
console = Console()


@app.command()
def run(
    source: Annotated[
        Path,
        typer.Argument(
            help="Path to CSV file or directory containing CSV files",
            exists=True,
            file_okay=True,
            dir_okay=True,
            resolve_path=True,
        ),
    ],
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
    skip_llm: Annotated[
        bool,
        typer.Option(
            "--skip-llm",
            help="Skip phases that require LLM",
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

        dataraum run /path/to/data --phase import --skip-llm

        dataraum run /path/to/data -v         # Show INFO level logs

        dataraum run /path/to/data -vv        # Show DEBUG level logs

        dataraum run /path/to/data --log-format json  # JSON logs for cloud
    """
    setup_logging(verbosity=verbose, log_format=log_format)

    from dataraum.pipeline.runner import RunConfig
    from dataraum.pipeline.runner import run as run_pipeline

    config = RunConfig(
        source_path=source,
        output_dir=output,
        source_name=name,
        target_phase=phase,
        skip_llm=skip_llm,
    )

    # Run pipeline - always returns Result.ok with RunResult
    result = run_pipeline(config)
    run_result = result.unwrap()

    # Print user-facing output
    if not quiet:
        console.print("\n[bold]Pipeline Run[/bold]")
        console.print("=" * 60)
        console.print(f"Source: {config.source_path}")
        console.print(f"Output: {config.output_dir}")
        console.print(f"Source ID: {run_result.source_id}")

        if config.target_phase:
            console.print(f"Target Phase: {config.target_phase}")
        if config.skip_llm:
            console.print("LLM Phases: Skipped")

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

        # LLM metrics
        if run_result.total_llm_calls > 0:
            console.print()
            console.print("[bold]LLM Usage[/bold]")
            console.print("-" * 60)
            console.print(f"  Calls: {run_result.total_llm_calls}")
            console.print(f"  Tokens: {run_result.total_llm_tokens:,}")

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


@app.command()
def status(
    output_dir: Annotated[
        Path,
        typer.Argument(
            help="Output directory containing pipeline databases",
            exists=True,
            dir_okay=True,
            file_okay=False,
            resolve_path=True,
        ),
    ] = Path("./pipeline_output"),
) -> None:
    """Show status of a pipeline run.

    Displays information about completed phases, sources, and tables.
    """
    _status_impl(output_dir)


def _status_impl(output_dir: Path) -> None:
    """Sync implementation of status command."""
    from sqlalchemy import func, select

    from dataraum.core import ConnectionConfig, ConnectionManager
    from dataraum.pipeline.db_models import PhaseCheckpoint
    from dataraum.storage import Column, Source, Table

    config = ConnectionConfig.for_directory(output_dir)

    # Check if databases exist
    if not config.sqlite_path.exists():
        console.print(f"[red]No metadata database found at {config.sqlite_path}[/red]")
        raise typer.Exit(1)

    manager = ConnectionManager(config)
    manager.initialize()

    try:
        with manager.session_scope() as session:
            # Get sources
            sources_result = session.execute(select(Source))
            sources = sources_result.scalars().all()

            if not sources:
                console.print("[yellow]No sources found in database[/yellow]")
                return

            console.print(f"\n[bold]Pipeline Status[/bold] - {output_dir}\n")

            for source in sources:
                # Source info
                console.print(f"[cyan]Source:[/cyan] {source.name} ({source.source_id[:8]}...)")

                # Tables
                tables_result = session.execute(
                    select(Table).where(Table.source_id == source.source_id)
                )
                tables = tables_result.scalars().all()

                # Group by layer
                by_layer: dict[str, list[Table]] = {}
                for t in tables:
                    layer = t.layer or "unknown"
                    by_layer.setdefault(layer, []).append(t)

                table = RichTable(show_header=True, header_style="bold")
                table.add_column("Layer")
                table.add_column("Tables", justify="right")
                table.add_column("Total Rows", justify="right")

                for layer in ["raw", "typed", "quarantine"]:
                    if layer in by_layer:
                        layer_tables = by_layer[layer]
                        total_rows = sum(t.row_count or 0 for t in layer_tables)
                        table.add_row(layer, str(len(layer_tables)), f"{total_rows:,}")

                console.print(table)

                # Columns count
                columns_result = session.execute(
                    select(func.count())
                    .select_from(Column)
                    .join(Table)
                    .where(Table.source_id == source.source_id)
                )
                columns_count = columns_result.scalar() or 0
                console.print(f"Total columns: {columns_count}")

                # Phase executions
                phases_result = session.execute(
                    select(PhaseCheckpoint)
                    .where(PhaseCheckpoint.source_id == source.source_id)
                    .order_by(PhaseCheckpoint.started_at)
                )
                phases = phases_result.scalars().all()

                if phases:
                    console.print("\n[bold]Phase History:[/bold]")
                    phase_table = RichTable(show_header=True, header_style="bold")
                    phase_table.add_column("Phase")
                    phase_table.add_column("Status")
                    phase_table.add_column("Duration")
                    phase_table.add_column("Started")

                    for p in phases:
                        duration = ""
                        if p.started_at and p.completed_at:
                            delta = p.completed_at - p.started_at
                            duration = f"{delta.total_seconds():.1f}s"

                        status_color = {
                            "completed": "green",
                            "failed": "red",
                            "skipped": "yellow",
                            "running": "blue",
                        }.get(p.status, "white")

                        started = p.started_at.strftime("%H:%M:%S") if p.started_at else ""

                        phase_table.add_row(
                            p.phase_name,
                            f"[{status_color}]{p.status}[/{status_color}]",
                            duration,
                            started,
                        )

                    console.print(phase_table)

                    # Timing summary
                    completed = [
                        p for p in phases if p.status == "completed" and p.duration_seconds
                    ]
                    if completed:
                        total = sum(p.duration_seconds for p in completed)
                        sorted_phases = sorted(
                            completed, key=lambda p: p.duration_seconds, reverse=True
                        )

                        console.print("\n[bold]Phase Timing (slowest first):[/bold]")
                        timing_table = RichTable(show_header=True, header_style="bold")
                        timing_table.add_column("Phase")
                        timing_table.add_column("Duration", justify="right")
                        timing_table.add_column("% of Total", justify="right")
                        timing_table.add_column("LLM Calls", justify="right")
                        timing_table.add_column("LLM Tokens", justify="right")

                        for p in sorted_phases[:10]:
                            pct = (p.duration_seconds / total * 100) if total > 0 else 0
                            llm_calls = str(p.llm_calls) if p.llm_calls > 0 else "-"
                            llm_tokens = (
                                f"{p.llm_input_tokens + p.llm_output_tokens:,}"
                                if (p.llm_input_tokens + p.llm_output_tokens) > 0
                                else "-"
                            )
                            timing_table.add_row(
                                p.phase_name,
                                f"{p.duration_seconds:.2f}s",
                                f"{pct:.1f}%",
                                llm_calls,
                                llm_tokens,
                            )

                        console.print(timing_table)
                        console.print(f"\n[cyan]Total:[/cyan] {total:.2f}s")

                        # Show aggregate LLM usage
                        total_llm_calls = sum(p.llm_calls for p in completed)
                        total_llm_tokens = sum(
                            p.llm_input_tokens + p.llm_output_tokens for p in completed
                        )
                        if total_llm_calls > 0:
                            console.print(
                                f"[cyan]Total LLM:[/cyan] {total_llm_calls} calls, {total_llm_tokens:,} tokens"
                            )

                console.print()
    finally:
        manager.close()


@app.command()
def reset(
    output_dir: Annotated[
        Path,
        typer.Argument(
            help="Output directory to reset",
        ),
    ] = Path("./pipeline_output"),
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            "-f",
            help="Skip confirmation prompt",
        ),
    ] = False,
) -> None:
    """Reset pipeline output by deleting databases.

    Removes metadata.db and data.duckdb from the output directory.
    """
    metadata_db = output_dir / "metadata.db"
    duckdb_file = output_dir / "data.duckdb"

    # Check what exists
    files_to_delete = []
    if metadata_db.exists():
        files_to_delete.append(metadata_db)
    if duckdb_file.exists():
        files_to_delete.append(duckdb_file)
    # Also check for WAL files
    for wal_file in output_dir.glob("*.db-wal"):
        files_to_delete.append(wal_file)
    for shm_file in output_dir.glob("*.db-shm"):
        files_to_delete.append(shm_file)

    if not files_to_delete:
        console.print(f"[yellow]No database files found in {output_dir}[/yellow]")
        return

    console.print("\n[bold]Files to delete:[/bold]")
    for f in files_to_delete:
        size_kb = f.stat().st_size / 1024
        console.print(f"  {f.name} ({size_kb:.1f} KB)")

    if not force:
        confirm = typer.confirm("\nDelete these files?")
        if not confirm:
            console.print("[yellow]Cancelled[/yellow]")
            raise typer.Exit(0)

    # Delete files
    for f in files_to_delete:
        f.unlink()
        console.print(f"[green]Deleted {f.name}[/green]")

    console.print("\n[green]Reset complete. Run 'dataraum run' to start fresh.[/green]")


@app.command()
def inspect(
    output_dir: Annotated[
        Path,
        typer.Argument(
            help="Output directory containing pipeline databases",
            exists=True,
            dir_okay=True,
            file_okay=False,
            resolve_path=True,
        ),
    ] = Path("./pipeline_output"),
) -> None:
    """Inspect graph definitions and execution context.

    Shows loaded graphs, applicable filters, and execution context.
    """
    _inspect_impl(output_dir)


def _inspect_impl(output_dir: Path) -> None:
    """Sync implementation of inspect command."""
    from sqlalchemy import select

    from dataraum.analysis.semantic.db_models import SemanticAnnotation
    from dataraum.analysis.typing.db_models import TypeDecision
    from dataraum.core import ConnectionConfig, ConnectionManager
    from dataraum.graphs import GraphLoader
    from dataraum.storage import Column, Source, Table

    config = ConnectionConfig.for_directory(output_dir)

    if not config.sqlite_path.exists():
        console.print(f"[red]No metadata database found at {config.sqlite_path}[/red]")
        raise typer.Exit(1)

    manager = ConnectionManager(config)
    manager.initialize()

    try:
        with manager.session_scope() as session:
            # Check for sources
            sources_result = session.execute(select(Source))
            sources = sources_result.scalars().all()

            if not sources:
                console.print("[yellow]No sources found. Run the pipeline first.[/yellow]")
                return

            # Check for tables
            tables_result = session.execute(select(Table))
            tables = tables_result.scalars().all()

            if not tables:
                console.print("[yellow]No tables found. Run import phase first.[/yellow]")
                return

            console.print("\n[bold]Graph Loader Status[/bold]\n")

            # Load graphs
            loader = GraphLoader()
            graphs = loader.load_all()

            console.print(f"Loaded {len(graphs)} graphs")

            # Separate by type
            filters = loader.get_filter_graphs()
            metrics = loader.get_metric_graphs()

            # Filter graphs table
            if filters:
                console.print(f"\n[cyan]Filter Graphs ({len(filters)}):[/cyan]")
                filter_table = RichTable(show_header=True, header_style="bold")
                filter_table.add_column("ID")
                filter_table.add_column("Name")
                filter_table.add_column("Applies To")

                for g in filters:
                    applies_to = ""
                    if g.metadata.applies_to:
                        if g.metadata.applies_to.semantic_role:
                            applies_to = f"role: {g.metadata.applies_to.semantic_role}"
                        elif g.metadata.applies_to.data_type:
                            applies_to = f"type: {g.metadata.applies_to.data_type}"
                        elif g.metadata.applies_to.column_pattern:
                            pattern = g.metadata.applies_to.column_pattern
                            applies_to = f"pattern: {pattern[:30]}..."
                    filter_table.add_row(g.graph_id, g.metadata.name, applies_to)

                console.print(filter_table)

            # Metric graphs table
            if metrics:
                console.print(f"\n[cyan]Metric Graphs ({len(metrics)}):[/cyan]")
                metric_table = RichTable(show_header=True, header_style="bold")
                metric_table.add_column("ID")
                metric_table.add_column("Name")
                metric_table.add_column("Scope")

                for g in metrics:
                    scope = getattr(g, "scope", None)
                    scope_str = scope.value if scope else ""
                    metric_table.add_row(g.graph_id, g.metadata.name, scope_str)

                console.print(metric_table)

            # Load errors
            errors = loader.get_load_errors()
            if errors:
                console.print(f"\n[red]Load Errors ({len(errors)}):[/red]")
                for err in errors:
                    console.print(f"  - {err}")

            # Build column metadata for filter matching
            console.print("\n[bold]Dataset Filter Coverage[/bold]\n")

            columns_metadata = []
            for table in tables:
                cols_result = session.execute(
                    select(Column).where(Column.table_id == table.table_id)
                )
                columns = cols_result.scalars().all()

                for col in columns:
                    # Get type decision
                    type_result = session.execute(
                        select(TypeDecision).where(TypeDecision.column_id == col.column_id)
                    )
                    type_dec = type_result.scalar_one_or_none()

                    # Get semantic annotation
                    sem_result = session.execute(
                        select(SemanticAnnotation).where(
                            SemanticAnnotation.column_id == col.column_id
                        )
                    )
                    sem_ann = sem_result.scalar_one_or_none()

                    columns_metadata.append(
                        {
                            "column_name": col.column_name,
                            "table_name": table.table_name,
                            "data_type": type_dec.decided_type if type_dec else None,
                            "semantic_role": sem_ann.semantic_role if sem_ann else None,
                            "has_profile": True,
                        }
                    )

            # Get filter summary
            summary = loader.get_quality_filter_summary(columns_metadata)

            console.print(f"Total unique filters: {summary['total_filters']}")
            console.print(f"Filter coverage: {summary['filter_coverage']:.1%}")
            if summary["filter_ids"]:
                console.print(f"Filters applied: {', '.join(summary['filter_ids'])}")

            # Show columns with filters
            filters_by_col = loader.get_filters_for_dataset(columns_metadata)
            columns_with_filters: list[tuple[str, str, list[Any]]] = [
                (
                    str(c["table_name"]),
                    str(c["column_name"]),
                    filters_by_col.get(str(c["column_name"]), []),
                )
                for c in columns_metadata
                if filters_by_col.get(str(c["column_name"]))
            ]

            if columns_with_filters:
                console.print("\n[cyan]Columns with filters:[/cyan]")
                col_table = RichTable(show_header=True, header_style="bold")
                col_table.add_column("Table")
                col_table.add_column("Column")
                col_table.add_column("Filters")

                for tbl_name, col_name, col_filters in columns_with_filters[:15]:
                    filter_names = [f.graph_id for f in col_filters]
                    col_table.add_row(tbl_name, col_name, ", ".join(filter_names))

                console.print(col_table)

                if len(columns_with_filters) > 15:
                    console.print(f"  ... and {len(columns_with_filters) - 15} more")

            # Execution context sample
            console.print("\n[bold]Execution Context (Sample)[/bold]\n")

            if tables:
                from dataraum.graphs.context import (
                    build_execution_context,
                    format_context_for_prompt,
                )

                table_ids = [t.table_id for t in tables[:3]]
                with manager.duckdb_cursor() as cursor:
                    context = build_execution_context(
                        session=session,
                        table_ids=table_ids,
                        duckdb_conn=cursor,
                    )

                console.print(f"Tables in context: {context.total_tables}")
                console.print(f"Columns in context: {context.total_columns}")
                console.print(f"Relationships: {context.total_relationships}")
                console.print(f"Graph pattern: {context.graph_pattern}")

                formatted = format_context_for_prompt(context)
                lines = formatted.split("\n")
                console.print("\n[dim]Formatted context preview:[/dim]")
                for line in lines[:15]:
                    console.print(f"  {line}")
                if len(lines) > 15:
                    console.print(f"  [dim]... ({len(lines) - 15} more lines)[/dim]")

            console.print()
    finally:
        manager.close()


@app.command()
def phases() -> None:
    """List available pipeline phases and their dependencies."""
    from dataraum.pipeline.base import PIPELINE_DAG

    console.print("\n[bold]Pipeline Phases[/bold]\n")

    table = RichTable(show_header=True, header_style="bold")
    table.add_column("Phase")
    table.add_column("Description")
    table.add_column("Dependencies")
    table.add_column("LLM")

    for phase_def in PIPELINE_DAG:
        deps = ", ".join(phase_def.dependencies) if phase_def.dependencies else "-"
        llm = "Yes" if phase_def.requires_llm else "No"
        table.add_row(phase_def.name, phase_def.description, deps, llm)

    console.print(table)
    console.print()


@app.command()
def contracts(
    output_dir: Annotated[
        Path,
        typer.Argument(
            help="Output directory containing pipeline databases",
            exists=True,
            dir_okay=True,
            file_okay=False,
            resolve_path=True,
        ),
    ] = Path("./pipeline_output"),
    contract: Annotated[
        str | None,
        typer.Option(
            "--contract",
            "-c",
            help="Evaluate a specific contract (default: all)",
        ),
    ] = None,
    verbose: Annotated[
        int,
        typer.Option(
            "--verbose",
            "-v",
            count=True,
            help="Increase logging verbosity (-v=INFO, -vv=DEBUG)",
        ),
    ] = 0,
) -> None:
    """Evaluate data quality contracts.

    Shows which contracts the data meets and which it doesn't.
    Use --contract to evaluate a specific contract and see details.

    Examples:

        dataraum contracts ./pipeline_output

        dataraum contracts ./pipeline_output --contract executive_dashboard
    """
    setup_logging(verbosity=verbose)
    _contracts_impl(output_dir, contract)


def _contracts_impl(output_dir: Path, contract_name: str | None) -> None:
    """Sync implementation of contracts command."""
    from sqlalchemy import select

    from dataraum.core import ConnectionConfig, ConnectionManager
    from dataraum.entropy.analysis.aggregator import ColumnSummary, EntropyAggregator
    from dataraum.entropy.contracts import (
        ConfidenceLevel,
        evaluate_all_contracts,
        evaluate_contract,
        get_contract,
        list_contracts,
    )
    from dataraum.entropy.core.storage import EntropyRepository
    from dataraum.storage import Source, Table

    config = ConnectionConfig.for_directory(output_dir)

    if not config.sqlite_path.exists():
        console.print(f"[red]No metadata database found at {config.sqlite_path}[/red]")
        raise typer.Exit(1)

    manager = ConnectionManager(config)
    manager.initialize()

    try:
        with manager.session_scope() as session:
            # Get sources
            sources_result = session.execute(select(Source))
            sources = sources_result.scalars().all()

            if not sources:
                console.print("[yellow]No sources found in database[/yellow]")
                return

            # Get first source (or could iterate)
            source = sources[0]

            # Get tables for this source
            tables_result = session.execute(
                select(Table).where(Table.source_id == source.source_id)
            )
            tables = tables_result.scalars().all()

            if not tables:
                console.print("[yellow]No tables found. Run pipeline first.[/yellow]")
                return

            table_ids = [t.table_id for t in tables]

            console.print(f"\n[bold]Contract Evaluation[/bold] - {source.name}\n")

            # Build column summaries for contract evaluation
            repo = EntropyRepository(session)
            aggregator = EntropyAggregator()

            typed_table_ids = repo.get_typed_table_ids(table_ids)
            column_summaries: dict[str, ColumnSummary] = {}
            compound_risks: list[Any] = []

            if typed_table_ids:
                table_map, column_map = repo.get_table_column_mapping(typed_table_ids)
                entropy_objects = repo.load_for_tables(typed_table_ids, enforce_typed=True)

                if entropy_objects:
                    column_summaries, _ = aggregator.summarize_columns_by_table(
                        entropy_objects=entropy_objects,
                        table_map=table_map,
                        column_map=column_map,
                    )
                    for summary in column_summaries.values():
                        compound_risks.extend(summary.compound_risks)

            if contract_name:
                # Evaluate specific contract
                profile = get_contract(contract_name)
                if profile is None:
                    console.print(f"[red]Contract not found: {contract_name}[/red]")
                    console.print("\nAvailable contracts:")
                    for c in list_contracts():
                        console.print(f"  - {c['name']}: {c['description']}")
                    raise typer.Exit(1)

                evaluation = evaluate_contract(column_summaries, contract_name, compound_risks)
                _print_contract_detail(evaluation, profile)

            else:
                # Evaluate all contracts
                evaluations = evaluate_all_contracts(column_summaries, compound_risks)

                def _get_threshold(name: str) -> float:
                    """Get threshold for sorting, defaulting to 1.0 if not found."""
                    c = get_contract(name)
                    return c.overall_threshold if c else 1.0

                # Sort by strictness (stricter contracts have lower thresholds)
                sorted_evals = sorted(
                    evaluations.items(),
                    key=lambda x: _get_threshold(x[0]),
                    reverse=True,  # Most lenient first
                )

                console.print("[bold]Contract Compliance:[/bold]\n")

                table = RichTable(show_header=True, header_style="bold")
                table.add_column("Status")
                table.add_column("Contract")
                table.add_column("Description")
                table.add_column("Issues", justify="right")

                for name, evaluation in sorted_evals:
                    emoji = evaluation.confidence_level.emoji
                    label = evaluation.confidence_level.label

                    # Color based on status
                    if evaluation.confidence_level == ConfidenceLevel.GREEN:
                        status = f"[green]{emoji} {label}[/green]"
                    elif evaluation.confidence_level == ConfidenceLevel.YELLOW:
                        status = f"[yellow]{emoji} {label}[/yellow]"
                    elif evaluation.confidence_level == ConfidenceLevel.ORANGE:
                        status = f"[yellow]{emoji} {label}[/yellow]"
                    else:
                        status = f"[red]{emoji} {label}[/red]"

                    profile = get_contract(name)
                    if profile:
                        desc = (
                            profile.description[:40] + "..."
                            if len(profile.description) > 40
                            else profile.description
                        )
                    else:
                        desc = ""

                    issues = len(evaluation.violations) + len(evaluation.warnings)
                    issue_str = str(issues) if issues > 0 else "-"

                    table.add_row(status, name, desc, issue_str)

                console.print(table)

                # Summary
                passing = [e for e in evaluations.values() if e.is_compliant]
                console.print(
                    f"\n[cyan]Passing:[/cyan] {len(passing)}/{len(evaluations)} contracts"
                )

                if passing:
                    # Find strictest passing
                    strictest = min(
                        passing,
                        key=lambda e: _get_threshold(e.contract_name),
                    )
                    console.print(f"[cyan]Strictest passing:[/cyan] {strictest.contract_name}")

                console.print(
                    "\nUse [cyan]--contract NAME[/cyan] to see details for a specific contract."
                )

            console.print()
    finally:
        manager.close()


def _print_contract_detail(
    evaluation: ContractEvaluation,
    profile: ContractProfile,
) -> None:
    """Print detailed evaluation for a single contract."""
    from dataraum.entropy.contracts import ConfidenceLevel

    # Header with status
    emoji = evaluation.confidence_level.emoji
    label = evaluation.confidence_level.label

    if evaluation.confidence_level == ConfidenceLevel.GREEN:
        status_color = "green"
    elif evaluation.confidence_level in (ConfidenceLevel.YELLOW, ConfidenceLevel.ORANGE):
        status_color = "yellow"
    else:
        status_color = "red"

    console.print(f"[bold]Contract:[/bold] {profile.display_name}")
    console.print(f"[bold]Status:[/bold] [{status_color}]{emoji} {label}[/{status_color}]")
    console.print(f"[dim]{profile.description}[/dim]\n")

    # Overall score
    overall_status = "✓" if evaluation.overall_score <= profile.overall_threshold else "✗"
    console.print(
        f"[bold]Overall Score:[/bold] {evaluation.overall_score:.2f} "
        f"(threshold: {profile.overall_threshold}) {overall_status}"
    )

    # Dimension breakdown
    console.print("\n[bold]Dimension Scores:[/bold]")

    dim_table = RichTable(show_header=True, header_style="bold")
    dim_table.add_column("Dimension")
    dim_table.add_column("Score", justify="right")
    dim_table.add_column("Threshold", justify="right")
    dim_table.add_column("Status")

    for dim, threshold in profile.dimension_thresholds.items():
        score = evaluation.dimension_scores.get(dim, 0.0)

        if score <= threshold:
            status = "[green]✓ PASS[/green]"
        elif score <= threshold * 1.2:
            status = "[yellow]⚠ WARN[/yellow]"
        else:
            status = "[red]✗ FAIL[/red]"

        dim_table.add_row(
            dim,
            f"{score:.2f}",
            f"{threshold:.2f}",
            status,
        )

    console.print(dim_table)

    # Violations
    if evaluation.violations:
        console.print("\n[bold red]Violations:[/bold red]")
        for v in evaluation.violations:
            if v.dimension:
                console.print(f"  [red]✗[/red] {v.dimension}: {v.actual:.2f} > {v.max_allowed:.2f}")
                if v.affected_columns:
                    cols = ", ".join(v.affected_columns[:5])
                    if len(v.affected_columns) > 5:
                        cols += f" (+{len(v.affected_columns) - 5} more)"
                    console.print(f"    Affected: {cols}")
            elif v.condition:
                console.print(f"  [red]✗[/red] {v.details}")
            else:
                console.print(f"  [red]✗[/red] {v.details}")

    # Warnings
    if evaluation.warnings:
        console.print("\n[bold yellow]Warnings:[/bold yellow]")
        for w in evaluation.warnings:
            console.print(f"  [yellow]⚠[/yellow] {w.details}")

    # Path to compliance
    if not evaluation.is_compliant and evaluation.worst_dimension:
        console.print("\n[bold]Path to Compliance:[/bold]")
        console.print(
            f"  Focus on: [cyan]{evaluation.worst_dimension}[/cyan] "
            f"(score: {evaluation.worst_dimension_score:.2f})"
        )
        console.print(f"  Estimated effort: {evaluation.estimated_effort_to_comply}")


@app.command()
def query(
    question: Annotated[
        str,
        typer.Argument(
            help="Natural language question to answer",
        ),
    ],
    output_dir: Annotated[
        Path,
        typer.Option(
            "--output",
            "-o",
            help="Output directory containing pipeline databases",
            exists=True,
            dir_okay=True,
            file_okay=False,
            resolve_path=True,
        ),
    ] = Path("./pipeline_output"),
    contract: Annotated[
        str | None,
        typer.Option(
            "--contract",
            "-c",
            help="Contract to evaluate against (e.g., 'executive_dashboard')",
        ),
    ] = None,
    auto_contract: Annotated[
        bool,
        typer.Option(
            "--auto-contract",
            help="Automatically select the strictest passing contract",
        ),
    ] = False,
    show_sql: Annotated[
        bool,
        typer.Option(
            "--show-sql",
            help="Show the generated SQL",
        ),
    ] = False,
    ephemeral: Annotated[
        bool,
        typer.Option(
            "--ephemeral",
            help="Don't save this query to the library (default: saves successful queries)",
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
) -> None:
    """Ask a question about the data using natural language.

    The Query Agent converts your question into SQL and returns results
    with a confidence level based on data quality.

    By default, successful queries are saved to the query library for future
    reuse. Use --ephemeral to skip saving.

    Examples:

        dataraum query "What was total revenue?" -o ./pipeline_output

        dataraum query "Show sales by region" -o ./output --contract executive_dashboard

        dataraum query "Monthly trend" -o ./output --auto-contract --show-sql

        dataraum query "Quick test" -o ./output --ephemeral
    """
    setup_logging(verbosity=verbose)
    _query_impl(question, output_dir, contract, auto_contract, show_sql, ephemeral)


def _query_impl(
    question: str,
    output_dir: Path,
    contract_name: str | None,
    auto_contract: bool,
    show_sql: bool,
    ephemeral: bool,
) -> None:
    """Sync implementation of query command."""
    from sqlalchemy import select

    from dataraum.core import ConnectionConfig, ConnectionManager
    from dataraum.entropy.contracts import ConfidenceLevel
    from dataraum.query import answer_question
    from dataraum.storage import Source, Table

    config = ConnectionConfig.for_directory(output_dir)

    if not config.sqlite_path.exists():
        console.print(f"[red]No metadata database found at {config.sqlite_path}[/red]")
        raise typer.Exit(1)

    manager = ConnectionManager(config)
    manager.initialize()

    try:
        with manager.session_scope() as session:
            # Get sources
            sources_result = session.execute(select(Source))
            sources = sources_result.scalars().all()

            if not sources:
                console.print("[yellow]No sources found in database[/yellow]")
                return

            source = sources[0]

            # Get tables
            tables_result = session.execute(
                select(Table).where(Table.source_id == source.source_id)
            )
            tables = tables_result.scalars().all()

            if not tables:
                console.print("[yellow]No tables found. Run pipeline first.[/yellow]")
                return

            # Get DuckDB cursor (properly managed context)
            with manager.duckdb_cursor() as cursor:
                # Call the query agent
                result = answer_question(
                    question=question,
                    session=session,
                    duckdb_conn=cursor,
                    source_id=source.source_id,
                    contract=contract_name,
                    auto_contract=auto_contract,
                    manager=manager,
                    ephemeral=ephemeral,
                )

            if not result.success or not result.value:
                console.print(f"[red]Error: {result.error}[/red]")
                raise typer.Exit(1)

            query_result = result.value

            # Display confidence header
            emoji = query_result.confidence_level.emoji
            label = query_result.confidence_level.label
            contract_display = query_result.contract or "default"

            if query_result.confidence_level == ConfidenceLevel.GREEN:
                status_color = "green"
            elif query_result.confidence_level == ConfidenceLevel.YELLOW:
                status_color = "yellow"
            elif query_result.confidence_level == ConfidenceLevel.ORANGE:
                status_color = "yellow"
            else:
                status_color = "red"

            console.print(
                f"\n[{status_color}]{emoji} Data Quality: {label}[/{status_color}] "
                f"for [cyan]{contract_display}[/cyan]\n"
            )

            # Display answer
            console.print(query_result.answer)

            # Display data table if available
            if query_result.data and query_result.columns and len(query_result.data) <= 50:
                console.print()
                data_table = RichTable(show_header=True, header_style="bold")
                for col in query_result.columns:
                    data_table.add_column(col)

                for row in query_result.data[:50]:
                    data_table.add_row(*[str(row.get(c, "")) for c in query_result.columns])

                console.print(data_table)

                if len(query_result.data) > 50:
                    console.print(f"[dim]... showing 50 of {len(query_result.data)} rows[/dim]")

            # Display SQL if requested
            if show_sql and query_result.sql:
                console.print("\n[bold]Generated SQL:[/bold]")
                console.print(f"[dim]{query_result.sql}[/dim]")

            # Display assumptions
            if query_result.assumptions:
                console.print("\n[bold]Assumptions:[/bold]")
                for a in query_result.assumptions:
                    console.print(f"  - {a.assumption} ([dim]{a.basis.value}[/dim])")

            # Display warnings for non-green confidence
            if query_result.confidence_level in (
                ConfidenceLevel.ORANGE,
                ConfidenceLevel.RED,
            ):
                if query_result.contract_evaluation:
                    eval_ = query_result.contract_evaluation
                    if eval_.violations:
                        console.print("\n[bold yellow]Quality Issues:[/bold yellow]")
                        for v in eval_.violations[:5]:
                            if v.dimension:
                                console.print(
                                    f"  [yellow]⚠[/yellow] {v.dimension}: "
                                    f"{v.actual:.2f} (threshold: {v.max_allowed:.2f})"
                                )
                            elif v.details:
                                console.print(f"  [yellow]⚠[/yellow] {v.details}")

            # Show path to compliance for blocked queries
            if query_result.confidence_level == ConfidenceLevel.RED:
                console.print("\n[bold]To improve data quality:[/bold]")
                console.print(
                    f"  Run: [cyan]dataraum contracts {output_dir} --contract "
                    f"{query_result.contract}[/cyan]"
                )

            console.print()
    finally:
        manager.close()


@app.command()
def entropy(
    output_dir: Annotated[
        Path,
        typer.Argument(
            help="Output directory containing pipeline databases",
            exists=True,
            dir_okay=True,
            file_okay=False,
            resolve_path=True,
        ),
    ] = Path("./pipeline_output"),
    table: Annotated[
        str | None,
        typer.Option(
            "--table",
            "-t",
            help="Filter to a specific table",
        ),
    ] = None,
    status: Annotated[
        str | None,
        typer.Option(
            "--status",
            "-s",
            help="Filter by readiness status (ready, investigate, blocked)",
        ),
    ] = None,
    issues: Annotated[
        bool,
        typer.Option(
            "--issues",
            "-i",
            help="Show all issues in linter-style format",
        ),
    ] = False,
    verbose: Annotated[
        int,
        typer.Option(
            "--verbose",
            "-v",
            count=True,
            help="Show more detail (-v for explanations, -vv for assumptions/actions)",
        ),
    ] = 0,
) -> None:
    """Explore entropy metrics and data quality issues.

    Shows a summary of data uncertainty across dimensions, helping developers
    understand what assumptions the system makes and what to fix.

    Examples:

        dataraum entropy ./pipeline_output

        dataraum entropy ./output -v

        dataraum entropy ./output --table master_txn_table

        dataraum entropy ./output --status investigate

        dataraum entropy ./output --issues
    """
    _entropy_impl(output_dir, table, status, issues, verbose)


def _entropy_impl(
    output_dir: Path,
    table_filter: str | None,
    status_filter: str | None,
    show_issues: bool,
    verbose: int,
) -> None:
    """Implementation of entropy command."""
    from sqlalchemy import select

    from dataraum.core import ConnectionConfig, ConnectionManager
    from dataraum.entropy.db_models import (
        EntropyInterpretationRecord,
        EntropyObjectRecord,
        EntropySnapshotRecord,
    )
    from dataraum.storage import Source

    config = ConnectionConfig.for_directory(output_dir)

    if not config.sqlite_path.exists():
        console.print(f"[red]No metadata database found at {config.sqlite_path}[/red]")
        raise typer.Exit(1)

    manager = ConnectionManager(config)
    manager.initialize()

    try:
        with manager.session_scope() as session:
            # Get source
            sources_result = session.execute(select(Source))
            sources = sources_result.scalars().all()

            if not sources:
                console.print("[yellow]No sources found in database[/yellow]")
                return

            source = sources[0]

            # Get snapshot for summary
            snapshot_result = session.execute(
                select(EntropySnapshotRecord)
                .where(EntropySnapshotRecord.source_id == source.source_id)
                .order_by(EntropySnapshotRecord.snapshot_at.desc())
                .limit(1)
            )
            snapshot = snapshot_result.scalar_one_or_none()

            if not snapshot:
                console.print("[yellow]No entropy data found. Run entropy phase first.[/yellow]")
                return

            # Build interpretation query with filters
            interp_query = select(EntropyInterpretationRecord).where(
                EntropyInterpretationRecord.source_id == source.source_id
            )

            if table_filter:
                interp_query = interp_query.where(
                    EntropyInterpretationRecord.table_name == table_filter
                )

            if status_filter:
                interp_query = interp_query.where(
                    EntropyInterpretationRecord.readiness == status_filter
                )

            interp_query = interp_query.order_by(EntropyInterpretationRecord.composite_score.desc())

            interp_result = session.execute(interp_query)
            interpretations = interp_result.scalars().all()

            # Get entropy objects for issue counts
            obj_query = select(EntropyObjectRecord).where(
                EntropyObjectRecord.source_id == source.source_id
            )
            obj_result = session.execute(obj_query)
            entropy_objects = obj_result.scalars().all()

            if show_issues:
                _print_issues_view(interpretations, entropy_objects, verbose)
            else:
                _print_summary_view(
                    source.name, snapshot, interpretations, entropy_objects, table_filter, verbose
                )

            console.print()
    finally:
        manager.close()


def _print_summary_view(
    source_name: str,
    snapshot: Any,
    interpretations: Sequence[Any],
    entropy_objects: Sequence[Any],
    table_filter: str | None,
    verbose: int,
) -> None:
    """Print summary view of entropy state."""
    # Header
    title = f"Entropy Summary - {source_name}"
    if table_filter:
        title += f" (table: {table_filter})"
    console.print(f"\n[bold]{title}[/bold]\n")

    # Overall readiness with color
    readiness_colors = {
        "ready": "green",
        "investigate": "yellow",
        "blocked": "red",
    }
    readiness_icons = {
        "ready": "🟢",
        "investigate": "🟡",
        "blocked": "🔴",
    }
    color = readiness_colors.get(snapshot.overall_readiness, "white")
    icon = readiness_icons.get(snapshot.overall_readiness, "⚪")

    console.print(
        f"[{color}]{icon} Overall Readiness: {snapshot.overall_readiness.upper()}[/{color}]"
    )
    console.print(f"Composite Score: {snapshot.avg_composite_score:.3f}")
    console.print()

    # Dimension breakdown
    console.print("[bold]Entropy by Dimension:[/bold]")
    dim_table = RichTable(show_header=True, header_style="bold", box=None)
    dim_table.add_column("Dimension")
    dim_table.add_column("Score", justify="right")
    dim_table.add_column("Bar")

    dimensions = [
        ("Structural", snapshot.avg_structural_entropy),
        ("Semantic", snapshot.avg_semantic_entropy),
        ("Value", snapshot.avg_value_entropy),
        ("Computational", snapshot.avg_computational_entropy),
    ]

    for name, score in dimensions:
        bar_len = int(score * 20)
        bar = "█" * bar_len + "░" * (20 - bar_len)
        if score > 0.3:
            bar = f"[red]{bar}[/red]"
        elif score > 0.15:
            bar = f"[yellow]{bar}[/yellow]"
        else:
            bar = f"[green]{bar}[/green]"
        dim_table.add_row(name, f"{score:.3f}", bar)

    console.print(dim_table)
    console.print()

    # Issue counts
    high_entropy = [i for i in interpretations if i.composite_score > 0.2]
    investigate = [i for i in interpretations if i.readiness == "investigate"]
    blocked = [i for i in interpretations if i.readiness == "blocked"]

    console.print("[bold]Issue Summary:[/bold]")
    console.print(f"  Total columns analyzed: {len(interpretations)}")
    console.print(f"  High entropy (>0.2): {len(high_entropy)}")
    if investigate:
        console.print(f"  [yellow]Needs investigation: {len(investigate)}[/yellow]")
    if blocked:
        console.print(f"  [red]Blocked: {len(blocked)}[/red]")
    console.print()

    # Top high-entropy columns
    if interpretations:
        show_count = 10 if verbose else 5
        top_columns = interpretations[:show_count]

        console.print(f"[bold]Top {len(top_columns)} High-Entropy Columns:[/bold]")
        col_table = RichTable(show_header=True, header_style="bold")
        col_table.add_column("Table")
        col_table.add_column("Column")
        col_table.add_column("Score", justify="right")
        col_table.add_column("Status")

        if verbose >= 1:
            col_table.add_column("Top Issue")

        for interp in top_columns:
            status_color = readiness_colors.get(interp.readiness, "white")
            status_icon = readiness_icons.get(interp.readiness, "⚪")

            row = [
                interp.table_name,
                interp.column_name or "(table-level)",
                f"{interp.composite_score:.3f}",
                f"[{status_color}]{status_icon}[/{status_color}]",
            ]

            if verbose >= 1:
                # Show first line of explanation as top issue
                explanation = interp.explanation or ""
                first_line = explanation.split(".")[0] if explanation else "-"
                if len(first_line) > 50:
                    first_line = first_line[:47] + "..."
                row.append(f"[dim]{first_line}[/dim]")

            col_table.add_row(*row)

        console.print(col_table)

        if len(interpretations) > show_count:
            console.print(f"[dim]  ... and {len(interpretations) - show_count} more columns[/dim]")

        # Verbose: show explanations
        if verbose >= 2 and top_columns:
            console.print()
            console.print("[bold]Details:[/bold]")
            for interp in top_columns:
                label = (
                    f"{interp.table_name}.{interp.column_name}"
                    if interp.column_name
                    else f"{interp.table_name} (table-level)"
                )
                console.print(f"\n[cyan]{label}[/cyan]")
                console.print(f"  {interp.explanation}")

                if interp.assumptions_json:
                    assumptions = interp.assumptions_json
                    console.print(f"  [bold]Assumptions ({len(assumptions)}):[/bold]")
                    for a in assumptions:
                        console.print(
                            f"    - [{a.get('dimension', '')}] {a.get('assumption_text', '')}"
                        )
                        console.print(
                            f"      Confidence: {a.get('confidence', '')} | Impact: {a.get('impact', '')}"
                        )

                if interp.resolution_actions_json:
                    actions = interp.resolution_actions_json
                    console.print(f"  [bold]Actions ({len(actions)}):[/bold]")
                    for a in actions:
                        priority = a.get("priority", "medium")
                        action_name = a.get("action", "")
                        description = a.get("description", "")
                        effort = a.get("effort", "")
                        console.print(f"    - ({priority}) {action_name}: {description}")
                        console.print(f"      Effort: {effort}")


def _print_issues_view(
    interpretations: Sequence[Any],
    entropy_objects: Sequence[Any],
    verbose: int,
) -> None:
    """Print linter-style issues view."""
    console.print("\n[bold]Entropy Issues[/bold]\n")

    if not interpretations:
        console.print("[green]No issues found.[/green]")
        return

    # Group by severity
    blocked = [i for i in interpretations if i.readiness == "blocked"]
    investigate = [i for i in interpretations if i.readiness == "investigate"]
    high_entropy = [
        i for i in interpretations if i.readiness == "ready" and i.composite_score > 0.2
    ]

    def print_issue_group(title: str, items: list[Any], color: str, icon: str) -> None:
        if not items:
            return
        console.print(f"[{color}][bold]{icon} {title} ({len(items)})[/bold][/{color}]")
        for item in items:
            loc = (
                f"{item.table_name}.{item.column_name}"
                if item.column_name
                else f"{item.table_name} (table-level)"
            )
            score = f"{item.composite_score:.3f}"

            # Get top dimension issue
            top_issue = ""
            if item.assumptions_json and len(item.assumptions_json) > 0:
                first_assumption = item.assumptions_json[0]
                dim = first_assumption.get("dimension", "")
                top_issue = f" [{dim}]" if dim else ""

            console.print(f"  [{color}]{icon}[/{color}] {loc}: {score}{top_issue}")

            if verbose == 1:
                # Show first assumption (compact)
                if item.assumptions_json:
                    assumption_text = item.assumptions_json[0].get("assumption_text", "")
                    if len(assumption_text) > 70:
                        assumption_text = assumption_text[:67] + "..."
                    console.print(f"      [dim]→ {assumption_text}[/dim]")

            if verbose >= 2:
                # Show all assumptions
                if item.assumptions_json:
                    for a in item.assumptions_json:
                        dim = a.get("dimension", "")
                        text = a.get("assumption_text", "")
                        console.print(f"      [dim]→ [{dim}] {text}[/dim]")
                # Show all recommended actions
                if item.resolution_actions_json:
                    for action in item.resolution_actions_json:
                        action_desc = action.get("description", "")
                        priority = action.get("priority", "medium")
                        effort = action.get("effort", "")
                        console.print(f"      [dim]Fix ({priority}, {effort}): {action_desc}[/dim]")

        console.print()

    print_issue_group("BLOCKED", blocked, "red", "✗")
    print_issue_group("INVESTIGATE", investigate, "yellow", "⚠")
    print_issue_group("HIGH ENTROPY", high_entropy, "cyan", "●")

    # Summary
    total = len(blocked) + len(investigate) + len(high_entropy)
    console.print(f"[bold]Total: {total} issues[/bold]")
    if blocked:
        console.print(f"  [red]✗ {len(blocked)} blocked[/red]")
    if investigate:
        console.print(f"  [yellow]⚠ {len(investigate)} need investigation[/yellow]")
    if high_entropy:
        console.print(f"  [cyan]● {len(high_entropy)} high entropy[/cyan]")


def main() -> None:
    """Entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()
