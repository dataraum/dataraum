"""CLI for dataraum pipeline.

Provides commands for running, inspecting, and monitoring the pipeline.

Usage:
    dataraum run /path/to/data
    dataraum run /path/to/data --phase import
    dataraum status ./pipeline_output
    dataraum inspect ./pipeline_output
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console
from rich.table import Table as RichTable

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
) -> None:
    """Run the pipeline on CSV data.

    Examples:

        dataraum run /path/to/csv/directory

        dataraum run /path/to/file.csv --output ./my_output

        dataraum run /path/to/data --phase import --skip-llm
    """
    from dataraum_context.pipeline.runner import RunConfig
    from dataraum_context.pipeline.runner import run as run_pipeline

    config = RunConfig(
        source_path=source,
        output_dir=output,
        source_name=name,
        target_phase=phase,
        skip_llm=skip_llm,
        verbose=not quiet,
    )

    result = asyncio.run(run_pipeline(config))
    raise typer.Exit(0 if result.success else 1)


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
    asyncio.run(_status_async(output_dir))


async def _status_async(output_dir: Path) -> None:
    """Async implementation of status command."""
    from sqlalchemy import func, select

    from dataraum_context.core import ConnectionConfig, ConnectionManager
    from dataraum_context.pipeline.db_models import PhaseCheckpoint
    from dataraum_context.storage import Column, Source, Table

    config = ConnectionConfig.for_directory(output_dir)

    # Check if databases exist
    if not config.sqlite_path.exists():
        console.print(f"[red]No metadata database found at {config.sqlite_path}[/red]")
        raise typer.Exit(1)

    manager = ConnectionManager(config)
    await manager.initialize()

    try:
        async with manager.session_scope() as session:
            # Get sources
            sources_result = await session.execute(select(Source))
            sources = sources_result.scalars().all()

            if not sources:
                console.print("[yellow]No sources found in database[/yellow]")
                return

            console.print(f"\n[bold]Pipeline Status[/bold] - {output_dir}\n")

            for source in sources:
                # Source info
                console.print(f"[cyan]Source:[/cyan] {source.name} ({source.source_id[:8]}...)")

                # Tables
                tables_result = await session.execute(
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
                columns_result = await session.execute(
                    select(func.count())
                    .select_from(Column)
                    .join(Table)
                    .where(Table.source_id == source.source_id)
                )
                columns_count = columns_result.scalar() or 0
                console.print(f"Total columns: {columns_count}")

                # Phase executions
                phases_result = await session.execute(
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

                console.print()
    finally:
        await manager.close()


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
    asyncio.run(_inspect_async(output_dir))


async def _inspect_async(output_dir: Path) -> None:
    """Async implementation of inspect command."""
    from sqlalchemy import select

    from dataraum_context.analysis.semantic.db_models import SemanticAnnotation
    from dataraum_context.analysis.typing.db_models import TypeDecision
    from dataraum_context.core import ConnectionConfig, ConnectionManager
    from dataraum_context.graphs import GraphLoader
    from dataraum_context.storage import Column, Source, Table

    config = ConnectionConfig.for_directory(output_dir)

    if not config.sqlite_path.exists():
        console.print(f"[red]No metadata database found at {config.sqlite_path}[/red]")
        raise typer.Exit(1)

    manager = ConnectionManager(config)
    await manager.initialize()

    try:
        async with manager.session_scope() as session:
            # Check for sources
            sources_result = await session.execute(select(Source))
            sources = sources_result.scalars().all()

            if not sources:
                console.print("[yellow]No sources found. Run the pipeline first.[/yellow]")
                return

            # Check for tables
            tables_result = await session.execute(select(Table))
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
                cols_result = await session.execute(
                    select(Column).where(Column.table_id == table.table_id)
                )
                columns = cols_result.scalars().all()

                for col in columns:
                    # Get type decision
                    type_result = await session.execute(
                        select(TypeDecision).where(TypeDecision.column_id == col.column_id)
                    )
                    type_dec = type_result.scalar_one_or_none()

                    # Get semantic annotation
                    sem_result = await session.execute(
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
                from dataraum_context.graphs.context import (
                    build_execution_context,
                    format_context_for_prompt,
                )

                table_ids = [t.table_id for t in tables[:3]]
                context = await build_execution_context(
                    session=session,
                    table_ids=table_ids,
                    duckdb_conn=manager.duckdb_conn,
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
        await manager.close()


@app.command()
def phases() -> None:
    """List available pipeline phases and their dependencies."""
    from dataraum_context.pipeline.base import PIPELINE_DAG

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


def main() -> None:
    """Entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()
