"""Dev subcommand - developer utilities for pipeline debugging."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.table import Table as RichTable

from dataraum.cli.common import OutputDirArg, VerticalOption, console

app = typer.Typer(
    name="dev",
    help="Developer utilities (phases, inspect, reset).",
    no_args_is_help=True,
)


@app.command()
def phases(
    reset: Annotated[
        str | None,
        typer.Option("--reset", help="Reset a specific phase (delete its data and checkpoint)"),
    ] = None,
    output_dir: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Pipeline output directory"),
    ] = None,
) -> None:
    """List available pipeline phases and their dependencies."""
    if reset:
        _reset_phase(reset, output_dir)
        return

    from dataraum.pipeline.registry import get_registry

    console.print("\n[bold]Pipeline Phases[/bold]\n")

    table = RichTable(show_header=True, header_style="bold")
    table.add_column("Phase")
    table.add_column("Description")
    table.add_column("Dependencies")

    registry = get_registry()
    for name, cls in registry.items():
        instance = cls()
        deps = ", ".join(instance.dependencies) if instance.dependencies else "-"
        table.add_row(name, instance.description, deps)

    console.print(table)
    console.print()


@app.command()
def inspect(
    output_dir: OutputDirArg = Path("./pipeline_output"),
    vertical: VerticalOption = "finance",
) -> None:
    """Inspect graph definitions and execution context.

    Shows loaded graphs, applicable filters, and execution context.
    """
    from typing import Any

    from sqlalchemy import select

    from dataraum.analysis.semantic.db_models import SemanticAnnotation
    from dataraum.analysis.typing.db_models import TypeDecision
    from dataraum.cli.common import get_manager
    from dataraum.graphs import GraphLoader
    from dataraum.storage import Column, Source, Table

    manager = get_manager(output_dir)

    try:
        with manager.session_scope() as session:
            sources_result = session.execute(select(Source))
            sources = sources_result.scalars().all()

            if not sources:
                console.print("[yellow]No sources found. Run the pipeline first.[/yellow]")
                return

            tables_result = session.execute(select(Table))
            tables = tables_result.scalars().all()

            if not tables:
                console.print("[yellow]No tables found. Run import phase first.[/yellow]")
                return

            console.print("\n[bold]Graph Loader Status[/bold]\n")

            loader = GraphLoader(vertical=vertical)
            graphs = loader.load_all()
            console.print(f"Loaded {len(graphs)} graphs")

            filters = loader.get_filter_graphs()
            metrics = loader.get_metric_graphs()

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

            errors = loader.get_load_errors()
            if errors:
                console.print(f"\n[red]Load Errors ({len(errors)}):[/red]")
                for err in errors:
                    console.print(f"  - {err}")

            console.print("\n[bold]Dataset Filter Coverage[/bold]\n")

            columns_metadata: list[dict[str, Any]] = []
            for table_obj in tables:
                cols_result = session.execute(
                    select(Column).where(Column.table_id == table_obj.table_id)
                )
                columns = cols_result.scalars().all()

                for col in columns:
                    type_result = session.execute(
                        select(TypeDecision).where(TypeDecision.column_id == col.column_id)
                    )
                    type_dec = type_result.scalar_one_or_none()

                    sem_result = session.execute(
                        select(SemanticAnnotation).where(
                            SemanticAnnotation.column_id == col.column_id
                        )
                    )
                    sem_ann = sem_result.scalar_one_or_none()

                    columns_metadata.append(
                        {
                            "column_name": col.column_name,
                            "table_name": table_obj.table_name,
                            "data_type": type_dec.decided_type if type_dec else None,
                            "semantic_role": sem_ann.semantic_role if sem_ann else None,
                            "has_profile": True,
                        }
                    )

            summary = loader.get_quality_filter_summary(columns_metadata)

            console.print(f"Total unique filters: {summary['total_filters']}")
            console.print(f"Filter coverage: {summary['filter_coverage']:.1%}")
            if summary["filter_ids"]:
                console.print(f"Filters applied: {', '.join(summary['filter_ids'])}")

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

            console.print("\n[bold]Execution Context (Sample)[/bold]\n")

            if tables:
                from dataraum.graphs.context import (
                    build_execution_context,
                    format_metadata_document,
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

                formatted = format_metadata_document(context)
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
def context(
    output_dir: OutputDirArg = Path("./pipeline_output"),
) -> None:
    """Print the full metadata document that agents receive.

    Shows exactly what the query and graph agents see when generating SQL.
    Requires a completed pipeline run (at minimum through the typing phase).
    """
    from sqlalchemy import select

    from dataraum.core.connections import get_manager_for_directory
    from dataraum.graphs.context import build_execution_context, format_metadata_document
    from dataraum.storage import Source, Table

    try:
        manager = get_manager_for_directory(output_dir)
    except FileNotFoundError:
        console.print(f"[red]No pipeline output found at {output_dir}[/red]")
        raise typer.Exit(1) from None

    try:
        with manager.session_scope() as session:
            source = session.execute(
                select(Source).order_by(Source.created_at).limit(1)
            ).scalar_one_or_none()
            if not source:
                console.print("[red]No sources found. Run the pipeline first.[/red]")
                raise typer.Exit(1)

            tables = (
                session.execute(
                    select(Table).where(
                        Table.source_id == source.source_id, Table.layer == "typed"
                    )
                )
                .scalars()
                .all()
            )
            if not tables:
                console.print("[red]No typed tables found. Run at least the typing phase.[/red]")
                raise typer.Exit(1)

            table_ids = [t.table_id for t in tables]

            with manager.duckdb_cursor() as cursor:
                ctx = build_execution_context(
                    session=session,
                    table_ids=table_ids,
                    duckdb_conn=cursor,
                )

            document = format_metadata_document(ctx, source_name=source.name)
            console.print(document)
    finally:
        manager.close()


@app.command()
def reset(
    output_dir: Annotated[
        Path,
        typer.Argument(help="Output directory to reset"),
    ] = Path("./pipeline_output"),
    force: Annotated[
        bool,
        typer.Option("--force", "-f", help="Skip confirmation prompt"),
    ] = False,
) -> None:
    """Reset pipeline output by deleting databases.

    Removes metadata.db and data.duckdb from the output directory.
    """
    metadata_db = output_dir / "metadata.db"
    duckdb_file = output_dir / "data.duckdb"

    files_to_delete = []
    if metadata_db.exists():
        files_to_delete.append(metadata_db)
    if duckdb_file.exists():
        files_to_delete.append(duckdb_file)
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

    for f in files_to_delete:
        f.unlink()
        console.print(f"[green]Deleted {f.name}[/green]")

    console.print("\n[green]Reset complete. Run 'dataraum run' to start fresh.[/green]")


def _reset_phase(phase_name: str, output_dir: Path | None) -> None:
    """Reset a specific phase for the most recent source."""
    from sqlalchemy import select

    from dataraum.cli.common import get_manager
    from dataraum.pipeline.registry import get_phase_class
    from dataraum.pipeline.status import reset_phase
    from dataraum.storage import Source

    if not get_phase_class(phase_name):
        console.print(f"[red]Unknown phase: {phase_name}[/red]")
        raise typer.Exit(1)

    manager = get_manager(output_dir or Path("./pipeline_output"))
    try:
        with manager.session_scope() as session:
            source = session.execute(
                select(Source).order_by(Source.created_at.desc()).limit(1)
            ).scalar_one_or_none()
            if not source:
                console.print("[red]No sources found[/red]")
                raise typer.Exit(1)

            deleted = reset_phase(session, source.source_id, phase_name)
            console.print(
                f"Reset phase [bold]{phase_name}[/bold] "
                f"for source [bold]{source.name}[/bold]: {deleted} rows deleted"
            )
    finally:
        manager.close()
