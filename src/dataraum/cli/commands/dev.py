"""Dev subcommand - developer utilities for pipeline debugging."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.table import Table as RichTable

from dataraum.cli.common import OutputDirArg, console

app = typer.Typer(
    name="dev",
    help="Developer utilities (phases, context).",
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

    from dataraum.pipeline.pipeline_config import load_phase_declarations

    console.print("\n[bold]Pipeline Phases[/bold]\n")

    table = RichTable(show_header=True, header_style="bold")
    table.add_column("Phase")
    table.add_column("Description")
    table.add_column("Dependencies")

    declarations = load_phase_declarations()
    for name, decl in declarations.items():
        deps = ", ".join(decl.dependencies) if decl.dependencies else "-"
        table.add_row(name, decl.description, deps)

    console.print(table)
    console.print()


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
                    select(Table).where(Table.source_id == source.source_id, Table.layer == "typed")
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
