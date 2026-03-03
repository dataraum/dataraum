"""Phases command - list available pipeline phases."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.table import Table as RichTable

from dataraum.cli.common import console


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
