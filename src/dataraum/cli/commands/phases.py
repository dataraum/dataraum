"""Phases command - list available pipeline phases."""

from __future__ import annotations

from rich.table import Table as RichTable

from dataraum.cli.common import console


def phases() -> None:
    """List available pipeline phases and their dependencies."""
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
