"""Phases command - list available pipeline phases."""

from __future__ import annotations

from rich.table import Table as RichTable

from dataraum.cli.common import console


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
