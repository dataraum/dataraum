"""Status command - show pipeline status."""

from __future__ import annotations

from pathlib import Path

from dataraum.cli.common import OutputDirArg


def status(
    output_dir: OutputDirArg = Path("./pipeline_output"),
) -> None:
    """Show status of a pipeline run.

    Displays information about completed phases, sources, and tables.

    Examples:

        dataraum status ./pipeline_output
    """
    from dataraum.cli.tui import run_app

    run_app(output_dir, initial_screen="home")
