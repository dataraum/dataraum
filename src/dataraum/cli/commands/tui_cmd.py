"""TUI command - launch interactive terminal interface."""

from __future__ import annotations

from pathlib import Path

from dataraum.cli.common import OutputDirArg


def tui(
    output_dir: OutputDirArg = Path("./pipeline_output"),
) -> None:
    """Launch interactive terminal interface.

    Opens a full-screen dashboard for exploring pipeline results.
    Navigate between screens with keyboard shortcuts:

        h=Home  e=Entropy  c=Contracts  a=Actions  /=Query  q=Quit

    Examples:

        dataraum tui ./pipeline_output
    """
    from dataraum.cli.tui import run_app

    run_app(output_dir, initial_screen="home")
