"""Contracts command - evaluate data quality contracts."""

from __future__ import annotations

from pathlib import Path

from dataraum.cli.common import OutputDirArg


def contracts(
    output_dir: OutputDirArg = Path("./pipeline_output"),
) -> None:
    """Evaluate data quality contracts.

    Shows which contracts the data meets and which it doesn't.

    Examples:

        dataraum contracts ./pipeline_output
    """
    from dataraum.cli.tui import run_app

    run_app(output_dir, initial_screen="contracts")
