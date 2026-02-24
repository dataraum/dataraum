"""Entropy command - explore entropy metrics and data quality issues."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from dataraum.cli.common import OutputDirArg


def entropy(
    output_dir: OutputDirArg = Path("./pipeline_output"),
    table: Annotated[
        str | None,
        typer.Option(
            "--table",
            "-t",
            help="Filter to a specific table",
        ),
    ] = None,
) -> None:
    """Explore entropy metrics and data quality issues.

    Shows a summary of data uncertainty across dimensions, helping developers
    understand what assumptions the system makes and what to fix.

    Examples:

        dataraum entropy ./pipeline_output

        dataraum entropy ./output --table master_txn_table
    """
    from dataraum.cli.tui import run_app

    run_app(output_dir, initial_screen="entropy", table_filter=table)
