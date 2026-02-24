"""Query command - ask questions about data using natural language."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from dataraum.cli.common import OutputDirOption


def query(
    question: Annotated[
        str,
        typer.Argument(
            help="Natural language question to answer",
        ),
    ],
    output_dir: OutputDirOption = Path("./pipeline_output"),
) -> None:
    """Ask a question about the data using natural language.

    The Query Agent converts your question into SQL and returns results
    with a confidence level based on data quality.

    Examples:

        dataraum query "What was total revenue?" -o ./pipeline_output

        dataraum query "Show sales by region" -o ./output
    """
    from dataraum.cli.tui import run_app

    run_app(output_dir, initial_screen="query", query=question)
