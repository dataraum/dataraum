"""Main CLI application entry point."""

from __future__ import annotations

import typer

from dataraum.cli.commands import query, run, status

app = typer.Typer(
    name="dataraum",
    help="DataRaum Context Engine - extract rich metadata from data sources.",
    no_args_is_help=True,
)

# Register commands
app.command()(run.run)
app.command()(status.status)
app.command()(query.query)


def main() -> None:
    """Entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()
