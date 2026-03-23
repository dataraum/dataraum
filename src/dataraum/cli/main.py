"""Main CLI application entry point."""

from __future__ import annotations

import typer

from dataraum.cli.commands import dev, query, run, sources, tui_cmd

app = typer.Typer(
    name="dataraum",
    help="DataRaum Context Engine - extract rich metadata from data sources.",
    no_args_is_help=True,
)

# User commands
app.command()(run.run)
app.command(name="tui")(tui_cmd.tui)
app.command()(query.query)

# Subcommand groups
app.add_typer(sources.app, name="sources")
app.add_typer(dev.app, name="dev")


def main() -> None:
    """Entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()
