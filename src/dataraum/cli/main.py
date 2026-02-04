"""Main CLI application entry point."""

from __future__ import annotations

import typer

from dataraum.cli.commands import contracts, entropy, inspect, phases, query, reset, run, status

app = typer.Typer(
    name="dataraum",
    help="DataRaum Context Engine - extract rich metadata from data sources.",
    no_args_is_help=True,
)

# Register commands
app.command()(run.run)
app.command()(status.status)
app.command()(reset.reset)
app.command()(inspect.inspect)
app.command()(phases.phases)
app.command()(contracts.contracts)
app.command()(query.query)
app.command()(entropy.entropy)


def main() -> None:
    """Entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()
