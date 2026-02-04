"""Shared CLI utilities and constants."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Annotated

import typer
from dotenv import load_dotenv
from rich.console import Console

from dataraum.core.logging import configure_logging

if TYPE_CHECKING:
    from dataraum.core import ConnectionManager

# Load .env file from current directory (for API keys, etc.)
load_dotenv()

# Shared console instance
console = Console()

# Common type aliases for typer options
OutputDirArg = Annotated[
    Path,
    typer.Argument(
        help="Output directory containing pipeline databases",
        exists=True,
        dir_okay=True,
        file_okay=False,
        resolve_path=True,
    ),
]

OutputDirOption = Annotated[
    Path,
    typer.Option(
        "--output",
        "-o",
        help="Output directory containing pipeline databases",
        exists=True,
        dir_okay=True,
        file_okay=False,
        resolve_path=True,
    ),
]

TuiFlag = Annotated[
    bool,
    typer.Option(
        "--tui",
        help="Launch interactive TUI instead of printing summary",
    ),
]

JsonFlag = Annotated[
    bool,
    typer.Option(
        "--json",
        help="Output as JSON for scripting",
    ),
]

VerboseOption = Annotated[
    int,
    typer.Option(
        "--verbose",
        "-v",
        count=True,
        help="Increase logging verbosity (-v=INFO, -vv=DEBUG)",
    ),
]


def setup_logging(verbosity: int = 0, log_format: str = "console") -> None:
    """Configure structured logging based on verbosity level.

    Args:
        verbosity: 0=WARNING, 1=INFO, 2+=DEBUG
        log_format: "console" for development, "json" for production/cloud
    """
    if verbosity >= 2:
        level = "DEBUG"
    elif verbosity >= 1:
        level = "INFO"
    else:
        level = "WARNING"

    configure_logging(
        log_level=level,
        log_format=log_format,
        show_timestamps=verbosity >= 1,
        color=log_format == "console",
    )


def get_manager(output_dir: Path) -> ConnectionManager:
    """Create and initialize a ConnectionManager for the output directory.

    Returns the manager. Caller is responsible for closing it.
    """
    from dataraum.core import ConnectionConfig, ConnectionManager

    config = ConnectionConfig.for_directory(output_dir)

    if not config.sqlite_path.exists():
        console.print(f"[red]No metadata database found at {config.sqlite_path}[/red]")
        raise typer.Exit(1)

    manager = ConnectionManager(config)
    manager.initialize()
    return manager
