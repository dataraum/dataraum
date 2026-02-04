"""Textual TUI for DataRaum CLI.

Provides interactive terminal interfaces for exploring entropy,
contracts, queries, and data context.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


def run_app(
    output_dir: Path,
    initial_screen: str = "home",
    table_filter: str | None = None,
    query: str | None = None,
) -> None:
    """Launch the Textual TUI application.

    Args:
        output_dir: Path to pipeline output directory
        initial_screen: Screen to show on startup (home, entropy, contracts, query)
        table_filter: Optional table name to filter to
        query: Optional query to pre-fill in query screen
    """
    from dataraum.cli.tui.app import DataraumApp

    app = DataraumApp(
        output_dir=output_dir,
        initial_screen=initial_screen,
        table_filter=table_filter,
        query=query,
    )
    app.run()


__all__ = ["run_app"]
