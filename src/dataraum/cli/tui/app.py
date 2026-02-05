"""Main Textual application for DataRaum TUI."""

from __future__ import annotations

from pathlib import Path

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.widgets import Footer, Header

from dataraum.cli.common import get_manager
from dataraum.core import ConnectionManager


class DataraumApp(App[None]):
    """DataRaum interactive TUI application.

    Provides screens for:
    - Home: Overview of sources and tables
    - Entropy: Entropy dashboard with drill-down
    - Contracts: Contract evaluation with traffic lights
    - Query: Natural language query interface
    """

    CSS_PATH = "styles.tcss"
    TITLE = "DataRaum"

    BINDINGS = [
        Binding("q", "quit", "Quit", show=True),
        Binding("h", "switch_screen('home')", "Home", show=True),
        Binding("e", "switch_screen('entropy')", "Entropy", show=True),
        Binding("c", "switch_screen('contracts')", "Contracts", show=True),
        Binding("/", "switch_screen('query')", "Query", show=True),
        Binding("?", "show_help", "Help", show=True),
    ]

    def __init__(
        self,
        output_dir: Path,
        initial_screen: str = "home",
        table_filter: str | None = None,
        query: str | None = None,
    ) -> None:
        """Initialize the app.

        Args:
            output_dir: Path to pipeline output directory
            initial_screen: Screen to show on startup
            table_filter: Optional table name to filter to
            query: Optional query to pre-fill
        """
        super().__init__()
        self.output_dir = output_dir
        self.initial_screen = initial_screen
        self.table_filter = table_filter
        self.initial_query = query
        self._manager: ConnectionManager | None = None

    @property
    def manager(self) -> ConnectionManager:
        """Lazy-load the connection manager."""
        if self._manager is None:
            self._manager = get_manager(self.output_dir)
        return self._manager

    def compose(self) -> ComposeResult:
        """Create child widgets for the app."""
        yield Header()
        yield Footer()

    def on_mount(self) -> None:
        """Handle app mount - install screens and switch to initial."""
        from dataraum.cli.tui.screens.contracts import ContractsScreen
        from dataraum.cli.tui.screens.entropy import EntropyScreen
        from dataraum.cli.tui.screens.home import HomeScreen
        from dataraum.cli.tui.screens.query import QueryScreen

        # Install all screens
        self.install_screen(HomeScreen(self.output_dir), name="home")
        self.install_screen(
            EntropyScreen(self.output_dir, table_filter=self.table_filter),
            name="entropy",
        )
        self.install_screen(ContractsScreen(self.output_dir), name="contracts")
        self.install_screen(
            QueryScreen(self.output_dir, initial_query=self.initial_query),
            name="query",
        )

        # Switch to initial screen
        self.push_screen(self.initial_screen)

    def push_table_screen(self, table_name: str) -> None:
        """Push a table detail screen for the given table."""
        from dataraum.cli.tui.screens.table import TableScreen

        screen = TableScreen(self.output_dir, table_name)
        self.push_screen(screen)

    async def action_switch_screen(self, screen_name: str) -> None:
        """Switch to a different screen.

        Uses pop_screen + push_screen pattern to avoid issues with
        result callbacks when switching between installed screens.
        """
        # Pop the current screen (back to the default or previous)
        if len(self._screen_stack) > 1:
            self.pop_screen()
        # Push the requested screen
        self.push_screen(screen_name)

    def action_show_help(self) -> None:
        """Show help dialog."""
        self.notify(
            "Keys: [h]ome, [e]ntropy, [c]ontracts, [/]query, [q]uit\n"
            "Use arrow keys to navigate, Enter to select",
            title="Help",
            timeout=5,
        )

    def on_unmount(self) -> None:
        """Clean up resources when app closes."""
        if self._manager is not None:
            self._manager.close()
