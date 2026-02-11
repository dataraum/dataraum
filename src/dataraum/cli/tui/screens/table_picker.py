"""Table picker screen - quick navigation to any table."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from textual.app import ComposeResult
from textual.containers import Container
from textual.screen import Screen
from textual.widgets import DataTable, Static

from dataraum.cli.common import get_manager


class TablePickerScreen(Screen[None]):
    """Quick table selection screen accessible via 't' key."""

    BINDINGS = [
        ("escape", "app.pop_screen", "Back"),
    ]

    def __init__(self, output_dir: Path, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.output_dir = output_dir
        self._table_names: list[str] = []

    def compose(self) -> ComposeResult:
        """Create the picker layout."""
        with Container(classes="screen-container"):
            yield Static("Select a Table", classes="screen-title")
            yield DataTable(id="picker-table")

    def on_mount(self) -> None:
        """Load tables when screen mounts."""
        self._load_tables()

    def _load_tables(self) -> None:
        """Load all typed tables from the database."""
        from sqlalchemy import func, select

        from dataraum.storage import Column, Source, Table

        manager = get_manager(self.output_dir)

        try:
            with manager.session_scope() as session:
                sources_result = session.execute(select(Source))
                sources = sources_result.scalars().all()

                if not sources:
                    return

                source = sources[0]

                tables_result = session.execute(
                    select(Table).where(
                        Table.source_id == source.source_id,
                        Table.layer == "typed",
                    )
                )
                tables = tables_result.scalars().all()

                table_widget = self.query_one("#picker-table", DataTable)
                table_widget.cursor_type = "row"

                table_widget.add_column("Table", key="table")
                table_widget.add_column("Columns", key="columns")
                table_widget.add_column("Rows", key="rows")

                self._table_names = []
                for tbl in tables:
                    col_count = session.execute(
                        select(func.count(Column.column_id)).where(Column.table_id == tbl.table_id)
                    ).scalar()

                    table_widget.add_row(
                        tbl.table_name,
                        str(col_count or 0),
                        str(tbl.row_count or "-"),
                    )
                    self._table_names.append(tbl.table_name)
        finally:
            manager.close()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        """Handle table selection - push TableScreen."""
        if event.cursor_row < len(self._table_names):
            table_name = self._table_names[event.cursor_row]
            from dataraum.cli.tui.app import DataraumApp

            app = self.app
            if isinstance(app, DataraumApp):
                # Pop the picker first, then push the table screen
                self.app.pop_screen()
                app.push_table_screen(table_name)
