"""Home screen - overview of sources and tables."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from textual.app import ComposeResult
from textual.containers import Container, Vertical
from textual.screen import Screen
from textual.widgets import DataTable, Static

from dataraum.cli.common import get_manager


class HomeScreen(Screen[None]):
    """Home screen showing sources, tables, and overall status."""

    BINDINGS = [
        ("escape", "app.pop_screen", "Back"),
        ("r", "refresh", "Refresh"),
    ]

    def __init__(self, output_dir: Path, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.output_dir = output_dir
        self._data_loaded = False

    def compose(self) -> ComposeResult:
        """Create the screen layout."""
        yield Container(
            Vertical(
                Static("DataRaum Status", classes="screen-title"),
                Static("Loading...", id="status-line"),
                # Tables section
                Container(
                    Static("Tables", classes="section-title"),
                    DataTable(id="tables-table"),
                    classes="tables-section",
                ),
                # Pipeline status
                Container(
                    Static("Pipeline Status", classes="section-title"),
                    DataTable(id="pipeline-table"),
                    classes="pipeline-section",
                ),
                classes="main-content",
            ),
            classes="screen-container",
        )

    def on_mount(self) -> None:
        """Load data when screen mounts."""
        self._load_data()

    def action_refresh(self) -> None:
        """Refresh the data."""
        self._data_loaded = False
        self._load_data()

    def _load_data(self) -> None:
        """Load status data from database."""
        if self._data_loaded:
            return

        from sqlalchemy import select

        from dataraum.storage import Source, Table

        manager = get_manager(self.output_dir)

        try:
            with manager.session_scope() as session:
                # Get sources
                sources_result = session.execute(select(Source))
                sources = sources_result.scalars().all()

                if not sources:
                    self._show_error("No sources found")
                    return

                source = sources[0]

                # Get tables
                tables_result = session.execute(
                    select(Table).where(Table.source_id == source.source_id)
                )
                tables = tables_result.scalars().all()

                # Update UI
                self._update_status(source, tables)
                self._update_tables_table(session, tables)
                self._update_pipeline_status(session, source)

                self._data_loaded = True
        finally:
            manager.close()

    def _show_error(self, message: str) -> None:
        """Display an error message."""
        status = self.query_one("#status-line", Static)
        status.update(f"[red]{message}[/red]")

    def _update_status(self, source: Any, tables: Sequence[Any]) -> None:
        """Update the status line."""
        self.query_one(".screen-title", Static).update(f"DataRaum: {source.name}")

        status = self.query_one("#status-line", Static)
        status.update(f"Source: {source.name} | Tables: {len(tables)}")

    def _update_tables_table(self, session: Any, tables: Sequence[Any]) -> None:
        """Update the tables data table."""
        from sqlalchemy import func, select

        from dataraum.storage import Column

        table_widget = self.query_one("#tables-table", DataTable)
        table_widget.clear(columns=True)

        table_widget.add_column("Table", key="table")
        table_widget.add_column("Columns", key="columns")
        table_widget.add_column("Rows", key="rows")

        for tbl in tables:
            # Count columns
            col_count = session.execute(
                select(func.count(Column.column_id)).where(Column.table_id == tbl.table_id)
            ).scalar()

            table_widget.add_row(
                tbl.table_name,
                str(col_count or 0),
                str(tbl.row_count or "-"),
            )

    def _update_pipeline_status(self, session: Any, source: Any) -> None:
        """Update the pipeline status table."""
        from sqlalchemy import select

        from dataraum.pipeline.db_models import PhaseCheckpoint

        table_widget = self.query_one("#pipeline-table", DataTable)
        table_widget.clear(columns=True)

        table_widget.add_column("Phase", key="phase")
        table_widget.add_column("Status", key="status")
        table_widget.add_column("Duration", key="duration")

        # Get pipeline checkpoints
        states_result = session.execute(
            select(PhaseCheckpoint)
            .where(PhaseCheckpoint.source_id == source.source_id)
            .order_by(PhaseCheckpoint.started_at)
        )
        states = states_result.scalars().all()

        status_icons = {
            "completed": "[green]Completed[/green]",
            "running": "[yellow]Running[/yellow]",
            "failed": "[red]Failed[/red]",
            "pending": "[dim]Pending[/dim]",
        }

        for state in states:
            status = status_icons.get(state.status, state.status)

            if state.completed_at and state.started_at:
                duration = (state.completed_at - state.started_at).total_seconds()
                duration_str = f"{duration:.1f}s"
            else:
                duration_str = "-"

            table_widget.add_row(state.phase_name, status, duration_str)
