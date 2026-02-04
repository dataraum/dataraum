"""Entropy dashboard screen."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from textual.app import ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import DataTable, Label, ProgressBar, Static

from dataraum.cli.common import get_manager


class EntropyBar(Static):
    """A visual entropy bar with color coding."""

    def __init__(self, label: str, score: float, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.label = label
        self.score = score

    def compose(self) -> ComposeResult:
        """Compose the entropy bar."""
        # Color based on score
        if self.score > 0.3:
            bar_class = "entropy-high"
        elif self.score > 0.15:
            bar_class = "entropy-medium"
        else:
            bar_class = "entropy-low"

        yield Horizontal(
            Label(f"{self.label}:", classes="entropy-label"),
            ProgressBar(total=1.0, show_eta=False, classes=f"entropy-bar {bar_class}"),
            Label(f"{self.score:.3f}", classes="entropy-value"),
            classes="entropy-row",
        )

    def on_mount(self) -> None:
        """Set the progress bar value after mount."""
        bar = self.query_one(ProgressBar)
        bar.advance(self.score)


class EntropyScreen(Screen[None]):
    """Entropy dashboard with dimension breakdown and column details."""

    BINDINGS = [
        ("escape", "app.pop_screen", "Back"),
        ("r", "refresh", "Refresh"),
    ]

    def __init__(
        self,
        output_dir: Path,
        table_filter: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.output_dir = output_dir
        self.table_filter = table_filter
        self._data_loaded = False

    def compose(self) -> ComposeResult:
        """Create the screen layout."""
        yield Container(
            Vertical(
                # Header section
                Static("Entropy Dashboard", classes="screen-title"),
                Static("Loading...", id="summary-status"),
                # Dimension bars
                Container(
                    Static("Entropy by Dimension", classes="section-title"),
                    Vertical(id="dimension-bars"),
                    classes="dimension-section",
                ),
                # Issues summary
                Container(
                    Static("Issue Summary", classes="section-title"),
                    Static("", id="issue-summary"),
                    classes="issue-section",
                ),
                # Column table
                Container(
                    Static("High-Entropy Columns", classes="section-title"),
                    DataTable(id="columns-table"),
                    classes="columns-section",
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
        """Load entropy data from database."""
        if self._data_loaded:
            return

        from sqlalchemy import select

        from dataraum.entropy.db_models import (
            EntropyInterpretationRecord,
            EntropySnapshotRecord,
        )
        from dataraum.storage import Source

        manager = get_manager(self.output_dir)

        try:
            with manager.session_scope() as session:
                # Get source
                sources_result = session.execute(select(Source))
                sources = sources_result.scalars().all()

                if not sources:
                    self._show_error("No sources found")
                    return

                source = sources[0]

                # Get latest snapshot
                snapshot_result = session.execute(
                    select(EntropySnapshotRecord)
                    .where(EntropySnapshotRecord.source_id == source.source_id)
                    .order_by(EntropySnapshotRecord.snapshot_at.desc())
                    .limit(1)
                )
                snapshot = snapshot_result.scalar_one_or_none()

                if not snapshot:
                    self._show_error("No entropy data. Run entropy phase first.")
                    return

                # Get interpretations
                interp_query = select(EntropyInterpretationRecord).where(
                    EntropyInterpretationRecord.source_id == source.source_id
                )

                if self.table_filter:
                    interp_query = interp_query.where(
                        EntropyInterpretationRecord.table_name == self.table_filter
                    )

                interp_query = interp_query.order_by(
                    EntropyInterpretationRecord.composite_score.desc()
                )
                interp_result = session.execute(interp_query)
                interpretations = interp_result.scalars().all()

                # Update UI
                self._update_summary(source.name, snapshot)
                self._update_dimensions(snapshot)
                self._update_issues(interpretations)
                self._update_columns_table(interpretations)

                self._data_loaded = True
        finally:
            manager.close()

    def _show_error(self, message: str) -> None:
        """Display an error message."""
        status = self.query_one("#summary-status", Static)
        status.update(f"[red]{message}[/red]")

    def _update_summary(self, source_name: str, snapshot: Any) -> None:
        """Update the summary section."""
        readiness_icons = {
            "ready": "[green]Ready[/green]",
            "investigate": "[yellow]Investigate[/yellow]",
            "blocked": "[red]Blocked[/red]",
        }

        status_text = readiness_icons.get(snapshot.overall_readiness, snapshot.overall_readiness)

        title_suffix = f" - {self.table_filter}" if self.table_filter else ""
        self.query_one(".screen-title", Static).update(
            f"Entropy Dashboard: {source_name}{title_suffix}"
        )

        status = self.query_one("#summary-status", Static)
        status.update(
            f"Status: {status_text} | Composite Score: {snapshot.avg_composite_score:.3f}"
        )

    def _update_dimensions(self, snapshot: Any) -> None:
        """Update the dimension bars."""
        container = self.query_one("#dimension-bars", Vertical)
        container.remove_children()

        dimensions = [
            ("Structural", snapshot.avg_structural_entropy),
            ("Semantic", snapshot.avg_semantic_entropy),
            ("Value", snapshot.avg_value_entropy),
            ("Computational", snapshot.avg_computational_entropy),
        ]

        for name, score in dimensions:
            container.mount(EntropyBar(name, score))

    def _update_issues(self, interpretations: Sequence[Any]) -> None:
        """Update the issue summary."""
        high_entropy = [i for i in interpretations if i.composite_score > 0.2]
        investigate = [i for i in interpretations if i.readiness == "investigate"]
        blocked = [i for i in interpretations if i.readiness == "blocked"]

        summary_parts = [f"Total columns: {len(interpretations)}"]
        if high_entropy:
            summary_parts.append(f"High entropy: {len(high_entropy)}")
        if investigate:
            summary_parts.append(f"[yellow]Investigate: {len(investigate)}[/yellow]")
        if blocked:
            summary_parts.append(f"[red]Blocked: {len(blocked)}[/red]")

        summary = self.query_one("#issue-summary", Static)
        summary.update(" | ".join(summary_parts))

    def _update_columns_table(self, interpretations: Sequence[Any]) -> None:
        """Update the columns data table."""
        table = self.query_one("#columns-table", DataTable)
        table.clear(columns=True)

        # Add columns
        table.add_column("Table", key="table")
        table.add_column("Column", key="column")
        table.add_column("Score", key="score")
        table.add_column("Status", key="status")
        table.add_column("Top Issue", key="issue")

        readiness_icons = {
            "ready": "[green]Ready[/green]",
            "investigate": "[yellow]Investigate[/yellow]",
            "blocked": "[red]Blocked[/red]",
        }

        # Add rows (top 20)
        for interp in interpretations[:20]:
            status = readiness_icons.get(interp.readiness, interp.readiness)

            # Get first line of explanation
            explanation = interp.explanation or ""
            first_line = explanation.split(".")[0] if explanation else "-"
            if len(first_line) > 40:
                first_line = first_line[:37] + "..."

            table.add_row(
                interp.table_name,
                interp.column_name,
                f"{interp.composite_score:.3f}",
                status,
                first_line,
            )

        if len(interpretations) > 20:
            table.add_row("", f"... +{len(interpretations) - 20} more", "", "", "")
