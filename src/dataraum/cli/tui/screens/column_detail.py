"""Column detail screen - deep dive into entropy issues for a column."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from textual.app import ComposeResult
from textual.containers import Container, Horizontal, ScrollableContainer, Vertical
from textual.screen import Screen
from textual.widgets import DataTable, Label, ProgressBar, Static

from dataraum.cli.common import get_manager


class ColumnDetailScreen(Screen[None]):
    """Column detail screen showing entropy breakdown, assumptions, and actions."""

    BINDINGS = [
        ("escape", "app.pop_screen", "Back"),
        ("r", "refresh", "Refresh"),
    ]

    def __init__(
        self,
        output_dir: Path,
        table_name: str,
        column_name: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.output_dir = output_dir
        self.table_name = table_name
        self.column_name = column_name
        self._data_loaded = False

    def compose(self) -> ComposeResult:
        """Create the screen layout."""
        yield Container(
            ScrollableContainer(
                Vertical(
                    # Header
                    Static(
                        f"Column: {self.table_name}.{self.column_name}",
                        classes="screen-title",
                    ),
                    Static("Loading...", id="status-line"),
                    # Explanation section
                    Container(
                        Static("Explanation", classes="section-title"),
                        Static("", id="explanation-text"),
                        classes="explanation-section",
                    ),
                    # Entropy dimensions section
                    Container(
                        Static("Entropy by Dimension", classes="section-title"),
                        Vertical(id="dimension-bars"),
                        classes="dimension-section",
                    ),
                    # Assumptions section
                    Container(
                        Static("Assumptions", classes="section-title"),
                        DataTable(id="assumptions-table"),
                        classes="assumptions-section",
                    ),
                    # Resolution actions section
                    Container(
                        Static("Resolution Actions", classes="section-title"),
                        DataTable(id="actions-table"),
                        classes="actions-section",
                    ),
                    # Entropy objects section (raw measurements)
                    Container(
                        Static("Raw Entropy Measurements", classes="section-title"),
                        DataTable(id="entropy-objects-table"),
                        classes="entropy-objects-section",
                    ),
                    classes="main-content",
                ),
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
        """Load column entropy data from database."""
        if self._data_loaded:
            return

        from sqlalchemy import select

        from dataraum.entropy.db_models import (
            EntropyInterpretationRecord,
            EntropyObjectRecord,
        )
        from dataraum.storage import Column, Source, Table

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

                # Get table
                table_result = session.execute(
                    select(Table).where(
                        Table.source_id == source.source_id,
                        Table.table_name == self.table_name,
                    )
                )
                table = table_result.scalar_one_or_none()

                if not table:
                    self._show_error(f"Table not found: {self.table_name}")
                    return

                # Get column
                column_result = session.execute(
                    select(Column).where(
                        Column.table_id == table.table_id,
                        Column.column_name == self.column_name,
                    )
                )
                column = column_result.scalar_one_or_none()

                if not column:
                    self._show_error(f"Column not found: {self.column_name}")
                    return

                # Get entropy interpretation
                interp_result = session.execute(
                    select(EntropyInterpretationRecord).where(
                        EntropyInterpretationRecord.column_id == column.column_id
                    )
                )
                interpretation = interp_result.scalar_one_or_none()

                # Get entropy objects for this column
                entropy_objects_result = session.execute(
                    select(EntropyObjectRecord)
                    .where(EntropyObjectRecord.column_id == column.column_id)
                    .order_by(EntropyObjectRecord.score.desc())
                )
                entropy_objects = entropy_objects_result.scalars().all()

                # Update UI
                self._update_status(interpretation)
                self._update_explanation(interpretation)
                self._update_dimensions(entropy_objects)
                self._update_assumptions(interpretation)
                self._update_actions(interpretation)
                self._update_entropy_objects(entropy_objects)

                self._data_loaded = True
        finally:
            manager.close()

    def _show_error(self, message: str) -> None:
        """Display an error message."""
        status = self.query_one("#status-line", Static)
        status.update(f"[red]{message}[/red]")

    def _update_status(self, interpretation: Any | None) -> None:
        """Update the status line."""
        status = self.query_one("#status-line", Static)

        if not interpretation:
            status.update("[yellow]No entropy interpretation available[/yellow]")
            return

        readiness_colors = {
            "ready": "green",
            "investigate": "yellow",
            "blocked": "red",
        }
        readiness_icons = {
            "ready": "Ready",
            "investigate": "Investigate",
            "blocked": "Blocked",
        }

        color = readiness_colors.get(interpretation.readiness, "white")
        label = readiness_icons.get(interpretation.readiness, interpretation.readiness)

        status.update(
            f"[{color}]Status: {label}[/{color}] | Score: {interpretation.composite_score:.3f}"
        )

    def _update_explanation(self, interpretation: Any | None) -> None:
        """Update the explanation section."""
        explanation_widget = self.query_one("#explanation-text", Static)

        if not interpretation or not interpretation.explanation:
            explanation_widget.update("[dim]No explanation available[/dim]")
            return

        explanation_widget.update(interpretation.explanation)

    def _update_dimensions(self, entropy_objects: Sequence[Any]) -> None:
        """Update the entropy dimension bars."""
        container = self.query_one("#dimension-bars", Vertical)
        container.remove_children()

        if not entropy_objects:
            container.mount(Static("[dim]No entropy measurements[/dim]"))
            return

        # Group by layer
        layer_scores: dict[str, list[float]] = {}
        for obj in entropy_objects:
            layer = obj.layer
            if layer not in layer_scores:
                layer_scores[layer] = []
            layer_scores[layer].append(obj.score)

        # Calculate averages and display
        layer_order = ["structural", "semantic", "value", "computational"]
        for layer in layer_order:
            scores = layer_scores.get(layer, [])
            avg_score = sum(scores) / len(scores) if scores else 0.0

            bar_class = (
                "entropy-high"
                if avg_score > 0.3
                else "entropy-medium"
                if avg_score > 0.15
                else "entropy-low"
            )

            row = Horizontal(
                Label(f"{layer.capitalize()}:", classes="entropy-label"),
                ProgressBar(total=1.0, show_eta=False, classes=f"entropy-bar {bar_class}"),
                Label(f"{avg_score:.3f}", classes="entropy-value"),
                classes="entropy-row",
            )
            container.mount(row)

            # Set progress after mount
            bar = row.query_one(ProgressBar)
            bar.advance(avg_score)

    def _update_assumptions(self, interpretation: Any | None) -> None:
        """Update the assumptions table."""
        table = self.query_one("#assumptions-table", DataTable)
        table.clear(columns=True)

        table.add_column("Dimension", key="dimension")
        table.add_column("Assumption", key="assumption")
        table.add_column("Confidence", key="confidence")
        table.add_column("Impact", key="impact")

        if not interpretation:
            table.add_row("-", "No interpretation available", "-", "-")
            return

        assumptions = interpretation.assumptions_json
        if isinstance(assumptions, dict):
            assumptions = list(assumptions.values()) if assumptions else []
        elif not isinstance(assumptions, list):
            assumptions = []

        if not assumptions:
            table.add_row("-", "No assumptions recorded", "-", "-")
            return

        for a in assumptions:
            if not isinstance(a, dict):
                continue

            dim = a.get("dimension", "-")
            text = a.get("assumption_text", "")
            confidence = a.get("confidence", "-")
            impact = a.get("impact", "")

            # Truncate long text for display
            if len(text) > 60:
                text = text[:57] + "..."
            if len(impact) > 40:
                impact = impact[:37] + "..."

            # Color confidence
            conf_colors = {"high": "green", "medium": "yellow", "low": "red"}
            conf_color = conf_colors.get(confidence.lower() if confidence else "", "white")

            table.add_row(
                dim,
                text,
                f"[{conf_color}]{confidence}[/{conf_color}]",
                f"[dim]{impact}[/dim]",
            )

    def _update_actions(self, interpretation: Any | None) -> None:
        """Update the resolution actions table."""
        table = self.query_one("#actions-table", DataTable)
        table.clear(columns=True)

        table.add_column("Priority", key="priority")
        table.add_column("Action", key="action")
        table.add_column("Description", key="description")
        table.add_column("Effort", key="effort")

        if not interpretation:
            table.add_row("-", "No interpretation available", "-", "-")
            return

        actions = interpretation.resolution_actions_json
        if isinstance(actions, dict):
            actions = list(actions.values()) if actions else []
        elif not isinstance(actions, list):
            actions = []

        if not actions:
            table.add_row("-", "No actions recommended", "-", "-")
            return

        # Sort by priority
        priority_order = {"high": 0, "medium": 1, "low": 2}
        actions = sorted(
            actions,
            key=lambda x: priority_order.get(
                x.get("priority", "medium").lower() if isinstance(x, dict) else "medium", 1
            ),
        )

        for action in actions:
            if not isinstance(action, dict):
                continue

            priority = action.get("priority", "medium")
            action_name = action.get("action", "-")
            description = action.get("description", "")
            effort = action.get("effort", "-")

            # Truncate long text
            if len(description) > 50:
                description = description[:47] + "..."

            # Color priority
            priority_colors = {"high": "red", "medium": "yellow", "low": "green"}
            priority_color = priority_colors.get(priority.lower() if priority else "", "white")

            table.add_row(
                f"[{priority_color}]{priority}[/{priority_color}]",
                action_name,
                description,
                effort,
            )

    def _update_entropy_objects(self, entropy_objects: Sequence[Any]) -> None:
        """Update the raw entropy measurements table."""
        table = self.query_one("#entropy-objects-table", DataTable)
        table.clear(columns=True)

        table.add_column("Layer", key="layer")
        table.add_column("Dimension", key="dimension")
        table.add_column("Sub-dimension", key="sub_dimension")
        table.add_column("Score", key="score")
        table.add_column("Confidence", key="confidence")

        if not entropy_objects:
            table.add_row("-", "No measurements", "-", "-", "-")
            return

        for obj in entropy_objects:
            score_color = "red" if obj.score > 0.3 else "yellow" if obj.score > 0.15 else "green"

            table.add_row(
                obj.layer,
                obj.dimension,
                obj.sub_dimension,
                f"[{score_color}]{obj.score:.3f}[/{score_color}]",
                f"{obj.confidence:.2f}",
            )
