"""Entropy dashboard screen with tree navigation and stacked details."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from textual.app import ComposeResult
from textual.containers import Container, Horizontal, ScrollableContainer, Vertical
from textual.screen import Screen
from textual.widgets import (
    Collapsible,
    Label,
    ProgressBar,
    Static,
    Tree,
)
from textual.widgets.tree import TreeNode

from dataraum.cli.common import get_manager


class EntropyBar(Static):
    """A visual entropy bar with color coding."""

    def __init__(self, label: str, score: float, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.label = label
        self.score = score

    def compose(self) -> ComposeResult:
        """Compose the entropy bar."""
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
    """Entropy dashboard with tree navigation and tabbed detail panel."""

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
        # Store interpretations keyed by "table.column" for lookup
        self._interp_map: dict[str, Any] = {}
        self._selected_key: str | None = None

    def compose(self) -> ComposeResult:
        """Create the screen layout with tree and detail panel."""
        with Container(classes="screen-container"):
            yield Static("Entropy Dashboard", classes="screen-title")
            yield Static("Loading...", id="summary-status")
            with Horizontal(classes="split-layout"):
                # Left panel: Tree navigation (full height)
                with Vertical(classes="left-panel"):
                    yield Tree("Tables", id="entropy-tree")
                # Right panel: Stacked sections
                with ScrollableContainer(classes="right-panel"):
                    yield Static("Select a column from the tree", id="detail-header")
                    # Overview section
                    with Container(id="overview-section", classes="detail-section"):
                        yield Static("[bold]Overview[/bold]", classes="section-title")
                        yield Static("", id="overview-content")
                    # Assumptions section
                    with Container(id="assumptions-section", classes="detail-section"):
                        yield Static("[bold]Assumptions[/bold]", classes="section-title")
                        yield Vertical(id="assumptions-list")
                    # Actions section
                    with Container(id="actions-section", classes="detail-section"):
                        yield Static("[bold]Actions[/bold]", classes="section-title")
                        yield Vertical(id="actions-list")

    def on_mount(self) -> None:
        """Load data when screen mounts."""
        self._load_data()

    def action_refresh(self) -> None:
        """Refresh the data."""
        self._data_loaded = False
        self._interp_map.clear()
        self._selected_key = None
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

                # Build lookup map
                for interp in interpretations:
                    if interp.column_name:
                        key = f"{interp.table_name}.{interp.column_name}"
                    else:
                        key = f"{interp.table_name}.(table)"
                    self._interp_map[key] = interp

                # Update UI
                self._update_summary(source.name, snapshot, interpretations)
                self._update_tree(interpretations)

                # Select first item if available (delay to ensure widgets are mounted)
                if interpretations:
                    first = interpretations[0]
                    if first.column_name:
                        self._selected_key = f"{first.table_name}.{first.column_name}"
                    else:
                        self._selected_key = f"{first.table_name}.(table)"
                    # Delay update until after mount completes
                    self.call_after_refresh(self._update_detail_panel, first)

                self._data_loaded = True
        finally:
            manager.close()

    def _show_error(self, message: str) -> None:
        """Display an error message."""
        status = self.query_one("#summary-status", Static)
        status.update(f"[red]{message}[/red]")

    def _update_summary(
        self, source_name: str, snapshot: Any, interpretations: Sequence[Any]
    ) -> None:
        """Update the summary section with merged stats."""
        readiness_colors = {
            "ready": "green",
            "investigate": "yellow",
            "blocked": "red",
        }
        color = readiness_colors.get(snapshot.overall_readiness, "white")

        title_suffix = f" - {self.table_filter}" if self.table_filter else ""
        self.query_one(".screen-title", Static).update(
            f"Entropy Dashboard: {source_name}{title_suffix}"
        )

        # Compute quick stats
        total = len(interpretations)
        high = sum(1 for i in interpretations if i.composite_score > 0.2)
        investigate = sum(1 for i in interpretations if i.readiness == "investigate")
        blocked = sum(1 for i in interpretations if i.readiness == "blocked")

        # Build status line with all stats
        parts = [
            f"[{color}]{snapshot.overall_readiness.upper()}[/{color}]",
            f"Score: {snapshot.avg_composite_score:.3f}",
            f"Columns: {total}",
            f"High: {high}",
        ]
        if investigate:
            parts.append(f"[yellow]Investigate: {investigate}[/yellow]")
        if blocked:
            parts.append(f"[red]Blocked: {blocked}[/red]")

        status = self.query_one("#summary-status", Static)
        status.update(" | ".join(parts))

    def _update_tree(self, interpretations: Sequence[Any]) -> None:
        """Build the tree with tables and columns."""
        tree: Tree[str] = self.query_one("#entropy-tree", Tree)
        tree.clear()
        tree.root.expand()

        # Group by table
        tables: dict[str, list[Any]] = {}
        for interp in interpretations:
            if interp.table_name not in tables:
                tables[interp.table_name] = []
            tables[interp.table_name].append(interp)

        # Build tree nodes
        for table_name, columns in tables.items():
            # Get table-level status (highest severity)
            table_status = "ready"
            for col in columns:
                if col.readiness == "blocked":
                    table_status = "blocked"
                    break
                elif col.readiness == "investigate" and table_status != "blocked":
                    table_status = "investigate"

            table_color = {"ready": "green", "investigate": "yellow", "blocked": "red"}.get(
                table_status, "white"
            )
            table_label = f"[{table_color}]{table_name}[/{table_color}]"
            table_node = tree.root.add(table_label, data=f"{table_name}.(table)")

            # Add columns
            for col in columns:
                if not col.column_name:
                    continue  # Skip table-level for now

                col_color = {"ready": "green", "investigate": "yellow", "blocked": "red"}.get(
                    col.readiness, "white"
                )
                col_label = f"[{col_color}]{col.column_name}[/{col_color}]"
                table_node.add_leaf(col_label, data=f"{table_name}.{col.column_name}")

            table_node.expand()

    def on_tree_node_selected(self, event: Tree.NodeSelected[str]) -> None:
        """Handle tree node selection."""
        node: TreeNode[str] = event.node
        if node.data and node.data in self._interp_map:
            self._selected_key = node.data
            self._update_detail_panel(self._interp_map[node.data])

    def _update_detail_panel(self, interp: Any) -> None:
        """Update the detail panel with the selected interpretation."""
        # Update header
        header = self.query_one("#detail-header", Static)
        col_name = interp.column_name or "(table-level)"
        readiness_color = {"ready": "green", "investigate": "yellow", "blocked": "red"}.get(
            interp.readiness, "white"
        )
        header.update(
            f"[bold]{interp.table_name}.{col_name}[/bold] | "
            f"[{readiness_color}]{interp.readiness.upper()}[/{readiness_color}] | "
            f"Score: {interp.composite_score:.3f}"
        )

        # Update stacked sections
        self._update_overview_section(interp)
        self._update_assumptions_section(interp)
        self._update_actions_section(interp)

    def _update_overview_section(self, interp: Any) -> None:
        """Update the Overview section content."""
        overview = self.query_one("#overview-content", Static)
        explanation = interp.explanation or "No explanation available"
        overview.update(explanation)

    def _update_assumptions_section(self, interp: Any) -> None:
        """Update the Assumptions section with expandable items."""
        container = self.query_one("#assumptions-list", Vertical)
        container.remove_children()

        assumptions = interp.assumptions_json
        if isinstance(assumptions, dict):
            assumptions = list(assumptions.values()) if assumptions else []
        elif not isinstance(assumptions, list):
            assumptions = []

        if not assumptions:
            container.mount(Static("[dim]No assumptions recorded[/dim]"))
            return

        conf_colors = {"high": "green", "medium": "yellow", "low": "red"}

        for a in assumptions:
            if not isinstance(a, dict):
                continue

            dim = a.get("dimension", "-")
            text = a.get("assumption_text", "")
            conf = a.get("confidence", "-")
            impact = a.get("impact", "")
            basis = a.get("basis", "")

            conf_color = conf_colors.get(str(conf).lower(), "white")

            title = f"[{conf_color}]{conf.upper()}[/{conf_color}] {dim}"

            # Full content for expanded state
            content = (
                f"[bold]Assumption:[/bold]\n{text}\n\n"
                f"[bold]Impact:[/bold]\n{impact}\n\n"
                f"[dim]Confidence: {conf} | Basis: {basis}[/dim]"
            )

            collapsible = Collapsible(
                Static(content, classes="assumption-content"),
                title=title,
                collapsed=True,
            )
            container.mount(collapsible)

    def _update_actions_section(self, interp: Any) -> None:
        """Update the Actions section with expandable items."""
        container = self.query_one("#actions-list", Vertical)
        container.remove_children()

        actions = interp.resolution_actions_json
        if isinstance(actions, dict):
            actions = list(actions.values()) if actions else []
        elif not isinstance(actions, list):
            actions = []

        if not actions:
            container.mount(Static("[dim]No actions recommended[/dim]"))
            return

        # Sort by priority
        priority_order = {"high": 0, "medium": 1, "low": 2}
        actions = sorted(
            [a for a in actions if isinstance(a, dict)],
            key=lambda x: priority_order.get(str(x.get("priority", "medium")).lower(), 1),
        )

        priority_colors = {"high": "red", "medium": "yellow", "low": "green"}

        for action in actions:
            priority = action.get("priority", "medium")
            action_name = action.get("action", "-")
            description = action.get("description", "")
            effort = action.get("effort", "-")
            expected_impact = action.get("expected_impact", "")

            p_color = priority_colors.get(str(priority).lower(), "white")

            # Title for collapsed state
            title = f"[{p_color}]{priority.upper()}[/{p_color}] {action_name} [dim](effort: {effort})[/dim]"

            # Full content for expanded state
            content = f"[bold]Description:[/bold]\n{description}"
            if expected_impact:
                content += f"\n\n[bold]Expected Impact:[/bold]\n{expected_impact}"
            content += f"\n\n[dim]Priority: {priority} | Effort: {effort}[/dim]"

            collapsible = Collapsible(
                Static(content, classes="action-content"),
                title=title,
                collapsed=True,
            )
            container.mount(collapsible)
