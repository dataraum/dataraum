"""Entropy dashboard screen with tree navigation and detailed view."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from textual.app import ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import (
    Collapsible,
    Label,
    ProgressBar,
    Static,
    TabbedContent,
    TabPane,
    Tree,
)
from textual.widgets.tree import TreeNode

from dataraum.cli.common import get_manager
from dataraum.cli.tui.formatting import format_evidence_field


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
        # Store interpretations keyed by "table.column" or "table.(table)" for lookup
        self._interp_map: dict[str, Any] = {}
        # Store entropy objects keyed by column_id for lookup
        self._entropy_objects_map: dict[str, list[Any]] = {}
        # Store table-level entropy objects keyed by "table.(table)" key
        self._table_entropy_objects: dict[str, list[Any]] = {}
        self._selected_key: str | None = None
        # Store compound risks keyed by column key ("table.column")
        self._compound_risks_map: dict[str, list[Any]] = {}

    def compose(self) -> ComposeResult:
        """Create the screen layout with tree and detail panel."""
        with Container(classes="screen-container"):
            yield Static("Entropy Dashboard", classes="screen-title")
            yield Static("Loading...", id="summary-status")
            with Horizontal(classes="split-layout"):
                # Left panel: Tree navigation (full height)
                with Vertical(classes="left-panel"):
                    yield Tree("Tables", id="entropy-tree")
                # Right panel: Layer bars, tabs, raw table
                with Vertical(classes="right-panel"):
                    yield Static("Select a column from the tree", id="detail-header")
                    # Layer breakdown bars (always visible)
                    with Container(id="layer-section"):
                        yield Static("", id="layer-bars")
                    # Tabs for Overview/Assumptions/Actions
                    with TabbedContent(id="detail-tabs"):
                        with TabPane("Overview", id="tab-overview"):
                            yield Static("", id="overview-content")
                        with TabPane("Evidence", id="tab-evidence"):
                            yield Static("", id="evidence-content")
                        with TabPane("Assumptions", id="tab-assumptions"):
                            yield Vertical(id="assumptions-list")
                        with TabPane("Actions", id="tab-actions"):
                            yield Vertical(id="actions-list")

    def on_mount(self) -> None:
        """Load data when screen mounts."""
        self._load_data()

    def action_refresh(self) -> None:
        """Refresh the data."""
        self._data_loaded = False
        self._interp_map.clear()
        self._entropy_objects_map.clear()
        self._table_entropy_objects.clear()
        self._compound_risks_map.clear()
        self._selected_key = None
        self._load_data()

    def _load_data(self) -> None:
        """Load entropy data from database."""
        if self._data_loaded:
            return

        from sqlalchemy import select

        from dataraum.entropy.db_models import (
            CompoundRiskRecord,
            EntropyInterpretationRecord,
            EntropyObjectRecord,
            EntropySnapshotRecord,
        )
        from dataraum.storage import Source, Table

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

                # Build lookup map and collect column_ids
                column_ids: list[str] = []
                for interp in interpretations:
                    if interp.column_name:
                        key = f"{interp.table_name}.{interp.column_name}"
                    else:
                        key = f"{interp.table_name}.(table)"
                    self._interp_map[key] = interp
                    if interp.column_id:
                        column_ids.append(interp.column_id)

                # Load entropy objects for all columns
                if column_ids:
                    entropy_objects_result = session.execute(
                        select(EntropyObjectRecord)
                        .where(EntropyObjectRecord.column_id.in_(column_ids))
                        .order_by(
                            EntropyObjectRecord.column_id,
                            EntropyObjectRecord.score.desc(),
                        )
                    )
                    for obj in entropy_objects_result.scalars().all():
                        col_id = obj.column_id
                        if col_id is None:
                            continue
                        if col_id not in self._entropy_objects_map:
                            self._entropy_objects_map[col_id] = []
                        self._entropy_objects_map[col_id].append(obj)

                # Load table-level entropy objects (column_id IS NULL, table_id set)
                tables_result = session.execute(
                    select(Table).where(Table.source_id == source.source_id)
                )
                tables = tables_result.scalars().all()
                table_id_to_name: dict[str, str] = {t.table_id: t.table_name for t in tables}
                table_ids = list(table_id_to_name.keys())

                if table_ids:
                    table_obj_result = session.execute(
                        select(EntropyObjectRecord)
                        .where(
                            EntropyObjectRecord.table_id.in_(table_ids),
                            EntropyObjectRecord.column_id.is_(None),
                        )
                        .order_by(EntropyObjectRecord.score.desc())
                    )
                    for obj in table_obj_result.scalars().all():
                        t_name = table_id_to_name.get(obj.table_id or "", "unknown")
                        table_key = f"{t_name}.(table)"
                        self._table_entropy_objects.setdefault(table_key, []).append(obj)

                # Load compound risks keyed by column target
                if table_ids:
                    cr_result = session.execute(
                        select(CompoundRiskRecord).where(CompoundRiskRecord.table_id.in_(table_ids))
                    )
                    for cr in cr_result.scalars().all():
                        target = cr.target
                        if target.startswith("column:"):
                            col_key = target[7:]  # Remove "column:" prefix
                            self._compound_risks_map.setdefault(col_key, []).append(cr)

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
                    # Delay selection and update until after mount completes
                    self.call_after_refresh(self._select_and_update, self._selected_key, first)

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
        table_level = sum(1 for i in interpretations if not i.column_name)

        # Build status line with all stats
        parts = [
            f"[{color}]{snapshot.overall_readiness.upper()}[/{color}]",
            f"Score: {snapshot.avg_composite_score:.3f}",
            f"Columns: {total - table_level}",
        ]
        if table_level:
            parts.append(f"Table-level: {table_level}")
        parts.append(f"High: {high}")
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

            # Add table-level interpretation node if it exists
            table_key = f"{table_name}.(table)"
            if table_key in self._interp_map or table_key in self._table_entropy_objects:
                table_node.add_leaf(
                    "[dim](table-level)[/dim]",
                    data=table_key,
                )

            # Add columns
            for col in columns:
                if not col.column_name:
                    continue  # Already added as table-level node

                col_color = {"ready": "green", "investigate": "yellow", "blocked": "red"}.get(
                    col.readiness, "white"
                )
                col_label = f"[{col_color}]{col.column_name}[/{col_color}]"
                table_node.add_leaf(col_label, data=f"{table_name}.{col.column_name}")

            table_node.expand()

    def _select_and_update(self, key: str, interp: Any) -> None:
        """Select a tree node by key and update the detail panel."""
        tree: Tree[str] = self.query_one("#entropy-tree", Tree)
        # Find and select the node with matching data
        for node in tree.root.children:
            if node.data == key:
                tree.select_node(node)
                break
            for child in node.children:
                if child.data == key:
                    tree.select_node(child)
                    break
        self._update_detail_panel(interp)

    def on_tree_node_selected(self, event: Tree.NodeSelected[str]) -> None:
        """Handle tree node selection."""
        node: TreeNode[str] = event.node
        if not node.data:
            return

        self._selected_key = node.data

        # Check interpretation map first
        if node.data in self._interp_map:
            self._update_detail_panel(self._interp_map[node.data])
        elif node.data in self._table_entropy_objects:
            # Table-level node with entropy objects but no interpretation
            self._update_table_level_panel(node.data)

    def _update_table_level_panel(self, key: str) -> None:
        """Update detail panel for table-level entropy objects without interpretation."""
        header = self.query_one("#detail-header", Static)
        header.update(
            f"[bold]{key.replace('.(table)', '')}[/bold] | [dim]Table-level metrics[/dim]"
        )

        # Get table-level entropy objects
        objects = self._table_entropy_objects.get(key, [])

        # Update layer bars
        self._update_layer_bars(objects)

        # Overview: show summary from objects
        overview = self.query_one("#overview-content", Static)
        if objects:
            overview.update(
                f"Table-level entropy measurements: {len(objects)} objects across "
                f"{len({o.layer for o in objects})} layers"
            )
        else:
            overview.update("[dim]No table-level measurements[/dim]")

        # Update evidence tab
        self._update_evidence_section(objects)

        # Clear assumptions/actions
        container = self.query_one("#assumptions-list", Vertical)
        container.remove_children()
        container.mount(Static("[dim]No assumptions for table-level metrics[/dim]"))

        actions_container = self.query_one("#actions-list", Vertical)
        actions_container.remove_children()
        actions_container.mount(Static("[dim]No actions for table-level metrics[/dim]"))

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

        # Get entropy objects for this column or table-level
        if interp.column_id:
            entropy_objects = self._entropy_objects_map.get(interp.column_id, [])
        else:
            table_key = f"{interp.table_name}.(table)"
            entropy_objects = self._table_entropy_objects.get(table_key, [])

        # Update all sections
        self._update_layer_bars(entropy_objects)
        self._update_overview_section(interp)
        self._update_evidence_section(entropy_objects)
        self._update_assumptions_section(interp)
        self._update_actions_section(interp, entropy_objects)

    def _update_overview_section(self, interp: Any) -> None:
        """Update the Overview section with explanation and compound risks."""
        overview = self.query_one("#overview-content", Static)
        parts: list[str] = []

        # LLM explanation
        explanation = interp.explanation or "No explanation available"
        parts.append(explanation)

        # Compound risks for this column
        col_key = f"{interp.table_name}.{interp.column_name}" if interp.column_name else ""
        risks = self._compound_risks_map.get(col_key, [])
        if risks:
            parts.append("")
            parts.append("[bold]Compound Risks[/bold]")
            for risk in risks:
                level_colors = {"critical": "red", "high": "yellow", "medium": "white"}
                r_color = level_colors.get(risk.risk_level, "white")
                parts.append(
                    f"  [{r_color}]{risk.risk_level.upper()}[/{r_color}] "
                    f"({risk.multiplier:.1f}x multiplier) "
                    f"Score: {risk.combined_score:.3f}"
                )
                # Show contributing dimensions
                dim_scores = risk.dimension_scores or {}
                if risk.dimensions:
                    dim_parts = []
                    for dim in risk.dimensions:
                        s = dim_scores.get(dim, 0.0)
                        dim_parts.append(f"{dim}: {s:.2f}")
                    parts.append(f"    Dimensions: {', '.join(dim_parts)}")
                if risk.impact:
                    parts.append(f"    Impact: {risk.impact}")

        overview.update("\n".join(parts))

    def _update_evidence_section(self, entropy_objects: Sequence[Any]) -> None:
        """Update the Evidence tab with full detector-specific evidence."""
        content = self.query_one("#evidence-content", Static)

        if not entropy_objects:
            content.update("[dim]No entropy measurements[/dim]")
            return

        parts: list[str] = []

        for obj in entropy_objects:
            # Section header per entropy object
            score_color = "red" if obj.score > 0.3 else "yellow" if obj.score > 0.15 else "green"
            parts.append(
                f"[bold]{obj.layer} > {obj.dimension} > {obj.sub_dimension}[/bold]  "
                f"[{score_color}]{obj.score:.3f}[/{score_color}]  "
                f"[dim]confidence: {obj.confidence:.2f}[/dim]"
            )

            # All evidence fields
            evidence = obj.evidence or {}
            if isinstance(evidence, list):
                evidence = evidence[0] if evidence and isinstance(evidence[0], dict) else {}

            if evidence:
                for key, value in evidence.items():
                    parts.append(f"  {format_evidence_field(key, value)}")
            else:
                parts.append("  [dim]No evidence data[/dim]")

            # Resolution options from this detector
            if obj.resolution_options:
                for opt in obj.resolution_options:
                    if not isinstance(opt, dict):
                        continue
                    action = opt.get("action", "?")
                    reduction = opt.get("expected_entropy_reduction", 0)
                    effort = opt.get("effort", "?")
                    cascade = opt.get("cascade_dimensions", [])
                    parts.append(
                        f"  [dim]-> {action}[/dim]  reduction: {reduction:.0%}, effort: {effort}"
                    )
                    if cascade:
                        parts.append(f"     cascades: {', '.join(cascade)}")

            parts.append("")  # Blank line between objects

        content.update("\n".join(parts))

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

    def _update_actions_section(
        self, interp: Any, entropy_objects: Sequence[Any] | None = None
    ) -> None:
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

        # Build cascade lookup from raw entropy objects' resolution_options
        cascade_by_action: dict[str, list[str]] = {}
        reduction_by_action: dict[str, float] = {}
        if entropy_objects:
            for obj in entropy_objects:
                if not obj.resolution_options:
                    continue
                for opt in obj.resolution_options:
                    if not isinstance(opt, dict):
                        continue
                    act = opt.get("action", "")
                    if act and act not in cascade_by_action:
                        cascade_by_action[act] = opt.get("cascade_dimensions", [])
                        reduction_by_action[act] = opt.get("expected_entropy_reduction", 0.0)

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
            parameters = action.get("parameters", {})

            p_color = priority_colors.get(str(priority).lower(), "white")

            # Title with reduction if available
            reduction = reduction_by_action.get(action_name, 0.0)
            reduction_str = f" {reduction:.0%}" if reduction else ""
            title = (
                f"[{p_color}]{priority.upper()}[/{p_color}] {action_name} "
                f"[dim](effort: {effort}{reduction_str})[/dim]"
            )

            # Full content for expanded state
            content = f"[bold]Description:[/bold]\n{description}"
            if expected_impact:
                content += f"\n\n[bold]Expected Impact:[/bold]\n{expected_impact}"

            # Parameters
            if parameters and isinstance(parameters, dict):
                param_lines = [f"  {k}: {v}" for k, v in parameters.items()]
                content += "\n\n[bold]Parameters:[/bold]\n" + "\n".join(param_lines)

            # Cascade dimensions from raw resolution options
            cascade = cascade_by_action.get(action_name, [])
            if cascade:
                content += "\n\n[bold]Cascades to:[/bold]\n  " + ", ".join(cascade)

            content += f"\n\n[dim]Priority: {priority} | Effort: {effort}[/dim]"

            collapsible = Collapsible(
                Static(content, classes="action-content"),
                title=title,
                collapsed=True,
            )
            container.mount(collapsible)

    def _update_layer_bars(self, entropy_objects: Sequence[Any]) -> None:
        """Update the layer breakdown bars."""
        container = self.query_one("#layer-bars", Static)

        if not entropy_objects:
            container.update("[dim]No measurements[/dim]")
            return

        # Group by layer and calculate averages
        layer_scores: dict[str, list[float]] = {}
        for obj in entropy_objects:
            layer = obj.layer
            if layer not in layer_scores:
                layer_scores[layer] = []
            layer_scores[layer].append(obj.score)

        # Build single line with all layers
        parts: list[str] = []
        layer_order = ["structural", "semantic", "value", "computational"]
        for layer in layer_order:
            scores = layer_scores.get(layer, [])
            if not scores:
                continue  # Skip layers with no data
            avg_score = sum(scores) / len(scores)

            # Color based on score
            if avg_score > 0.3:
                color = "red"
            elif avg_score > 0.15:
                color = "yellow"
            else:
                color = "green"

            short_label = layer[:3].upper()
            parts.append(f"[{color}]{short_label}: {avg_score:.2f}[/{color}]")

        container.update(" | ".join(parts) if parts else "[dim]No layer data[/dim]")
