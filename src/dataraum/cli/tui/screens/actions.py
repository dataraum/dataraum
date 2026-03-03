"""Actions screen - unified prioritized view of resolution actions."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from textual.app import ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import (
    Static,
    TabbedContent,
    TabPane,
    Tree,
)
from textual.widgets.tree import TreeNode

from dataraum.cli.common import get_manager
from dataraum.cli.tui.formatting import (
    format_evidence_field,
    format_priority_color,
    format_score_color,
)


@dataclass
class MergedAction:
    """A deduplicated, merged resolution action from multiple sources."""

    action: str
    priority: str = "medium"  # high, medium, low
    description: str = ""
    effort: str = "medium"
    expected_impact: str = ""
    parameters: dict[str, Any] = field(default_factory=dict)

    # Aggregated data
    affected_columns: list[str] = field(default_factory=list)
    priority_score: float = 0.0

    # Source tracking
    from_llm: bool = False

    # Related entropy objects for evidence
    related_objects: list[Any] = field(default_factory=list)

    # Contract violations this fixes
    fixes_violations: list[str] = field(default_factory=list)


class ActionsScreen(Screen[None]):
    """Unified actions screen showing prioritized resolution actions."""

    BINDINGS = [
        ("escape", "app.pop_screen", "Back"),
        ("r", "refresh", "Refresh"),
    ]

    def __init__(self, output_dir: Path, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.output_dir = output_dir
        self._data_loaded = False
        self._actions: list[MergedAction] = []
        self._actions_by_priority: dict[str, list[MergedAction]] = {}
        self._selected_action: MergedAction | None = None

    def compose(self) -> ComposeResult:
        """Create the screen layout with tree and detail panel."""
        with Container(classes="screen-container"):
            yield Static("Resolution Actions", classes="screen-title")
            yield Static("Loading...", id="summary-status")
            with Horizontal(classes="split-layout"):
                # Left panel: Tree grouped by priority
                with Vertical(classes="left-panel"):
                    yield Tree("Actions", id="actions-tree")
                # Right panel: Detail tabs
                with Vertical(classes="right-panel"):
                    yield Static("Select an action from the tree", id="detail-header")
                    with TabbedContent(id="detail-tabs"):
                        with TabPane("Action", id="tab-action"):
                            yield Static("", id="action-content")
                        with TabPane("Evidence", id="tab-evidence"):
                            yield Static("", id="evidence-content")
                        with TabPane("Impact", id="tab-impact"):
                            yield Static("", id="impact-content")

    def on_mount(self) -> None:
        """Load data when screen mounts."""
        self._load_data()

    def action_refresh(self) -> None:
        """Refresh the data."""
        self._data_loaded = False
        self._actions.clear()
        self._actions_by_priority.clear()
        self._selected_action = None
        self._load_data()

    def _load_data(self) -> None:
        """Load and merge action data from all sources."""
        if self._data_loaded:
            return

        from sqlalchemy import select

        from dataraum.entropy.contracts import evaluate_all_contracts
        from dataraum.entropy.db_models import (
            EntropyObjectRecord,
        )
        from dataraum.entropy.interpretation_db_models import EntropyInterpretationRecord
        from dataraum.entropy.views.network_context import build_for_network
        from dataraum.entropy.views.query_context import network_to_column_summaries
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

                # Get tables
                tables_result = session.execute(
                    select(Table).where(Table.source_id == source.source_id)
                )
                tables = tables_result.scalars().all()

                if not tables:
                    self._show_error("No tables found. Run pipeline first.")
                    return

                table_ids = [t.table_id for t in tables]

                # Build column_id -> column_key mapping
                col_id_to_key: dict[str, str] = {}
                for tbl in tables:
                    cols_result = session.execute(
                        select(Column).where(Column.table_id == tbl.table_id)
                    )
                    for col in cols_result.scalars().all():
                        col_id_to_key[col.column_id] = f"{tbl.table_name}.{col.column_name}"

                # Source 1: ColumnSummary from network
                network_ctx = build_for_network(session, table_ids)
                column_summaries: dict[str, Any] = network_to_column_summaries(network_ctx)

                # Source 2: LLM resolution_actions_json from interpretations
                interp_result = session.execute(
                    select(EntropyInterpretationRecord).where(
                        EntropyInterpretationRecord.source_id == source.source_id,
                        EntropyInterpretationRecord.column_name.isnot(None),
                    )
                )
                interp_by_col: dict[str, Any] = {}
                for interp in interp_result.scalars().all():
                    col_key = f"{interp.table_name}.{interp.column_name}"
                    interp_by_col[col_key] = interp

                # Source 3: Raw entropy objects for evidence
                entropy_objects_by_col: dict[str, list[Any]] = {}
                if table_ids:
                    eo_result = session.execute(
                        select(EntropyObjectRecord)
                        .where(EntropyObjectRecord.table_id.in_(table_ids))
                        .order_by(EntropyObjectRecord.score.desc())
                    )
                    for obj in eo_result.scalars().all():
                        col_key = col_id_to_key.get(obj.column_id, "") if obj.column_id else ""
                        if col_key:
                            entropy_objects_by_col.setdefault(col_key, []).append(obj)

                # Source 4: Contract violations
                evaluations = evaluate_all_contracts(column_summaries)
                violation_dims: dict[str, list[str]] = {}
                for eval_result in evaluations.values():
                    for v in eval_result.violations:
                        if v.dimension:
                            violation_dims.setdefault(v.dimension, []).extend(v.affected_columns)

                # Merge all sources into MergedAction list
                self._actions = self._merge_actions(
                    interp_by_col=interp_by_col,
                    entropy_objects_by_col=entropy_objects_by_col,
                    violation_dims=violation_dims,
                )

                # Group by priority
                self._actions_by_priority = defaultdict(list)
                for action in self._actions:
                    self._actions_by_priority[action.priority].append(action)

                # Update UI
                self._update_summary(source.name)
                self._update_tree()

                # Select first action
                if self._actions:
                    self._selected_action = self._actions[0]
                    self.call_after_refresh(self._select_first_action)

                self._data_loaded = True
        finally:
            manager.close()

    def _merge_actions(
        self,
        interp_by_col: dict[str, Any],
        entropy_objects_by_col: dict[str, list[Any]],
        violation_dims: dict[str, list[str]],
    ) -> list[MergedAction]:
        """Merge actions from all sources, deduplicate by action name."""
        actions_map: dict[str, MergedAction] = {}

        # From LLM interpretation resolution_actions_json
        for col_key, interp in interp_by_col.items():
            actions = interp.resolution_actions_json
            if isinstance(actions, dict):
                actions = list(actions.values()) if actions else []
            elif not isinstance(actions, list):
                continue

            for action_dict in actions:
                if not isinstance(action_dict, dict):
                    continue

                action_name = action_dict.get("action", "")
                if not action_name:
                    continue

                if action_name not in actions_map:
                    actions_map[action_name] = MergedAction(
                        action=action_name,
                        from_llm=True,
                    )

                ma = actions_map[action_name]
                ma.from_llm = True

                # LLM provides richer metadata, use it if not yet set
                if not ma.description:
                    ma.description = action_dict.get("description", "")
                if not ma.expected_impact:
                    ma.expected_impact = action_dict.get("expected_impact", "")
                if not ma.parameters:
                    ma.parameters = action_dict.get("parameters", {})

                if action_dict.get("effort"):
                    ma.effort = str(action_dict["effort"])

                if col_key not in ma.affected_columns:
                    ma.affected_columns.append(col_key)

                # Add related entropy objects
                for obj in entropy_objects_by_col.get(col_key, []):
                    if obj not in ma.related_objects:
                        ma.related_objects.append(obj)

        # Map contract violations to actions
        for dim, cols in violation_dims.items():
            for ma in actions_map.values():
                # Check if any affected column overlaps
                overlap = set(ma.affected_columns) & set(cols)
                if overlap and dim not in ma.fixes_violations:
                    ma.fixes_violations.append(dim)

        # Calculate priority scores
        effort_factors = {"low": 1.0, "medium": 2.0, "high": 4.0}
        for ma in actions_map.values():
            effort_factor = effort_factors.get(ma.effort, 2.0)
            impact = len(ma.affected_columns) * 0.1
            ma.priority_score = impact / effort_factor

        # Derive priority labels from score thresholds (replaces LLM-assigned labels)
        for ma in actions_map.values():
            if ma.priority_score > 1.0:
                ma.priority = "high"
            elif ma.priority_score > 0.3:
                ma.priority = "medium"
            else:
                ma.priority = "low"

        # Sort by priority_score descending
        result = sorted(
            actions_map.values(),
            key=lambda a: -a.priority_score,
        )

        return result

    def _show_error(self, message: str) -> None:
        """Display an error message."""
        status = self.query_one("#summary-status", Static)
        status.update(f"[red]{message}[/red]")

    def _update_summary(self, source_name: str) -> None:
        """Update summary line."""
        self.query_one(".screen-title", Static).update(f"Resolution Actions: {source_name}")

        high = len(self._actions_by_priority.get("high", []))
        medium = len(self._actions_by_priority.get("medium", []))
        low = len(self._actions_by_priority.get("low", []))

        parts = []
        if high:
            parts.append(f"[red]HIGH: {high}[/red]")
        if medium:
            parts.append(f"[yellow]MEDIUM: {medium}[/yellow]")
        if low:
            parts.append(f"[green]LOW: {low}[/green]")

        # Top action
        if self._actions:
            top = self._actions[0]
            parts.append(f"Top: {top.action} ({len(top.affected_columns)} cols)")

        status = self.query_one("#summary-status", Static)
        status.update(" | ".join(parts) if parts else "[dim]No actions found[/dim]")

    def _update_tree(self) -> None:
        """Build tree grouped by priority."""
        tree: Tree[str] = self.query_one("#actions-tree", Tree)
        tree.clear()
        tree.root.expand()

        priority_labels = {
            "high": ("[red]HIGH[/red]", "red"),
            "medium": ("[yellow]MEDIUM[/yellow]", "yellow"),
            "low": ("[green]LOW[/green]", "green"),
        }

        for priority in ["high", "medium", "low"]:
            actions = self._actions_by_priority.get(priority, [])
            if not actions:
                continue

            label, _color = priority_labels[priority]
            priority_node = tree.root.add(f"{label} ({len(actions)})", data=f"priority:{priority}")

            for action in actions:
                cols = len(action.affected_columns)
                node_label = f"{action.action} [dim]({cols} col{'s' if cols != 1 else ''})[/dim]"
                priority_node.add_leaf(node_label, data=f"action:{action.action}")

            priority_node.expand()

    def _select_first_action(self) -> None:
        """Select the first action in the tree."""
        if self._actions:
            self._update_detail_panel(self._actions[0])

    def on_tree_node_selected(self, event: Tree.NodeSelected[str]) -> None:
        """Handle tree node selection."""
        node: TreeNode[str] = event.node
        if not node.data:
            return

        if node.data.startswith("action:"):
            action_name = node.data[7:]
            for action in self._actions:
                if action.action == action_name:
                    self._selected_action = action
                    self._update_detail_panel(action)
                    break

    def _update_detail_panel(self, action: MergedAction) -> None:
        """Update all detail tabs for selected action."""
        # Header
        header = self.query_one("#detail-header", Static)
        p_color = format_priority_color(action.priority)
        source_str = " [dim](LLM)[/dim]" if action.from_llm else ""

        header.update(
            f"[bold]{action.action}[/bold] | "
            f"[{p_color}]{action.priority.upper()}[/{p_color}] | "
            f"Effort: {action.effort} | "
            f"Columns: {len(action.affected_columns)}{source_str}"
        )

        self._update_action_tab(action)
        self._update_evidence_tab(action)
        self._update_impact_tab(action)

    def _update_action_tab(self, action: MergedAction) -> None:
        """Update the Action tab with description, effort, parameters."""
        content = self.query_one("#action-content", Static)
        parts: list[str] = []

        if action.description:
            parts.append("[bold]Description[/bold]")
            parts.append(action.description)
            parts.append("")

        parts.append(f"[bold]Effort:[/bold] {action.effort}")

        if action.expected_impact:
            parts.append("\n[bold]Expected Impact[/bold]")
            parts.append(action.expected_impact)

        if action.parameters and isinstance(action.parameters, dict):
            parts.append("\n[bold]Parameters[/bold]")
            for k, v in action.parameters.items():
                parts.append(f"  {k}: {v}")

        if action.affected_columns:
            parts.append(f"\n[bold]Affected Columns ({len(action.affected_columns)})[/bold]")
            for col in action.affected_columns:
                parts.append(f"  {col}")

        content.update("\n".join(parts))

    def _update_evidence_tab(self, action: MergedAction) -> None:
        """Update the Evidence tab with entropy objects for affected columns."""
        content = self.query_one("#evidence-content", Static)

        if not action.related_objects:
            content.update("[dim]No evidence data[/dim]")
            return

        parts: list[str] = []

        # Group related objects by column
        objects_by_col: dict[str, list[Any]] = defaultdict(list)
        for obj in action.related_objects:
            # Use target to get column key
            target = obj.target if hasattr(obj, "target") else ""
            col_key = target.replace("column:", "") if target.startswith("column:") else target
            objects_by_col[col_key].append(obj)

        for col_key, objects in objects_by_col.items():
            if col_key:
                parts.append(f"[cyan bold]{col_key}[/cyan bold]")

            for obj in objects[:5]:  # Limit to 5 per column
                color = format_score_color(obj.score)
                parts.append(
                    f"  [{color}]{obj.score:.3f}[/{color}] "
                    f"[bold]{obj.layer}.{obj.dimension}.{obj.sub_dimension}[/bold]"
                )

                evidence = obj.evidence or {}
                if isinstance(evidence, list):
                    evidence = evidence[0] if evidence and isinstance(evidence[0], dict) else {}
                if evidence:
                    for key, value in list(evidence.items())[:3]:
                        parts.append(f"    {format_evidence_field(key, value)}")

            parts.append("")

        content.update("\n".join(parts))

    def _update_impact_tab(self, action: MergedAction) -> None:
        """Update the Impact tab with violation data."""
        content = self.query_one("#impact-content", Static)
        parts: list[str] = []

        # Contract violations this fixes
        if action.fixes_violations:
            parts.append(f"[bold]Fixes Contract Violations ({len(action.fixes_violations)})[/bold]")
            for dim in action.fixes_violations:
                parts.append(f"  [red]{dim}[/red]")
            parts.append("")

        # Priority score
        parts.append(f"[bold]Priority Score:[/bold] {action.priority_score:.3f}")
        parts.append("[dim]Score = (affected_cols * 0.1 + network_impact) / effort_factor[/dim]")

        if not parts:
            parts.append("[dim]No impact data available[/dim]")

        content.update("\n".join(parts))
