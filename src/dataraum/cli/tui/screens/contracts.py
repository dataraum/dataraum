"""Contracts screen - contract evaluation with tree navigation and detail view."""

from __future__ import annotations

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
from dataraum.cli.tui.formatting import format_evidence_field


class ContractsScreen(Screen[None]):
    """Contracts screen with tree navigation and dimension-specific detail panel."""

    BINDINGS = [
        ("escape", "app.pop_screen", "Back"),
        ("r", "refresh", "Refresh"),
    ]

    def __init__(self, output_dir: Path, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.output_dir = output_dir
        self._data_loaded = False
        self._contract_names: list[str] = []
        self._evaluations: dict[str, Any] = {}
        self._profiles: dict[str, Any] = {}
        self._selected_contract: str | None = None
        # Entropy objects keyed by ("layer.dimension", column_key) for dimension-specific evidence
        # e.g. ("structural.types", "orders.amount") -> EntropyObjectRecord
        self._entropy_by_dim_col: dict[tuple[str, str], list[Any]] = {}
        # Violations/warnings by dimension for tree node -> data mapping
        self._violations_by_dim: dict[str, Any] = {}
        # Interpretations keyed by column key ("table.column") for LLM resolution actions
        self._interp_by_col: dict[str, Any] = {}
        # Column summaries keyed by "table.column" for resolution hints
        self._column_summaries: dict[str, Any] = {}

    def compose(self) -> ComposeResult:
        """Create the screen layout with tree and detail panel."""
        with Container(classes="screen-container"):
            yield Static("Contract Evaluation", classes="screen-title")
            yield Static("Loading...", id="summary-status")
            # Contract tabs (populated dynamically)
            yield TabbedContent(id="contract-tabs")
            # Blocking conditions banner (shown when applicable)
            yield Static("", id="blocking-banner")
            # Split layout: dimension tree + detail panel
            with Horizontal(classes="split-layout"):
                # Left panel: Dimension tree
                with Vertical(classes="left-panel"):
                    yield Tree("", id="contracts-tree")
                # Right panel: Detail for selected dimension
                with Vertical(classes="right-panel"):
                    yield Static("Select a dimension from the tree", id="detail-header")
                    with TabbedContent(id="detail-tabs"):
                        with TabPane("Evidence", id="tab-evidence"):
                            yield Static("", id="evidence-content")
                        with TabPane("Actions", id="tab-actions"):
                            yield Static("", id="actions-content")

    def on_mount(self) -> None:
        """Load data when screen mounts."""
        self._load_data()

    def action_refresh(self) -> None:
        """Refresh the data."""
        self._data_loaded = False
        self._evaluations.clear()
        self._profiles.clear()
        self._entropy_by_dim_col.clear()
        self._violations_by_dim.clear()
        self._interp_by_col.clear()
        self._column_summaries.clear()
        self._selected_contract = None
        self._load_data()

    def _load_data(self) -> None:
        """Load contract evaluation data."""
        if self._data_loaded:
            return

        from sqlalchemy import select

        from dataraum.entropy.contracts import (
            evaluate_all_contracts,
            get_contract,
        )
        from dataraum.entropy.db_models import EntropyObjectRecord
        from dataraum.entropy.interpretation_db_models import EntropyInterpretationRecord
        from dataraum.entropy.views.network_context import build_for_network
        from dataraum.entropy.views.query_context import _network_to_column_summaries
        from dataraum.storage import Column, Source, Table

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

                if not tables:
                    self._show_error("No tables found. Run pipeline first.")
                    return

                table_ids = [t.table_id for t in tables]

                # Build column summaries via network
                network_ctx = build_for_network(session, table_ids)
                column_summaries = _network_to_column_summaries(network_ctx)

                self._column_summaries = column_summaries

                # Load entropy objects for dimension-specific evidence
                # Build column_id -> column_key mapping
                col_id_to_key: dict[str, str] = {}
                for tbl in tables:
                    cols_result = session.execute(
                        select(Column).where(Column.table_id == tbl.table_id)
                    )
                    for col in cols_result.scalars().all():
                        col_id_to_key[col.column_id] = f"{tbl.table_name}.{col.column_name}"

                # Load all entropy objects for source tables
                # Key by ("layer.dimension", column_key) to match contract dimension format
                if table_ids:
                    eo_result = session.execute(
                        select(EntropyObjectRecord).where(
                            EntropyObjectRecord.table_id.in_(table_ids)
                        )
                    )
                    for obj in eo_result.scalars().all():
                        col_key = col_id_to_key.get(obj.column_id, "") if obj.column_id else ""
                        if col_key:
                            # Contract dimensions are "layer.dimension" (e.g. "structural.types")
                            # EntropyObjectRecord stores layer and dimension separately
                            contract_dim = f"{obj.layer}.{obj.dimension}"
                            dim_key = (contract_dim, col_key)
                            self._entropy_by_dim_col.setdefault(dim_key, []).append(obj)

                # Load interpretations keyed by column key for action data
                interp_result = session.execute(
                    select(EntropyInterpretationRecord).where(
                        EntropyInterpretationRecord.source_id == source.source_id,
                        EntropyInterpretationRecord.column_name.isnot(None),
                    )
                )
                for interp in interp_result.scalars().all():
                    col_key = f"{interp.table_name}.{interp.column_name}"
                    self._interp_by_col[col_key] = interp

                # Evaluate all contracts and store
                self._evaluations = evaluate_all_contracts(column_summaries)

                # Store profiles
                for name in self._evaluations:
                    profile = get_contract(name)
                    if profile:
                        self._profiles[name] = profile

                # Update UI
                self._update_summary(source.name)
                self._update_contracts_tabs()

                # Select first contract
                if self._contract_names:
                    self._selected_contract = self._contract_names[0]
                    self.call_after_refresh(self._select_and_update, self._selected_contract)

                self._data_loaded = True
        finally:
            manager.close()

    def _show_error(self, message: str) -> None:
        """Display an error message."""
        status = self.query_one("#summary-status", Static)
        status.update(f"[red]{message}[/red]")

    def _update_summary(self, source_name: str) -> None:
        """Update the summary line with merged stats."""
        self.query_one(".screen-title", Static).update(f"Contract Evaluation: {source_name}")

        passing = [e for e in self._evaluations.values() if e.is_compliant]
        total_violations = sum(len(e.violations) for e in self._evaluations.values())
        total_warnings = sum(len(e.warnings) for e in self._evaluations.values())

        # Find strictest passing
        strictest = ""
        if passing:

            def _get_threshold(name: str) -> float:
                p = self._profiles.get(name)
                return p.overall_threshold if p else 1.0

            strictest_eval = min(
                passing,
                key=lambda e: _get_threshold(e.contract_name),
            )
            strictest = strictest_eval.contract_name

        # Build status line
        parts = [f"[green]PASS {len(passing)}/{len(self._evaluations)}[/green]"]
        if strictest:
            parts.append(f"Strictest: {strictest}")
        if total_violations:
            parts.append(f"[red]Violations: {total_violations}[/red]")
        if total_warnings:
            parts.append(f"[yellow]Warnings: {total_warnings}[/yellow]")

        status = self.query_one("#summary-status", Static)
        status.update(" | ".join(parts))

    def _update_contracts_tabs(self) -> None:
        """Build the contract tabs."""
        from dataraum.entropy.contracts import ConfidenceLevel

        tabs = self.query_one("#contract-tabs", TabbedContent)
        tabs.clear_panes()

        def _get_threshold(name: str) -> float:
            p = self._profiles.get(name)
            return p.overall_threshold if p else 1.0

        # Sort by strictness (most lenient first)
        sorted_evals = sorted(
            self._evaluations.items(),
            key=lambda x: _get_threshold(x[0]),
            reverse=True,
        )

        self._contract_names = [name for name, _ in sorted_evals]

        for name, evaluation in sorted_evals:
            if evaluation.confidence_level == ConfidenceLevel.GREEN:
                icon = "●"
            elif evaluation.confidence_level in (
                ConfidenceLevel.YELLOW,
                ConfidenceLevel.ORANGE,
            ):
                icon = "◐"
            else:
                icon = "○"

            tab_title = f"{icon} {name}"
            pane = TabPane(tab_title, Static(""), id=f"contract-tab-{name}")
            tabs.add_pane(pane)

    def on_tabbed_content_tab_activated(self, event: TabbedContent.TabActivated) -> None:
        """Handle contract tab selection."""
        if event.tabbed_content.id != "contract-tabs":
            return

        pane_id = event.pane.id or ""
        if pane_id.startswith("contract-tab-"):
            contract_name = pane_id[13:]
            self._selected_contract = contract_name
            self._update_for_contract(contract_name)

    def _select_and_update(self, contract_name: str) -> None:
        """Select a contract tab and update tree."""
        tabs = self.query_one("#contract-tabs", TabbedContent)
        pane_id = f"contract-tab-{contract_name}"
        try:
            tabs.active = pane_id
        except Exception:
            pass
        self._update_for_contract(contract_name)

    def _update_for_contract(self, contract_name: str) -> None:
        """Update tree and detail panel for selected contract."""
        evaluation = self._evaluations.get(contract_name)
        profile = self._profiles.get(contract_name)

        if not evaluation or not profile:
            return

        # Build dimension tree
        self._build_dimension_tree(evaluation, profile)

        # Reset detail panel
        self.query_one("#detail-header", Static).update(
            "[dim]Select a dimension from the tree[/dim]"
        )
        self.query_one("#evidence-content", Static).update("")
        self.query_one("#actions-content", Static).update("")

    def _build_dimension_tree(self, evaluation: Any, profile: Any) -> None:
        """Build tree organized by layer > dimension with status colors."""
        tree: Tree[str] = self.query_one("#contracts-tree", Tree)
        tree.clear()
        tree.show_root = False
        tree.root.expand()

        # Index violations/warnings by dimension for quick lookup
        self._violations_by_dim = {}
        for v in evaluation.violations:
            if v.dimension:
                self._violations_by_dim[v.dimension] = v
        for w in evaluation.warnings:
            if w.dimension and w.dimension not in self._violations_by_dim:
                self._violations_by_dim[w.dimension] = w

        # Group contract dimensions by layer
        layers: dict[str, list[tuple[str, float]]] = {}
        for dim, threshold in profile.dimension_thresholds.items():
            parts = dim.split(".", 1)
            layer = parts[0] if len(parts) > 1 else dim
            layers.setdefault(layer, []).append((dim, threshold))

        # Build tree nodes — layers are direct children of root
        for layer, dims in layers.items():
            # Determine layer status from its dimensions
            layer_status = "pass"
            for dim, _threshold in dims:
                v = self._violations_by_dim.get(dim)
                if v and v.severity == "blocking":
                    layer_status = "fail"
                    break
                elif v:
                    layer_status = "warn"

            layer_color = {"pass": "green", "warn": "yellow", "fail": "red"}[layer_status]
            layer_node = tree.root.add(
                f"[{layer_color}]{layer}[/{layer_color}]",
                data=f"layer:{layer}",
            )

            for dim, threshold in dims:
                score = evaluation.dimension_scores.get(dim, 0.0)
                v = self._violations_by_dim.get(dim)

                # Determine color from status
                if v and v.severity == "blocking":
                    color = "red"
                elif v:
                    color = "yellow"
                else:
                    color = "green"

                # Sub-dimension label (part after the dot)
                sub_dim = dim.split(".", 1)[1] if "." in dim else dim
                affected_count = len(v.affected_columns) if v else 0
                affected_str = f" ({affected_count})" if affected_count else ""

                label = (
                    f"[{color}]{sub_dim}[/{color}] "
                    f"[dim]{score:.2f}/{threshold:.2f}[/dim]"
                    f"{affected_str}"
                )
                layer_node.add_leaf(label, data=f"dim:{dim}")

            layer_node.expand()

        # Update blocking conditions banner (outside tree)
        blocking_conditions = [v for v in evaluation.violations if not v.dimension]
        banner = self.query_one("#blocking-banner", Static)
        if blocking_conditions:
            parts = []
            for v in blocking_conditions:
                desc = v.details or v.condition or "Blocked"
                parts.append(desc)
            banner.update("[red bold]Blocking:[/red bold] " + " | ".join(parts))
        else:
            banner.update("[green]No blocking conditions[/green]")

    def on_tree_node_selected(self, event: Tree.NodeSelected[str]) -> None:
        """Handle dimension tree node selection."""
        node: TreeNode[str] = event.node
        if not node.data:
            return

        if node.data.startswith("dim:"):
            dimension = node.data[4:]
            v = self._violations_by_dim.get(dimension)
            evaluation = self._evaluations.get(self._selected_contract or "")
            profile = self._profiles.get(self._selected_contract or "")
            if evaluation and profile:
                score = evaluation.dimension_scores.get(dimension, 0.0)
                threshold = profile.dimension_thresholds.get(dimension, 0.0)
                self._show_dimension_detail(dimension, score, threshold, v)

    def _show_dimension_detail(
        self,
        dimension: str,
        score: float,
        threshold: float,
        violation: Any | None,
    ) -> None:
        """Update right panel for selected dimension."""
        header = self.query_one("#detail-header", Static)

        # Status tag
        if violation and violation.severity == "blocking":
            status_tag = "[red]FAIL[/red]"
        elif violation:
            status_tag = "[yellow]WARN[/yellow]"
        else:
            status_tag = "[green]PASS[/green]"

        affected_count = len(violation.affected_columns) if violation else 0
        affected_str = f" | Affected: {affected_count}" if affected_count else ""

        header.update(
            f"[bold]{dimension}[/bold] | {status_tag} | "
            f"Score: {score:.3f} | Threshold: {threshold:.3f}{affected_str}"
        )

        affected_columns = violation.affected_columns if violation else []

        # Update all tabs
        self._update_evidence_panel(dimension, affected_columns)
        self._update_actions_panel(dimension, affected_columns)

    def _update_evidence_panel(self, dimension: str, affected_columns: list[str]) -> None:
        """Show dimension-specific evidence for affected columns."""
        content_parts: list[str] = []

        for col_key in affected_columns:
            dim_key = (dimension, col_key)
            objects = self._entropy_by_dim_col.get(dim_key, [])

            content_parts.append(f"[cyan bold]{col_key}[/cyan bold]")

            if objects:
                for obj in objects:
                    score_color = (
                        "red" if obj.score > 0.3 else "yellow" if obj.score > 0.15 else "green"
                    )
                    content_parts.append(
                        f"  [bold]{obj.sub_dimension}[/bold]  "
                        f"[{score_color}]{obj.score:.3f}[/{score_color}]"
                    )

                    # Show all evidence fields
                    evidence = obj.evidence or {}
                    if isinstance(evidence, list):
                        evidence = evidence[0] if evidence and isinstance(evidence[0], dict) else {}
                    if evidence:
                        for key, value in evidence.items():
                            content_parts.append(f"    {format_evidence_field(key, value)}")
                    else:
                        content_parts.append("    [dim]No evidence data[/dim]")
            else:
                content_parts.append("  [dim]No dimension-specific evidence[/dim]")

            content_parts.append("")

        if not content_parts:
            content_parts.append("[dim]No affected columns for this dimension[/dim]")

        self.query_one("#evidence-content", Static).update("\n".join(content_parts))

    def _update_actions_panel(self, dimension: str, affected_columns: list[str]) -> None:
        """Show resolution actions from LLM interpretations, filtered by dimension."""
        content_parts: list[str] = []
        priority_colors = {"high": "red", "medium": "yellow", "low": "green"}

        # Collect top resolution hints from ColumnSummary for affected columns
        hint_counts: dict[str, int] = {}  # action -> number of columns affected
        for col_key in affected_columns:
            summary = self._column_summaries.get(col_key)
            if not summary or not summary.top_resolution_hints:
                continue
            for hint in summary.top_resolution_hints:
                hint_counts[hint.action] = hint_counts.get(hint.action, 0) + 1

        # Show summary header if hints found
        if hint_counts:
            top_action = max(hint_counts, key=lambda a: hint_counts[a])
            top_count = hint_counts[top_action]
            content_parts.append(
                f"[bold]Top action:[/bold] {top_action}, "
                f"affects {top_count} column{'s' if top_count != 1 else ''}"
            )
            content_parts.append("")

        # Primary source: LLM interpretation actions filtered by dimension
        for col_key in affected_columns:
            interp = self._interp_by_col.get(col_key)
            if not interp:
                continue

            actions = interp.resolution_actions_json
            if isinstance(actions, dict):
                actions = list(actions.values()) if actions else []
            elif not isinstance(actions, list):
                continue

            # Filter to actions relevant to this dimension
            col_actions = [
                a
                for a in actions
                if isinstance(a, dict) and _action_matches_dimension(a, dimension)
            ]

            if not col_actions:
                continue

            content_parts.append(f"[cyan bold]{col_key}[/cyan bold]")

            for action in col_actions:
                priority = action.get("priority", "medium")
                action_name = action.get("action", "-")
                description = action.get("description", "")
                effort = action.get("effort", "-")
                expected_impact = action.get("expected_impact", "")
                parameters = action.get("parameters", {})

                p_color = priority_colors.get(str(priority).lower(), "white")

                # Header: priority + action + effort
                content_parts.append(
                    f"  [{p_color}]{priority.upper()}[/{p_color}] {action_name} "
                    f"[dim](effort: {effort})[/dim]"
                )

                if description:
                    content_parts.append(f"    {description}")
                if expected_impact:
                    content_parts.append(f"    [dim]impact: {expected_impact}[/dim]")

                # Parameters
                if parameters and isinstance(parameters, dict):
                    param_str = ", ".join(f"{k}={v}" for k, v in parameters.items())
                    content_parts.append(f"    [dim]params: {param_str}[/dim]")

            content_parts.append("")

        if not content_parts:
            content_parts.append("[dim]No resolution actions for this dimension[/dim]")

        self.query_one("#actions-content", Static).update("\n".join(content_parts))


def _action_matches_dimension(action: dict[str, Any], dimension: str) -> bool:
    """Check if an LLM resolution action is relevant to a contract dimension.

    Matches by checking the action's expected_impact and action name against
    the dimension path (e.g. "structural.types", "semantic.units").
    """
    # Extract the sub-dimension part (e.g. "types" from "structural.types")
    parts = dimension.split(".", 1)
    sub = parts[1] if len(parts) > 1 else ""

    # Check expected_impact text for dimension references
    impact = str(action.get("expected_impact", "")).lower()
    if dimension.lower() in impact or sub.lower() in impact:
        return True

    # Map common action names to their related dimensions
    # Actions follow taxonomy: document_*, investigate_*, transform_*, create_*
    action_name = str(action.get("action", "")).lower()
    action_to_dim = {
        "types": ["document_type_override", "transform_quarantine_values"],
        "units": ["document_unit"],
        "nulls": ["document_null_semantics", "transform_filter_nulls", "transform_impute_values"],
        "outliers": ["transform_winsorize", "transform_exclude_outliers", "investigate_outliers"],
        "relations": [
            "document_relationship",
            "document_join_path",
            "transform_fix_referential_integrity",
        ],
        "business_meaning": [
            "document_description",
            "document_business_name",
            "document_entity_type",
        ],
        "temporal": ["document_timestamp_role", "transform_resolve_temporal_mismatch"],
        "derived_values": ["document_formula", "investigate_formula_mismatches"],
    }

    related_actions = action_to_dim.get(sub, [])
    if any(ra in action_name for ra in related_actions):
        return True

    # Also match layer-level keywords in the action or description
    description = str(action.get("description", "")).lower()
    if sub and sub.lower() in description:
        return True

    return False
