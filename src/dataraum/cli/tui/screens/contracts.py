"""Contracts screen - contract evaluation with split-panel detail view."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from textual.app import ComposeResult
from textual.containers import Container, Vertical, VerticalScroll
from textual.screen import Screen
from textual.widgets import (
    Collapsible,
    DataTable,
    Static,
    TabbedContent,
    TabPane,
)

from dataraum.cli.common import get_manager


class ContractsScreen(Screen[None]):
    """Contracts screen with split-panel layout showing contract list and details."""

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
        # Entropy interpretations keyed by "table.column" for showing details
        self._interpretations: dict[str, Any] = {}

    def compose(self) -> ComposeResult:
        """Create the screen layout with tabbed contract navigation."""
        with Container(classes="screen-container"):
            yield Static("Contract Evaluation", classes="screen-title")
            yield Static("Loading...", id="summary-status")
            # Contract tabs (populated dynamically)
            yield TabbedContent(id="contract-tabs")
            # Detail panel (updated based on selected contract tab)
            with Vertical(id="contract-detail"):
                yield Static("Select a contract", id="detail-header")
                # Tabs for Overview/Violations/Warnings
                with TabbedContent(id="detail-tabs"):
                    with TabPane("Overview", id="tab-overview"):
                        yield Static("", id="overview-content")
                    with TabPane("Violations", id="tab-violations"):
                        yield Vertical(id="violations-list")
                    with TabPane("Warnings", id="tab-warnings"):
                        yield Vertical(id="warnings-list")
                # Dimensions table
                with Container(id="dimensions-section"):
                    yield Static("[bold]Dimension Scores[/bold]", classes="section-title")
                    yield DataTable(id="dimensions-table")

    def on_mount(self) -> None:
        """Load data when screen mounts."""
        self._load_data()

    def action_refresh(self) -> None:
        """Refresh the data."""
        self._data_loaded = False
        self._evaluations.clear()
        self._profiles.clear()
        self._interpretations.clear()
        self._selected_contract = None
        self._load_data()

    def _load_data(self) -> None:
        """Load contract evaluation data."""
        if self._data_loaded:
            return

        from sqlalchemy import select

        from dataraum.entropy.analysis.aggregator import ColumnSummary, EntropyAggregator
        from dataraum.entropy.contracts import (
            evaluate_all_contracts,
            get_contract,
        )
        from dataraum.entropy.core.storage import EntropyRepository
        from dataraum.entropy.db_models import EntropyInterpretationRecord
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

                if not tables:
                    self._show_error("No tables found. Run pipeline first.")
                    return

                table_ids = [t.table_id for t in tables]

                # Build column summaries
                repo = EntropyRepository(session)
                aggregator = EntropyAggregator()

                typed_table_ids = repo.get_typed_table_ids(table_ids)
                column_summaries: dict[str, ColumnSummary] = {}
                compound_risks: list[Any] = []

                if typed_table_ids:
                    table_map, column_map = repo.get_table_column_mapping(typed_table_ids)
                    entropy_objects = repo.load_for_tables(typed_table_ids, enforce_typed=True)

                    if entropy_objects:
                        column_summaries, _ = aggregator.summarize_columns_by_table(
                            entropy_objects=entropy_objects,
                            table_map=table_map,
                            column_map=column_map,
                        )
                        for summary in column_summaries.values():
                            compound_risks.extend(summary.compound_risks)

                # Load entropy interpretations for showing findings/actions
                interp_result = session.execute(
                    select(EntropyInterpretationRecord).where(
                        EntropyInterpretationRecord.source_id == source.source_id
                    )
                )
                for interp in interp_result.scalars().all():
                    if interp.column_name:
                        key = f"{interp.table_name}.{interp.column_name}"
                        self._interpretations[key] = interp

                # Evaluate all contracts and store
                self._evaluations = evaluate_all_contracts(column_summaries, compound_risks)

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
            # Status icon based on confidence
            if evaluation.confidence_level == ConfidenceLevel.GREEN:
                icon = "●"
            elif evaluation.confidence_level in (
                ConfidenceLevel.YELLOW,
                ConfidenceLevel.ORANGE,
            ):
                icon = "◐"
            else:
                icon = "○"

            # Tab title with icon - add empty Static to avoid showing pane ID
            tab_title = f"{icon} {name}"
            pane = TabPane(tab_title, Static(""), id=f"contract-tab-{name}")
            tabs.add_pane(pane)

    def on_tabbed_content_tab_activated(self, event: TabbedContent.TabActivated) -> None:
        """Handle contract tab selection."""
        if event.tabbed_content.id != "contract-tabs":
            return

        # Extract contract name from pane id
        pane_id = event.pane.id or ""
        if pane_id.startswith("contract-tab-"):
            contract_name = pane_id[13:]  # Remove "contract-tab-" prefix
            self._selected_contract = contract_name
            self._update_detail_panel(contract_name)

    def _select_and_update(self, contract_name: str) -> None:
        """Select a contract tab and update detail panel."""
        tabs = self.query_one("#contract-tabs", TabbedContent)
        pane_id = f"contract-tab-{contract_name}"
        # Activate the tab
        try:
            tabs.active = pane_id
        except Exception:
            pass  # Tab may not exist yet
        self._update_detail_panel(contract_name)

    def _update_detail_panel(self, contract_name: str) -> None:
        """Update the detail panel with selected contract."""
        from dataraum.entropy.contracts import ConfidenceLevel

        evaluation = self._evaluations.get(contract_name)
        profile = self._profiles.get(contract_name)

        if not evaluation or not profile:
            return

        # Update header with status and blocking conditions
        header = self.query_one("#detail-header", Static)
        if evaluation.confidence_level == ConfidenceLevel.GREEN:
            status_str = "[green]PASS[/green]"
        elif evaluation.confidence_level == ConfidenceLevel.YELLOW:
            status_str = "[yellow]WARN[/yellow]"
        elif evaluation.confidence_level == ConfidenceLevel.ORANGE:
            status_str = "[yellow]LOW[/yellow]"
        else:
            status_str = "[red]FAIL[/red]"

        # Collect blocking conditions (violations without dimension)
        blocking_parts: list[str] = []
        for v in evaluation.violations:
            if not v.dimension:
                # Use details if available, otherwise condition name
                desc = v.details or v.condition or "Blocked"
                blocking_parts.append(desc)

        header_lines = [f"[bold]{profile.display_name}[/bold] | {status_str}"]
        if blocking_parts:
            # Show full blocking descriptions on second line
            for desc in blocking_parts:
                header_lines.append(f"[red]{desc}[/red]")

        header.update("\n".join(header_lines))

        # Update all sections
        self._update_overview(evaluation, profile)
        self._update_violations(evaluation)
        self._update_warnings(evaluation)
        self._update_dimensions_table(evaluation, profile)

    def _update_overview(self, evaluation: Any, profile: Any) -> None:
        """Update the overview tab with description and compliance path."""
        overview = self.query_one("#overview-content", Static)

        content_parts = [f"[dim]{profile.description}[/dim]", ""]

        if evaluation.is_compliant:
            content_parts.append("[green]Contract is compliant![/green]")
        else:
            content_parts.append("[bold]Path to Compliance:[/bold]")
            if evaluation.worst_dimension:
                content_parts.append(
                    f"Focus on: [cyan]{evaluation.worst_dimension}[/cyan] "
                    f"(score: {evaluation.worst_dimension_score:.3f})"
                )
            if evaluation.estimated_effort_to_comply:
                content_parts.append(f"Estimated effort: {evaluation.estimated_effort_to_comply}")
            if evaluation.violations:
                content_parts.append("")
                content_parts.append("[bold]Top recommendations:[/bold]")
                for v in evaluation.violations[:3]:
                    if v.dimension:
                        content_parts.append(
                            f"  • Reduce {v.dimension} from {v.actual:.3f} "
                            f"to below {v.max_allowed:.3f}"
                        )

        overview.update("\n".join(content_parts))

    def _update_violations(self, evaluation: Any) -> None:
        """Update the violations tab with collapsible items showing actionable details."""
        container = self.query_one("#violations-list", Vertical)
        container.remove_children()

        # Only show dimension-specific violations (blocking conditions are in header)
        dimension_violations = [v for v in evaluation.violations if v.dimension]

        if not dimension_violations:
            container.mount(Static("[dim]No dimension violations[/dim]"))
            return

        for v in dimension_violations:
            affected = v.affected_columns or []
            # Title with dimension, column count, and score
            title = (
                f"[red]{v.dimension}[/red] | "
                f"{len(affected)} columns | "
                f"{v.actual:.3f} (max: {v.max_allowed:.3f})"
            )

            # Build content as single string for all affected columns
            content_parts: list[str] = []

            for col_key in affected:  # Show all columns
                interp = self._interpretations.get(col_key)
                if interp:
                    content_parts.append(f"[cyan bold]{col_key}[/cyan bold]")
                    # Show full finding (explanation)
                    if interp.explanation:
                        content_parts.append(f"[dim]Finding:[/dim] {interp.explanation}")
                    # Show all recommended actions
                    if interp.resolution_actions_json:
                        actions = interp.resolution_actions_json
                        if actions:
                            content_parts.append("[dim]Actions:[/dim]")
                            for action in actions:
                                action_name = action.get("action", "Unknown")
                                priority = action.get("priority", "")
                                description = action.get("description", "")
                                priority_str = f" [{priority}]" if priority else ""
                                content_parts.append(
                                    f"  [green]•[/green] {action_name}{priority_str}"
                                )
                                if description:
                                    content_parts.append(f"    {description}")
                    content_parts.append("")  # Spacer between columns
                else:
                    content_parts.append(f"[dim]{col_key}[/dim] (no interpretation)")
                    content_parts.append("")

            content_text = "\n".join(content_parts)
            # Create scrollable container with content, pass to Collapsible
            scroll = VerticalScroll(
                Static(content_text),
                classes="violation-scroll",
            )
            collapsible = Collapsible(scroll, title=title, collapsed=True)
            container.mount(collapsible)

    def _update_warnings(self, evaluation: Any) -> None:
        """Update the warnings tab with collapsible items showing details."""
        container = self.query_one("#warnings-list", Vertical)
        container.remove_children()

        # Only show dimension-specific warnings
        dimension_warnings = [w for w in evaluation.warnings if w.dimension]

        if not dimension_warnings:
            container.mount(Static("[dim]No dimension warnings[/dim]"))
            return

        for w in dimension_warnings:
            affected = w.affected_columns or []
            # Title with dimension, column count, and score (approaching threshold)
            title = (
                f"[yellow]{w.dimension}[/yellow] | "
                f"{len(affected)} columns | "
                f"{w.actual:.3f} (threshold: {w.max_allowed:.3f})"
            )

            # Build content with per-column findings
            content_parts: list[str] = []

            for col_key in affected:
                interp = self._interpretations.get(col_key)
                if interp:
                    content_parts.append(f"[cyan bold]{col_key}[/cyan bold]")
                    if interp.explanation:
                        content_parts.append(f"[dim]Finding:[/dim] {interp.explanation}")
                    if interp.resolution_actions_json:
                        actions = interp.resolution_actions_json
                        if actions:
                            content_parts.append("[dim]Actions:[/dim]")
                            for action in actions:
                                action_name = action.get("action", "Unknown")
                                priority = action.get("priority", "")
                                description = action.get("description", "")
                                priority_str = f" [{priority}]" if priority else ""
                                content_parts.append(
                                    f"  [green]•[/green] {action_name}{priority_str}"
                                )
                                if description:
                                    content_parts.append(f"    {description}")
                    content_parts.append("")
                else:
                    content_parts.append(f"[dim]{col_key}[/dim] (no interpretation)")
                    content_parts.append("")

            content_text = "\n".join(content_parts)
            scroll = VerticalScroll(
                Static(content_text),
                classes="violation-scroll",
            )
            collapsible = Collapsible(scroll, title=title, collapsed=True)
            container.mount(collapsible)

    def _update_dimensions_table(self, evaluation: Any, profile: Any) -> None:
        """Update the dimensions score table."""
        table = self.query_one("#dimensions-table", DataTable)
        table.clear(columns=True)

        table.add_column("Dimension", key="dimension")
        table.add_column("Score", key="score")
        table.add_column("Threshold", key="threshold")
        table.add_column("Status", key="status")

        for dim, threshold in profile.dimension_thresholds.items():
            score = evaluation.dimension_scores.get(dim, 0.0)

            if score <= threshold:
                status = "[green]Pass[/green]"
            elif score <= threshold * 1.2:
                status = "[yellow]Warn[/yellow]"
            else:
                status = "[red]Fail[/red]"

            table.add_row(dim, f"{score:.3f}", f"{threshold:.3f}", status)
