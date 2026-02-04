"""Contracts screen - contract evaluation with traffic lights."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from textual.app import ComposeResult
from textual.containers import Container, Vertical
from textual.screen import Screen
from textual.widgets import DataTable, Static

from dataraum.cli.common import get_manager


class ContractsScreen(Screen[None]):
    """Contracts screen showing contract compliance with traffic light indicators."""

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
                Static("Contract Evaluation", classes="screen-title"),
                Static("Loading...", id="status-line"),
                # Contracts table
                Container(
                    Static("Contract Compliance", classes="section-title"),
                    DataTable(id="contracts-table"),
                    classes="contracts-section",
                ),
                # Summary
                Container(
                    Static("Summary", classes="section-title"),
                    Static("", id="summary-text"),
                    classes="summary-section",
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

                # Evaluate all contracts
                evaluations = evaluate_all_contracts(column_summaries, compound_risks)

                # Update UI
                self._update_status(source.name)
                self._update_contracts_table(evaluations, get_contract)
                self._update_summary(evaluations, get_contract)

                self._data_loaded = True
        finally:
            manager.close()

    def _show_error(self, message: str) -> None:
        """Display an error message."""
        status = self.query_one("#status-line", Static)
        status.update(f"[red]{message}[/red]")

    def _update_status(self, source_name: str) -> None:
        """Update the status line."""
        self.query_one(".screen-title", Static).update(f"Contract Evaluation: {source_name}")
        status = self.query_one("#status-line", Static)
        status.update(f"Source: {source_name}")

    def _update_contracts_table(self, evaluations: dict[str, Any], get_contract: Any) -> None:
        """Update the contracts data table."""
        from dataraum.entropy.contracts import ConfidenceLevel

        table_widget = self.query_one("#contracts-table", DataTable)
        table_widget.clear(columns=True)

        table_widget.add_column("Status", key="status")
        table_widget.add_column("Contract", key="contract")
        table_widget.add_column("Description", key="description")
        table_widget.add_column("Issues", key="issues")

        def _get_threshold(name: str) -> float:
            c = get_contract(name)
            return c.overall_threshold if c else 1.0

        # Sort by strictness (most lenient first)
        sorted_evals = sorted(
            evaluations.items(),
            key=lambda x: _get_threshold(x[0]),
            reverse=True,
        )

        for name, evaluation in sorted_evals:
            # Traffic light status
            if evaluation.confidence_level == ConfidenceLevel.GREEN:
                status = "[green]Pass[/green]"
            elif evaluation.confidence_level == ConfidenceLevel.YELLOW:
                status = "[yellow]Warn[/yellow]"
            elif evaluation.confidence_level == ConfidenceLevel.ORANGE:
                status = "[yellow]Low[/yellow]"
            else:
                status = "[red]Fail[/red]"

            profile = get_contract(name)
            if profile:
                desc = (
                    profile.description[:35] + "..."
                    if len(profile.description) > 35
                    else profile.description
                )
            else:
                desc = ""

            issues = len(evaluation.violations) + len(evaluation.warnings)
            issue_str = str(issues) if issues > 0 else "-"

            table_widget.add_row(status, name, desc, issue_str)

    def _update_summary(self, evaluations: dict[str, Any], get_contract: Any) -> None:
        """Update the summary text."""
        passing = [e for e in evaluations.values() if e.is_compliant]

        summary_parts = [f"Passing: {len(passing)}/{len(evaluations)} contracts"]

        if passing:

            def _get_threshold(name: str) -> float:
                c = get_contract(name)
                return c.overall_threshold if c else 1.0

            strictest = min(
                passing,
                key=lambda e: _get_threshold(e.contract_name),
            )
            summary_parts.append(f"Strictest passing: {strictest.contract_name}")

        summary = self.query_one("#summary-text", Static)
        summary.update(" | ".join(summary_parts))
