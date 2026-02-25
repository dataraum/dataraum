"""Query screen - natural language query interface with split-panel layout."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from textual.app import ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import (
    Collapsible,
    DataTable,
    Input,
    Static,
    TabbedContent,
    TabPane,
    Tree,
)
from textual.widgets.tree import TreeNode

from dataraum.cli.common import get_manager


class QueryScreen(Screen[None]):
    """Query screen with split-panel layout: history tree + tabbed detail."""

    BINDINGS = [
        ("escape", "app.pop_screen", "Back"),
        ("ctrl+l", "clear_history", "Clear"),
    ]

    def __init__(self, output_dir: Path, initial_query: str | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.output_dir = output_dir
        self.initial_query = initial_query
        self._history: list[dict[str, Any]] = []
        self._current_result: Any | None = None

    def compose(self) -> ComposeResult:
        """Create the screen layout."""
        with Container(classes="screen-container"):
            yield Static("Query", classes="screen-title")
            yield Static("Ask questions about your data in natural language", id="subtitle")
            # Input section
            with Container(classes="input-section"):
                yield Input(placeholder="Enter your question...", id="query-input")
            # Status bar (confidence + entropy + reuse)
            yield Static("", id="query-status-bar")
            # Split layout: history left, detail right
            with Horizontal(classes="split-layout"):
                # Left panel: Query history tree
                with Vertical(classes="left-panel"):
                    yield Tree("History", id="query-history-tree")
                # Right panel: Answer + tabs
                with Vertical(classes="right-panel"):
                    # Answer display
                    yield Static("", id="answer-display")
                    # Tabbed content: Results, SQL, Assumptions, Details
                    with TabbedContent(id="query-detail-tabs"):
                        with TabPane("Results", id="tab-results"):
                            yield DataTable(id="results-table")
                        with TabPane("SQL", id="tab-sql"):
                            yield Static("", id="sql-content", classes="sql-content-area")
                        with TabPane("Assumptions", id="tab-assumptions"):
                            yield Vertical(id="query-assumptions-list")
                        with TabPane("Details", id="tab-details"):
                            yield Static("", id="details-content")

    def on_mount(self) -> None:
        """Initialize the screen."""
        # Set up results table
        results_table = self.query_one("#results-table", DataTable)
        results_table.cursor_type = "row"

        # Set up history tree
        tree: Tree[str] = self.query_one("#query-history-tree", Tree)
        tree.root.expand()

        # Load history from database
        self._load_history()

        # Focus input
        self.query_one("#query-input", Input).focus()

        # If initial query provided, run it
        if self.initial_query:
            input_widget = self.query_one("#query-input", Input)
            input_widget.value = self.initial_query

    def on_input_submitted(self, event: Input.Submitted) -> None:
        """Handle query submission."""
        if event.input.id == "query-input" and event.value.strip():
            self._execute_query(event.value.strip())
            event.input.value = ""

    def action_clear_history(self) -> None:
        """Clear the query history display."""
        self._history.clear()
        tree: Tree[str] = self.query_one("#query-history-tree", Tree)
        tree.clear()
        tree.root.expand()

    def on_tree_node_selected(self, event: Tree.NodeSelected[str]) -> None:
        """Handle history tree node selection."""
        node: TreeNode[str] = event.node
        if node.data is None:
            return

        # Find the history entry by index
        try:
            idx = int(node.data)
            if 0 <= idx < len(self._history):
                entry = self._history[idx]
                result = entry.get("result")
                if result:
                    self._display_result(result)
        except (ValueError, IndexError):
            pass

    def _load_history(self) -> None:
        """Load recent queries from database."""
        from sqlalchemy import select

        from dataraum.query.db_models import QueryExecutionRecord
        from dataraum.storage import Source

        manager = get_manager(self.output_dir)

        try:
            with manager.session_scope() as session:
                sources_result = session.execute(select(Source))
                sources = sources_result.scalars().all()

                if not sources:
                    return

                source = sources[0]

                # Get recent query executions
                queries_result = session.execute(
                    select(QueryExecutionRecord)
                    .where(QueryExecutionRecord.source_id == source.source_id)
                    .order_by(QueryExecutionRecord.executed_at.desc())
                    .limit(10)
                )
                queries = queries_result.scalars().all()

                tree: Tree[str] = self.query_one("#query-history-tree", Tree)
                for q in queries:
                    confidence = q.confidence_level or "-"
                    question_text = q.question or "-"
                    question_display = (
                        question_text[:40] + "..." if len(question_text) > 40 else question_text
                    )
                    icon = _confidence_icon(confidence)
                    tree.root.add_leaf(f"{icon} {question_display}", data=None)
        finally:
            manager.close()

    def _execute_query(self, question: str) -> None:
        """Execute a natural language query."""
        from sqlalchemy import select

        from dataraum.query import answer_question
        from dataraum.storage import Source

        manager = get_manager(self.output_dir)

        # Show loading state
        answer_display = self.query_one("#answer-display", Static)
        answer_display.update("[dim]Processing query...[/dim]")

        try:
            with manager.session_scope() as session:
                sources_result = session.execute(select(Source))
                sources = sources_result.scalars().all()

                if not sources:
                    answer_display.update("[red]No sources found[/red]")
                    return

                source = sources[0]

                with manager.duckdb_cursor() as cursor:
                    result = answer_question(
                        question=question,
                        session=session,
                        duckdb_conn=cursor,
                        source_id=source.source_id,
                        manager=manager,
                    )

                if not result.success or not result.value:
                    answer_display.update(f"[red]Error: {result.error}[/red]")
                    return

                query_result = result.value

                # Store in history
                idx = len(self._history)
                self._history.append({"question": question, "result": query_result})

                # Add to history tree
                tree: Tree[str] = self.query_one("#query-history-tree", Tree)
                icon = _confidence_icon(query_result.confidence_level.value)
                q_display = question[:40] + "..." if len(question) > 40 else question
                tree.root.add_leaf(f"{icon} {q_display}", data=str(idx))

                # Display the result
                self._display_result(query_result)

        finally:
            manager.close()

    def _display_result(self, query_result: Any) -> None:
        """Display a QueryResult in the detail panel."""
        self._current_result = query_result

        # Update status bar
        self._update_status_bar(query_result)

        # Update answer display
        self._update_answer(query_result)

        # Update all tabs
        self._update_results_table(query_result.columns, query_result.data)
        self._update_sql_tab(query_result)
        self._update_assumptions_tab(query_result)
        self._update_details_tab(query_result)

    def _update_status_bar(self, query_result: Any) -> None:
        """Update the status bar with confidence, entropy, and reuse info."""
        status_bar = self.query_one("#query-status-bar", Static)

        confidence_colors = {
            "green": "green",
            "yellow": "yellow",
            "orange": "yellow",
            "red": "red",
        }
        color = confidence_colors.get(query_result.confidence_level.value, "white")
        emoji = query_result.confidence_level.emoji

        parts = [f"[{color}]{emoji} {query_result.confidence_level.label}[/{color}]"]

        if query_result.entropy_score is not None:
            e_color = (
                "red"
                if query_result.entropy_score > 0.3
                else "yellow"
                if query_result.entropy_score > 0.15
                else "green"
            )
            parts.append(f"Entropy: [{e_color}]{query_result.entropy_score:.3f}[/{e_color}]")

        if query_result.entropy_action:
            action_labels = {
                "answer_confidently": ("green", "Confident"),
                "answer_with_assumptions": ("yellow", "With Assumptions"),
                "ask_or_caveat": ("dark_orange", "Review Carefully"),
                "refuse": ("red", "Insufficient Data"),
            }
            a_color, a_label = action_labels.get(
                query_result.entropy_action, ("white", query_result.entropy_action)
            )
            parts.append(f"[{a_color}]{a_label}[/{a_color}]")

        if query_result.contract:
            parts.append(f"Contract: {query_result.contract}")

        status_bar.update(" | ".join(parts))

    def _update_answer(self, query_result: Any) -> None:
        """Update the answer display."""
        answer_display = self.query_one("#answer-display", Static)

        if not query_result.success:
            answer_display.update(f"[red]Error: {query_result.error}[/red]")
            return

        answer_display.update(query_result.answer)

    def _update_results_table(
        self, columns: list[str] | None, data: list[dict[str, Any]] | None
    ) -> None:
        """Update the results data table."""
        table = self.query_one("#results-table", DataTable)
        table.clear(columns=True)

        if not columns or not data:
            return

        # Add columns
        for col in columns:
            table.add_column(col, key=col)

        # Add rows (limit to 50)
        for row in data[:50]:
            table.add_row(*[str(row.get(c, "")) for c in columns])

        if len(data) > 50:
            table.add_row(
                *[f"... +{len(data) - 50} more" if i == 0 else "" for i in range(len(columns))]
            )

    def _update_sql_tab(self, query_result: Any) -> None:
        """Update the SQL tab with full SQL display."""
        sql_content = self.query_one("#sql-content", Static)

        if not query_result.sql:
            sql_content.update("[dim]No SQL generated[/dim]")
            return

        sql_content.update(query_result.sql)

    def _update_assumptions_tab(self, query_result: Any) -> None:
        """Update the Assumptions tab with collapsible items."""
        container = self.query_one("#query-assumptions-list", Vertical)
        container.remove_children()

        if not query_result.assumptions:
            container.mount(Static("[dim]No assumptions made[/dim]"))
            return

        conf_colors = {"high": "green", "medium": "yellow", "low": "red"}

        for a in query_result.assumptions:
            conf_value = a.confidence
            if conf_value >= 0.8:
                conf_label = "high"
            elif conf_value >= 0.5:
                conf_label = "medium"
            else:
                conf_label = "low"

            conf_color = conf_colors.get(conf_label, "white")
            title = f"[{conf_color}]{conf_label.upper()}[/{conf_color}] {a.dimension}"

            content = (
                f"[bold]Assumption:[/bold]\n{a.assumption}\n\n"
                f"[bold]Target:[/bold] {a.target}\n\n"
                f"[dim]Confidence: {a.confidence:.2f} | Basis: {a.basis.value}[/dim]"
            )

            collapsible = Collapsible(
                Static(content, classes="assumption-content"),
                title=title,
                collapsed=True,
            )
            container.mount(collapsible)

    def _update_details_tab(self, query_result: Any) -> None:
        """Update the Details tab with analysis metadata."""
        details = self.query_one("#details-content", Static)

        parts: list[str] = []

        # Interpreted question
        if query_result.interpreted_question:
            parts.append(f"[bold]Interpreted as:[/bold]\n{query_result.interpreted_question}")
            parts.append("")

        # Metric type
        parts.append(f"[bold]Metric type:[/bold] {query_result.metric_type}")
        parts.append("")

        # Column mappings
        if query_result.column_mappings:
            parts.append("[bold]Column Mappings:[/bold]")
            for concept, column in query_result.column_mappings.items():
                parts.append(f"  {concept} → {column}")
            parts.append("")

        # Entropy assessment
        if query_result.entropy_action:
            action_labels = {
                "answer_confidently": "Confident",
                "answer_with_assumptions": "With Assumptions",
                "ask_or_caveat": "Review Carefully",
                "refuse": "Insufficient Data",
            }
            ea_label = action_labels.get(query_result.entropy_action, query_result.entropy_action)
            parts.append("[bold]Entropy Assessment:[/bold]")
            parts.append(f"  Action: {ea_label}")
            if query_result.assumptions:
                parts.append(
                    f"  Query clarity: {len(query_result.assumptions)} assumption(s) needed"
                )
            parts.append("")

        # Validation notes
        if query_result.validation_notes:
            parts.append("[bold]Validation Notes:[/bold]")
            for note in query_result.validation_notes:
                parts.append(f"  • {note}")
            parts.append("")

        # Execution info
        parts.append(f"[dim]Execution ID: {query_result.execution_id}[/dim]")
        parts.append(f"[dim]Executed at: {query_result.executed_at}[/dim]")

        details.update("\n".join(parts))


def _confidence_icon(level: str) -> str:
    """Get icon for confidence level."""
    level_lower = level.lower()
    if level_lower == "green":
        return "●"
    elif level_lower in ("yellow", "orange"):
        return "◐"
    else:
        return "○"
