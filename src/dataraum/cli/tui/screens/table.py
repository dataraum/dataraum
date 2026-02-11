"""Table detail screen - column details, semantics, relationships, and quality."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from textual.app import ComposeResult
from textual.containers import Container
from textual.screen import Screen
from textual.widgets import DataTable, Static, TabbedContent, TabPane

from dataraum.cli.common import get_manager
from dataraum.cli.tui.formatting import format_score_color


class TableScreen(Screen[None]):
    """Table detail screen with columns table, tabbed details, and sample data."""

    BINDINGS = [
        ("escape", "app.pop_screen", "Back"),
        ("r", "refresh", "Refresh"),
    ]

    def __init__(self, output_dir: Path, table_name: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.output_dir = output_dir
        self.table_name = table_name
        self._data_loaded = False
        self._column_names: list[str] = []
        # Entropy objects keyed by column_id for the Entropy tab
        self._entropy_objects_by_col: dict[str, list[Any]] = {}
        # column_name -> column_id mapping for row highlight lookup
        self._col_name_to_id: dict[str, str] = {}

    def compose(self) -> ComposeResult:
        """Create the screen layout."""
        with Container(classes="screen-container"):
            yield Static(f"Table: {self.table_name}", classes="screen-title")
            yield Static("Loading...", id="table-info")
            # Columns section
            with Container(classes="columns-section"):
                yield Static("Columns", classes="section-title")
                yield DataTable(id="columns-table")
            # Tabbed detail panels
            with TabbedContent(id="table-detail-tabs"):
                with TabPane("Statistics", id="tab-statistics"):
                    yield DataTable(id="stats-table")
                with TabPane("Semantics", id="tab-semantics"):
                    yield DataTable(id="semantics-table")
                with TabPane("Relationships", id="tab-relationships"):
                    yield DataTable(id="relationships-table")
                with TabPane("Quality", id="tab-quality"):
                    yield DataTable(id="quality-table")
                with TabPane("Entity", id="tab-entity"):
                    yield Static("", id="entity-content")
                with TabPane("Entropy", id="tab-entropy"):
                    yield Static("", id="entropy-content")
            # Sample data section
            with Container(classes="sample-section"):
                yield Static("Sample Data", classes="section-title")
                yield DataTable(id="sample-table")

    def on_mount(self) -> None:
        """Load data when screen mounts."""
        self._load_data()

    def action_refresh(self) -> None:
        """Refresh the data."""
        self._data_loaded = False
        self._load_data()

    def _load_data(self) -> None:
        """Load table data from database."""
        if self._data_loaded:
            return

        from sqlalchemy import select

        from dataraum.analysis.relationships.db_models import Relationship
        from dataraum.analysis.semantic.db_models import SemanticAnnotation, TableEntity
        from dataraum.analysis.statistics.db_models import StatisticalProfile
        from dataraum.analysis.typing.db_models import TypeDecision
        from dataraum.entropy.db_models import EntropyInterpretationRecord, EntropyObjectRecord
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

                # Get typed table — all detail tabs (entropy, semantics, etc.) require typed layer
                table_result = session.execute(
                    select(Table).where(
                        Table.source_id == source.source_id,
                        Table.table_name == self.table_name,
                        Table.layer == "typed",
                    )
                )
                table = table_result.scalar_one_or_none()

                if not table:
                    self._show_error(f"Table not found: {self.table_name}")
                    return

                # Get columns
                columns_result = session.execute(
                    select(Column).where(Column.table_id == table.table_id)
                )
                columns = columns_result.scalars().all()

                # Build column_id -> column_name map
                col_name_map = {c.column_id: c.column_name for c in columns}
                self._col_name_to_id = {c.column_name: c.column_id for c in columns}

                # Get type decisions
                type_decisions: dict[str, Any] = {}
                for col in columns:
                    td_result = session.execute(
                        select(TypeDecision).where(TypeDecision.column_id == col.column_id)
                    )
                    td = td_result.scalar_one_or_none()
                    if td:
                        type_decisions[col.column_id] = td

                # Get entropy interpretations
                entropy_interps: dict[str, Any] = {}
                for col in columns:
                    ei_result = session.execute(
                        select(EntropyInterpretationRecord).where(
                            EntropyInterpretationRecord.column_id == col.column_id
                        )
                    )
                    ei = ei_result.scalar_one_or_none()
                    if ei:
                        entropy_interps[col.column_id] = ei

                # Get semantic annotations
                semantic_annotations: dict[str, Any] = {}
                for col in columns:
                    sa_result = session.execute(
                        select(SemanticAnnotation).where(
                            SemanticAnnotation.column_id == col.column_id
                        )
                    )
                    sa = sa_result.scalar_one_or_none()
                    if sa:
                        semantic_annotations[col.column_id] = sa

                # Get relationships (from this table)
                relationships_result = session.execute(
                    select(Relationship).where(
                        Relationship.from_table_id == table.table_id,
                    )
                )
                relationships_from = relationships_result.scalars().all()

                # Also get relationships TO this table
                relationships_to_result = session.execute(
                    select(Relationship).where(
                        Relationship.to_table_id == table.table_id,
                    )
                )
                relationships_to = relationships_to_result.scalars().all()
                all_relationships = list(relationships_from) + list(relationships_to)

                # Get statistical profiles
                stat_profiles: dict[str, Any] = {}
                for col in columns:
                    sp_result = session.execute(
                        select(StatisticalProfile)
                        .where(StatisticalProfile.column_id == col.column_id)
                        .order_by(StatisticalProfile.profiled_at.desc())
                        .limit(1)
                    )
                    sp = sp_result.scalar_one_or_none()
                    if sp:
                        stat_profiles[col.column_id] = sp

                # Get table entity
                entity_result = session.execute(
                    select(TableEntity).where(TableEntity.table_id == table.table_id)
                )
                table_entity = entity_result.scalar_one_or_none()

                # Load entropy objects for all columns in this table
                column_ids = [c.column_id for c in columns]
                if column_ids:
                    eo_result = session.execute(
                        select(EntropyObjectRecord)
                        .where(EntropyObjectRecord.column_id.in_(column_ids))
                        .order_by(
                            EntropyObjectRecord.column_id,
                            EntropyObjectRecord.score.desc(),
                        )
                    )
                    for obj in eo_result.scalars().all():
                        if obj.column_id:
                            self._entropy_objects_by_col.setdefault(obj.column_id, []).append(obj)

                # Update UI
                self._update_table_info(table)
                self._update_columns_table(
                    columns, type_decisions, entropy_interps, semantic_annotations
                )
                self._update_statistics_tab(columns, stat_profiles)
                self._update_semantics_tab(columns, semantic_annotations)
                self._update_relationships_tab(all_relationships, col_name_map)
                self._update_quality_tab(columns, stat_profiles)
                self._update_entity_tab(table_entity)

                # Load sample data
                with manager.duckdb_cursor() as cursor:
                    self._load_sample_data(cursor, table.table_name, columns)

                self._data_loaded = True
        finally:
            manager.close()

    def _show_error(self, message: str) -> None:
        """Display an error message."""
        info = self.query_one("#table-info", Static)
        info.update(f"[red]{message}[/red]")

    def _update_table_info(self, table: Any) -> None:
        """Update the table info section."""
        info = self.query_one("#table-info", Static)
        layer = table.layer or "unknown"
        rows = table.row_count or "?"
        info.update(f"Layer: {layer} | Rows: {rows}")

    def _update_columns_table(
        self,
        columns: Sequence[Any],
        type_decisions: dict[str, Any],
        entropy_interps: dict[str, Any],
        semantic_annotations: dict[str, Any],
    ) -> None:
        """Update the columns data table with Role column."""
        table = self.query_one("#columns-table", DataTable)
        table.clear(columns=True)
        table.cursor_type = "row"

        self._column_names = [col.column_name for col in columns]

        table.add_column("Column", key="column")
        table.add_column("Type", key="type")
        table.add_column("Role", key="role")
        table.add_column("Entropy", key="entropy")
        table.add_column("Status", key="status")

        readiness_icons = {
            "ready": "[green]Ready[/green]",
            "investigate": "[yellow]Investigate[/yellow]",
            "blocked": "[red]Blocked[/red]",
        }

        for col in columns:
            td = type_decisions.get(col.column_id)
            ei = entropy_interps.get(col.column_id)
            sa = semantic_annotations.get(col.column_id)

            col_type = td.decided_type if td else col.inferred_type or "-"
            role = sa.semantic_role if sa else "-"
            entropy = f"{ei.composite_score:.3f}" if ei else "-"
            status = readiness_icons.get(ei.readiness, "-") if ei else "-"

            table.add_row(col.column_name, col_type, role, entropy, status)

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        """Handle row selection to navigate to entropy screen for this table."""
        if event.data_table.id != "columns-table":
            return

        if event.cursor_row >= len(self._column_names):
            return

        from dataraum.cli.tui.screens.entropy import EntropyScreen

        screen = EntropyScreen(self.output_dir, table_filter=self.table_name)
        self.app.push_screen(screen)

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        """Update Entropy tab when cursor moves over a column row."""
        if event.data_table.id != "columns-table":
            return

        if event.cursor_row >= len(self._column_names):
            return

        col_name = self._column_names[event.cursor_row]
        col_id = self._col_name_to_id.get(col_name)
        if col_id:
            self._update_entropy_tab(col_name, col_id)

    def _update_entropy_tab(self, col_name: str, col_id: str) -> None:
        """Update the Entropy tab with dimension breakdown for a column."""
        content = self.query_one("#entropy-content", Static)
        objects = self._entropy_objects_by_col.get(col_id, [])

        if not objects:
            content.update(f"[dim]No entropy data for {col_name}[/dim]")
            return

        parts: list[str] = []
        parts.append(f"[bold]{col_name}[/bold]")
        parts.append("")

        # Layer bars summary
        layer_scores: dict[str, list[float]] = {}
        for obj in objects:
            layer_scores.setdefault(obj.layer, []).append(obj.score)

        layer_order = ["structural", "semantic", "value", "computational"]
        bar_parts: list[str] = []
        for layer in layer_order:
            scores = layer_scores.get(layer, [])
            if not scores:
                continue
            avg = sum(scores) / len(scores)
            color = format_score_color(avg)
            short = layer[:3].upper()
            bar_parts.append(f"[{color}]{short}: {avg:.2f}[/{color}]")

        if bar_parts:
            parts.append(" | ".join(bar_parts))
            parts.append("")

        # Dimension scores with top resolution hint per dimension
        for obj in objects:
            color = format_score_color(obj.score)
            parts.append(
                f"  [{color}]{obj.score:.3f}[/{color}] "
                f"[bold]{obj.dimension}.{obj.sub_dimension}[/bold]  "
                f"[dim]conf: {obj.confidence:.2f}[/dim]"
            )

            # Show top resolution hint for this dimension
            if obj.resolution_options:
                for opt in obj.resolution_options[:1]:
                    if not isinstance(opt, dict):
                        continue
                    action = opt.get("action", "?")
                    reduction = opt.get("expected_entropy_reduction", 0)
                    effort = opt.get("effort", "?")
                    parts.append(
                        f"    [dim]-> {action} (reduction: {reduction:.0%}, effort: {effort})[/dim]"
                    )

        content.update("\n".join(parts))

    def _update_statistics_tab(self, columns: Sequence[Any], stat_profiles: dict[str, Any]) -> None:
        """Update the Statistics tab with per-column type counts and basic stats."""
        table = self.query_one("#stats-table", DataTable)
        table.clear(columns=True)

        table.add_column("Column", key="column")
        table.add_column("Total", key="total")
        table.add_column("Nulls", key="nulls")
        table.add_column("Distinct", key="distinct")
        table.add_column("Unique", key="unique")

        for col in columns:
            sp = stat_profiles.get(col.column_id)
            if sp:
                is_unique = "Yes" if sp.is_unique else "No"
                table.add_row(
                    col.column_name,
                    str(sp.total_count),
                    str(sp.null_count),
                    str(sp.distinct_count or "-"),
                    is_unique,
                )
            else:
                table.add_row(col.column_name, "-", "-", "-", "-")

    def _update_semantics_tab(
        self, columns: Sequence[Any], semantic_annotations: dict[str, Any]
    ) -> None:
        """Update the Semantics tab with business names and descriptions."""
        table = self.query_one("#semantics-table", DataTable)
        table.clear(columns=True)

        table.add_column("Column", key="column")
        table.add_column("Business Name", key="business_name")
        table.add_column("Entity", key="entity")
        table.add_column("Concept", key="concept")
        table.add_column("Conf", key="confidence")

        has_data = False
        for col in columns:
            sa = semantic_annotations.get(col.column_id)
            if sa:
                has_data = True
                conf = f"{sa.confidence:.2f}" if sa.confidence else "-"
                table.add_row(
                    col.column_name,
                    sa.business_name or "-",
                    sa.entity_type or "-",
                    sa.business_concept or "-",
                    conf,
                )

        if not has_data:
            table.add_row("-", "No semantic annotations", "-", "-", "-")

    def _update_relationships_tab(
        self, relationships: Sequence[Any], col_name_map: dict[str, str]
    ) -> None:
        """Update the Relationships tab."""
        table = self.query_one("#relationships-table", DataTable)
        table.clear(columns=True)

        table.add_column("From", key="from_col")
        table.add_column("To", key="to_col")
        table.add_column("Type", key="type")
        table.add_column("Cardinality", key="cardinality")
        table.add_column("Conf", key="confidence")
        table.add_column("Method", key="method")

        if not relationships:
            table.add_row("-", "No relationships detected", "-", "-", "-", "-")
            return

        for rel in relationships:
            from_name = col_name_map.get(rel.from_column_id, rel.from_column_id[:8])
            to_name = col_name_map.get(rel.to_column_id, rel.to_column_id[:8])
            table.add_row(
                from_name,
                to_name,
                rel.relationship_type or "-",
                rel.cardinality or "-",
                f"{rel.confidence:.2f}",
                rel.detection_method or "-",
            )

    def _update_quality_tab(self, columns: Sequence[Any], stat_profiles: dict[str, Any]) -> None:
        """Update the Quality tab with null ratios, cardinality, uniqueness."""
        table = self.query_one("#quality-table", DataTable)
        table.clear(columns=True)

        table.add_column("Column", key="column")
        table.add_column("Null %", key="null_ratio")
        table.add_column("Cardinality", key="cardinality")
        table.add_column("Numeric", key="numeric")

        for col in columns:
            sp = stat_profiles.get(col.column_id)
            if sp:
                null_pct = f"{sp.null_ratio * 100:.1f}%" if sp.null_ratio is not None else "-"
                card = f"{sp.cardinality_ratio:.3f}" if sp.cardinality_ratio is not None else "-"
                numeric = "Yes" if sp.is_numeric else "No"
                table.add_row(col.column_name, null_pct, card, numeric)
            else:
                table.add_row(col.column_name, "-", "-", "-")

    def _update_entity_tab(self, table_entity: Any | None) -> None:
        """Update the Entity tab with table-level entity detection."""
        content = self.query_one("#entity-content", Static)

        if not table_entity:
            content.update("[dim]No entity detection available[/dim]")
            return

        parts: list[str] = []
        parts.append(f"[bold]Detected Entity:[/bold] {table_entity.detected_entity_type}")

        if table_entity.description:
            parts.append(f"\n{table_entity.description}")

        parts.append("")

        if table_entity.confidence is not None:
            parts.append(f"[bold]Confidence:[/bold] {table_entity.confidence:.2f}")

        # Fact/Dimension classification
        if table_entity.is_fact_table:
            parts.append("[bold]Classification:[/bold] [cyan]Fact Table[/cyan]")
        elif table_entity.is_dimension_table:
            parts.append("[bold]Classification:[/bold] [cyan]Dimension Table[/cyan]")

        # Grain columns
        if table_entity.grain_columns:
            grain = table_entity.grain_columns
            if isinstance(grain, list):
                parts.append(f"\n[bold]Grain Columns:[/bold] {', '.join(str(g) for g in grain)}")
            elif isinstance(grain, dict):
                parts.append("\n[bold]Grain Columns:[/bold]")
                for k, v in grain.items():
                    parts.append(f"  {k}: {v}")

        if table_entity.detection_source:
            parts.append(f"\n[dim]Source: {table_entity.detection_source}[/dim]")

        content.update("\n".join(parts))

    def _load_sample_data(self, cursor: Any, table_name: str, columns: Sequence[Any]) -> None:
        """Load sample data from DuckDB."""
        sample_table = self.query_one("#sample-table", DataTable)
        sample_table.clear(columns=True)

        try:
            col_names = [c.column_name for c in columns[:10]]
            cols_sql = ", ".join(f'"{c}"' for c in col_names)
            cursor.execute(f'SELECT {cols_sql} FROM "{table_name}" LIMIT 10')
            rows = cursor.fetchall()

            for col_name in col_names:
                display_name = col_name[:15] + "..." if len(col_name) > 15 else col_name
                sample_table.add_column(display_name, key=col_name)

            for row in rows:
                sample_table.add_row(*[str(v)[:30] if v is not None else "" for v in row])

        except Exception as e:
            sample_table.add_column("Error")
            sample_table.add_row(f"Could not load sample: {e}")
