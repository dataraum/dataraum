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
from dataraum.cli.tui.formatting import escape_markup, format_evidence_field


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
        # Table metadata for context enrichment
        self._type_decisions: dict[str, Any] = {}  # column_id -> TypeDecision
        self._semantic_annotations: dict[str, Any] = {}  # column_id -> SemanticAnnotation
        self._stat_profiles: dict[str, Any] = {}  # column_id -> StatisticalProfile
        self._table_entities: dict[str, Any] = {}  # table_id -> TableEntity
        self._relationships: dict[str, list[Any]] = {}  # table_id -> [Relationship]
        self._table_id_to_name: dict[str, str] = {}
        self._typed_table_name_to_id: dict[str, str] = {}  # typed layer only
        self._column_id_to_name: dict[str, str] = {}

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
                        with TabPane("Context", id="tab-context"):
                            yield Static("", id="context-content")
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
        self._type_decisions.clear()
        self._semantic_annotations.clear()
        self._stat_profiles.clear()
        self._table_entities.clear()
        self._relationships.clear()
        self._table_id_to_name.clear()
        self._typed_table_name_to_id.clear()
        self._column_id_to_name.clear()
        self._selected_key = None
        self._load_data()

    def _load_data(self) -> None:
        """Load entropy data from database."""
        if self._data_loaded:
            return

        from sqlalchemy import select

        from dataraum.entropy.db_models import (
            EntropyObjectRecord,
            EntropySnapshotRecord,
        )
        from dataraum.entropy.interpretation_db_models import EntropyInterpretationRecord
        from dataraum.storage import Column, Source, Table

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
                    EntropyInterpretationRecord.table_name,
                    EntropyInterpretationRecord.column_name,
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
                self._table_id_to_name = {t.table_id: t.table_name for t in tables}
                self._typed_table_name_to_id = {
                    t.table_name: t.table_id for t in tables if t.layer == "typed"
                }
                table_ids = list(self._table_id_to_name.keys())

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
                        t_name = self._table_id_to_name.get(obj.table_id or "", "unknown")
                        table_key = f"{t_name}.(table)"
                        self._table_entropy_objects.setdefault(table_key, []).append(obj)

                # Batch load table metadata — strictly from typed layer
                typed_table_ids = list(self._typed_table_name_to_id.values())

                if typed_table_ids:
                    col_result = session.execute(
                        select(Column).where(Column.table_id.in_(typed_table_ids))
                    )
                    for col in col_result.scalars().all():
                        self._column_id_to_name[col.column_id] = col.column_name

                if column_ids:
                    from dataraum.analysis.statistics.db_models import StatisticalProfile
                    from dataraum.analysis.typing.db_models import TypeDecision

                    td_result = session.execute(
                        select(TypeDecision).where(TypeDecision.column_id.in_(column_ids))
                    )
                    for td in td_result.scalars().all():
                        self._type_decisions[td.column_id] = td

                    from dataraum.analysis.semantic.db_models import SemanticAnnotation

                    sa_result = session.execute(
                        select(SemanticAnnotation).where(
                            SemanticAnnotation.column_id.in_(column_ids)
                        )
                    )
                    for sa in sa_result.scalars().all():
                        self._semantic_annotations[sa.column_id] = sa

                    sp_result = session.execute(
                        select(StatisticalProfile)
                        .where(StatisticalProfile.column_id.in_(column_ids))
                        .order_by(StatisticalProfile.profiled_at.desc())
                    )
                    for sp in sp_result.scalars().all():
                        if sp.column_id not in self._stat_profiles:
                            self._stat_profiles[sp.column_id] = sp

                if typed_table_ids:
                    from dataraum.analysis.relationships.db_models import Relationship
                    from dataraum.analysis.semantic.db_models import TableEntity

                    te_result = session.execute(
                        select(TableEntity).where(TableEntity.table_id.in_(typed_table_ids))
                    )
                    for te in te_result.scalars().all():
                        self._table_entities[te.table_id] = te

                    from sqlalchemy import or_

                    rel_result = session.execute(
                        select(Relationship).where(
                            or_(
                                Relationship.from_table_id.in_(typed_table_ids),
                                Relationship.to_table_id.in_(typed_table_ids),
                            ),
                            Relationship.detection_method == "llm",
                        )
                    )
                    for rel in rel_result.scalars().all():
                        self._relationships.setdefault(rel.from_table_id, []).append(rel)
                        if rel.to_table_id != rel.from_table_id:
                            self._relationships.setdefault(rel.to_table_id, []).append(rel)

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
        table_level = sum(1 for i in interpretations if not i.column_name)

        # Build status line with all stats
        parts = [
            f"[{color}]{snapshot.overall_readiness.upper()}[/{color}]",
            f"Score: {snapshot.avg_entropy_score:.3f}",
            f"Columns: {total - table_level}",
        ]
        if table_level:
            parts.append(f"Table-level: {table_level}")

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
            table_color = "white"
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

                col_label = col.column_name
                if col.column_id:
                    td = self._type_decisions.get(col.column_id)
                    if td:
                        col_label += f" [dim]{td.decided_type}[/dim]"
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
        elif node.data.endswith(".(table)"):
            # Table node — show whatever table-level data we have
            self._update_table_level_panel(node.data)

    def _update_table_level_panel(self, key: str) -> None:
        """Update detail panel for table-level entropy objects without interpretation."""
        header = self.query_one("#detail-header", Static)
        table_name = key.replace(".(table)", "")
        context_hint = self._build_table_hint(table_name)
        header.update(f"[bold]{table_name}[/bold]{context_hint} | [dim]Table-level metrics[/dim]")

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

        # Update context and evidence tabs
        self._update_context_section(table_key=key)
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
        # Update header with context hint
        header = self.query_one("#detail-header", Static)
        col_name = interp.column_name or "(table-level)"
        context_hint = self._build_header_hint(interp)
        header.update(f"[bold]{interp.table_name}.{col_name}[/bold]{context_hint}")

        # Get entropy objects for this column or table-level
        if interp.column_id:
            entropy_objects = self._entropy_objects_map.get(interp.column_id, [])
        else:
            table_key = f"{interp.table_name}.(table)"
            entropy_objects = self._table_entropy_objects.get(table_key, [])

        # Update all sections
        self._update_layer_bars(entropy_objects)
        self._update_overview_section(interp)
        if interp.column_name:
            self._update_context_section(interp=interp)
        else:
            self._update_context_section(table_key=f"{interp.table_name}.(table)")
        self._update_evidence_section(entropy_objects)
        self._update_assumptions_section(interp)
        self._update_actions_section(interp, entropy_objects)

    def _update_overview_section(self, interp: Any) -> None:
        """Update the Overview section with explanation and blocked columns."""

        overview = self.query_one("#overview-content", Static)
        parts: list[str] = []

        # LLM explanation (escape to prevent markup injection)
        explanation = escape_markup(interp.explanation or "No explanation available")
        parts.append(explanation)

        overview.update("\n".join(parts))

    def _build_header_hint(self, interp: Any) -> str:
        """Build a [dim] context hint for the detail header."""
        parts: list[str] = []
        if interp.column_id:
            td = self._type_decisions.get(interp.column_id)
            if td:
                parts.append(td.decided_type)
            sa = self._semantic_annotations.get(interp.column_id)
            if sa and sa.semantic_role:
                parts.append(sa.semantic_role)
        else:
            parts = self._table_hint_parts(interp.table_name)
        return f" [dim]({', '.join(parts)})[/dim]" if parts else ""

    def _build_table_hint(self, table_name: str) -> str:
        """Build a [dim] context hint for a table-level header."""
        parts = self._table_hint_parts(table_name)
        return f" [dim]({', '.join(parts)})[/dim]" if parts else ""

    def _table_hint_parts(self, table_name: str) -> list[str]:
        """Get hint parts for a table (entity type + fact/dimension)."""
        table_id = self._resolve_typed_table_id(table_name)
        if table_id:
            te = self._table_entities.get(table_id)
            if te:
                parts = [te.detected_entity_type]
                if te.is_fact_table:
                    parts.append("Fact")
                elif te.is_dimension_table:
                    parts.append("Dimension")
                return parts
        return []

    def _update_context_section(
        self, interp: Any | None = None, table_key: str | None = None
    ) -> None:
        """Update the Context tab with type/semantics/stats or entity/relationships."""
        content = self.query_one("#context-content", Static)
        parts: list[str] = []

        if interp and interp.column_id:
            col_id = interp.column_id

            # Type section
            td = self._type_decisions.get(col_id)
            if td:
                parts.append("[bold]Type[/bold]")
                parts.append(f"  Decided type: {td.decided_type}")
                parts.append(f"  Source: {td.decision_source}")
                if td.decision_reason:
                    parts.append(f"  Reason: {td.decision_reason}")
                if td.previous_type:
                    parts.append(f"  Previous: {td.previous_type}")
            else:
                parts.append("[dim]No type decision available[/dim]")

            parts.append("")

            # Semantics section
            sa = self._semantic_annotations.get(col_id)
            if sa:
                parts.append("[bold]Semantics[/bold]")
                if sa.business_name:
                    parts.append(f"  Business name: {sa.business_name}")
                if sa.semantic_role:
                    parts.append(f"  Role: {sa.semantic_role}")
                if sa.entity_type:
                    parts.append(f"  Entity type: {sa.entity_type}")
                if sa.business_concept:
                    parts.append(f"  Concept: {sa.business_concept}")
                if sa.confidence is not None:
                    parts.append(f"  Confidence: {sa.confidence:.2f}")
                if sa.annotation_source:
                    parts.append(f"  Source: {sa.annotation_source}")
            else:
                parts.append("[dim]No semantic annotation available[/dim]")

            parts.append("")

            # Statistics section
            sp = self._stat_profiles.get(col_id)
            if sp:
                parts.append("[bold]Statistics[/bold]")
                parts.append(f"  Total: {sp.total_count:,}")
                null_str = f"{sp.null_count:,}"
                if sp.null_ratio is not None:
                    null_str += f" ({sp.null_ratio:.1%})"
                parts.append(f"  Nulls: {null_str}")
                if sp.distinct_count is not None:
                    parts.append(f"  Distinct: {sp.distinct_count:,}")
                if sp.cardinality_ratio is not None:
                    parts.append(f"  Cardinality: {sp.cardinality_ratio:.3f}")
                if sp.is_unique is not None:
                    parts.append(f"  Unique: {'Yes' if sp.is_unique else 'No'}")
                if sp.is_numeric is not None:
                    parts.append(f"  Numeric: {'Yes' if sp.is_numeric else 'No'}")
            else:
                parts.append("[dim]No statistical profile available[/dim]")

        elif table_key:
            table_name = table_key.replace(".(table)", "")
            table_id = self._resolve_typed_table_id(table_name)

            if table_id:
                # Entity section
                te = self._table_entities.get(table_id)
                if te:
                    parts.append("[bold]Entity[/bold]")
                    parts.append(f"  Type: {te.detected_entity_type}")
                    if te.is_fact_table:
                        parts.append("  Classification: Fact table")
                    elif te.is_dimension_table:
                        parts.append("  Classification: Dimension table")
                    if te.description:
                        parts.append(f"  Description: {te.description}")
                    if te.confidence is not None:
                        parts.append(f"  Confidence: {te.confidence:.2f}")
                    if te.grain_columns:
                        grain = te.grain_columns
                        if isinstance(grain, list):
                            names = [self._column_id_to_name.get(c, c[:8]) for c in grain]
                            parts.append(f"  Grain: {', '.join(names)}")
                        elif isinstance(grain, dict):
                            parts.append(f"  Grain: {grain}")
                    if te.detection_source:
                        parts.append(f"  Source: {te.detection_source}")
                else:
                    parts.append("[dim]No entity information available[/dim]")

                parts.append("")

                # Relationships section
                rels = self._relationships.get(table_id, [])
                if rels:
                    parts.append("[bold]Relationships[/bold]")
                    for rel in rels[:10]:
                        ft = self._table_id_to_name.get(rel.from_table_id, "?")
                        tt = self._table_id_to_name.get(rel.to_table_id, "?")
                        fc = self._column_id_to_name.get(rel.from_column_id, "?")
                        tc = self._column_id_to_name.get(rel.to_column_id, "?")
                        conf = f"{rel.confidence:.2f}" if rel.confidence else "?"
                        parts.append(f"  {ft}.{fc} -> {tt}.{tc}")
                        card = f", {rel.cardinality}" if rel.cardinality else ""
                        method = f", {rel.detection_method}" if rel.detection_method else ""
                        parts.append(
                            f"    {rel.relationship_type}{card}, confidence: {conf}{method}"
                        )
                    if len(rels) > 10:
                        parts.append(f"  [dim]... and {len(rels) - 10} more[/dim]")
                else:
                    parts.append("[dim]No relationships detected[/dim]")
            else:
                parts.append("[dim]No table metadata available[/dim]")

        else:
            parts.append("[dim]Select a column or table to view context[/dim]")

        content.update("\n".join(parts))

    def _resolve_typed_table_id(self, table_name: str) -> str | None:
        """Look up the typed-layer table_id for a table_name."""
        return self._typed_table_name_to_id.get(table_name)

    @staticmethod
    def _safe_markup(line: str) -> str:
        """Return line as-is if valid Textual markup, escaped otherwise."""
        from textual.content import Content

        try:
            Content.from_markup(line)
            return line
        except Exception:
            return escape_markup(line)

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
            layer = escape_markup(str(obj.layer))
            dim = escape_markup(str(obj.dimension))
            sub = escape_markup(str(obj.sub_dimension))
            parts.append(
                f"[bold]{layer} > {dim} > {sub}[/bold]  "
                f"[{score_color}]{obj.score:.3f}[/{score_color}]"
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
                    action = escape_markup(str(opt.get("action", "?")))
                    effort = escape_markup(str(opt.get("effort", "?")))
                    parts.append(f"  [dim]-> {action}[/dim]  effort: {effort}")

            parts.append("")  # Blank line between objects

        combined = "\n".join(parts)
        try:
            content.update(combined)
        except Exception:
            # Validate per-line — escape only the problematic lines
            content.update("\n".join(self._safe_markup(p) for p in parts))

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

            dim = escape_markup(str(a.get("dimension", "-")))
            text = escape_markup(str(a.get("assumption_text", "")))
            conf = str(a.get("confidence", "-"))
            impact = escape_markup(str(a.get("impact", "")))
            basis = escape_markup(str(a.get("basis", "")))

            conf_color = conf_colors.get(conf.lower(), "white")

            title = f"[{conf_color}]{escape_markup(conf.upper())}[/{conf_color}] {dim}"

            # Full content for expanded state
            content = (
                f"[bold]Assumption:[/bold]\n{text}\n\n"
                f"[bold]Impact:[/bold]\n{impact}\n\n"
                f"[dim]Confidence: {escape_markup(conf)} | Basis: {basis}[/dim]"
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

        # Sort by priority
        priority_order = {"high": 0, "medium": 1, "low": 2}
        actions = sorted(
            [a for a in actions if isinstance(a, dict)],
            key=lambda x: priority_order.get(str(x.get("priority", "medium")).lower(), 1),
        )

        priority_colors = {"high": "red", "medium": "yellow", "low": "green"}

        for action in actions:
            priority = str(action.get("priority", "medium"))
            action_name = str(action.get("action", "-"))
            description = str(action.get("description", ""))
            effort = str(action.get("effort", "-"))
            expected_impact = str(action.get("expected_impact", ""))
            parameters = action.get("parameters", {})

            p_color = priority_colors.get(priority.lower(), "white")

            title = (
                f"[{p_color}]{escape_markup(priority.upper())}[/{p_color}] {escape_markup(action_name)} "
                f"[dim](effort: {escape_markup(effort)})[/dim]"
            )

            # Full content for expanded state
            content = f"[bold]Description:[/bold]\n{escape_markup(description)}"
            if expected_impact:
                content += f"\n\n[bold]Expected Impact:[/bold]\n{escape_markup(expected_impact)}"

            # Parameters
            if parameters and isinstance(parameters, dict):
                param_lines = [
                    f"  {escape_markup(str(k))}: {escape_markup(str(v))}"
                    for k, v in parameters.items()
                ]
                content += "\n\n[bold]Parameters:[/bold]\n" + "\n".join(param_lines)

            content += f"\n\n[dim]Priority: {escape_markup(priority)} | Effort: {escape_markup(effort)}[/dim]"

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
