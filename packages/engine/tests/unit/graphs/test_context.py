"""Tests for the graph-shaped served-context renderer (DAT-734).

``format_served_context`` renders the GraphAgent's grounding document: the
concept graph (definitions + edges + prior groundings) served as structure,
the relations with their value sets and readiness markers, and the typed
knowledge sections (drivers, business processes, validation results). These
tests pin the load-bearing rendering rules the prompt's floors reference —
"complete" / "NOT enumerated" / "near-constant" value-set markers, the signed-
measure note, the ⛔ blocked marker, the fan-out warning, and the concept
bindings — plus the DAT-734 core: both groundings AND the reconciliation
verdict of a multi-grounded concept appear in the served text.
"""

from __future__ import annotations

from dataraum.graphs.context import (
    BusinessCycleContext,
    ColumnContext,
    ConceptContext,
    ConceptReconciliation,
    ConformedDimensionContext,
    CycleStageContext,
    DriverContext,
    EnrichedViewContext,
    GraphExecutionContext,
    GroundingContext,
    GroundingUseContext,
    RelationshipContext,
    SliceContext,
    TableContext,
    ValidationContext,
    format_served_context,
)


def _column(**overrides) -> ColumnContext:
    base = {"column_id": "c1", "column_name": "amount", "table_name": "invoices"}
    base.update(overrides)
    return ColumnContext(**base)


def _table(columns: list[ColumnContext] | None = None, **overrides) -> TableContext:
    base = {"table_id": "t1", "table_name": "invoices", "duckdb_name": "typed_invoices"}
    base.update(overrides)
    t = TableContext(**base)
    t.columns = columns or []
    t.column_count = len(t.columns)
    return t


def _grounding(**overrides) -> GroundingContext:
    base = {
        "snippet_id": "sn_1",
        "concept": "account_balance",
        "relation": "enriched_journal",
        "select_expr": "SUM(amount)",
        "statement": "trial_balance",
    }
    base.update(overrides)
    return GroundingContext(**base)


class TestOverview:
    def test_header_and_counts(self) -> None:
        ctx = GraphExecutionContext(tables=[_table(columns=[_column()])])
        out = format_served_context(ctx, source_name="Finance Dataset")
        assert "# Data Catalog: Finance Dataset" in out
        assert "1 tables, 1 columns." in out

    def test_empty_context_renders(self) -> None:
        out = format_served_context(GraphExecutionContext())
        assert out.startswith("# Data Catalog: dataset")


class TestConceptGraph:
    """The traversal core rendered as structure (DAT-734)."""

    def test_multi_grounded_concept_serves_both_groundings_and_reconciliation(self) -> None:
        """The DAT-734 AC at the renderer: BOTH groundings + the tie-out verdict
        of the AP-class concept are in the served text."""
        concept = ConceptContext(
            name="account_balance",
            kind="measure",
            reconciles_with=[ConceptReconciliation(partner="account_balance", tolerance=0.01)],
            groundings=[
                _grounding(
                    where=["account_type IN ('asset','liability')"],
                    uses=[
                        GroundingUseContext("amount", "enriched_journal", "measure"),
                        GroundingUseContext("account_type", "enriched_journal", "filter"),
                    ],
                ),
                _grounding(snippet_id="sn_2", statement="balance_sheet"),
            ],
        )
        out = format_served_context(GraphExecutionContext(concepts=[concept]))

        assert "## Business Concepts" in out
        assert (
            "trial_balance @ enriched_journal: SUM(amount) "
            "WHERE account_type IN ('asset','liability')" in out
        )
        assert "balance_sheet @ enriched_journal: SUM(amount)" in out
        assert "reconciles: across its own groundings (tolerance 0.01) — must tie out" in out
        assert "uses: amount (measure), account_type (filter)" in out

    def test_definition_edges_and_hierarchy(self) -> None:
        concept = ConceptContext(
            name="accounts_payable",
            kind="measure",
            description="Amounts owed to suppliers",
            indicators=["payable", "creditor"],
            exclude_patterns=["receivable"],
            part_of_parents=["working_capital"],
            part_of_ancestry=["financial_position"],
            part_of_children=["trade_payables"],
            disjoint_with=["accounts_receivable"],
        )
        out = format_served_context(GraphExecutionContext(concepts=[concept]))
        assert "**accounts_payable** (measure): Amounts owed to suppliers" in out
        assert "indicators: payable, creditor" in out
        assert "exclude: receivable" in out
        assert "part of: working_capital (→ financial_position)" in out
        assert "subconcepts: trade_payables" in out
        assert "disjoint with: accounts_receivable" in out

    def test_failed_grounding_served_with_reason(self) -> None:
        concept = ConceptContext(
            name="revenue",
            groundings=[
                _grounding(
                    failed=True,
                    failure_mode="verifier_rejected",
                    failure_reason="no support",
                )
            ],
        )
        out = format_served_context(GraphExecutionContext(concepts=[concept]))
        assert "failed attempt [verifier_rejected]: no support" in out
        # A failed row never renders as a committed grounding (the section intro
        # mentions `grounded by` generically; the ENTRY list must be absent).
        assert "- grounded by:" not in out

    def test_cross_concept_reconciliation(self) -> None:
        concept = ConceptContext(
            name="revenue",
            reconciles_with=[ConceptReconciliation(partner="deferred_revenue")],
        )
        out = format_served_context(GraphExecutionContext(concepts=[concept]))
        assert "reconciles with: deferred_revenue" in out

    def test_no_section_without_concepts(self) -> None:
        assert "## Business Concepts" not in format_served_context(GraphExecutionContext())


class TestColumnTable:
    def test_materialization_column_rendered(self) -> None:
        col = _column(semantic_role="measure", data_type="DECIMAL", materialization="flow")
        out = format_served_context(GraphExecutionContext(tables=[_table(columns=[col])]))
        assert "| Column | Type | Role | Materialization | Notes |" in out
        assert "| amount | DECIMAL | measure | flow |" in out

    def test_signed_measure_range_note(self) -> None:
        col = _column(semantic_role="measure", numeric_min=-500.0, numeric_max=1200.0)
        out = format_served_context(GraphExecutionContext(tables=[_table(columns=[col])]))
        assert "Range: -500..1200." in out
        assert "Signed (has negatives) — SUM nets positive and negative values." in out

    def test_unsigned_measure_no_signed_note(self) -> None:
        col = _column(semantic_role="measure", numeric_min=0.0, numeric_max=900.0)
        out = format_served_context(GraphExecutionContext(tables=[_table(columns=[col])]))
        assert "Range: 0..900." in out
        assert "Signed" not in out

    def test_anchor_axis_note_for_measures(self) -> None:
        col = _column(semantic_role="measure", anchor_time_axis="txn_date")
        out = format_served_context(GraphExecutionContext(tables=[_table(columns=[col])]))
        assert "Anchor axis: txn_date." in out

    def test_blocked_and_investigate_markers(self) -> None:
        blocked = _column(column_name="a", entropy_scores={"readiness": "blocked"})
        investigate = _column(
            column_id="c2", column_name="b", entropy_scores={"readiness": "investigate"}
        )
        out = format_served_context(
            GraphExecutionContext(tables=[_table(columns=[blocked, investigate])])
        )
        assert "⛔ blocked." in out
        assert "⚠ investigate." in out

    def test_derived_formula_note(self) -> None:
        col = _column(is_derived=True, derived_formula="quantity * unit_price")
        out = format_served_context(GraphExecutionContext(tables=[_table(columns=[col])]))
        assert "Derived: quantity * unit_price." in out


class TestValueSets:
    """The prompt's floors quote these markers verbatim — pinned here."""

    def test_complete_value_set_rendered_with_counts(self) -> None:
        col = _column(
            column_name="account_type",
            semantic_role="dimension",
            distinct_count=2,
            top_values=[{"value": "asset", "count": 10}, {"value": "liability", "count": 5}],
        )
        out = format_served_context(GraphExecutionContext(tables=[_table(columns=[col])]))
        assert "**Value sets**" in out
        assert "**account_type** (complete, 2 distinct): asset (10), liability (5)" in out

    def test_high_card_column_explorable_never_enumerated(self) -> None:
        col = _column(
            column_name="account_name",
            semantic_role="dimension",
            distinct_count=4000,
            top_values=[{"value": f"v{i}", "count": 10 - i} for i in range(10)],
        )
        out = format_served_context(GraphExecutionContext(tables=[_table(columns=[col])]))
        assert "4000 distinct values — NOT enumerated" in out
        assert "resolve exact values with the search_values tool" in out

    def test_near_constant_column_flagged_not_served(self) -> None:
        col = _column(
            column_name="is_sale",
            semantic_role="dimension",
            distinct_count=2,
            top_values=[{"value": "true", "count": 996}, {"value": "false", "count": 4}],
        )
        out = format_served_context(GraphExecutionContext(tables=[_table(columns=[col])]))
        assert "near-constant" in out
        assert "NOT a discriminator" in out
        assert "true (996)" not in out

    def test_measure_role_has_no_value_set(self) -> None:
        col = _column(
            semantic_role="measure",
            distinct_count=2,
            top_values=[{"value": "1", "count": 3}],
        )
        out = format_served_context(GraphExecutionContext(tables=[_table(columns=[col])]))
        assert "**Value sets**" not in out


class TestStructure:
    def test_relationships_with_confirmation_and_fanout_warning(self) -> None:
        rels = [
            RelationshipContext(
                from_table="invoices",
                from_column="entry_id",
                to_table="journal",
                to_column="entry_id",
                relationship_type="foreign_key",
                cardinality="many-to-one",
                confidence=0.95,
                confirmation_source="judge",
                introduces_duplicates=True,
            ),
            RelationshipContext(
                from_table="a",
                from_column="x",
                to_table="b",
                to_column="y",
                relationship_type="foreign_key",
                confidence=0.4,
            ),
        ]
        out = format_served_context(GraphExecutionContext(relationships=rels))
        assert "| From | To | Cardinality | Confidence | Confirmed |" in out
        assert (
            "| invoices.entry_id | journal.entry_id | many-to-one | 0.95 | judge "
            "⚠ fan-out: SUM across this join double-counts (pre-aggregate) |" in out
        )
        # NULL confirmation renders explicitly — the blueprint's fall-loud gate.
        assert "| a.x | b.y | ? | 0.40 | unconfirmed |" in out

    def test_conformed_dimensions_section(self) -> None:
        ctx = GraphExecutionContext(
            conformed_dimensions=[
                ConformedDimensionContext(
                    table_a="journal",
                    table_b="statement",
                    dimension_table="accounts",
                    attribute="account_type",
                )
            ]
        )
        out = format_served_context(ctx)
        assert "## Conformed Dimensions" in out
        assert "- journal ↔ statement share accounts.account_type" in out

    def test_enriched_view_with_bases_and_slices(self) -> None:
        ctx = GraphExecutionContext(
            enriched_views=[
                EnrichedViewContext(
                    view_name="enriched_journal",
                    fact_table="journal",
                    dimension_columns=["account_id__account_type"],
                    is_grain_verified=True,
                    dimension_tables=["accounts"],
                )
            ],
            available_slices=[
                SliceContext(
                    column_name="account_id__account_type", table_name="journal", value_count=4
                )
            ],
        )
        out = format_served_context(ctx)
        assert "### enriched_journal (grain verified)" in out
        assert "Fact table: journal. Joins dimensions: accounts." in out
        assert "Joined columns: account_id__account_type." in out
        assert "Slice dimensions: account_id__account_type (4 values) — see Value sets" in out


class TestDrivers:
    def test_drivers_render_target_type_dims_and_slices(self) -> None:
        d = DriverContext(
            measure_label="amount",
            target_type="flow",
            grain="row",
            ranked_dimensions=[{"dimension": "region", "gain": 0.42}],
            interesting_slices=[
                {"dimension": "region", "value": "EMEA", "effect": 1.5, "support": 120}
            ],
        )
        out = format_served_context(GraphExecutionContext(drivers=[d]))
        assert "## Drivers" in out
        assert "### amount (flow, grain row)" in out
        assert "region (0.42)" in out
        assert "region=EMEA (effect +1.50, support 120)" in out

    def test_no_drivers_section_when_empty(self) -> None:
        assert "## Drivers" not in format_served_context(GraphExecutionContext())


class TestBusinessProcesses:
    def test_concept_bindings_rendered_as_filters(self) -> None:
        cycle = BusinessCycleContext(
            cycle_name="Order to Cash",
            cycle_type="order_to_cash",
            status_column="invoices.status",
            completion_value="paid",
            stages=[
                CycleStageContext(
                    stage_name="Invoiced",
                    stage_order=1,
                    indicator_column="status",
                    indicator_values=["open", "sent"],
                )
            ],
        )
        out = format_served_context(GraphExecutionContext(business_cycles=[cycle]))
        assert "## Business Processes" in out
        assert "Concept bindings (confirmed — use as the filter, do not improvise):" in out
        assert "\"Invoiced\" = WHERE status IN ('open', 'sent')" in out
        assert "\"order_to_cash completed\" = WHERE invoices.status = 'paid'" in out


class TestValidations:
    def test_unjudged_never_labeled_failed(self) -> None:
        vals = [
            ValidationContext("v1", "passed", "info", True, "ok"),
            ValidationContext("v2", "failed", "critical", False, "imbalance"),
            ValidationContext("v3", "error", "warning", False, "could not evaluate"),
        ]
        out = format_served_context(GraphExecutionContext(validations=vals))
        assert "PASSED: 1 | FAILED: 1 | UNJUDGED: 1" in out
        assert "[CRITICAL] v2: imbalance" in out
        assert "Unjudged (inconclusive or not executed — NOT data failures):" in out
        assert "[error] v3: could not evaluate" in out
