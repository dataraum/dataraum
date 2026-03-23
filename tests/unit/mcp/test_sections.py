"""Tests for get_context section builders."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

from dataraum.graphs.context import (
    BusinessCycleContext,
    ColumnContext,
    CycleStageContext,
    EnrichedViewContext,
    EntityFlowContext,
    GraphExecutionContext,
    RelationshipContext,
    SliceContext,
    TableContext,
    ValidationContext,
)
from dataraum.mcp.sections import (
    CONTEXT_SECTIONS,
    VALID_SECTIONS,
    build_contracts_section,
    build_cycles_section,
    build_quality_section,
    build_schema_section,
    build_semantics_section,
    build_snippets_section,
    build_validations_section,
)


def _make_column(**kwargs: Any) -> ColumnContext:
    defaults = {
        "column_id": "col-1",
        "column_name": "amount",
        "table_name": "orders",
    }
    defaults.update(kwargs)
    return ColumnContext(**defaults)


def _make_table(**kwargs: Any) -> TableContext:
    defaults = {
        "table_id": "tbl-1",
        "table_name": "orders",
    }
    defaults.update(kwargs)
    return TableContext(**defaults)


def _make_context(**kwargs: Any) -> GraphExecutionContext:
    return GraphExecutionContext(**kwargs)


class TestBuildSchemaSection:
    def test_empty_context(self) -> None:
        result = build_schema_section(_make_context())
        assert result["overview"]["total_tables"] == 0
        assert result["tables"] == []

    def test_tables_and_columns(self) -> None:
        col = _make_column(data_type="INTEGER", semantic_role="measure", null_ratio=0.05)
        table = _make_table(
            duckdb_name="typed_orders",
            is_fact_table=True,
            entity_type="transaction",
            row_count=1000,
            grain_columns=["order_id"],
            time_column="order_date",
            columns=[col],
        )
        ctx = _make_context(
            tables=[table],
            total_tables=1,
            total_columns=1,
            graph_pattern="star_schema",
            hub_tables=["orders"],
            leaf_tables=["products"],
        )

        result = build_schema_section(ctx)

        assert result["overview"]["total_tables"] == 1
        assert result["overview"]["graph_pattern"] == "star_schema"
        assert result["overview"]["hub_tables"] == ["orders"]

        t = result["tables"][0]
        assert t["name"] == "orders"
        assert t["duckdb_name"] == "typed_orders"
        assert t["type"] == "fact"
        assert t["row_count"] == 1000

        c = t["columns"][0]
        assert c["name"] == "amount"
        assert c["data_type"] == "INTEGER"
        assert c["null_ratio"] == 0.05

    def test_relationships(self) -> None:
        rel = RelationshipContext(
            from_table="orders",
            from_column="customer_id",
            to_table="customers",
            to_column="id",
            relationship_type="foreign_key",
            cardinality="many_to_one",
            confidence=0.95,
        )
        ctx = _make_context(relationships=[rel])
        result = build_schema_section(ctx)

        assert len(result["relationships"]) == 1
        r = result["relationships"][0]
        assert r["from_table"] == "orders"
        assert r["cardinality"] == "many_to_one"
        assert r["confidence"] == 0.95

    def test_non_deterministic_relationship(self) -> None:
        rel = RelationshipContext(
            from_table="a",
            from_column="b",
            to_table="c",
            to_column="d",
            relationship_type="fk",
            relationship_entropy={"is_deterministic": False},
        )
        ctx = _make_context(relationships=[rel])
        result = build_schema_section(ctx)
        assert result["relationships"][0]["non_deterministic"] is True

    def test_enriched_views(self) -> None:
        ev = EnrichedViewContext(
            view_name="enriched_orders",
            fact_table="orders",
            dimension_columns=["customer_name"],
            is_grain_verified=True,
        )
        ctx = _make_context(enriched_views=[ev])
        result = build_schema_section(ctx)

        v = result["enriched_views"][0]
        assert v["view_name"] == "enriched_orders"
        assert v["grain_verified"] is True


class TestBuildSemanticsSection:
    def test_empty_context(self) -> None:
        result = build_semantics_section(_make_context())
        assert result["tables"] == []

    def test_semantic_columns_included(self) -> None:
        col = _make_column(
            business_name="Order Amount",
            business_description="Total order value",
            business_concept="revenue",
            temporal_behavior="additive",
        )
        table = _make_table(
            table_description="Sales transactions",
            entity_type="transaction",
            columns=[col],
        )
        ctx = _make_context(tables=[table])
        result = build_semantics_section(ctx)

        t = result["tables"][0]
        assert t["description"] == "Sales transactions"
        c = t["columns"][0]
        assert c["business_name"] == "Order Amount"
        assert c["business_concept"] == "revenue"

    def test_columns_without_semantics_excluded(self) -> None:
        col = _make_column()  # No semantic data
        table = _make_table(columns=[col])
        ctx = _make_context(tables=[table])
        result = build_semantics_section(ctx)

        # Table included but no columns key (empty list omitted)
        assert "columns" not in result["tables"][0]

    def test_derived_columns(self) -> None:
        col = _make_column(is_derived=True, derived_formula="quantity * unit_price")
        table = _make_table(columns=[col])
        ctx = _make_context(tables=[table])
        result = build_semantics_section(ctx)

        c = result["tables"][0]["columns"][0]
        assert c["is_derived"] is True
        assert c["derived_formula"] == "quantity * unit_price"

    def test_slices_included(self) -> None:
        s = SliceContext(
            column_name="region",
            table_name="orders",
            value_count=5,
            business_context="Regional breakdown",
            distinct_values=["US", "EU", "APAC"],
        )
        ctx = _make_context(available_slices=[s])
        result = build_semantics_section(ctx)
        assert result["slices"][0]["column_name"] == "region"
        assert result["slices"][0]["business_context"] == "Regional breakdown"

    def test_availability_warning_when_no_semantics(self) -> None:
        col = _make_column()  # No business_name
        table = _make_table(columns=[col])
        ctx = _make_context(tables=[table])
        result = build_semantics_section(ctx)
        assert result["availability"]["status"] == "not_yet_available"


class TestBuildQualitySection:
    def test_empty_context(self) -> None:
        result = build_quality_section(_make_context())
        assert result["tables"] == []
        assert "availability" in result

    def test_quality_data(self) -> None:
        col = _make_column(
            flags=["moderate_nulls"],
        )
        table = _make_table(columns=[col], readiness_for_use="investigate")
        ctx = _make_context(
            tables=[table],
            entropy_summary={"overall_readiness": "investigate", "avg_entropy_score": 0.42},
        )
        result = build_quality_section(ctx)

        assert result["overall_readiness"] == "investigate"
        assert result["entropy_score"] == 0.42

        t = result["tables"][0]
        assert t["readiness"] == "investigate"
        c = t["columns"][0]
        assert c["flags"] == ["moderate_nulls"]

    def test_entropy_data(self) -> None:
        col = _make_column(
            entropy_scores={"readiness": "investigate", "composite": 0.3},
            resolution_hints=[{"action": "impute", "effort": "low"}],
        )
        table = _make_table(columns=[col])
        ctx = _make_context(tables=[table])
        result = build_quality_section(ctx)

        c = result["tables"][0]["columns"][0]
        assert c["entropy_scores"]["readiness"] == "investigate"
        assert len(c["resolution_actions"]) == 1

    def test_availability_warnings(self) -> None:
        col = _make_column()  # No quality data at all
        table = _make_table(columns=[col])
        ctx = _make_context(tables=[table])
        result = build_quality_section(ctx)

        assert "entropy_scores" in result["availability"]
        assert "hint" in result


class TestBuildValidationsSection:
    def test_no_validations(self) -> None:
        result = build_validations_section(_make_context())
        assert result["status"] == "not_yet_available"

    def test_mixed_results(self) -> None:
        validations = [
            ValidationContext(
                validation_id="v1",
                status="passed",
                severity="info",
                passed=True,
                message="Passed check",
            ),
            ValidationContext(
                validation_id="v2",
                status="failed",
                severity="critical",
                passed=False,
                message="FK violation",
                details={"summary": "10 orphan rows"},
            ),
        ]
        ctx = _make_context(validations=validations)
        result = build_validations_section(ctx)

        assert result["summary"]["passed"] == 1
        assert result["summary"]["failed"] == 1
        assert len(result["results"]) == 2

        failed = [r for r in result["results"] if not r["passed"]]
        assert failed[0]["details"]["summary"] == "10 orphan rows"


class TestBuildCyclesSection:
    def test_no_cycles(self) -> None:
        result = build_cycles_section(_make_context())
        assert result["status"] == "not_yet_available"

    def test_cycle_with_stages(self) -> None:
        stage = CycleStageContext(
            stage_name="placed",
            stage_order=1,
            indicator_column="status",
            indicator_values=["pending"],
            completion_rate=0.95,
        )
        entity = EntityFlowContext(
            entity_type="customer",
            entity_column="customer_id",
            entity_table="customers",
            fact_table="orders",
        )
        cycle = BusinessCycleContext(
            cycle_name="Order to Cash",
            cycle_type="order_to_cash",
            tables_involved=["orders", "invoices"],
            confidence=0.88,
            description="End-to-end order lifecycle",
            stages=[stage],
            entity_flows=[entity],
            total_records=5000,
            completed_cycles=4200,
            completion_rate=0.84,
            evidence=["Status column tracks lifecycle"],
        )
        score = MagicMock()
        score.validations_run = 5
        score.validations_passed = 4
        health = MagicMock()
        health.cycle_scores = [score]
        health.overall_health = 0.85

        ctx = _make_context(business_cycles=[cycle], cycle_health=health)
        result = build_cycles_section(ctx)

        assert len(result["cycles"]) == 1
        c = result["cycles"][0]
        assert c["name"] == "Order to Cash"
        assert c["stages"][0]["name"] == "placed"
        assert c["stages"][0]["completion_rate"] == 0.95
        assert c["entity_flows"][0]["entity_type"] == "customer"
        assert c["volume"]["total_records"] == 5000
        assert result["health"]["total_validations"] == 5


class TestBuildSnippetsSection:
    def test_no_snippets(self) -> None:
        session = MagicMock()
        with patch("dataraum.query.snippet_library.SnippetLibrary") as MockLib:
            lib = MockLib.return_value
            lib.find_all_graphs.return_value = []
            lib.get_search_vocabulary.return_value = {}

            result = build_snippets_section(session, "src-1")

        assert result["total_snippets"] == 0
        assert "hint" in result

    def test_snippets_with_graphs(self) -> None:
        session = MagicMock()

        snippet = MagicMock()
        snippet.snippet_id = "snip-1"
        snippet.standard_field = "revenue"
        snippet.sql = "SELECT SUM(amount) FROM orders"
        snippet.description = "Total revenue"
        snippet.snippet_type = "extract"
        snippet.statement = "income_statement"
        snippet.aggregation = "sum"
        snippet.parameter_value = None
        snippet.column_mappings = {"revenue": "orders.amount"}
        snippet.input_fields = None

        graph = MagicMock()
        graph.graph_id = "dso"
        graph.source = "graph:dso"
        graph.source_type = "graph"
        graph.snippets = [snippet]

        with patch("dataraum.query.snippet_library.SnippetLibrary") as MockLib:
            lib = MockLib.return_value
            lib.find_all_graphs.return_value = [graph]
            lib.get_search_vocabulary.return_value = {
                "standard_fields": ["revenue"],
                "statements": ["income_statement"],
                "aggregations": ["sum"],
                "graph_ids": ["dso"],
            }

            result = build_snippets_section(session, "src-1")

        assert result["total_snippets"] == 1
        g = result["graphs"][0]
        assert g["graph_id"] == "dso"
        assert g["source_type"] == "graph"
        s = g["snippets"][0]
        assert s["step_id"] == "revenue"
        assert s["sql"] == "SELECT SUM(amount) FROM orders"
        assert s["column_mappings"] == {"revenue": "orders.amount"}

        assert result["vocabulary"]["standard_fields"] == ["revenue"]


class TestBuildContractsSection:
    def test_no_contracts(self) -> None:
        session = MagicMock()
        with patch("dataraum.entropy.contracts.list_contracts", return_value=[]):
            result = build_contracts_section(session, [])
        assert result["contracts"] == []

    def test_contracts_without_entropy(self) -> None:
        session = MagicMock()
        contracts = [
            {
                "name": "agg_safe",
                "display_name": "Aggregation Safe",
                "description": "Safe for aggregation",
                "overall_threshold": 0.3,
            }
        ]
        with (
            patch("dataraum.entropy.contracts.list_contracts", return_value=contracts),
            patch(
                "dataraum.entropy.views.network_context.build_for_network",
                return_value=None,
            ),
        ):
            result = build_contracts_section(session, ["tbl-1"])

        assert len(result["contracts"]) == 1
        assert result["contracts"][0]["name"] == "agg_safe"
        assert "hint" in result  # Warns entropy not yet available


class TestSectionConstants:
    def test_valid_sections(self) -> None:
        expected = {
            "schema",
            "semantics",
            "quality",
            "validations",
            "cycles",
            "snippets",
            "contracts",
        }
        assert VALID_SECTIONS == expected

    def test_context_sections_subset(self) -> None:
        assert CONTEXT_SECTIONS < VALID_SECTIONS
        assert "snippets" not in CONTEXT_SECTIONS
        assert "contracts" not in CONTEXT_SECTIONS
