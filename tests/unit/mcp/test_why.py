"""Tests for why MCP tool — evidence assembly and resolution validation.

Tests evidence assembly at three levels (column/table/dataset),
dimension filtering, resolution option validation, and teach schema
integration. LLM calls are not tested here — that's for dataraum-eval.
"""

from __future__ import annotations

from uuid import uuid4

from sqlalchemy.orm import Session

from dataraum.analysis.semantic.db_models import SemanticAnnotation
from dataraum.entropy.views.network_context import (
    ColumnNetworkResult,
    ColumnNodeEvidence,
    EntropyForNetwork,
    IntentReadiness,
)
from dataraum.mcp.teach import VALID_TEACH_TYPES
from dataraum.mcp.why import (
    build_column_evidence,
    build_dataset_evidence,
    build_table_evidence,
    get_existing_teachings,
    get_teach_type_schemas,
    validate_resolution_option,
)
from dataraum.pipeline.fixes.models import DataFix
from dataraum.storage import Column, Source, Table


def _id() -> str:
    return str(uuid4())


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _setup_typed_tables(
    session: Session,
    tables: dict[str, list[str]] | None = None,
) -> tuple[str, dict[str, tuple[str, list[tuple[str, str]]]]]:
    """Create Source + typed Tables + Columns."""
    if tables is None:
        tables = {"orders": ["id", "amount", "region"]}

    source_id = _id()
    session.add(Source(source_id=source_id, name="test_source", source_type="csv"))

    result: dict[str, tuple[str, list[tuple[str, str]]]] = {}
    for base_name, col_names in tables.items():
        table_id = _id()
        session.add(
            Table(
                table_id=table_id,
                source_id=source_id,
                table_name=base_name,
                layer="typed",
                duckdb_path=f"typed_{base_name}",
                row_count=100,
            )
        )
        col_ids = []
        for i, name in enumerate(col_names):
            col_id = _id()
            col_ids.append((col_id, name))
            session.add(
                Column(
                    column_id=col_id,
                    table_id=table_id,
                    column_name=name,
                    column_position=i,
                    resolved_type="BIGINT" if name == "amount" else "VARCHAR",
                )
            )
        result[base_name] = (table_id, col_ids)

    session.flush()
    return source_id, result


def _make_column_result(
    target: str = "column:orders.amount",
    readiness: str = "investigate",
    nodes: list[ColumnNodeEvidence] | None = None,
    intents: list[IntentReadiness] | None = None,
) -> ColumnNetworkResult:
    """Build a ColumnNetworkResult for testing."""
    if nodes is None:
        nodes = [
            ColumnNodeEvidence(
                node_name="null_semantics",
                dimension_path="structural.null_semantics",
                state="high",
                score=0.72,
                impact_delta=0.25,
                evidence=[{"null_ratio": 0.15, "pattern": "systematic"}],
                detector_id="null_semantics",
            ),
            ColumnNodeEvidence(
                node_name="business_meaning",
                dimension_path="semantic.business_meaning",
                state="medium",
                score=0.45,
                impact_delta=0.12,
                evidence=[{"issue": "no business concept assigned"}],
                detector_id="business_meaning",
            ),
            ColumnNodeEvidence(
                node_name="types",
                dimension_path="structural.types",
                state="low",
                score=0.1,
                impact_delta=0.02,
                evidence=[],
                detector_id="types",
            ),
        ]
    if intents is None:
        intents = [
            IntentReadiness(
                intent_name="query",
                posterior={"low": 0.3, "medium": 0.3, "high": 0.4},
                dominant_state="high",
                p_high=0.4,
                readiness="investigate",
            ),
            IntentReadiness(
                intent_name="aggregation",
                posterior={"low": 0.2, "medium": 0.3, "high": 0.5},
                dominant_state="high",
                p_high=0.5,
                readiness="investigate",
            ),
        ]

    return ColumnNetworkResult(
        target=target,
        node_evidence=nodes,
        intents=intents,
        top_priority_node="null_semantics",
        top_priority_impact=0.25,
        nodes_observed=len(nodes),
        nodes_high=sum(1 for n in nodes if n.state == "high"),
        worst_intent_p_high=max(i.p_high for i in intents),
        readiness=readiness,
    )


def _make_network_ctx(
    columns: dict[str, ColumnNetworkResult] | None = None,
) -> EntropyForNetwork:
    """Build an EntropyForNetwork for testing."""
    if columns is None:
        columns = {
            "column:orders.amount": _make_column_result("column:orders.amount"),
            "column:orders.region": _make_column_result(
                "column:orders.region",
                readiness="ready",
                nodes=[
                    ColumnNodeEvidence(
                        node_name="types",
                        dimension_path="structural.types",
                        state="low",
                        score=0.05,
                        impact_delta=0.01,
                        detector_id="types",
                    ),
                ],
                intents=[
                    IntentReadiness(
                        intent_name="query",
                        posterior={"low": 0.8, "medium": 0.15, "high": 0.05},
                        p_high=0.05,
                        readiness="ready",
                    ),
                ],
            ),
        }

    blocked = sum(1 for c in columns.values() if c.readiness == "blocked")
    investigate = sum(1 for c in columns.values() if c.readiness == "investigate")
    ready = sum(1 for c in columns.values() if c.readiness == "ready")

    if blocked:
        overall = "blocked"
    elif investigate:
        overall = "investigate"
    else:
        overall = "ready"

    return EntropyForNetwork(
        columns=columns,
        total_columns=len(columns),
        columns_blocked=blocked,
        columns_investigate=investigate,
        columns_ready=ready,
        overall_readiness=overall,
    )


# ---------------------------------------------------------------------------
# Evidence assembly — column level
# ---------------------------------------------------------------------------


class TestBuildColumnEvidence:
    def test_includes_network_nodes(self, session: Session) -> None:
        source_id, _ = _setup_typed_tables(session)
        col_result = _make_column_result()

        evidence = build_column_evidence("column:orders.amount", col_result, session)

        assert evidence["target"] == "orders.amount"
        assert evidence["readiness"] == "investigate"
        assert len(evidence["network_evidence"]) == 3
        # Sorted by impact_delta descending
        assert evidence["network_evidence"][0]["node"] == "null_semantics"
        assert evidence["network_evidence"][0]["impact_delta"] == 0.25

    def test_includes_intents(self, session: Session) -> None:
        source_id, _ = _setup_typed_tables(session)
        col_result = _make_column_result()

        evidence = build_column_evidence("column:orders.amount", col_result, session)

        assert "query" in evidence["intents"]
        assert evidence["intents"]["query"]["p_high"] == 0.4
        assert evidence["intents"]["query"]["readiness"] == "investigate"
        assert "posterior" in evidence["intents"]["query"]

    def test_dimension_filter(self, session: Session) -> None:
        source_id, _ = _setup_typed_tables(session)
        col_result = _make_column_result()

        evidence = build_column_evidence(
            "column:orders.amount",
            col_result,
            session,
            dimension_filter="semantic",
        )

        nodes = evidence["network_evidence"]
        # Only semantic nodes should pass
        assert all(n["dimension_path"].startswith("semantic") for n in nodes)
        assert len(nodes) == 1
        assert nodes[0]["node"] == "business_meaning"

    def test_includes_semantic_metadata(self, session: Session) -> None:
        source_id, tables = _setup_typed_tables(session)
        col_id = tables["orders"][1][1][0]  # amount column

        session.add(
            SemanticAnnotation(
                annotation_id=_id(),
                column_id=col_id,
                semantic_role="measure",
                business_concept="revenue",
                annotation_source="llm",
            )
        )
        session.flush()

        col_result = _make_column_result()
        evidence = build_column_evidence("column:orders.amount", col_result, session)

        assert evidence["semantic"]["semantic_role"] == "measure"
        assert evidence["semantic"]["business_concept"] == "revenue"


# ---------------------------------------------------------------------------
# Evidence assembly — table level
# ---------------------------------------------------------------------------


class TestBuildTableEvidence:
    def test_aggregates_columns(self, session: Session) -> None:
        network_ctx = _make_network_ctx()

        evidence = build_table_evidence("orders", network_ctx, session)

        assert evidence["target"] == "orders"
        assert evidence["total_columns"] == 2
        assert evidence["columns_investigate"] == 1  # amount
        assert evidence["columns_ready"] == 1  # region

    def test_sorts_by_readiness(self, session: Session) -> None:
        network_ctx = _make_network_ctx()

        evidence = build_table_evidence("orders", network_ctx, session)

        # amount (investigate) should come before region (ready)
        assert evidence["columns"][0]["readiness"] == "investigate"
        assert evidence["columns"][1]["readiness"] == "ready"

    def test_dimension_filter_on_table(self, session: Session) -> None:
        network_ctx = _make_network_ctx()

        evidence = build_table_evidence("orders", network_ctx, session, dimension_filter="semantic")

        # Each column's top_issues should only contain semantic nodes
        for col in evidence["columns"]:
            for issue in col["top_issues"]:
                assert issue["node"] == "business_meaning"

    def test_filters_to_target_table(self, session: Session) -> None:
        # Add a second table
        columns = {
            "column:orders.amount": _make_column_result("column:orders.amount"),
            "column:invoices.total": _make_column_result("column:invoices.total"),
        }
        network_ctx = _make_network_ctx(columns)

        evidence = build_table_evidence("orders", network_ctx, session)

        assert evidence["total_columns"] == 1
        assert evidence["columns"][0]["column"] == "amount"


# ---------------------------------------------------------------------------
# Evidence assembly — dataset level
# ---------------------------------------------------------------------------


class TestBuildDatasetEvidence:
    def test_dataset_summary(self) -> None:
        network_ctx = _make_network_ctx()

        evidence = build_dataset_evidence(network_ctx)

        assert evidence["target"] == "dataset"
        assert evidence["overall_readiness"] == "investigate"
        assert evidence["total_columns"] == 2
        assert evidence["columns_investigate"] == 1
        assert evidence["columns_ready"] == 1

    def test_top_drivers_sorted_by_p_high(self) -> None:
        network_ctx = _make_network_ctx()

        evidence = build_dataset_evidence(network_ctx)

        # Only at-risk columns in top_drivers
        assert len(evidence["top_drivers"]) == 1
        assert evidence["top_drivers"][0]["target"] == "orders.amount"

    def test_dimension_filter_on_dataset(self) -> None:
        network_ctx = _make_network_ctx()

        evidence = build_dataset_evidence(network_ctx, dimension_filter="semantic")

        for driver in evidence["top_drivers"]:
            for node in driver["high_nodes"]:
                assert "semantic" in node["node"] or "business" in node["node"]


# ---------------------------------------------------------------------------
# Existing teachings
# ---------------------------------------------------------------------------


class TestGetExistingTeachings:
    def test_returns_teachings_for_source(self, session: Session) -> None:
        source_id, _ = _setup_typed_tables(session)

        session.add(
            DataFix(
                source_id=source_id,
                action="concept_property",
                target="metadata",
                dimension="semantic",
                description="Taught semantic role",
                table_name="orders",
                column_name="amount",
                status="applied",
                payload={"field_updates": {"semantic_role": "measure"}},
            )
        )
        session.flush()

        teachings = get_existing_teachings(session, source_id)
        assert len(teachings) == 1
        assert teachings[0]["action"] == "concept_property"

    def test_filters_by_table(self, session: Session) -> None:
        source_id, _ = _setup_typed_tables(session, {"orders": ["amount"], "invoices": ["total"]})

        session.add(
            DataFix(
                source_id=source_id,
                action="explanation",
                target="metadata",
                dimension="null_semantics",
                description="Explained nulls",
                table_name="orders",
                column_name="amount",
                status="applied",
                payload={"context": "nulls expected"},
            )
        )
        session.add(
            DataFix(
                source_id=source_id,
                action="explanation",
                target="metadata",
                dimension="types",
                description="Explained types",
                table_name="invoices",
                column_name="total",
                status="applied",
                payload={"context": "types explained"},
            )
        )
        session.flush()

        teachings = get_existing_teachings(session, source_id, table_name="orders")
        assert len(teachings) == 1
        assert teachings[0]["target"] == "orders.amount"


# ---------------------------------------------------------------------------
# Teach type schemas
# ---------------------------------------------------------------------------


class TestGetTeachTypeSchemas:
    def test_returns_all_types(self) -> None:
        schemas = get_teach_type_schemas()

        assert set(schemas.keys()) == VALID_TEACH_TYPES

    def test_schema_has_properties(self) -> None:
        schemas = get_teach_type_schemas()

        for teach_type, schema in schemas.items():
            assert "properties" in schema, f"{teach_type} missing properties"
            assert "required" in schema, f"{teach_type} missing required"


# ---------------------------------------------------------------------------
# Resolution option validation
# ---------------------------------------------------------------------------


class TestValidateResolutionOption:
    def test_valid_concept_option(self) -> None:
        option = {
            "teach_type": "concept",
            "params": {
                "name": "revenue",
                "indicators": ["revenue", "sales"],
            },
            "description": "Teach revenue concept",
            "expected_impact": "semantic.business_meaning",
        }

        result = validate_resolution_option(option)
        assert result.valid is True
        assert result.validation_warning is None
        assert result.teach_type == "concept"

    def test_valid_concept_property_option(self) -> None:
        option = {
            "teach_type": "concept_property",
            "target": "orders.amount",
            "params": {"field_updates": {"semantic_role": "measure"}},
            "description": "Set amount as measure",
        }

        result = validate_resolution_option(option)
        assert result.valid is True

    def test_invalid_params(self) -> None:
        option = {
            "teach_type": "concept",
            "params": {"bad_field": "value"},
            "description": "Bad params",
        }

        result = validate_resolution_option(option)
        assert result.valid is False
        assert result.validation_warning is not None

    def test_unknown_type(self) -> None:
        option = {
            "teach_type": "nonexistent",
            "params": {},
            "description": "Bad type",
        }

        result = validate_resolution_option(option)
        assert result.valid is False
        assert "Unknown teach type" in (result.validation_warning or "")

    def test_valid_explanation_option(self) -> None:
        option = {
            "teach_type": "explanation",
            "target": "orders.amount",
            "params": {
                "dimension": "null_semantics",
                "context": "Nulls are expected for cancelled orders",
            },
            "description": "Explain null pattern",
        }

        result = validate_resolution_option(option)
        assert result.valid is True

    def test_valid_relationship_option(self) -> None:
        option = {
            "teach_type": "relationship",
            "params": {
                "from_table": "orders",
                "from_column": "customer_id",
                "to_table": "customers",
                "to_column": "id",
                "relationship_type": "foreign_key",
            },
            "description": "Declare FK relationship",
        }

        result = validate_resolution_option(option)
        assert result.valid is True
