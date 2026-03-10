"""Bridge function unit tests — build_fix_documents() routing and edge cases."""

from __future__ import annotations

from dataraum.pipeline.fixes import FixInput
from dataraum.pipeline.fixes.bridge import _extract_template_fields, build_fix_documents
from dataraum.pipeline.fixes.models import FixDocument, FixSchema, FixSchemaField


def _append_schema() -> FixSchema:
    """Accept-finding style: append column refs to a list."""
    return FixSchema(
        action="accept_finding",
        target="config",
        config_path="entropy/thresholds.yaml",
        key_path=["detectors", "test", "accepted_columns"],
        operation="append",
        requires_rerun="quality_review",
    )


def _per_column_schema() -> FixSchema:
    """Business-meaning style: merge dict per affected column."""
    return FixSchema(
        action="document_business_meaning",
        target="config",
        config_path="phases/semantic.yaml",
        key_path=["overrides", "business_meaning"],
        operation="merge",
        requires_rerun="semantic",
        fields={
            "business_name": FixSchemaField(type="string", required=False),
            "entity_type": FixSchemaField(type="string", required=False),
            "description": FixSchemaField(type="string", required=False),
        },
    )


def _keyed_schema() -> FixSchema:
    """Confirm-relationship style: merge dict under template-derived key."""
    return FixSchema(
        action="confirm_relationship",
        target="config",
        config_path="phases/relationships.yaml",
        key_path=["overrides", "confirmed_relationships"],
        operation="merge",
        requires_rerun="relationships",
        key_template="{from_table}->{to_table}",
        fields={
            "from_table": FixSchemaField(type="string", required=True),
            "to_table": FixSchemaField(type="string", required=True),
            "relationship_type": FixSchemaField(type="enum", required=False),
        },
    )


# ---------------------------------------------------------------------------
# Routing
# ---------------------------------------------------------------------------


class TestBridgeRouting:
    """build_fix_documents routes by operation + key_template."""

    def test_append_route(self) -> None:
        docs = build_fix_documents(
            _append_schema(),
            FixInput(action_name="accept_finding", affected_columns=["t.a"]),
            "t", "a", "quality_review",
        )
        assert len(docs) == 1
        assert docs[0].payload["operation"] == "append"

    def test_per_column_route(self) -> None:
        docs = build_fix_documents(
            _per_column_schema(),
            FixInput(
                action_name="document_business_meaning",
                affected_columns=["t.a"],
                parameters={"business_name": "Amount"},
            ),
            "t", "a", "semantic",
        )
        assert len(docs) == 1
        assert docs[0].payload["operation"] == "merge"
        assert docs[0].payload["key_path"][-1] == "t.a"

    def test_keyed_route(self) -> None:
        docs = build_fix_documents(
            _keyed_schema(),
            FixInput(
                action_name="confirm_relationship",
                affected_columns=["t.a"],
                parameters={"from_table": "orders", "to_table": "products"},
            ),
            "orders", "id", "relationships",
        )
        assert len(docs) == 1
        assert docs[0].payload["key_path"][-1] == "orders->products"

    def test_non_config_target_returns_empty(self) -> None:
        schema = FixSchema(action="foo", target="metadata")
        docs = build_fix_documents(
            schema,
            FixInput(action_name="foo", affected_columns=["t.a"]),
            "t", "a", "dim",
        )
        assert docs == []


# ---------------------------------------------------------------------------
# Append documents
# ---------------------------------------------------------------------------


class TestAppendDocuments:
    def test_empty_columns(self) -> None:
        docs = build_fix_documents(
            _append_schema(),
            FixInput(action_name="accept_finding", affected_columns=[]),
            "t", "a", "quality_review",
        )
        assert docs == []

    def test_ordinals_sequential(self) -> None:
        docs = build_fix_documents(
            _append_schema(),
            FixInput(action_name="accept_finding", affected_columns=["t.a", "t.b", "t.c"]),
            "t", "a", "quality_review",
        )
        assert [d.ordinal for d in docs] == [0, 1, 2]

    def test_interpretation_becomes_reason(self) -> None:
        docs = build_fix_documents(
            _append_schema(),
            FixInput(
                action_name="accept_finding",
                affected_columns=["t.a"],
                interpretation="User reviewed",
            ),
            "t", "a", "quality_review",
        )
        assert docs[0].payload["reason"] == "User reviewed"

    def test_default_reason_when_no_interpretation(self) -> None:
        docs = build_fix_documents(
            _append_schema(),
            FixInput(action_name="accept_finding", affected_columns=["t.a"]),
            "t", "a", "quality_review",
        )
        assert "t" in docs[0].payload["reason"]


# ---------------------------------------------------------------------------
# Per-column documents
# ---------------------------------------------------------------------------


class TestPerColumnDocuments:
    def test_value_filtered_by_schema_fields(self) -> None:
        docs = build_fix_documents(
            _per_column_schema(),
            FixInput(
                action_name="document_business_meaning",
                affected_columns=["t.a"],
                parameters={
                    "business_name": "Amount",
                    "extra_field": "ignored",
                },
            ),
            "t", "a", "semantic",
        )
        value = docs[0].payload["value"]
        assert value == {"business_name": "Amount"}
        assert "extra_field" not in value

    def test_multiple_columns_produce_multiple_docs(self) -> None:
        docs = build_fix_documents(
            _per_column_schema(),
            FixInput(
                action_name="document_business_meaning",
                affected_columns=["t.a", "t.b"],
                parameters={"business_name": "Test"},
            ),
            "t", "a", "semantic",
        )
        assert len(docs) == 2
        keys = [d.payload["key_path"][-1] for d in docs]
        assert "t.a" in keys
        assert "t.b" in keys


# ---------------------------------------------------------------------------
# Keyed documents
# ---------------------------------------------------------------------------


class TestKeyedDocuments:
    def test_key_fields_excluded_from_value(self) -> None:
        docs = build_fix_documents(
            _keyed_schema(),
            FixInput(
                action_name="confirm_relationship",
                affected_columns=["t.a"],
                parameters={
                    "from_table": "orders",
                    "to_table": "products",
                    "relationship_type": "foreign_key",
                },
            ),
            "orders", "id", "relationships",
        )
        value = docs[0].payload["value"]
        assert "from_table" not in value
        assert "to_table" not in value
        assert value["relationship_type"] == "foreign_key"

    def test_always_produces_one_doc(self) -> None:
        """Keyed route always produces exactly 1 doc regardless of affected_columns."""
        docs = build_fix_documents(
            _keyed_schema(),
            FixInput(
                action_name="confirm_relationship",
                affected_columns=["t.a", "t.b", "t.c"],
                parameters={"from_table": "a", "to_table": "b"},
            ),
            "a", "id", "relationships",
        )
        assert len(docs) == 1

    def test_document_metadata(self) -> None:
        docs = build_fix_documents(
            _keyed_schema(),
            FixInput(
                action_name="confirm_relationship",
                affected_columns=["t.a"],
                parameters={"from_table": "x", "to_table": "y"},
            ),
            "x", "id", "relationships",
        )
        doc = docs[0]
        assert doc.target == "config"
        assert doc.action == "confirm_relationship"
        assert doc.table_name == "x"
        assert doc.dimension == "relationships"
        assert doc.ordinal == 0
        assert isinstance(doc, FixDocument)


# ---------------------------------------------------------------------------
# Template extraction
# ---------------------------------------------------------------------------


class TestExtractTemplateFields:
    def test_simple_template(self) -> None:
        assert _extract_template_fields("{a}->{b}") == {"a", "b"}

    def test_single_field(self) -> None:
        assert _extract_template_fields("{name}") == {"name"}

    def test_no_fields(self) -> None:
        assert _extract_template_fields("literal") == set()
