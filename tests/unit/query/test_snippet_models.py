"""Tests for SQL Knowledge Base database models."""

import pytest
from sqlalchemy.exc import IntegrityError

from dataraum.query.snippet_models import SnippetUsageRecord, SQLSnippetRecord


class TestSQLSnippetRecord:
    """Tests for SQLSnippetRecord model."""

    def test_create_extract_snippet(self, session):
        """Create an extract-type snippet."""
        record = SQLSnippetRecord(
            snippet_type="extract",
            standard_field="revenue",
            statement="income_statement",
            aggregation="sum",
            schema_mapping_id="schema_abc",
            sql="SELECT SUM(\"Betrag\") AS value FROM typed_transactions WHERE \"Kontoart\" IN ('Erlöse')",
            description="Sum of revenue from income statement",
            source="graph:dso",
            confidence=1.0,
        )
        session.add(record)
        session.flush()

        assert record.snippet_id is not None
        assert record.snippet_type == "extract"
        assert record.standard_field == "revenue"
        assert record.execution_count == 0
        assert record.is_validated is False

    def test_create_constant_snippet(self, session):
        """Create a constant-type snippet."""
        record = SQLSnippetRecord(
            snippet_type="constant",
            standard_field="days_in_period",
            parameter_value="30",
            schema_mapping_id="schema_abc",
            sql="SELECT 30 AS value",
            description="Analysis period of 30 days",
            source="graph:dso",
            confidence=1.0,
        )
        session.add(record)
        session.flush()

        assert record.snippet_type == "constant"
        assert record.parameter_value == "30"

    def test_create_formula_snippet(self, session):
        """Create a formula-type snippet."""
        record = SQLSnippetRecord(
            snippet_type="formula",
            schema_mapping_id="schema_abc",
            normalized_expression="({A} / {B}) * {C}",
            input_fields=["accounts_receivable", "days_in_period", "revenue"],
            sql=(
                "SELECT (SELECT value FROM accounts_receivable) / "
                "(SELECT value FROM revenue) * (SELECT value FROM days_in_period) AS value"
            ),
            description="DSO = (accounts_receivable / revenue) * days_in_period",
            source="graph:dso",
            confidence=0.9,
        )
        session.add(record)
        session.flush()

        assert record.snippet_type == "formula"
        assert record.normalized_expression == "({A} / {B}) * {C}"
        assert len(record.input_fields) == 3

    def test_create_query_snippet(self, session):
        """Create a query-derived snippet."""
        record = SQLSnippetRecord(
            snippet_type="query",
            schema_mapping_id="schema_abc",
            sql='SELECT DATE_TRUNC(\'month\', "Datum") as month, SUM("Betrag") as total FROM typed_transactions GROUP BY 1',
            description="Monthly revenue breakdown",
            embedding_text="Monthly revenue breakdown. Show me revenue by month.",
            source="query:exec_456",
            confidence=0.6,
        )
        session.add(record)
        session.flush()

        assert record.snippet_type == "query"
        assert record.embedding_text is not None

    def test_unique_constraint(self, session):
        """Duplicate semantic key should raise IntegrityError.

        Note: SQLite treats NULL != NULL in unique constraints, so all fields
        in the constraint must be non-NULL for uniqueness to be enforced.
        We set parameter_value="" to ensure all fields are non-NULL.
        """
        base_args = {
            "snippet_type": "extract",
            "standard_field": "revenue",
            "statement": "income_statement",
            "aggregation": "sum",
            "schema_mapping_id": "schema_abc",
            "parameter_value": "",  # Non-NULL so unique constraint fires
            "sql": "SELECT 1",
            "description": "test",
            "source": "graph:dso",
        }

        session.add(SQLSnippetRecord(**base_args))
        session.flush()

        # Same key should fail
        session.add(SQLSnippetRecord(**base_args))
        with pytest.raises(IntegrityError):
            session.flush()

    def test_different_schema_allowed(self, session):
        """Same standard_field but different schema_mapping_id is allowed."""
        base_args = {
            "snippet_type": "extract",
            "standard_field": "revenue",
            "statement": "income_statement",
            "aggregation": "sum",
            "sql": "SELECT 1",
            "description": "test",
            "source": "graph:dso",
        }

        session.add(SQLSnippetRecord(schema_mapping_id="schema_abc", **base_args))
        session.add(SQLSnippetRecord(schema_mapping_id="schema_xyz", **base_args))
        session.flush()  # Should not raise

    def test_different_parameter_value_allowed(self, session):
        """Same constant with different parameter values is allowed."""
        common_args = {
            "snippet_type": "constant",
            "standard_field": "days_in_period",
            "schema_mapping_id": "schema_abc",
            "description": "test",
            "source": "graph:dso",
        }

        session.add(SQLSnippetRecord(parameter_value="30", sql="SELECT 30 AS value", **common_args))
        session.add(SQLSnippetRecord(parameter_value="365", sql="SELECT 365 AS value", **common_args))
        session.flush()  # Should not raise

    def test_column_mappings_json(self, session):
        """Column mappings are stored as JSON."""
        record = SQLSnippetRecord(
            snippet_type="extract",
            standard_field="revenue",
            schema_mapping_id="schema_abc",
            sql="SELECT 1",
            description="test",
            source="graph:test",
            column_mappings={"revenue": "Betrag", "type": "Kontoart"},
        )
        session.add(record)
        session.flush()

        fetched = session.get(SQLSnippetRecord, record.snippet_id)
        assert fetched.column_mappings == {"revenue": "Betrag", "type": "Kontoart"}


class TestSnippetUsageRecord:
    """Tests for SnippetUsageRecord model."""

    def _create_snippet(self, session) -> SQLSnippetRecord:
        """Helper to create a snippet for usage tests."""
        record = SQLSnippetRecord(
            snippet_type="extract",
            standard_field="revenue",
            schema_mapping_id="schema_abc",
            sql="SELECT 1",
            description="test",
            source="graph:test",
        )
        session.add(record)
        session.flush()
        return record

    def test_create_exact_reuse(self, session):
        """Record an exact reuse."""
        snippet = self._create_snippet(session)
        usage = SnippetUsageRecord(
            execution_id="exec_001",
            execution_type="graph",
            snippet_id=snippet.snippet_id,
            usage_type="exact_reuse",
            match_confidence=1.0,
            sql_match_ratio=1.0,
            step_id="revenue",
        )
        session.add(usage)
        session.flush()

        assert usage.usage_id is not None
        assert usage.usage_type == "exact_reuse"

    def test_create_newly_generated(self, session):
        """Record a newly generated step (no snippet)."""
        usage = SnippetUsageRecord(
            execution_id="exec_002",
            execution_type="query",
            snippet_id=None,
            usage_type="newly_generated",
            match_confidence=0.0,
            sql_match_ratio=0.0,
            step_id="monthly_revenue",
        )
        session.add(usage)
        session.flush()

        assert usage.snippet_id is None
        assert usage.usage_type == "newly_generated"

    def test_snippet_relationship(self, session):
        """Usage record links back to snippet."""
        snippet = self._create_snippet(session)
        usage = SnippetUsageRecord(
            execution_id="exec_003",
            execution_type="graph",
            snippet_id=snippet.snippet_id,
            usage_type="adapted",
            match_confidence=0.9,
            sql_match_ratio=0.85,
        )
        session.add(usage)
        session.flush()

        # Navigate relationship
        assert usage.snippet is not None
        assert usage.snippet.snippet_id == snippet.snippet_id

    def test_cascade_delete(self, session):
        """Deleting snippet cascades to usage records."""
        snippet = self._create_snippet(session)
        usage = SnippetUsageRecord(
            execution_id="exec_004",
            execution_type="graph",
            snippet_id=snippet.snippet_id,
            usage_type="exact_reuse",
            match_confidence=1.0,
            sql_match_ratio=1.0,
        )
        session.add(usage)
        session.flush()

        usage_id = usage.usage_id
        session.delete(snippet)
        session.flush()

        assert session.get(SnippetUsageRecord, usage_id) is None
