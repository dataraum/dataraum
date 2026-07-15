"""Tests for SQL Knowledge Base database models."""

import pytest
from sqlalchemy.exc import IntegrityError

from dataraum.query.snippet_models import SQLSnippetRecord


class TestSQLSnippetRecord:
    """Tests for SQLSnippetRecord model."""

    def test_create_extract_snippet(self, session):
        """Create an extract-type snippet."""
        record = SQLSnippetRecord(
            workspace_id="ws_test",
            snippet_type="extract",
            standard_field="revenue",
            statement="income_statement",
            aggregation="sum",
            schema_mapping_id="schema_abc",
            sql='SELECT SUM("Betrag") AS value FROM typed_transactions WHERE "Kontoart" IN (\'Erlöse\')',
            description="Sum of revenue from income statement",
            source="graph:dso",
        )
        session.add(record)
        session.flush()

        assert record.snippet_id is not None
        assert record.snippet_type == "extract"
        assert record.standard_field == "revenue"
        assert record.execution_count == 0

    def test_create_constant_snippet(self, session):
        """Create a constant-type snippet."""
        record = SQLSnippetRecord(
            workspace_id="ws_test",
            snippet_type="constant",
            standard_field="days_in_period",
            parameter_value="30",
            schema_mapping_id="schema_abc",
            sql="SELECT 30 AS value",
            description="Analysis period of 30 days",
            source="graph:dso",
        )
        session.add(record)
        session.flush()

        assert record.snippet_type == "constant"
        assert record.parameter_value == "30"

    def test_create_formula_snippet(self, session):
        """Create a formula-type snippet."""
        record = SQLSnippetRecord(
            workspace_id="ws_test",
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
        )
        session.add(record)
        session.flush()

        assert record.snippet_type == "formula"
        assert record.normalized_expression == "({A} / {B}) * {C}"
        assert len(record.input_fields) == 3

    def test_create_query_snippet(self, session):
        """Create a query-derived snippet."""
        record = SQLSnippetRecord(
            workspace_id="ws_test",
            snippet_type="query",
            schema_mapping_id="schema_abc",
            sql='SELECT DATE_TRUNC(\'month\', "Datum") as month, SUM("Betrag") as total FROM typed_transactions GROUP BY 1',
            description="Monthly revenue breakdown",
            source="query:exec_456",
        )
        session.add(record)
        session.flush()

        assert record.snippet_type == "query"
        assert record.source == "query:exec_456"

    def test_unique_constraint(self, session):
        """Duplicate semantic key should raise IntegrityError.

        Note: SQLite treats NULL != NULL in unique constraints, so all fields
        in the constraint must be non-NULL for uniqueness to be enforced.
        We set parameter_value="" to ensure all fields are non-NULL.
        """
        base_args = {
            "workspace_id": "ws_test",
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
            "workspace_id": "ws_test",
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
            "workspace_id": "ws_test",
            "snippet_type": "constant",
            "standard_field": "days_in_period",
            "schema_mapping_id": "schema_abc",
            "description": "test",
            "source": "graph:dso",
        }

        session.add(SQLSnippetRecord(parameter_value="30", sql="SELECT 30 AS value", **common_args))
        session.add(
            SQLSnippetRecord(parameter_value="365", sql="SELECT 365 AS value", **common_args)
        )
        session.flush()  # Should not raise

    def test_snippet_type_check_constraint(self, session):
        """An unrecognized snippet_type is rejected at the DB layer (DAT-781: the
        two-layer enforcement standard — a closed-vocabulary column gets a
        CheckConstraint whenever it's touched)."""
        record = SQLSnippetRecord(
            workspace_id="ws_test",
            snippet_type="bogus",
            schema_mapping_id="schema_abc",
            sql="SELECT 1",
            description="test",
            source="graph:dso",
        )
        session.add(record)
        with pytest.raises(IntegrityError):
            session.flush()
