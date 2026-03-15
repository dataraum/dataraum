"""Tests for query agent models."""

from dataraum.entropy.contracts import ConfidenceLevel
from dataraum.graphs.models import AssumptionBasis
from dataraum.query.models import (
    QueryAnalysisOutput,
    QueryAssumptionOutput,
    QueryResult,
    assumption_output_to_query_assumption,
)


class TestQueryAnalysisOutput:
    """Tests for QueryAnalysisOutput model."""

    def test_json_schema_generation(self):
        """Model generates valid JSON schema for LLM tool use."""
        schema = QueryAnalysisOutput.model_json_schema()

        assert "properties" in schema
        assert "summary" in schema["properties"]
        assert "interpreted_question" in schema["properties"]
        assert "final_sql" in schema["properties"]
        assert "assumptions" in schema["properties"]


class TestQueryResult:
    """Tests for QueryResult dataclass."""

    def test_to_dict(self):
        """Result converts to dictionary."""
        result = QueryResult(
            execution_id="exec_789",
            question="Test question",
            answer="Test answer",
            confidence_level=ConfidenceLevel.YELLOW,
            contract="executive_dashboard",
        )

        d = result.to_dict()

        assert d["execution_id"] == "exec_789"
        assert d["question"] == "Test question"
        assert d["confidence_level"] == "yellow"
        assert d["confidence_emoji"] == "🟡"
        assert d["confidence_label"] == "MARGINAL"
        assert d["contract"] == "executive_dashboard"
        assert d["entropy_action"] is None

    def test_to_dict_entropy_action_always_none(self):
        """Result to_dict has entropy_action as None (field kept for DB compat)."""
        result = QueryResult(
            execution_id="exec_ea",
            question="Test",
        )

        d = result.to_dict()
        assert d["entropy_action"] is None

class TestAssumptionConversion:
    """Tests for assumption conversion."""

    def test_convert_assumption_inferred(self):
        """Convert inferred assumption."""
        output = QueryAssumptionOutput(
            dimension="semantic.units",
            target="column:orders.amount",
            assumption="Currency is EUR",
            basis="inferred",
            confidence=0.8,
        )

        assumption = assumption_output_to_query_assumption(output, "exec_123")

        assert assumption.dimension == "semantic.units"
        assert assumption.target == "column:orders.amount"
        assert assumption.assumption == "Currency is EUR"
        assert assumption.basis == AssumptionBasis.INFERRED
        assert assumption.confidence == 0.8
        assert assumption.execution_id == "exec_123"

    def test_convert_assumption_system_default(self):
        """Convert system default assumption."""
        output = QueryAssumptionOutput(
            dimension="value.nulls",
            target="column:orders.discount",
            assumption="NULL means no discount",
            basis="system_default",
            confidence=0.9,
        )

        assumption = assumption_output_to_query_assumption(output, "exec_456")

        assert assumption.basis == AssumptionBasis.SYSTEM_DEFAULT

    def test_convert_assumption_user_specified(self):
        """Convert user specified assumption."""
        output = QueryAssumptionOutput(
            dimension="structural.relations",
            target="relationship:orders->customers",
            assumption="Join on customer_id",
            basis="user_specified",
            confidence=1.0,
        )

        assumption = assumption_output_to_query_assumption(output, "exec_789")

        assert assumption.basis == AssumptionBasis.USER_SPECIFIED
        assert assumption.confidence == 1.0
