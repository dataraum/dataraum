"""Tests for query agent models."""

from dataraum.entropy.contracts import ConfidenceLevel
from dataraum.graphs.models import AssumptionBasis, QueryAssumption
from dataraum.query.models import (
    QueryAnalysisOutput,
    QueryAssumptionOutput,
    QueryResult,
    SQLStepOutput,
    assumption_output_to_query_assumption,
)


class TestQueryAnalysisOutput:
    """Tests for QueryAnalysisOutput model."""

    def test_minimal_output(self):
        """Output with minimal required fields."""
        output = QueryAnalysisOutput(
            interpreted_question="What was total revenue?",
            metric_type="scalar",
            final_sql="SELECT SUM(amount) FROM orders",
        )

        assert output.interpreted_question == "What was total revenue?"
        assert output.metric_type == "scalar"
        assert output.final_sql == "SELECT SUM(amount) FROM orders"
        assert output.steps == []
        assert output.assumptions == []

    def test_full_output(self):
        """Output with all fields populated."""
        output = QueryAnalysisOutput(
            interpreted_question="Calculate total revenue from completed orders",
            metric_type="scalar",
            steps=[
                SQLStepOutput(
                    step_id="filter_completed",
                    sql="SELECT * FROM orders WHERE status = 'completed'",
                    description="Filter to completed orders only",
                )
            ],
            final_sql="SELECT SUM(amount) FROM completed_orders",
            column_mappings={"revenue": "amount"},
            assumptions=[
                QueryAssumptionOutput(
                    dimension="semantic.units",
                    target="column:orders.amount",
                    assumption="Currency is EUR",
                    basis="inferred",
                    confidence=0.8,
                )
            ],
            validation_notes=["NULL values in amount column ignored"],
            suggested_format="scalar",
        )

        assert len(output.steps) == 1
        assert output.steps[0].step_id == "filter_completed"
        assert len(output.assumptions) == 1
        assert output.assumptions[0].dimension == "semantic.units"
        assert len(output.validation_notes) == 1

    def test_json_schema_generation(self):
        """Model generates valid JSON schema for LLM tool use."""
        schema = QueryAnalysisOutput.model_json_schema()

        assert "properties" in schema
        assert "interpreted_question" in schema["properties"]
        assert "final_sql" in schema["properties"]
        assert "assumptions" in schema["properties"]


class TestQueryResult:
    """Tests for QueryResult dataclass."""

    def test_successful_result(self):
        """Result for successful query."""
        result = QueryResult(
            execution_id="exec_123",
            question="What was total revenue?",
            answer="Total revenue was $1,000,000",
            sql="SELECT SUM(amount) FROM orders",
            data=[{"total": 1000000}],
            columns=["total"],
            confidence_level=ConfidenceLevel.GREEN,
            entropy_score=0.15,
            contract="exploratory_analysis",
        )

        assert result.success is True
        assert result.confidence_level == ConfidenceLevel.GREEN
        assert result.data == [{"total": 1000000}]

    def test_failed_result(self):
        """Result for failed query."""
        result = QueryResult(
            execution_id="exec_456",
            question="Invalid query",
            success=False,
            error="SQL execution failed",
            confidence_level=ConfidenceLevel.RED,
        )

        assert result.success is False
        assert result.error == "SQL execution failed"

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

    def test_format_cli_response_success(self):
        """CLI response formatting for success."""
        result = QueryResult(
            execution_id="exec_test",
            question="What was revenue?",
            answer="Total revenue was $500,000",
            data=[{"total": 500000}],
            columns=["total"],
            confidence_level=ConfidenceLevel.GREEN,
            contract="exploratory_analysis",
        )

        cli_output = result.format_cli_response()

        assert "GOOD" in cli_output
        assert "exploratory_analysis" in cli_output
        assert "500,000" in cli_output or "500000" in cli_output

    def test_format_cli_response_with_assumptions(self):
        """CLI response includes assumptions."""
        assumption = QueryAssumption.create(
            execution_id="exec_test",
            dimension="semantic.units",
            target="column:orders.amount",
            assumption="Currency is EUR",
            basis=AssumptionBasis.INFERRED,
            confidence=0.8,
        )

        result = QueryResult(
            execution_id="exec_test",
            question="What was revenue?",
            answer="Total revenue was €500,000",
            confidence_level=ConfidenceLevel.YELLOW,
            assumptions=[assumption],
        )

        cli_output = result.format_cli_response()

        assert "Assumptions:" in cli_output
        assert "Currency is EUR" in cli_output


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
