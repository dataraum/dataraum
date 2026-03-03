"""Tests for QueryDocument model."""

from dataraum.query.document import QueryAssumptionData, QueryDocument, SQLStep
from dataraum.query.models import QueryAnalysisOutput, QueryAssumptionOutput, SQLStepOutput


class TestQueryDocument:
    """Tests for QueryDocument dataclass."""

    def test_from_query_analysis(self):
        """Create document from QueryAnalysisOutput."""
        output = QueryAnalysisOutput(
            summary="Calculates total completed orders.",
            interpreted_question="How many orders are completed?",
            metric_type="scalar",
            steps=[
                SQLStepOutput(
                    step_id="count_completed",
                    sql="SELECT COUNT(*) FROM orders WHERE status = 'completed'",
                    description="Count completed orders",
                )
            ],
            final_sql="SELECT total FROM completed_count",
            column_mappings={"count": "total"},
            assumptions=[
                QueryAssumptionOutput(
                    dimension="value.nulls",
                    target="column:orders.status",
                    assumption="NULL status treated as unknown",
                    basis="system_default",
                    confidence=0.9,
                )
            ],
        )

        doc = QueryDocument.from_query_analysis(output)

        assert doc.summary == "Calculates total completed orders."
        assert len(doc.steps) == 1
        assert doc.steps[0].step_id == "count_completed"
        assert doc.final_sql == "SELECT total FROM completed_count"
        assert len(doc.assumptions) == 1
        assert doc.assumptions[0].dimension == "value.nulls"

    def test_from_query_analysis_with_override_assumptions(self):
        """Create document with overridden assumptions."""
        output = QueryAnalysisOutput(
            summary="Calculates revenue.",
            interpreted_question="What is revenue?",
            metric_type="scalar",
            steps=[],
            final_sql="SELECT SUM(amount) FROM orders",
            assumptions=[
                QueryAssumptionOutput(
                    dimension="original",
                    target="original",
                    assumption="Original assumption",
                    basis="inferred",
                    confidence=0.5,
                )
            ],
        )

        override_assumptions = [
            {
                "dimension": "override",
                "target": "override_target",
                "assumption": "Override assumption",
                "basis": "user_specified",
                "confidence": 1.0,
            }
        ]

        doc = QueryDocument.from_query_analysis(output, assumptions=override_assumptions)

        # Should use override assumptions, not original
        assert len(doc.assumptions) == 1
        assert doc.assumptions[0].dimension == "override"
        assert doc.assumptions[0].confidence == 1.0

    def test_to_dict(self):
        """Document converts to dictionary."""
        doc = QueryDocument(
            summary="Test summary.",
            steps=[
                SQLStep(step_id="s1", sql="SQL", description="Desc"),
            ],
            final_sql="FINAL SQL",
            column_mappings={"a": "b"},
            assumptions=[
                QueryAssumptionData(
                    dimension="d",
                    target="t",
                    assumption="a",
                    basis="inferred",
                    confidence=0.5,
                )
            ],
        )

        d = doc.to_dict()

        assert d["summary"] == "Test summary."
        assert len(d["steps"]) == 1
        assert d["steps"][0]["step_id"] == "s1"
        assert d["final_sql"] == "FINAL SQL"
        assert d["column_mappings"]["a"] == "b"
        assert len(d["assumptions"]) == 1
