"""Tests for QueryDocument model."""

from dataraum.query.document import QueryAssumptionData, QueryDocument, SQLStep
from dataraum.query.models import QueryAnalysisOutput, QueryAssumptionOutput, SQLStepOutput


class TestSQLStep:
    """Tests for SQLStep dataclass."""

    def test_create_step(self):
        """Create a SQLStep."""
        step = SQLStep(
            step_id="filter_active",
            sql="SELECT * FROM orders WHERE status = 'active'",
            description="Filter to active orders only",
        )

        assert step.step_id == "filter_active"
        assert "SELECT" in step.sql
        assert step.description == "Filter to active orders only"

    def test_to_dict(self):
        """Step converts to dictionary."""
        step = SQLStep(
            step_id="sum_amounts",
            sql="SELECT SUM(amount) FROM orders",
            description="Sum all order amounts",
        )

        d = step.to_dict()

        assert d["step_id"] == "sum_amounts"
        assert d["sql"] == "SELECT SUM(amount) FROM orders"
        assert d["description"] == "Sum all order amounts"


class TestQueryAssumptionData:
    """Tests for QueryAssumptionData dataclass."""

    def test_create_assumption(self):
        """Create an assumption."""
        assumption = QueryAssumptionData(
            dimension="semantic.units",
            target="column:orders.amount",
            assumption="Currency is EUR",
            basis="inferred",
            confidence=0.8,
        )

        assert assumption.dimension == "semantic.units"
        assert assumption.target == "column:orders.amount"
        assert assumption.assumption == "Currency is EUR"
        assert assumption.basis == "inferred"
        assert assumption.confidence == 0.8

    def test_to_dict(self):
        """Assumption converts to dictionary."""
        assumption = QueryAssumptionData(
            dimension="value.nulls",
            target="column:orders.discount",
            assumption="NULL means no discount",
            basis="system_default",
            confidence=0.9,
        )

        d = assumption.to_dict()

        assert d["dimension"] == "value.nulls"
        assert d["target"] == "column:orders.discount"
        assert d["assumption"] == "NULL means no discount"
        assert d["basis"] == "system_default"
        assert d["confidence"] == 0.9


class TestQueryDocument:
    """Tests for QueryDocument dataclass."""

    def test_create_minimal_document(self):
        """Create document with minimal fields."""
        doc = QueryDocument(
            summary="Calculates total revenue.",
            steps=[],
            final_sql="SELECT SUM(amount) FROM orders",
        )

        assert doc.summary == "Calculates total revenue."
        assert doc.steps == []
        assert doc.final_sql == "SELECT SUM(amount) FROM orders"
        assert doc.column_mappings == {}
        assert doc.assumptions == []

    def test_create_full_document(self):
        """Create document with all fields."""
        doc = QueryDocument(
            summary="Calculates revenue by region for Q3.",
            steps=[
                SQLStep(
                    step_id="filter_q3",
                    sql="SELECT * FROM orders WHERE quarter = 'Q3'",
                    description="Filter to Q3 orders",
                ),
                SQLStep(
                    step_id="group_region",
                    sql="SELECT region, SUM(amount) FROM q3_orders GROUP BY region",
                    description="Group by region and sum",
                ),
            ],
            final_sql="SELECT region, total FROM regional_revenue",
            column_mappings={"revenue": "amount", "region": "region"},
            assumptions=[
                QueryAssumptionData(
                    dimension="semantic.units",
                    target="column:orders.amount",
                    assumption="Currency is EUR",
                    basis="inferred",
                    confidence=0.8,
                )
            ],
        )

        assert len(doc.steps) == 2
        assert doc.steps[0].step_id == "filter_q3"
        assert len(doc.assumptions) == 1
        assert doc.column_mappings["revenue"] == "amount"

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

    def test_get_step_descriptions(self):
        """Get list of step descriptions."""
        doc = QueryDocument(
            summary="Summary",
            steps=[
                SQLStep(step_id="s1", sql="", description="Filter orders"),
                SQLStep(step_id="s2", sql="", description="Sum amounts"),
            ],
            final_sql="SELECT 1",
        )

        descriptions = doc.get_step_descriptions()

        assert descriptions == ["Filter orders", "Sum amounts"]

    def test_get_assumption_texts(self):
        """Get list of assumption texts."""
        doc = QueryDocument(
            summary="Summary",
            steps=[],
            final_sql="SELECT 1",
            assumptions=[
                QueryAssumptionData("d1", "t1", "Currency is EUR", "inferred", 0.8),
                QueryAssumptionData("d2", "t2", "Region is EMEA", "inferred", 0.7),
            ],
        )

        texts = doc.get_assumption_texts()

        assert texts == ["Currency is EUR", "Region is EMEA"]
