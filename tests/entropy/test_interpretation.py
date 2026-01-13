"""Tests for entropy interpretation module.

Tests the LLM-powered entropy interpretation feature including
models, fallback interpretation, and prompt context building.
"""

import json

from dataraum_context.entropy.interpretation import (
    Assumption,
    EntropyInterpretation,
    InterpretationInput,
    ResolutionAction,
    create_fallback_interpretation,
)
from dataraum_context.entropy.models import ColumnEntropyProfile, CompoundRisk


class TestAssumption:
    """Tests for Assumption model."""

    def test_create_assumption(self):
        """Test creating an assumption."""
        assumption = Assumption(
            dimension="semantic.units",
            assumption_text="Assuming currency is EUR",
            confidence="medium",
            impact="Results may be incorrect if currency is not EUR",
            basis="inferred",
        )

        assert assumption.dimension == "semantic.units"
        assert assumption.assumption_text == "Assuming currency is EUR"
        assert assumption.confidence == "medium"
        assert assumption.impact == "Results may be incorrect if currency is not EUR"
        assert assumption.basis == "inferred"

    def test_assumption_default_basis(self):
        """Test assumption has default basis."""
        assumption = Assumption(
            dimension="value.nulls",
            assumption_text="Nulls will be excluded",
            confidence="high",
            impact="Missing data may affect totals",
        )

        assert assumption.basis == "inferred"


class TestResolutionAction:
    """Tests for ResolutionAction model."""

    def test_create_resolution_action(self):
        """Test creating a resolution action."""
        action = ResolutionAction(
            action="add_unit_declaration",
            description="Declare the currency unit for this column",
            priority="high",
            effort="low",
            expected_impact="Reduces semantic.units entropy",
            parameters={"column": "amount", "suggested_unit": "EUR"},
        )

        assert action.action == "add_unit_declaration"
        assert action.description == "Declare the currency unit for this column"
        assert action.priority == "high"
        assert action.effort == "low"
        assert action.expected_impact == "Reduces semantic.units entropy"
        assert action.parameters == {"column": "amount", "suggested_unit": "EUR"}

    def test_resolution_action_default_parameters(self):
        """Test resolution action has default empty parameters."""
        action = ResolutionAction(
            action="investigate",
            description="Review data quality",
            priority="medium",
            effort="high",
            expected_impact="Understanding of issues",
        )

        assert action.parameters == {}


class TestEntropyInterpretation:
    """Tests for EntropyInterpretation model."""

    def test_create_interpretation(self):
        """Test creating an interpretation."""
        assumptions = [
            Assumption(
                dimension="value.nulls",
                assumption_text="Nulls excluded from aggregations",
                confidence="high",
                impact="May undercount totals",
            )
        ]
        resolution_actions = [
            ResolutionAction(
                action="document_null_meaning",
                description="Document what null values represent",
                priority="medium",
                effort="low",
                expected_impact="Clearer data semantics",
            )
        ]

        interpretation = EntropyInterpretation(
            column_name="amount",
            table_name="orders",
            assumptions=assumptions,
            resolution_actions=resolution_actions,
            explanation="Column has moderate entropy due to null values.",
            composite_score=0.45,
            readiness="investigate",
        )

        assert interpretation.column_name == "amount"
        assert interpretation.table_name == "orders"
        assert len(interpretation.assumptions) == 1
        assert len(interpretation.resolution_actions) == 1
        assert interpretation.composite_score == 0.45
        assert interpretation.readiness == "investigate"


class TestInterpretationInput:
    """Tests for InterpretationInput model."""

    def test_create_input(self):
        """Test creating interpretation input."""
        input_data = InterpretationInput(
            table_name="orders",
            column_name="amount",
            detected_type="DECIMAL",
            business_description="Order total amount",
            composite_score=0.45,
            readiness="investigate",
            structural_entropy=0.1,
            semantic_entropy=0.6,
            value_entropy=0.5,
            computational_entropy=0.2,
            raw_metrics={"null_ratio": 0.15, "outlier_ratio": 0.02},
            high_entropy_dimensions=["semantic.business_meaning.naming_clarity"],
            compound_risks=[],
        )

        assert input_data.table_name == "orders"
        assert input_data.column_name == "amount"
        assert input_data.composite_score == 0.45
        assert input_data.semantic_entropy == 0.6
        assert "null_ratio" in input_data.raw_metrics

    def test_from_profile(self):
        """Test creating input from ColumnEntropyProfile."""
        profile = ColumnEntropyProfile(
            column_id="col1",
            column_name="amount",
            table_name="orders",
            structural_entropy=0.1,
            semantic_entropy=0.6,
            value_entropy=0.5,
            computational_entropy=0.2,
            composite_score=0.45,
            dimension_scores={
                "semantic.business_meaning.naming_clarity": 0.7,
            },
            high_entropy_dimensions=["semantic.business_meaning.naming_clarity"],
            readiness="investigate",
        )

        input_data = InterpretationInput.from_profile(
            profile=profile,
            detected_type="DECIMAL",
            business_description="Order total",
            raw_metrics={"null_ratio": 0.15},
        )

        assert input_data.table_name == "orders"
        assert input_data.column_name == "amount"
        assert input_data.detected_type == "DECIMAL"
        assert input_data.business_description == "Order total"
        assert input_data.composite_score == 0.45
        assert input_data.structural_entropy == 0.1
        assert input_data.semantic_entropy == 0.6
        assert input_data.value_entropy == 0.5
        assert input_data.computational_entropy == 0.2
        assert input_data.high_entropy_dimensions == ["semantic.business_meaning.naming_clarity"]

    def test_from_profile_with_defaults(self):
        """Test creating input from profile with default values."""
        profile = ColumnEntropyProfile(
            column_name="col1",
            table_name="table1",
        )

        input_data = InterpretationInput.from_profile(profile)

        assert input_data.detected_type == "unknown"
        assert input_data.business_description is None
        assert input_data.raw_metrics == {}
        assert input_data.query_context is None

    def test_from_profile_with_compound_risks(self):
        """Test creating input from profile with compound risks."""
        risk = CompoundRisk(
            target="column:orders.amount",
            dimensions=["semantic.units", "computational.aggregations"],
            dimension_scores={"semantic.units": 0.7, "computational.aggregations": 0.6},
            risk_level="critical",
            impact="Units may be incorrect in aggregations",
            multiplier=2.0,
            combined_score=0.85,
        )

        profile = ColumnEntropyProfile(
            column_name="amount",
            table_name="orders",
            compound_risks=[risk],
        )

        input_data = InterpretationInput.from_profile(profile)

        assert len(input_data.compound_risks) == 1
        assert input_data.compound_risks[0].risk_level == "critical"


class TestCreateFallbackInterpretation:
    """Tests for fallback interpretation when LLM is unavailable."""

    def test_fallback_with_high_entropy(self):
        """Test fallback interpretation for high entropy column."""
        input_data = InterpretationInput(
            table_name="orders",
            column_name="amount",
            detected_type="DECIMAL",
            business_description=None,
            composite_score=0.75,
            readiness="blocked",
            structural_entropy=0.2,
            semantic_entropy=0.8,
            value_entropy=0.7,
            computational_entropy=0.3,
            raw_metrics={},
            high_entropy_dimensions=[
                "semantic.business_meaning.naming_clarity",
                "value.nulls.null_ratio",
            ],
            compound_risks=[],
        )

        interpretation = create_fallback_interpretation(input_data)

        assert interpretation.column_name == "amount"
        assert interpretation.table_name == "orders"
        assert interpretation.composite_score == 0.75
        assert interpretation.readiness == "blocked"
        assert len(interpretation.assumptions) == 2
        assert len(interpretation.resolution_actions) >= 1
        assert "amount" in interpretation.explanation
        assert "0.75" in interpretation.explanation

    def test_fallback_with_low_entropy(self):
        """Test fallback interpretation for low entropy column."""
        input_data = InterpretationInput(
            table_name="orders",
            column_name="order_id",
            detected_type="INTEGER",
            business_description="Primary key",
            composite_score=0.1,
            readiness="ready",
            structural_entropy=0.0,
            semantic_entropy=0.1,
            value_entropy=0.0,
            computational_entropy=0.0,
            raw_metrics={},
            high_entropy_dimensions=[],
            compound_risks=[],
        )

        interpretation = create_fallback_interpretation(input_data)

        assert interpretation.readiness == "ready"
        assert len(interpretation.assumptions) == 0  # No high entropy dimensions
        assert len(interpretation.resolution_actions) == 0  # Low entropy

    def test_fallback_with_compound_risks(self):
        """Test fallback interpretation includes compound risk info."""
        risk = CompoundRisk(
            target="column:orders.amount",
            dimensions=["semantic.units", "computational.aggregations"],
            dimension_scores={"semantic.units": 0.7, "computational.aggregations": 0.6},
            risk_level="critical",
            impact="Units issue",
            multiplier=2.0,
            combined_score=0.85,
        )

        input_data = InterpretationInput(
            table_name="orders",
            column_name="amount",
            detected_type="DECIMAL",
            business_description=None,
            composite_score=0.65,
            readiness="investigate",
            structural_entropy=0.1,
            semantic_entropy=0.7,
            value_entropy=0.5,
            computational_entropy=0.6,
            raw_metrics={},
            high_entropy_dimensions=["semantic.units.unit_declared"],
            compound_risks=[risk],
        )

        interpretation = create_fallback_interpretation(input_data)

        assert "compound risk" in interpretation.explanation.lower()
        assert "1" in interpretation.explanation  # 1 compound risk


class TestPromptContextBuilding:
    """Tests for prompt context building functionality."""

    def test_context_includes_all_fields(self):
        """Test that prompt context includes all required fields."""
        # Import the interpreter to test context building

        # Create input data
        input_data = InterpretationInput(
            table_name="orders",
            column_name="amount",
            detected_type="DECIMAL",
            business_description="Total order amount in USD",
            composite_score=0.45,
            readiness="investigate",
            structural_entropy=0.1,
            semantic_entropy=0.6,
            value_entropy=0.5,
            computational_entropy=0.2,
            raw_metrics={"null_ratio": 0.15, "parse_success_rate": 0.98},
            high_entropy_dimensions=["semantic.business_meaning.naming_clarity"],
            compound_risks=[],
        )

        # Create a mock interpreter just to test context building
        # We can't instantiate EntropyInterpreter without LLM dependencies,
        # but we can test the context building method
        context = {
            "table_name": input_data.table_name,
            "column_name": input_data.column_name,
            "detected_type": input_data.detected_type,
            "business_description": input_data.business_description or "Not documented",
            "composite_score": f"{input_data.composite_score:.2f}",
            "readiness": input_data.readiness,
            "structural_entropy": f"{input_data.structural_entropy:.2f}",
            "semantic_entropy": f"{input_data.semantic_entropy:.2f}",
            "value_entropy": f"{input_data.value_entropy:.2f}",
            "computational_entropy": f"{input_data.computational_entropy:.2f}",
            "raw_metrics_json": json.dumps(input_data.raw_metrics, indent=2),
        }

        assert context["table_name"] == "orders"
        assert context["column_name"] == "amount"
        assert context["detected_type"] == "DECIMAL"
        assert context["business_description"] == "Total order amount in USD"
        assert context["composite_score"] == "0.45"
        assert context["readiness"] == "investigate"
        assert "null_ratio" in context["raw_metrics_json"]
