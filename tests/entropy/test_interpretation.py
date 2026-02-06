"""Tests for entropy interpretation module.

Tests the LLM-powered entropy interpretation feature including
models and prompt context building.
"""

import json

from dataraum.entropy.analysis.aggregator import ColumnSummary
from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.interpretation import (
    Assumption,
    EntropyInterpretation,
    InterpretationInput,
    ResolutionAction,
)
from dataraum.entropy.models import CompoundRisk


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
            dimension_scores={"semantic.business_meaning.naming_clarity": 0.6},
            high_entropy_dimensions=["semantic.business_meaning.naming_clarity"],
            compound_risks=[],
        )

        assert input_data.table_name == "orders"
        assert input_data.column_name == "amount"
        assert input_data.composite_score == 0.45
        assert input_data.semantic_entropy == 0.6
        assert "null_ratio" in input_data.raw_metrics

    def test_from_summary(self):
        """Test creating input from ColumnSummary."""
        config = get_entropy_config()
        weights = config.composite_weights
        layer_scores = {
            "structural": 0.1,
            "semantic": 0.6,
            "value": 0.5,
            "computational": 0.2,
        }
        composite_score = (
            layer_scores["structural"] * weights["structural"]
            + layer_scores["semantic"] * weights["semantic"]
            + layer_scores["value"] * weights["value"]
            + layer_scores["computational"] * weights["computational"]
        )

        summary = ColumnSummary(
            column_id="col1",
            column_name="amount",
            table_id="t1",
            table_name="orders",
            composite_score=composite_score,
            readiness="investigate",
            layer_scores=layer_scores,
            dimension_scores={
                "semantic.business_meaning.naming_clarity": 0.7,
            },
            high_entropy_dimensions=["semantic.business_meaning.naming_clarity"],
        )

        input_data = InterpretationInput.from_summary(
            summary=summary,
            detected_type="DECIMAL",
            business_description="Order total",
            raw_metrics={"null_ratio": 0.15},
        )

        assert input_data.table_name == "orders"
        assert input_data.column_name == "amount"
        assert input_data.detected_type == "DECIMAL"
        assert input_data.business_description == "Order total"
        assert input_data.structural_entropy == 0.1
        assert input_data.semantic_entropy == 0.6
        assert input_data.value_entropy == 0.5
        assert input_data.computational_entropy == 0.2
        assert input_data.high_entropy_dimensions == ["semantic.business_meaning.naming_clarity"]

    def test_from_summary_with_defaults(self):
        """Test creating input from summary with default values."""
        summary = ColumnSummary(
            column_id="col1",
            column_name="col1",
            table_id="t1",
            table_name="table1",
        )

        input_data = InterpretationInput.from_summary(summary)

        assert input_data.detected_type == "unknown"
        assert input_data.business_description is None
        assert input_data.raw_metrics == {}

    def test_from_summary_with_compound_risks(self):
        """Test creating input from summary with compound risks."""
        risk = CompoundRisk(
            target="column:orders.amount",
            dimensions=["semantic.units", "computational.aggregations"],
            dimension_scores={"semantic.units": 0.7, "computational.aggregations": 0.6},
            risk_level="critical",
            impact="Units may be incorrect in aggregations",
            multiplier=2.0,
            combined_score=0.85,
        )

        summary = ColumnSummary(
            column_id="col1",
            column_name="amount",
            table_id="t1",
            table_name="orders",
            compound_risks=[risk],
        )

        input_data = InterpretationInput.from_summary(summary)

        assert len(input_data.compound_risks) == 1
        assert input_data.compound_risks[0].risk_level == "critical"


class TestInterpretationInputFields:
    """Tests for InterpretationInput field access and serialization."""

    def test_input_fields_accessible_for_batch_prompt(self):
        """Test that InterpretationInput fields can be serialized for batch prompt."""
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
            dimension_scores={"semantic.business_meaning.naming_clarity": 0.6},
            high_entropy_dimensions=["semantic.business_meaning.naming_clarity"],
            compound_risks=[],
        )

        # Build the batch column data structure (as done in interpret_batch)
        column_data = {
            "key": f"{input_data.table_name}.{input_data.column_name}",
            "table_name": input_data.table_name,
            "column_name": input_data.column_name,
            "detected_type": input_data.detected_type,
            "business_description": input_data.business_description or "Not documented",
            "composite_score": input_data.composite_score,
            "readiness": input_data.readiness,
            "structural_entropy": input_data.structural_entropy,
            "semantic_entropy": input_data.semantic_entropy,
            "value_entropy": input_data.value_entropy,
            "computational_entropy": input_data.computational_entropy,
            "high_entropy_dimensions": input_data.high_entropy_dimensions,
            "raw_metrics": input_data.raw_metrics,
        }

        assert column_data["key"] == "orders.amount"
        assert column_data["table_name"] == "orders"
        assert column_data["column_name"] == "amount"
        assert column_data["detected_type"] == "DECIMAL"
        assert column_data["business_description"] == "Total order amount in USD"
        assert column_data["composite_score"] == 0.45
        assert column_data["readiness"] == "investigate"
        assert "null_ratio" in column_data["raw_metrics"]

        # Verify it can be JSON serialized
        json_str = json.dumps(column_data)
        assert "orders.amount" in json_str
