"""Tests for entropy interpretation module.

Tests the LLM-powered entropy interpretation feature including
models and prompt context building.
"""

import json

import pytest
from pydantic import ValidationError

from dataraum.entropy.analysis.aggregator import ColumnSummary, TableSummary
from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.interpretation import (
    Assumption,
    EntropyInterpretation,
    EntropyInterpretationOutput,
    InterpretationInput,
    ResolutionAction,
    ResolutionActionOutput,
    TableEntropyInterpretationOutput,
    TableInterpretationInput,
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
                action="document_null_semantics",
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


class TestResolutionActionOutputSchema:
    """Tests for ResolutionActionOutput Pydantic model."""

    def test_parameters_field_in_schema(self):
        """Parameters field exists in the JSON schema for LLM tool use."""
        schema = ResolutionActionOutput.model_json_schema()
        assert "parameters" in schema["properties"]
        assert schema["properties"]["parameters"]["type"] == "object"

    def test_parameters_default_empty(self):
        """Parameters defaults to empty dict when not provided."""
        output = ResolutionActionOutput(
            action="document_unit",
            description="Declare unit",
            priority="high",
            effort="low",
            expected_impact="Reduces semantic.units entropy",
        )
        assert output.parameters == {}

    def test_parameters_populated(self):
        """Parameters populated when provided."""
        output = ResolutionActionOutput(
            action="document_unit",
            description="Declare unit",
            priority="high",
            effort="low",
            expected_impact="Reduces semantic.units entropy",
            parameters={"column_name": "amount", "unit": "EUR"},
        )
        assert output.parameters == {"column_name": "amount", "unit": "EUR"}


class TestEntropyInterpretationDashboardDict:
    """Tests for to_dashboard_dict method."""

    def test_column_level_key(self):
        """Column-level interpretation uses table.column key."""
        interp = EntropyInterpretation(
            column_name="amount",
            table_name="orders",
            assumptions=[],
            resolution_actions=[],
            explanation="Test",
            composite_score=0.5,
            readiness="investigate",
        )
        result = interp.to_dashboard_dict()
        assert result["column_key"] == "orders.amount"

    def test_table_level_key(self):
        """Table-level interpretation (column_name=None) uses table name as key."""
        interp = EntropyInterpretation(
            column_name=None,
            table_name="orders",
            assumptions=[],
            resolution_actions=[],
            explanation="Table-level interpretation",
            composite_score=0.35,
            readiness="investigate",
        )
        result = interp.to_dashboard_dict()
        assert result["column_key"] == "orders"
        assert result["column_name"] is None
        assert result["table_name"] == "orders"


class TestTableInterpretationInput:
    """Tests for TableInterpretationInput model."""

    def test_from_summary(self):
        """Create TableInterpretationInput from a TableSummary."""
        col1 = ColumnSummary(
            column_id="c1",
            column_name="amount",
            table_id="t1",
            table_name="orders",
            composite_score=0.45,
            readiness="investigate",
            layer_scores={"structural": 0.1, "semantic": 0.6, "value": 0.5, "computational": 0.2},
            dimension_scores={
                "semantic.units.unit_declaration": 0.8,
                "value.nulls.null_ratio": 0.3,
            },
            high_entropy_dimensions=["semantic.units.unit_declaration"],
        )
        col2 = ColumnSummary(
            column_id="c2",
            column_name="name",
            table_id="t1",
            table_name="orders",
            composite_score=0.2,
            readiness="ready",
            layer_scores={"structural": 0.1, "semantic": 0.2, "value": 0.1, "computational": 0.0},
            dimension_scores={
                "semantic.units.unit_declaration": 0.1,
                "value.nulls.null_ratio": 0.05,
            },
            high_entropy_dimensions=[],
        )

        table_summary = TableSummary(
            table_id="t1",
            table_name="orders",
            columns=[col1, col2],
            avg_composite_score=0.325,
            max_composite_score=0.45,
            avg_layer_scores={
                "structural": 0.1,
                "semantic": 0.4,
                "value": 0.3,
                "computational": 0.1,
            },
            readiness="investigate",
            high_entropy_columns=["amount"],
            blocked_columns=[],
        )

        result = TableInterpretationInput.from_summary(table_summary)

        assert result.table_name == "orders"
        assert result.avg_composite_score == 0.325
        assert result.max_composite_score == 0.45
        assert result.readiness == "investigate"
        assert result.column_count == 2
        assert result.high_entropy_columns == ["amount"]
        assert result.blocked_columns == []

        # Dimension score ranges should be computed from both columns
        assert "semantic.units.unit_declaration" in result.dimension_score_ranges
        unit_range = result.dimension_score_ranges["semantic.units.unit_declaration"]
        assert unit_range == (0.1, 0.8)

        null_range = result.dimension_score_ranges["value.nulls.null_ratio"]
        assert null_range == (0.05, 0.3)

    def test_from_summary_empty_columns(self):
        """Create TableInterpretationInput from summary with no columns."""
        table_summary = TableSummary(
            table_id="t1",
            table_name="empty_table",
            columns=[],
        )

        result = TableInterpretationInput.from_summary(table_summary)

        assert result.table_name == "empty_table"
        assert result.column_count == 0
        assert result.dimension_score_ranges == {}


class TestValidateOutput:
    """Tests for Pydantic model validation of LLM tool output."""

    def test_valid_with_columns_key(self):
        """Normal case: data has 'columns' wrapper."""
        data = {
            "columns": {
                "orders.amount": {
                    "explanation": "The amount column has moderate uncertainty.",
                    "assumptions": [],
                    "resolution_actions": [],
                }
            }
        }
        output = EntropyInterpretationOutput.model_validate(data)
        assert "orders.amount" in output.columns

    def test_rejects_missing_columns_key(self):
        """When 'columns' key is missing, validation should fail."""
        data = {
            "orders.amount": {
                "explanation": "The amount column has moderate uncertainty.",
                "assumptions": [],
                "resolution_actions": [],
            }
        }
        with pytest.raises(ValidationError):
            EntropyInterpretationOutput.model_validate(data)

    def test_rejects_missing_tables_key(self):
        """When 'tables' key is missing, validation should fail."""
        data = {
            "orders": {
                "explanation": "Table has moderate uncertainty.",
                "assumptions": [],
                "resolution_actions": [],
            }
        }
        with pytest.raises(ValidationError):
            TableEntropyInterpretationOutput.model_validate(data)
