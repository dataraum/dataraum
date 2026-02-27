"""Tests for entropy interpretation module.

Tests the LLM-powered entropy interpretation feature including
models and prompt context building.
"""


import pytest
from pydantic import ValidationError

from dataraum.entropy.interpretation import (
    EntropyInterpretation,
    ResolutionActionOutput,
    Tier1ColumnOutput,
    Tier1InterpretationOutput,
    Tier1TableInterpretationOutput,
    Tier2ExplanationOutput,
    Tier2InterpretationsOutput,
)


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
            effort="low",
            expected_impact="Reduces semantic.units entropy",
        )
        assert output.parameters == {}

    def test_parameters_populated(self):
        """Parameters populated when provided."""
        output = ResolutionActionOutput(
            action="document_unit",
            description="Declare unit",
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
        )
        result = interp.to_dashboard_dict()
        assert result["column_key"] == "orders"
        assert result["column_name"] is None
        assert result["table_name"] == "orders"

    def test_dashboard_dict_has_no_composite_score_or_readiness(self):
        """Dashboard dict should not contain composite_score or readiness."""
        interp = EntropyInterpretation(
            column_name="amount",
            table_name="orders",
            assumptions=[],
            resolution_actions=[],
            explanation="Test",
        )
        result = interp.to_dashboard_dict()
        assert "composite_score" not in result
        assert "readiness" not in result


class TestValidateOutput:
    """Tests for Pydantic model validation of LLM tool output."""

    def test_valid_with_columns_key(self):
        """Normal case: data has 'columns' wrapper."""
        data = {
            "columns": {
                "orders.amount": {
                    "assumptions": [],
                    "resolution_actions": [],
                }
            }
        }
        output = Tier1InterpretationOutput.model_validate(data)
        assert "orders.amount" in output.columns

    def test_rejects_missing_columns_key(self):
        """When 'columns' key is missing, validation should fail."""
        data = {
            "orders.amount": {
                "assumptions": [],
                "resolution_actions": [],
            }
        }
        with pytest.raises(ValidationError):
            Tier1InterpretationOutput.model_validate(data)

    def test_rejects_missing_tables_key(self):
        """When 'tables' key is missing, validation should fail."""
        data = {
            "orders": {
                "assumptions": [],
                "resolution_actions": [],
            }
        }
        with pytest.raises(ValidationError):
            Tier1TableInterpretationOutput.model_validate(data)


class TestTier1Models:
    """Tests for Tier 1 (structured extraction) Pydantic models."""

    def test_tier1_column_output_has_no_explanation(self):
        """Tier1ColumnOutput should not have an explanation field."""
        assert "explanation" not in Tier1ColumnOutput.model_fields

    def test_tier1_column_output_accepts_valid(self):
        """Tier1ColumnOutput accepts assumptions and actions."""
        output = Tier1ColumnOutput(
            assumptions=[
                {
                    "dimension": "semantic.units",
                    "assumption_text": "Amount is in EUR",
                    "confidence": "high",
                    "impact": "Aggregation will be wrong if not EUR",
                }
            ],
            resolution_actions=[
                {
                    "action": "document_unit",
                    "description": "Declare unit",
                    "effort": "low",
                    "expected_impact": "Reduces semantic.units entropy",
                }
            ],
        )
        assert len(output.assumptions) == 1
        assert len(output.resolution_actions) == 1

    def test_tier1_column_output_defaults_empty(self):
        """Tier1ColumnOutput defaults to empty lists."""
        output = Tier1ColumnOutput()
        assert output.assumptions == []
        assert output.resolution_actions == []

    def test_tier1_interpretation_output_accepts_valid(self):
        """Tier1InterpretationOutput wraps columns dict."""
        data = {
            "columns": {
                "orders.amount": {"assumptions": [], "resolution_actions": []},
                "orders.date": {"assumptions": [], "resolution_actions": []},
            }
        }
        output = Tier1InterpretationOutput.model_validate(data)
        assert len(output.columns) == 2
        assert "orders.amount" in output.columns

    def test_tier1_interpretation_output_rejects_missing_columns(self):
        """Tier1InterpretationOutput rejects data without 'columns' key."""
        with pytest.raises(ValidationError):
            Tier1InterpretationOutput.model_validate(
                {"orders.amount": {"assumptions": [], "resolution_actions": []}}
            )

    def test_tier1_table_output_has_no_explanation(self):
        """Tier1TableOutput should not have an explanation field."""
        assert "explanation" not in Tier1ColumnOutput.model_fields

    def test_tier1_table_interpretation_output_accepts_valid(self):
        """Tier1TableInterpretationOutput wraps tables dict."""
        data = {
            "tables": {
                "orders": {"assumptions": [], "resolution_actions": []},
            }
        }
        output = Tier1TableInterpretationOutput.model_validate(data)
        assert "orders" in output.tables

    def test_tier1_table_interpretation_output_rejects_missing_tables(self):
        """Tier1TableInterpretationOutput rejects data without 'tables' key."""
        with pytest.raises(ValidationError):
            Tier1TableInterpretationOutput.model_validate(
                {"orders": {"assumptions": [], "resolution_actions": []}}
            )


class TestTier2Models:
    """Tests for Tier 2 (explanation synthesis) Pydantic models."""

    def test_tier2_explanation_output_requires_explanation(self):
        """Tier2ExplanationOutput requires an explanation string."""
        output = Tier2ExplanationOutput(explanation="Clear quality issue.")
        assert output.explanation == "Clear quality issue."

    def test_tier2_explanation_output_rejects_missing_explanation(self):
        """Tier2ExplanationOutput rejects missing explanation."""
        with pytest.raises(ValidationError):
            Tier2ExplanationOutput.model_validate({})

    def test_tier2_interpretations_output_accepts_valid(self):
        """Tier2InterpretationsOutput wraps items dict."""
        data = {
            "items": {
                "orders.amount": {"explanation": "Amount lacks unit documentation."},
                "orders": {"explanation": "Table has systemic unit issues."},
            }
        }
        output = Tier2InterpretationsOutput.model_validate(data)
        assert len(output.items) == 2
        assert "orders.amount" in output.items
        assert "orders" in output.items

    def test_tier2_interpretations_output_rejects_missing_items(self):
        """Tier2InterpretationsOutput rejects data without 'items' key."""
        with pytest.raises(ValidationError):
            Tier2InterpretationsOutput.model_validate(
                {"orders.amount": {"explanation": "test"}}
            )
