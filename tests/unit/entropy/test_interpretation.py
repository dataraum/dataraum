"""Tests for entropy interpretation module.

Tests the LLM-powered entropy interpretation feature including
models and prompt context building.
"""

import pytest
from pydantic import ValidationError

from dataraum.entropy.interpretation import (
    EntropyInterpretationOutput,
    ResolutionActionOutput,
    TableEntropyInterpretationOutput,
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
