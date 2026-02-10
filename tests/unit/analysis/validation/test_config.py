"""Tests for validation config loading."""

import pytest

from dataraum.analysis.validation.config import (
    get_validation_spec,
    get_validation_specs_by_category,
    get_validation_specs_by_tags,
    load_all_validation_specs,
)
from dataraum.analysis.validation.models import ValidationSeverity, ValidationSpec


class TestValidationSpecModel:
    """Tests for ValidationSpec Pydantic model."""

    def test_valid_spec(self):
        """Test validating a complete spec."""
        data = {
            "validation_id": "test_check",
            "name": "Test Check",
            "description": "A test validation check",
            "category": "data_quality",
            "severity": "warning",
            "check_type": "constraint",
            "parameters": {"threshold": 0.05},
            "sql_hints": "Check for null values",
            "expected_outcome": "No nulls expected",
            "tags": ["test", "quality"],
            "version": "1.0",
        }

        spec = ValidationSpec.model_validate(data)

        assert spec.validation_id == "test_check"
        assert spec.name == "Test Check"
        assert spec.description == "A test validation check"
        assert spec.category == "data_quality"
        assert spec.severity == ValidationSeverity.WARNING
        assert spec.check_type == "constraint"
        assert spec.parameters == {"threshold": 0.05}
        assert spec.sql_hints == "Check for null values"
        assert spec.expected_outcome == "No nulls expected"
        assert spec.tags == ["test", "quality"]
        assert spec.version == "1.0"
        assert spec.source == "config"

    def test_minimal_spec(self):
        """Test validating a minimal spec with only required fields."""
        data = {
            "validation_id": "minimal_check",
            "name": "Minimal",
            "description": "Minimal check",
            "category": "general",
            "check_type": "custom",
        }

        spec = ValidationSpec.model_validate(data)

        assert spec.validation_id == "minimal_check"
        assert spec.severity == ValidationSeverity.ERROR  # Default
        assert spec.check_type == "custom"
        assert spec.parameters == {}
        assert spec.tags == []

    def test_missing_required_field_raises(self):
        """Test that missing required fields raise validation error."""
        from pydantic import ValidationError

        data = {
            "validation_id": "test",
            # Missing: name, description, category, check_type
        }

        with pytest.raises(ValidationError):
            ValidationSpec.model_validate(data)


class TestLoadAllValidationSpecs:
    """Tests for loading all validation specs."""

    def test_loads_specs_from_actual_config(self):
        """Test that actual config files can be loaded."""
        specs = load_all_validation_specs()

        # We should have at least the financial validation specs
        assert len(specs) >= 4

        # Check for expected specs
        assert "double_entry_balance" in specs
        assert "trial_balance" in specs
        assert "sign_conventions" in specs

    def test_each_load_returns_fresh_dict(self):
        """Test that each call returns a new dict (no caching)."""
        specs1 = load_all_validation_specs()
        specs2 = load_all_validation_specs()

        assert specs1 is not specs2
        assert specs1.keys() == specs2.keys()


class TestGetValidationSpecsByCategory:
    """Tests for filtering specs by category."""

    def test_get_financial_specs(self):
        """Test getting financial category specs."""
        specs = get_validation_specs_by_category("financial")

        assert len(specs) >= 4
        for spec in specs:
            assert spec.category == "financial"

    def test_get_nonexistent_category(self):
        """Test that nonexistent category returns empty list."""
        specs = get_validation_specs_by_category("nonexistent_category")

        assert specs == []


class TestGetValidationSpecsByTags:
    """Tests for filtering specs by tags."""

    def test_get_specs_by_single_tag(self):
        """Test filtering by a single tag."""
        specs = get_validation_specs_by_tags(["accounting"])

        assert len(specs) >= 1
        for spec in specs:
            assert "accounting" in spec.tags

    def test_get_specs_by_multiple_tags(self):
        """Test filtering by multiple tags (OR logic)."""
        specs = get_validation_specs_by_tags(["accounting", "data-quality"])

        # Should return specs that have either tag
        for spec in specs:
            assert len(set(spec.tags) & {"accounting", "data-quality"}) > 0


class TestGetValidationSpec:
    """Tests for getting a specific spec by ID."""

    def test_get_existing_spec(self):
        """Test getting an existing spec by ID."""
        spec = get_validation_spec("double_entry_balance")

        assert spec is not None
        assert spec.validation_id == "double_entry_balance"
        assert spec.name == "Double Entry Balance"

    def test_get_nonexistent_spec(self):
        """Test that nonexistent ID returns None."""
        spec = get_validation_spec("nonexistent_spec_id")

        assert spec is None
