"""Tests for validation config loading."""

from pathlib import Path

import yaml

from dataraum.analysis.validation.config import (
    clear_cache,
    get_validation_spec,
    get_validation_specs_by_category,
    get_validation_specs_by_tags,
    load_all_validation_specs,
    load_validation_spec,
)
from dataraum.analysis.validation.models import ValidationSeverity


class TestLoadValidationSpec:
    """Tests for loading individual validation specs."""

    def test_load_valid_spec(self, tmp_path: Path):
        """Test loading a valid validation spec from YAML."""
        spec_content = {
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

        yaml_file = tmp_path / "test_check.yaml"
        with open(yaml_file, "w") as f:
            yaml.dump(spec_content, f)

        spec = load_validation_spec(yaml_file)

        assert spec is not None
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

    def test_load_minimal_spec(self, tmp_path: Path):
        """Test loading a minimal spec with only required fields."""
        spec_content = {
            "validation_id": "minimal_check",
            "name": "Minimal",
            "description": "Minimal check",
            "category": "general",
        }

        yaml_file = tmp_path / "minimal.yaml"
        with open(yaml_file, "w") as f:
            yaml.dump(spec_content, f)

        spec = load_validation_spec(yaml_file)

        assert spec is not None
        assert spec.validation_id == "minimal_check"
        assert spec.severity == ValidationSeverity.ERROR  # Default
        assert spec.check_type == "custom"  # Default
        assert spec.parameters == {}
        assert spec.tags == []

    def test_load_spec_uses_filename_as_fallback_id(self, tmp_path: Path):
        """Test that filename is used as validation_id if not specified."""
        spec_content = {
            "name": "My Check",
            "description": "A check",
            "category": "test",
        }

        yaml_file = tmp_path / "my_fallback_id.yaml"
        with open(yaml_file, "w") as f:
            yaml.dump(spec_content, f)

        spec = load_validation_spec(yaml_file)

        assert spec is not None
        assert spec.validation_id == "my_fallback_id"

    def test_load_spec_handles_invalid_severity(self, tmp_path: Path):
        """Test that invalid severity falls back to ERROR."""
        spec_content = {
            "validation_id": "test",
            "name": "Test",
            "description": "Test",
            "category": "test",
            "severity": "invalid_severity",
        }

        yaml_file = tmp_path / "test.yaml"
        with open(yaml_file, "w") as f:
            yaml.dump(spec_content, f)

        spec = load_validation_spec(yaml_file)

        assert spec is not None
        assert spec.severity == ValidationSeverity.ERROR

    def test_load_spec_returns_none_for_empty_file(self, tmp_path: Path):
        """Test that empty YAML files return None."""
        yaml_file = tmp_path / "empty.yaml"
        yaml_file.write_text("")

        spec = load_validation_spec(yaml_file)
        assert spec is None

    def test_load_spec_returns_none_for_invalid_yaml(self, tmp_path: Path):
        """Test that invalid YAML returns None."""
        yaml_file = tmp_path / "invalid.yaml"
        yaml_file.write_text("this: is: not: valid: yaml: [")

        spec = load_validation_spec(yaml_file)
        assert spec is None


class TestLoadAllValidationSpecs:
    """Tests for loading all validation specs."""

    def test_loads_specs_from_actual_config(self):
        """Test that actual config files can be loaded."""
        clear_cache()  # Ensure fresh load
        specs = load_all_validation_specs()

        # We should have at least the financial validation specs
        assert len(specs) >= 4

        # Check for expected specs
        assert "double_entry_balance" in specs
        assert "trial_balance" in specs
        assert "sign_conventions" in specs

    def test_specs_are_cached(self):
        """Test that specs are cached after first load."""
        clear_cache()

        # First load
        specs1 = load_all_validation_specs()

        # Second load should return same object (cached)
        specs2 = load_all_validation_specs()

        assert specs1 is specs2

    def test_cache_can_be_cleared(self):
        """Test that cache can be cleared."""
        clear_cache()
        specs1 = load_all_validation_specs()

        clear_cache()
        specs2 = load_all_validation_specs()

        # After clear, should be different objects
        assert specs1 is not specs2
        # But same content
        assert specs1.keys() == specs2.keys()


class TestGetValidationSpecsByCategory:
    """Tests for filtering specs by category."""

    def test_get_financial_specs(self):
        """Test getting financial category specs."""
        clear_cache()
        specs = get_validation_specs_by_category("financial")

        assert len(specs) >= 4
        for spec in specs:
            assert spec.category == "financial"

    def test_get_nonexistent_category(self):
        """Test that nonexistent category returns empty list."""
        clear_cache()
        specs = get_validation_specs_by_category("nonexistent_category")

        assert specs == []


class TestGetValidationSpecsByTags:
    """Tests for filtering specs by tags."""

    def test_get_specs_by_single_tag(self):
        """Test filtering by a single tag."""
        clear_cache()
        specs = get_validation_specs_by_tags(["accounting"])

        assert len(specs) >= 1
        for spec in specs:
            assert "accounting" in spec.tags

    def test_get_specs_by_multiple_tags(self):
        """Test filtering by multiple tags (OR logic)."""
        clear_cache()
        specs = get_validation_specs_by_tags(["accounting", "data-quality"])

        # Should return specs that have either tag
        for spec in specs:
            assert len(set(spec.tags) & {"accounting", "data-quality"}) > 0


class TestGetValidationSpec:
    """Tests for getting a specific spec by ID."""

    def test_get_existing_spec(self):
        """Test getting an existing spec by ID."""
        clear_cache()
        spec = get_validation_spec("double_entry_balance")

        assert spec is not None
        assert spec.validation_id == "double_entry_balance"
        assert spec.name == "Double Entry Balance"

    def test_get_nonexistent_spec(self):
        """Test that nonexistent ID returns None."""
        clear_cache()
        spec = get_validation_spec("nonexistent_spec_id")

        assert spec is None
