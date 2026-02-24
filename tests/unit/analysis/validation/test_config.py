"""Tests for validation config loading."""


from dataraum.analysis.validation.config import (
    get_validation_spec,
    get_validation_specs_by_category,
    get_validation_specs_by_tags,
    load_all_validation_specs,
)


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
