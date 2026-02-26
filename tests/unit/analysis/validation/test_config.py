"""Tests for validation config loading."""

import pytest

from dataraum.analysis.validation.config import (
    get_validation_spec,
    get_validation_specs_by_category,
    get_validation_specs_by_tags,
    get_validation_specs_for_cycles,
    load_all_validation_specs,
)

VERTICAL = "finance"


class TestLoadAllValidationSpecs:
    """Tests for loading all validation specs."""

    def test_loads_specs_from_actual_config(self):
        """Test that actual config files can be loaded."""
        specs = load_all_validation_specs(VERTICAL)

        # We should have at least the financial validation specs
        assert len(specs) >= 4

        # Check for expected specs
        assert "double_entry_balance" in specs
        assert "trial_balance" in specs
        assert "sign_conventions" in specs

    def test_each_load_returns_fresh_dict(self):
        """Test that each call returns a new dict (no caching)."""
        specs1 = load_all_validation_specs(VERTICAL)
        specs2 = load_all_validation_specs(VERTICAL)

        assert specs1 is not specs2
        assert specs1.keys() == specs2.keys()

    def test_nonexistent_vertical_raises(self):
        """Test that a nonexistent vertical raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match="not found"):
            load_all_validation_specs("nonexistent_vertical_xyz")


class TestGetValidationSpecsByCategory:
    """Tests for filtering specs by category."""

    def test_get_financial_specs(self):
        """Test getting financial category specs."""
        specs = get_validation_specs_by_category("financial", VERTICAL)

        assert len(specs) >= 4
        for spec in specs:
            assert spec.category == "financial"

    def test_get_nonexistent_category(self):
        """Test that nonexistent category returns empty list."""
        specs = get_validation_specs_by_category("nonexistent_category", VERTICAL)

        assert specs == []


class TestGetValidationSpecsByTags:
    """Tests for filtering specs by tags."""

    def test_get_specs_by_single_tag(self):
        """Test filtering by a single tag."""
        specs = get_validation_specs_by_tags(["accounting"], VERTICAL)

        assert len(specs) >= 1
        for spec in specs:
            assert "accounting" in spec.tags

    def test_get_specs_by_multiple_tags(self):
        """Test filtering by multiple tags (OR logic)."""
        specs = get_validation_specs_by_tags(["accounting", "data-quality"], VERTICAL)

        # Should return specs that have either tag
        for spec in specs:
            assert len(set(spec.tags) & {"accounting", "data-quality"}) > 0


class TestGetValidationSpec:
    """Tests for getting a specific spec by ID."""

    def test_get_existing_spec(self):
        """Test getting an existing spec by ID."""
        spec = get_validation_spec("double_entry_balance", VERTICAL)

        assert spec is not None
        assert spec.validation_id == "double_entry_balance"
        assert spec.name == "Double Entry Balance"

    def test_get_nonexistent_spec(self):
        """Test that nonexistent ID returns None."""
        spec = get_validation_spec("nonexistent_spec_id", VERTICAL)

        assert spec is None


# IDs of universal specs (relevant_cycles = [])
UNIVERSAL_IDS = {"fiscal_period_integrity", "stage_date_ordering", "orphan_transactions"}


class TestGetValidationSpecsForCycles:
    """Tests for filtering specs by detected cycle types."""

    def test_returns_gl_specs_for_journal_entry_cycle(self):
        """journal_entry_cycle should include double_entry, trial_balance, sign_conventions + universals."""
        specs = get_validation_specs_for_cycles(["journal_entry_cycle"], VERTICAL)
        ids = {s.validation_id for s in specs}

        assert "double_entry_balance" in ids
        assert "trial_balance" in ids
        assert "sign_conventions" in ids
        assert UNIVERSAL_IDS <= ids

    def test_returns_p2p_specs_for_procure_to_pay(self):
        """procure_to_pay should include three_way_match + universals."""
        specs = get_validation_specs_for_cycles(["procure_to_pay"], VERTICAL)
        ids = {s.validation_id for s in specs}

        assert "three_way_match" in ids
        assert UNIVERSAL_IDS <= ids
        # GL-specific specs should not appear
        assert "double_entry_balance" not in ids
        assert "sign_conventions" not in ids

    def test_universal_specs_always_included(self):
        """Universal specs appear regardless of cycle type."""
        specs = get_validation_specs_for_cycles(["some_unknown_cycle"], VERTICAL)
        ids = {s.validation_id for s in specs}

        assert UNIVERSAL_IDS <= ids

    def test_empty_cycle_list_returns_only_universal(self):
        """No cycle types → only universal specs (empty relevant_cycles)."""
        specs = get_validation_specs_for_cycles([], VERTICAL)
        ids = {s.validation_id for s in specs}

        assert ids == UNIVERSAL_IDS
