"""Tests for dimension label lookup in entropy config."""

from dataraum.entropy.config import get_dimension_label, get_entropy_config


class TestGetDimensionLabel:
    """Tests for get_dimension_label() function."""

    def test_exact_match(self):
        """Exact dimension path returns configured label."""
        label = get_dimension_label("structural.types")
        assert label == "Type Consistency"

    def test_prefix_match(self):
        """Two-segment prefix matches when exact path not in config."""
        # "semantic.units.unit_declaration" should match "semantic.units"
        label = get_dimension_label("semantic.units.unit_declaration")
        assert label == "Unit Documentation"

    def test_prefix_match_business_meaning(self):
        """Prefix match works for business_meaning sub-dimensions."""
        label = get_dimension_label("semantic.business_meaning.naming_clarity")
        assert label == "Business Documentation"

    def test_fallback_title_case(self):
        """Unknown dimension falls back to title-cased last segment."""
        label = get_dimension_label("unknown.layer.some_dimension")
        assert label == "Some Dimension"

    def test_fallback_single_segment(self):
        """Single-segment path falls back to title-cased segment."""
        label = get_dimension_label("orphan_dimension")
        assert label == "Orphan Dimension"

    def test_all_configured_labels(self):
        """All configured dimension labels are accessible."""
        config = get_entropy_config()
        for dimension_path, expected_label in config.dimension_labels.items():
            assert get_dimension_label(dimension_path) == expected_label

    def test_value_nulls(self):
        """value.nulls returns Data Completeness."""
        assert get_dimension_label("value.nulls") == "Data Completeness"

    def test_value_nulls_sub_dimension(self):
        """value.nulls.null_ratio returns Data Completeness via prefix match."""
        assert get_dimension_label("value.nulls.null_ratio") == "Data Completeness"

    def test_computational_derived(self):
        """computational.derived_values returns Computation Reliability."""
        assert get_dimension_label("computational.derived_values") == "Computation Reliability"
