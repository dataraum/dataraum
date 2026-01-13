"""Tests for quality formatting utilities."""

from dataraum_context.core.formatting import (
    THRESHOLDS,
    ThresholdConfig,
    format_list_with_overflow,
    generate_interpretation,
    generate_recommendation,
    map_to_severity,
    severity_emoji,
)


class TestMapToSeverity:
    """Tests for map_to_severity function."""

    def test_below_all_thresholds(self):
        """Value below lowest threshold returns that severity."""
        thresholds = {"none": 1.0, "moderate": 5.0, "high": 10.0}
        assert map_to_severity(0.5, thresholds) == "none"

    def test_at_threshold(self):
        """Value at exact threshold returns that severity."""
        thresholds = {"none": 1.0, "moderate": 5.0, "high": 10.0}
        assert map_to_severity(1.0, thresholds) == "none"
        assert map_to_severity(5.0, thresholds) == "moderate"

    def test_between_thresholds(self):
        """Value between thresholds returns appropriate severity."""
        thresholds = {"none": 1.0, "moderate": 5.0, "high": 10.0}
        assert map_to_severity(3.0, thresholds) == "moderate"
        assert map_to_severity(7.5, thresholds) == "high"

    def test_above_all_thresholds(self):
        """Value above all thresholds returns default severity."""
        thresholds = {"none": 1.0, "moderate": 5.0, "high": 10.0}
        assert map_to_severity(15.0, thresholds) == "severe"
        assert map_to_severity(15.0, thresholds, default="critical") == "critical"


class TestThresholdConfig:
    """Tests for ThresholdConfig class."""

    def test_ascending_thresholds(self):
        """Higher values are more severe by default."""
        config = ThresholdConfig(
            thresholds={"none": 1.0, "moderate": 5.0, "high": 10.0},
            default_severity="severe",
        )
        assert config.get_severity(0.5) == "none"
        assert config.get_severity(3.0) == "moderate"
        assert config.get_severity(7.5) == "high"
        assert config.get_severity(15.0) == "severe"

    def test_descending_thresholds(self):
        """Lower values can be more severe (e.g., completeness)."""
        config = ThresholdConfig(
            thresholds={"none": 0.99, "moderate": 0.8, "high": 0.5},
            default_severity="severe",
            ascending=False,
        )
        assert config.get_severity(1.0) == "none"
        assert config.get_severity(0.9) == "moderate"
        assert config.get_severity(0.6) == "high"
        assert config.get_severity(0.3) == "severe"


class TestCommonThresholds:
    """Tests for predefined threshold configurations."""

    def test_vif_thresholds(self):
        """VIF thresholds match research-based values."""
        assert THRESHOLDS.VIF.get_severity(1.0) == "none"
        assert THRESHOLDS.VIF.get_severity(2.0) == "low"
        assert THRESHOLDS.VIF.get_severity(4.0) == "moderate"
        assert THRESHOLDS.VIF.get_severity(7.0) == "high"
        assert THRESHOLDS.VIF.get_severity(15.0) == "severe"

    def test_condition_index_thresholds(self):
        """Condition Index thresholds match standard values."""
        assert THRESHOLDS.CONDITION_INDEX.get_severity(5.0) == "none"
        assert THRESHOLDS.CONDITION_INDEX.get_severity(20.0) == "moderate"
        assert THRESHOLDS.CONDITION_INDEX.get_severity(50.0) == "severe"

    def test_completeness_thresholds(self):
        """Completeness uses descending thresholds (lower = worse)."""
        assert THRESHOLDS.COMPLETENESS.get_severity(1.0) == "none"
        assert THRESHOLDS.COMPLETENESS.get_severity(0.97) == "low"
        assert THRESHOLDS.COMPLETENESS.get_severity(0.85) == "moderate"
        assert THRESHOLDS.COMPLETENESS.get_severity(0.6) == "high"
        assert THRESHOLDS.COMPLETENESS.get_severity(0.3) == "severe"


class TestSeverityEmoji:
    """Tests for severity_emoji function."""

    def test_no_emoji_for_none_severity(self):
        """'none' severity has no emoji."""
        assert severity_emoji("none") == ""

    def test_emoji_disabled(self):
        """Can disable emoji output."""
        assert severity_emoji("severe", include_emoji=False) == ""

    def test_case_insensitive(self):
        """Severity lookup is case-insensitive."""
        assert severity_emoji("NONE") == ""
        assert severity_emoji("None") == ""


class TestGenerateInterpretation:
    """Tests for generate_interpretation function."""

    def test_matching_template(self):
        """Uses matching template for severity."""
        templates = {
            "none": "No issues detected",
            "high": "{metric_name} is elevated at {value:.1f}",
        }
        result = generate_interpretation("high", templates, metric_name="VIF", value=7.5)
        assert result == "VIF is elevated at 7.5"

    def test_default_template(self):
        """Falls back to default when severity not found."""
        templates = {"none": "All good"}
        result = generate_interpretation(
            "unknown",
            templates,
            default_template="{metric_name}: {value}",
            metric_name="X",
            value=5,
        )
        assert result == "X: 5"

    def test_case_insensitive_lookup(self):
        """Template lookup is case-insensitive."""
        templates = {"high": "Elevated"}
        assert generate_interpretation("HIGH", templates) == "Elevated"


class TestGenerateRecommendation:
    """Tests for generate_recommendation function."""

    def test_matching_template(self):
        """Uses matching template for severity."""
        templates = {"severe": "Remove {column}"}
        result = generate_recommendation("severe", templates, column="price_usd")
        assert "Remove price_usd" in result

    def test_emoji_can_be_disabled(self):
        """Can disable emoji in recommendations."""
        templates = {"severe": "Remove {column}"}
        result = generate_recommendation(
            "severe", templates, include_emoji=False, column="price_usd"
        )
        assert result == "Remove price_usd"


class TestFormatListWithOverflow:
    """Tests for format_list_with_overflow function."""

    def test_empty_list(self):
        """Empty list returns empty string."""
        assert format_list_with_overflow([]) == ""

    def test_list_within_limit(self):
        """List within limit shows all items."""
        assert format_list_with_overflow(["a", "b", "c"], max_display=3) == "a, b, c"
        assert format_list_with_overflow(["a", "b"], max_display=3) == "a, b"

    def test_list_exceeds_limit(self):
        """List exceeding limit shows overflow."""
        result = format_list_with_overflow(["a", "b", "c", "d", "e"], max_display=3)
        assert result == "a, b, c, and 2 others"

    def test_custom_conjunction(self):
        """Can customize conjunction word."""
        result = format_list_with_overflow(["a", "b", "c", "d"], max_display=2, conjunction="plus")
        assert result == "a, b, plus 2 others"
