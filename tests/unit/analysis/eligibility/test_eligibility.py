"""Tests for column eligibility logic."""

from dataraum.analysis.eligibility.config import (
    EligibilityConfig,
    EligibilityRule,
    EligibilityThresholds,
    load_eligibility_config,
)
from dataraum.analysis.eligibility.evaluator import (
    evaluate_condition,
    evaluate_rules,
    extract_metrics,
    format_reason,
    is_likely_key,
)


def _make_config(
    max_null_ratio: float = 1.0,
    warn_null_ratio: float = 0.5,
    eliminate_single_value: bool = True,
) -> EligibilityConfig:
    """Create a config with standard rules for testing."""
    return EligibilityConfig(
        version="1.0",
        thresholds=EligibilityThresholds(
            max_null_ratio=max_null_ratio,
            eliminate_single_value=eliminate_single_value,
            warn_null_ratio=warn_null_ratio,
        ),
        rules=[
            EligibilityRule(
                id="all_null",
                condition="null_ratio >= max_null_ratio",
                status="INELIGIBLE",
                reason="Column has {null_ratio:.0%} null values",
            ),
            EligibilityRule(
                id="single_value",
                condition="distinct_count == 1 and eliminate_single_value",
                status="WARN",
                reason="Column has single value - no variance for statistical analysis",
            ),
            EligibilityRule(
                id="high_null",
                condition="null_ratio > warn_null_ratio",
                status="WARN",
                reason="High null ratio ({null_ratio:.0%})",
            ),
        ],
        default_status="ELIGIBLE",
        key_patterns=["_id$", "^id$", "_key$"],
    )


class TestEvaluateCondition:
    """Tests for evaluate_condition."""

    def test_simple_comparison_true(self):
        assert evaluate_condition("null_ratio >= 1.0", {"null_ratio": 1.0})

    def test_simple_comparison_false(self):
        assert not evaluate_condition("null_ratio >= 1.0", {"null_ratio": 0.5})

    def test_and_condition(self):
        ctx = {"distinct_count": 1, "eliminate_single_value": True}
        assert evaluate_condition("distinct_count == 1 and eliminate_single_value", ctx)

    def test_and_condition_false(self):
        ctx = {"distinct_count": 1, "eliminate_single_value": False}
        assert not evaluate_condition("distinct_count == 1 and eliminate_single_value", ctx)

    def test_none_value_returns_false(self):
        assert not evaluate_condition("null_ratio >= 1.0", {"null_ratio": None})

    def test_longest_key_first_substitution(self):
        """max_null_ratio must not corrupt null_ratio."""
        ctx = {"null_ratio": 1.0, "max_null_ratio": 1.0}
        assert evaluate_condition("null_ratio >= max_null_ratio", ctx)


class TestEvaluateRules:
    """Tests for evaluate_rules."""

    def test_all_null_ineligible(self):
        config = _make_config()
        metrics = {
            "null_ratio": 1.0,
            "distinct_count": 0,
            "cardinality_ratio": 0.0,
            "total_count": 100,
        }
        status, rule_id, reason = evaluate_rules(config, metrics, "col")
        assert status == "INELIGIBLE"
        assert rule_id == "all_null"

    def test_single_value_warned(self):
        """Single-value columns get WARN (kept but flagged), not INELIGIBLE."""
        config = _make_config()
        metrics = {
            "null_ratio": 0.0,
            "distinct_count": 1,
            "cardinality_ratio": 0.01,
            "total_count": 100,
        }
        status, rule_id, reason = evaluate_rules(config, metrics, "col")
        assert status == "WARN"
        assert rule_id == "single_value"

    def test_high_null_warn(self):
        config = _make_config()
        metrics = {
            "null_ratio": 0.7,
            "distinct_count": 5,
            "cardinality_ratio": 0.05,
            "total_count": 100,
        }
        status, rule_id, reason = evaluate_rules(config, metrics, "col")
        assert status == "WARN"
        assert rule_id == "high_null"

    def test_eligible_when_no_rules_match(self):
        config = _make_config()
        metrics = {
            "null_ratio": 0.1,
            "distinct_count": 50,
            "cardinality_ratio": 0.5,
            "total_count": 100,
        }
        status, rule_id, reason = evaluate_rules(config, metrics, "col")
        assert status == "ELIGIBLE"
        assert rule_id is None

    def test_none_null_ratio_returns_eligible(self):
        config = _make_config()
        metrics = {
            "null_ratio": None,
            "distinct_count": 5,
            "cardinality_ratio": 0.5,
            "total_count": 100,
        }
        status, rule_id, reason = evaluate_rules(config, metrics, "col")
        assert status == "ELIGIBLE"

    def test_first_matching_rule_wins(self):
        """Rules evaluated in order, first match wins."""
        config = _make_config(max_null_ratio=0.5, warn_null_ratio=0.3)
        metrics = {
            "null_ratio": 0.6,
            "distinct_count": 5,
            "cardinality_ratio": 0.05,
            "total_count": 100,
        }
        status, rule_id, _ = evaluate_rules(config, metrics, "col")
        assert status == "INELIGIBLE"
        assert rule_id == "all_null"


class TestIsLikelyKey:
    """Tests for is_likely_key."""

    def test_matches_id_suffix(self):
        assert is_likely_key("customer_id", ["_id$", "^id$", "_key$"])

    def test_matches_exact_id(self):
        assert is_likely_key("id", ["_id$", "^id$", "_key$"])

    def test_matches_key_suffix(self):
        assert is_likely_key("order_key", ["_id$", "^id$", "_key$"])

    def test_no_match(self):
        assert not is_likely_key("amount", ["_id$", "^id$", "_key$"])

    def test_case_insensitive(self):
        assert is_likely_key("Customer_ID", ["_id$", "^id$", "_key$"])


class TestExtractMetrics:
    """Tests for extract_metrics."""

    def test_none_profile(self):
        metrics = extract_metrics(None)
        assert metrics["null_ratio"] is None
        assert metrics["distinct_count"] is None

    def test_with_profile(self):
        class FakeProfile:
            null_ratio = 0.1
            distinct_count = 50
            cardinality_ratio = 0.5
            total_count = 100

        metrics = extract_metrics(FakeProfile())
        assert metrics["null_ratio"] == 0.1
        assert metrics["distinct_count"] == 50
        assert metrics["total_count"] == 100


class TestFormatReason:
    """Tests for format_reason."""

    def test_formats_template(self):
        result = format_reason(
            "Column has {null_ratio:.0%} null values",
            {"null_ratio": 0.75},
        )
        assert result == "Column has 75% null values"

    def test_missing_key_returns_template(self):
        result = format_reason("Column has {missing_key}", {})
        assert result == "Column has {missing_key}"


class TestLoadEligibilityConfig:
    """Tests for config loading from YAML."""

    def test_loads_from_yaml(self):
        config = load_eligibility_config()
        assert config.version == "1.0"
        assert config.thresholds.max_null_ratio == 1.0
        assert config.thresholds.warn_null_ratio == 0.5
        assert config.thresholds.eliminate_single_value is True
        assert len(config.rules) >= 4
        assert config.default_status == "ELIGIBLE"

    def test_single_value_rule_is_warn_not_ineligible(self):
        """Production config should WARN on single-value columns, not drop them."""
        config = load_eligibility_config()
        single_value_rules = [r for r in config.rules if r.id == "single_value"]
        assert len(single_value_rules) == 1
        assert single_value_rules[0].status == "WARN"

    def test_from_dict_requires_version(self):
        """Missing required fields should raise KeyError."""
        import pytest

        with pytest.raises(KeyError):
            EligibilityConfig.from_dict({})

    def test_from_dict_requires_thresholds(self):
        import pytest

        with pytest.raises(KeyError):
            EligibilityConfig.from_dict(
                {"version": "1.0", "rules": [], "default_status": "ELIGIBLE"}
            )
