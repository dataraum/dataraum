"""Tests for entropy behavior configuration and SQL comment formatting."""

from __future__ import annotations

from dataraum_context.graphs.entropy_behavior import (
    BehaviorMode,
    CompoundRiskAction,
    CompoundRiskBehavior,
    DimensionBehavior,
    EntropyAction,
    EntropyBehaviorConfig,
    format_assumptions_for_response,
    format_entropy_sql_comments,
    get_default_config,
)


class TestEntropyBehaviorConfig:
    """Tests for EntropyBehaviorConfig."""

    def test_balanced_mode_defaults(self) -> None:
        """Balanced mode has expected defaults."""
        config = EntropyBehaviorConfig.balanced()

        assert config.mode == BehaviorMode.BALANCED
        assert config.clarification_threshold == 0.6
        assert config.refusal_threshold == 0.8
        assert config.auto_assume is True
        assert config.show_entropy_scores is False
        assert config.assumption_disclosure == "when_made"

    def test_strict_mode_defaults(self) -> None:
        """Strict mode has expected defaults."""
        config = EntropyBehaviorConfig.strict()

        assert config.mode == BehaviorMode.STRICT
        assert config.clarification_threshold == 0.3
        assert config.refusal_threshold == 0.6
        assert config.auto_assume is False
        assert config.show_entropy_scores is True
        assert config.assumption_disclosure == "always"

    def test_lenient_mode_defaults(self) -> None:
        """Lenient mode has expected defaults."""
        config = EntropyBehaviorConfig.lenient()

        assert config.mode == BehaviorMode.LENIENT
        assert config.clarification_threshold == 0.8
        assert config.refusal_threshold == 0.95
        assert config.auto_assume is True
        assert config.assumption_disclosure == "minimal"


class TestDetermineAction:
    """Tests for determine_action method."""

    def test_low_entropy_answers_confidently(self) -> None:
        """Low entropy (< 0.3) should answer confidently."""
        config = EntropyBehaviorConfig.balanced()

        action = config.determine_action(max_entropy=0.2)
        assert action == EntropyAction.ANSWER_CONFIDENTLY

    def test_medium_entropy_answers_with_assumptions(self) -> None:
        """Medium entropy (0.3-0.6) should answer with assumptions."""
        config = EntropyBehaviorConfig.balanced()

        action = config.determine_action(max_entropy=0.4)
        assert action == EntropyAction.ANSWER_WITH_ASSUMPTIONS

    def test_high_entropy_asks_or_caveats(self) -> None:
        """High entropy (0.6-0.8) should ask or caveat."""
        config = EntropyBehaviorConfig.balanced()

        action = config.determine_action(max_entropy=0.7)
        assert action == EntropyAction.ASK_OR_CAVEAT

    def test_critical_entropy_refuses(self) -> None:
        """Critical entropy (> 0.8) should refuse."""
        config = EntropyBehaviorConfig.balanced()

        action = config.determine_action(max_entropy=0.9)
        assert action == EntropyAction.REFUSE

    def test_critical_compound_risk_refuses(self) -> None:
        """Critical compound risk should refuse regardless of entropy."""
        config = EntropyBehaviorConfig.balanced()

        action = config.determine_action(
            max_entropy=0.3,  # Low entropy
            has_critical_compound_risk=True,
        )
        assert action == EntropyAction.REFUSE

    def test_high_compound_risk_asks(self) -> None:
        """High compound risk should ask/caveat."""
        config = EntropyBehaviorConfig.balanced()

        action = config.determine_action(
            max_entropy=0.3,
            has_high_compound_risk=True,
        )
        assert action == EntropyAction.ASK_OR_CAVEAT

    def test_strict_mode_asks_at_medium_entropy(self) -> None:
        """Strict mode should ask at medium entropy."""
        config = EntropyBehaviorConfig.strict()

        action = config.determine_action(max_entropy=0.4)
        assert action == EntropyAction.ASK_OR_CAVEAT

    def test_lenient_mode_answers_at_high_entropy(self) -> None:
        """Lenient mode should answer with assumptions at high entropy."""
        config = EntropyBehaviorConfig.lenient()

        action = config.determine_action(max_entropy=0.7)
        assert action == EntropyAction.ANSWER_WITH_ASSUMPTIONS


class TestDimensionThresholds:
    """Tests for dimension-specific thresholds."""

    def test_get_threshold_for_known_dimension(self) -> None:
        """Should return override threshold for configured dimension."""
        config = EntropyBehaviorConfig.balanced()
        config.dimension_overrides = [
            DimensionBehavior(
                dimension="semantic.units",
                clarification_threshold=0.4,
            )
        ]

        threshold = config.get_threshold_for_dimension("semantic.units")
        assert threshold == 0.4

    def test_get_threshold_for_unknown_dimension(self) -> None:
        """Should return default threshold for unconfigured dimension."""
        config = EntropyBehaviorConfig.balanced()

        threshold = config.get_threshold_for_dimension("some.other")
        assert threshold == config.clarification_threshold


class TestGetDefaultConfig:
    """Tests for get_default_config function."""

    def test_balanced_config(self) -> None:
        """Balanced mode should have dimension overrides."""
        config = get_default_config("balanced")

        assert config.mode == BehaviorMode.BALANCED
        assert len(config.dimension_overrides) > 0

    def test_strict_config(self) -> None:
        """Strict mode should have dimension overrides."""
        config = get_default_config("strict")

        assert config.mode == BehaviorMode.STRICT
        assert len(config.dimension_overrides) > 0

    def test_lenient_config(self) -> None:
        """Lenient mode should have dimension overrides."""
        config = get_default_config("lenient")

        assert config.mode == BehaviorMode.LENIENT
        assert len(config.dimension_overrides) > 0

    def test_default_dimension_overrides(self) -> None:
        """Default config should have currency and relations overrides."""
        config = get_default_config("balanced")

        dimensions = [d.dimension for d in config.dimension_overrides]
        assert "semantic.units" in dimensions
        assert "structural.relations" in dimensions


class TestFormatEntropySqlComments:
    """Tests for format_entropy_sql_comments function."""

    def test_low_entropy_comment(self) -> None:
        """Low entropy should show high confidence."""
        result = format_entropy_sql_comments(entropy_score=0.15)

        assert "high confidence" in result.lower()
        assert "0.15" in result

    def test_medium_entropy_comment(self) -> None:
        """Medium entropy should show assumptions."""
        result = format_entropy_sql_comments(entropy_score=0.45)

        assert "assumptions" in result.lower()
        assert "0.45" in result

    def test_high_entropy_comment(self) -> None:
        """High entropy should show warning."""
        result = format_entropy_sql_comments(entropy_score=0.72)

        assert "WARNING" in result
        assert "0.72" in result
        assert "VERIFY" in result

    def test_includes_assumptions(self) -> None:
        """Should include assumption comments."""
        result = format_entropy_sql_comments(
            entropy_score=0.5,
            assumptions=[
                {"dimension": "semantic.units", "assumption": "Currency is EUR"},
            ],
        )

        assert "ASSUMPTION" in result
        assert "semantic.units" in result
        assert "Currency is EUR" in result

    def test_includes_warnings(self) -> None:
        """Should include warning comments."""
        result = format_entropy_sql_comments(
            entropy_score=0.5,
            warnings=["Multiple join paths exist"],
        )

        assert "WARNING" in result
        assert "Multiple join paths" in result


class TestFormatAssumptionsForResponse:
    """Tests for format_assumptions_for_response function."""

    def test_empty_assumptions(self) -> None:
        """Empty assumptions returns empty string."""
        result = format_assumptions_for_response([])
        assert result == ""

    def test_minimal_disclosure(self) -> None:
        """Minimal mode shows only count."""
        result = format_assumptions_for_response(
            [{"assumption": "Test", "confidence": 0.8}],
            disclosure_mode="minimal",
        )

        assert "1 assumptions made" in result
        assert "Test" not in result

    def test_when_made_disclosure(self) -> None:
        """when_made mode shows assumptions without confidence."""
        result = format_assumptions_for_response(
            [{"assumption": "Currency is EUR", "confidence": 0.8}],
            disclosure_mode="when_made",
        )

        assert "Currency is EUR" in result
        assert "0.8" not in result  # No confidence shown
        assert "80%" not in result

    def test_always_disclosure(self) -> None:
        """always mode shows assumptions with confidence."""
        result = format_assumptions_for_response(
            [{"assumption": "Currency is EUR", "confidence": 0.8}],
            disclosure_mode="always",
        )

        assert "Currency is EUR" in result
        assert "80%" in result


class TestCompoundRiskBehavior:
    """Tests for CompoundRiskBehavior configuration."""

    def test_defaults(self) -> None:
        """Default compound risk behavior."""
        behavior = CompoundRiskBehavior()

        assert behavior.critical_action == CompoundRiskAction.REFUSE
        assert behavior.critical_explain is True
        assert behavior.high_action == CompoundRiskAction.WARN_STRONGLY
        assert behavior.high_require_confirmation is True
        assert behavior.medium_action == CompoundRiskAction.NOTE_IN_RESPONSE

    def test_custom_behavior(self) -> None:
        """Custom compound risk behavior."""
        behavior = CompoundRiskBehavior(
            critical_action=CompoundRiskAction.WARN_STRONGLY,
            high_action=CompoundRiskAction.NOTE_IN_RESPONSE,
        )

        assert behavior.critical_action == CompoundRiskAction.WARN_STRONGLY
        assert behavior.high_action == CompoundRiskAction.NOTE_IN_RESPONSE
