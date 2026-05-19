"""Tests for entropy behavior configuration and SQL comment formatting."""

from __future__ import annotations

from dataraum.graphs.entropy_behavior import (
    BehaviorMode,
    EntropyBehaviorConfig,
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
