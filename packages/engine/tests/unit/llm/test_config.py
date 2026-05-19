"""Tests for LLM config model, especially extra fields."""

from dataraum.llm.config import FeatureConfig


class TestFeatureConfigExtra:
    """Verify that extra YAML keys are preserved on FeatureConfig."""

    def test_batch_size_preserved(self) -> None:
        cfg = FeatureConfig(enabled=True, batch_size=8)
        assert cfg.batch_size == 8  # type: ignore[attr-defined]

    def test_baseline_filter_preserved(self) -> None:
        cfg = FeatureConfig(
            enabled=True,
            baseline_filter={"enabled": True, "p_high_threshold": 0.40},
        )
        bf = cfg.baseline_filter  # type: ignore[attr-defined]
        assert bf["enabled"] is True
        assert bf["p_high_threshold"] == 0.40

    def test_unknown_extra_field(self) -> None:
        cfg = FeatureConfig(enabled=True, some_future_key="value")
        assert cfg.some_future_key == "value"  # type: ignore[attr-defined]

    def test_default_fields_unchanged(self) -> None:
        cfg = FeatureConfig()
        assert cfg.enabled is True
        assert cfg.model_tier == "balanced"
