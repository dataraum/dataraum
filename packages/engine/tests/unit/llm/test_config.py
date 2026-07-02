"""Tests for LLM config model, especially extra fields."""

import pytest
from pydantic import ValidationError

from dataraum.llm.config import FeatureConfig, LLMFeatures


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


class TestLLMFeaturesRegistration:
    """Every YAML feature key must be a DECLARED LLMFeatures field (DAT-603).

    Pydantic's default silently drops unknown keys — that made ``sql_repair``
    (present in llm/config.yaml, never declared here) parse to None and disabled
    the repair path without a trace. Unknown keys must fail loud instead.
    """

    def test_yaml_only_feature_fails_loud(self) -> None:
        with pytest.raises(ValidationError, match="not_a_registered_feature"):
            LLMFeatures(
                semantic_analysis=FeatureConfig(),
                not_a_registered_feature=FeatureConfig(),  # type: ignore[call-arg]
            )

    def test_sql_repair_and_graph_sql_generation_are_declared(self) -> None:
        features = LLMFeatures(
            semantic_analysis=FeatureConfig(),
            sql_repair=FeatureConfig(model_tier="fast", max_repair_attempts=2),
            graph_sql_generation=FeatureConfig(model_tier="balanced", effort="low"),
        )
        assert features.sql_repair is not None
        assert features.sql_repair.enabled is True
        assert features.graph_sql_generation is not None
        assert features.graph_sql_generation.effort == "low"
