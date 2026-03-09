"""Tests for the outlier exclusion fix handler."""

from __future__ import annotations

from typing import Any

from dataraum.entropy.detectors.value.outliers import OutlierRateDetector
from dataraum.pipeline.fix_registry import get_default_fix_registry
from dataraum.pipeline.fixes import FixInput


class TestOutlierDetectorFixableActions:
    def test_declares_transform_exclude_outliers(self) -> None:
        detector = OutlierRateDetector()
        assert "transform_exclude_outliers" in detector.fixable_actions


class TestFixRegistryOutlierHandler:
    def test_registry_has_handler(self) -> None:
        registry = get_default_fix_registry()
        entry = registry.find("transform_exclude_outliers")
        assert entry is not None

    def test_handler_is_callable(self) -> None:
        registry = get_default_fix_registry()
        entry = registry.find("transform_exclude_outliers")
        assert entry is not None
        assert callable(entry.handler)

    def test_handler_targets_statistical_quality(self) -> None:
        registry = get_default_fix_registry()
        entry = registry.find("transform_exclude_outliers")
        assert entry is not None
        assert entry.phase_name == "statistical_quality"


class TestHandleExcludeOutliers:
    def _get_handler(self):
        registry = get_default_fix_registry()
        entry = registry.find("transform_exclude_outliers")
        assert entry is not None
        return entry.handler

    def test_single_column(self) -> None:
        handler = self._get_handler()
        fix_input = FixInput(
            action_name="transform_exclude_outliers",
            affected_columns=["orders.amount"],
            interpretation="Exclude outliers for orders.amount",
        )
        result = handler(fix_input, {})

        assert result.requires_rerun == "statistical_quality"
        assert len(result.config_patches) == 1

        patch = result.config_patches[0]
        assert patch.config_path == "phases/statistical_quality.yaml"
        assert patch.operation == "append"
        assert patch.key_path == ["exclude_outlier_columns"]
        assert patch.value == "orders.amount"

    def test_multiple_columns(self) -> None:
        handler = self._get_handler()
        fix_input = FixInput(
            action_name="transform_exclude_outliers",
            affected_columns=["orders.amount", "orders.quantity"],
            interpretation="Exclude outliers for amount and quantity",
        )
        result = handler(fix_input, {})

        assert len(result.config_patches) == 2
        values = [p.value for p in result.config_patches]
        assert "orders.amount" in values
        assert "orders.quantity" in values

    def test_summary_lists_columns(self) -> None:
        handler = self._get_handler()
        fix_input = FixInput(
            action_name="transform_exclude_outliers",
            affected_columns=["orders.amount"],
        )
        result = handler(fix_input, {})
        assert "orders.amount" in result.summary

    def test_no_affected_columns(self) -> None:
        handler = self._get_handler()
        fix_input = FixInput(
            action_name="transform_exclude_outliers",
            affected_columns=[],
        )
        result = handler(fix_input, {})
        assert result.config_patches == []
        assert result.requires_rerun == "statistical_quality"

    def test_uses_interpretation_as_reason(self) -> None:
        handler = self._get_handler()
        fix_input = FixInput(
            action_name="transform_exclude_outliers",
            affected_columns=["orders.amount"],
            interpretation="User confirmed outliers are valid business data",
        )
        result = handler(fix_input, {})
        assert result.config_patches[0].reason == "User confirmed outliers are valid business data"

    def test_fallback_reason_when_no_interpretation(self) -> None:
        handler = self._get_handler()
        fix_input = FixInput(
            action_name="transform_exclude_outliers",
            affected_columns=["orders.amount"],
        )
        result = handler(fix_input, {})
        assert "orders.amount" in result.config_patches[0].reason

    def test_config_dict_is_ignored(self) -> None:
        """Handler doesn't read existing config — it only writes patches."""
        handler = self._get_handler()
        fix_input = FixInput(
            action_name="transform_exclude_outliers",
            affected_columns=["orders.amount"],
        )
        config: dict[str, Any] = {"exclude_outlier_columns": ["existing.col"]}
        result = handler(fix_input, config)

        # Still produces its own patch regardless of existing config
        assert len(result.config_patches) == 1
        assert result.config_patches[0].value == "orders.amount"
