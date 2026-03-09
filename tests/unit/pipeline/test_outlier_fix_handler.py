"""Tests for the accept-finding fix handler (replaces outlier exclusion)."""

from __future__ import annotations

from typing import Any

from dataraum.entropy.detectors.value.outliers import OutlierRateDetector
from dataraum.pipeline.fix_registry import get_default_fix_registry
from dataraum.pipeline.fixes import FixInput


class TestOutlierDetectorFixableActions:
    def test_declares_accept_finding(self) -> None:
        detector = OutlierRateDetector()
        assert "accept_finding" in detector.fixable_actions


class TestFixRegistryAcceptHandler:
    def test_registry_has_handler(self) -> None:
        registry = get_default_fix_registry()
        entry = registry.find("accept_finding")
        assert entry is not None

    def test_handler_is_callable(self) -> None:
        registry = get_default_fix_registry()
        entry = registry.find("accept_finding")
        assert entry is not None
        assert callable(entry.handler)

    def test_handler_targets_quality_review(self) -> None:
        registry = get_default_fix_registry()
        entry = registry.find("accept_finding")
        assert entry is not None
        assert entry.phase_name == "quality_review"


class TestHandleAcceptFinding:
    def _get_handler(self):
        registry = get_default_fix_registry()
        entry = registry.find("accept_finding")
        assert entry is not None
        return entry.handler

    def test_single_column(self) -> None:
        handler = self._get_handler()
        fix_input = FixInput(
            action_name="accept_finding",
            affected_columns=["orders.amount"],
            parameters={"detector_id": "outlier_rate"},
            interpretation="Outliers are expected for this column",
        )
        result = handler(fix_input, {})

        assert result.requires_rerun == ""
        assert len(result.config_patches) == 1

        patch = result.config_patches[0]
        assert patch.config_path == "entropy/thresholds.yaml"
        assert patch.operation == "append"
        assert patch.key_path == ["detectors", "outlier_rate", "accepted_columns"]
        assert patch.value == "orders.amount"

    def test_multiple_columns(self) -> None:
        handler = self._get_handler()
        fix_input = FixInput(
            action_name="accept_finding",
            affected_columns=["orders.amount", "orders.quantity"],
            parameters={"detector_id": "outlier_rate"},
            interpretation="Outliers expected",
        )
        result = handler(fix_input, {})

        assert len(result.config_patches) == 2
        values = [p.value for p in result.config_patches]
        assert "orders.amount" in values
        assert "orders.quantity" in values

    def test_summary_includes_detector_and_columns(self) -> None:
        handler = self._get_handler()
        fix_input = FixInput(
            action_name="accept_finding",
            affected_columns=["orders.amount"],
            parameters={"detector_id": "benford"},
        )
        result = handler(fix_input, {})
        assert "benford" in result.summary
        assert "orders.amount" in result.summary

    def test_no_affected_columns(self) -> None:
        handler = self._get_handler()
        fix_input = FixInput(
            action_name="accept_finding",
            affected_columns=[],
            parameters={"detector_id": "outlier_rate"},
        )
        result = handler(fix_input, {})
        assert result.config_patches == []
        assert result.requires_rerun == ""

    def test_uses_interpretation_as_reason(self) -> None:
        handler = self._get_handler()
        fix_input = FixInput(
            action_name="accept_finding",
            affected_columns=["orders.amount"],
            parameters={"detector_id": "outlier_rate"},
            interpretation="User confirmed outliers are valid business data",
        )
        result = handler(fix_input, {})
        assert result.config_patches[0].reason == "User confirmed outliers are valid business data"

    def test_fallback_reason_when_no_interpretation(self) -> None:
        handler = self._get_handler()
        fix_input = FixInput(
            action_name="accept_finding",
            affected_columns=["orders.amount"],
            parameters={"detector_id": "outlier_rate"},
        )
        result = handler(fix_input, {})
        assert "outlier_rate" in result.config_patches[0].reason
        assert "orders.amount" in result.config_patches[0].reason

    def test_config_dict_is_ignored(self) -> None:
        """Handler doesn't read existing config — it only writes patches."""
        handler = self._get_handler()
        fix_input = FixInput(
            action_name="accept_finding",
            affected_columns=["orders.amount"],
            parameters={"detector_id": "outlier_rate"},
        )
        config: dict[str, Any] = {"accepted_columns": ["existing.col"]}
        result = handler(fix_input, config)

        assert len(result.config_patches) == 1
        assert result.config_patches[0].value == "orders.amount"
