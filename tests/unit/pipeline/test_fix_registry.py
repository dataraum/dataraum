"""Tests for FixRegistry validation."""

from __future__ import annotations

from unittest.mock import patch

from dataraum.entropy.dimensions import Dimension, Layer, SubDimension
from dataraum.pipeline.fix_registry import FixHandler, FixRegistry
from dataraum.pipeline.fixes import FixResult


def _noop_handler(fix_input, config):
    return FixResult(config_patches=[], requires_rerun="", summary="noop")


class TestFixRegistryValidation:
    """Tests for FixRegistry.validate() cross-registry checks."""

    def test_valid_registry_no_warnings(self):
        """No warnings when all actions have handlers and phases exist."""
        registry = FixRegistry()
        registry.register(
            FixHandler(
                action="transform_exclude_outliers",
                handler=_noop_handler,
                phase_name="statistical_quality",
            )
        )

        # Mock detector that declares the same action
        from dataraum.entropy.detectors.base import DetectorRegistry, EntropyDetector

        class StubDetector(EntropyDetector):
            detector_id = "stub"
            layer = Layer.VALUE
            dimension = Dimension.OUTLIERS
            sub_dimension = SubDimension.OUTLIER_RATE
            scope = "column"
            required_analyses = []

            @property
            def fixable_actions(self):
                return {"transform_exclude_outliers"}

            def detect(self, ctx):
                return []

        detector_reg = DetectorRegistry()
        detector_reg.register(StubDetector())

        # Phase registry with the target phase
        phase_reg = {"statistical_quality": object}

        with (
            patch(
                "dataraum.entropy.detectors.base.get_default_registry",
                return_value=detector_reg,
            ),
            patch(
                "dataraum.pipeline.registry.get_registry",
                return_value=phase_reg,
            ),
        ):
            warnings = registry.validate()

        assert warnings == []

    def test_warns_on_missing_handler(self):
        """Warning when detector declares action with no registered handler."""
        registry = FixRegistry()  # empty — no handlers

        from dataraum.entropy.detectors.base import DetectorRegistry, EntropyDetector

        class StubDetector(EntropyDetector):
            detector_id = "stub"
            layer = Layer.VALUE
            dimension = Dimension.OUTLIERS
            sub_dimension = SubDimension.OUTLIER_RATE
            scope = "column"
            required_analyses = []

            @property
            def fixable_actions(self):
                return {"missing_action"}

            def detect(self, ctx):
                return []

        detector_reg = DetectorRegistry()
        detector_reg.register(StubDetector())

        with (
            patch(
                "dataraum.entropy.detectors.base.get_default_registry",
                return_value=detector_reg,
            ),
            patch(
                "dataraum.pipeline.registry.get_registry",
                return_value={},
            ),
        ):
            warnings = registry.validate()

        assert len(warnings) == 1
        assert "missing_action" in warnings[0]
        assert "stub" in warnings[0]

    def test_warns_on_invalid_phase_name(self):
        """Warning when handler targets a non-existent phase."""
        registry = FixRegistry()
        registry.register(
            FixHandler(
                action="some_action",
                handler=_noop_handler,
                phase_name="nonexistent_phase",
            )
        )

        from dataraum.entropy.detectors.base import DetectorRegistry

        detector_reg = DetectorRegistry()  # empty — no detectors

        with (
            patch(
                "dataraum.entropy.detectors.base.get_default_registry",
                return_value=detector_reg,
            ),
            patch(
                "dataraum.pipeline.registry.get_registry",
                return_value={"typing": object},
            ),
        ):
            warnings = registry.validate()

        assert len(warnings) == 1
        assert "nonexistent_phase" in warnings[0]
        assert "some_action" in warnings[0]

    def test_multiple_warnings(self):
        """Multiple issues produce multiple warnings."""
        registry = FixRegistry()
        registry.register(
            FixHandler(
                action="action_a",
                handler=_noop_handler,
                phase_name="bad_phase",
            )
        )

        from dataraum.entropy.detectors.base import DetectorRegistry, EntropyDetector

        class StubDetector(EntropyDetector):
            detector_id = "stub"
            layer = Layer.VALUE
            dimension = Dimension.OUTLIERS
            sub_dimension = SubDimension.OUTLIER_RATE
            scope = "column"
            required_analyses = []

            @property
            def fixable_actions(self):
                return {"unregistered_action"}

            def detect(self, ctx):
                return []

        detector_reg = DetectorRegistry()
        detector_reg.register(StubDetector())

        with (
            patch(
                "dataraum.entropy.detectors.base.get_default_registry",
                return_value=detector_reg,
            ),
            patch(
                "dataraum.pipeline.registry.get_registry",
                return_value={},
            ),
        ):
            warnings = registry.validate()

        # One for missing handler, one for invalid phase
        assert len(warnings) == 2


class TestDefaultRegistryValidation:
    """The default registry should pass validation cleanly."""

    def test_builtin_registry_validates(self):
        """Built-in handlers + detectors are consistent."""
        import dataraum.pipeline.fix_registry as mod
        from dataraum.pipeline.fix_registry import get_default_fix_registry

        old = mod._default_registry
        mod._default_registry = None
        try:
            registry = get_default_fix_registry()
            warnings = registry.validate()
            assert warnings == [], f"Default registry has validation warnings: {warnings}"
        finally:
            mod._default_registry = old
