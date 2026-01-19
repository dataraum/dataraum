"""Tests for entropy detector infrastructure."""

import pytest

from dataraum_context.entropy.detectors import (
    DetectorRegistry,
    EntropyDetector,
    get_default_registry,
)
from dataraum_context.entropy.detectors.base import DetectorContext
from dataraum_context.entropy.models import EntropyObject


class MockDetector(EntropyDetector):
    """Mock detector for testing."""

    detector_id = "mock_detector"
    layer = "structural"
    dimension = "types"
    sub_dimension = "mock_sub"
    required_analyses = ["typing"]
    description = "Mock detector for testing"

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Return a mock entropy object."""
        typing_result = context.get_analysis("typing", {})
        parse_rate = typing_result.get("parse_success_rate", 1.0)
        score = 1.0 - parse_rate

        return [
            self.create_entropy_object(
                context=context,
                score=score,
                evidence=[{"parse_success_rate": parse_rate}],
            )
        ]


class TestDetectorContext:
    """Tests for DetectorContext."""

    def test_target_ref_column(self, sample_detector_context: DetectorContext):
        """Test target reference for column."""
        assert sample_detector_context.target_ref == "column:orders.amount"

    def test_target_ref_table_only(self):
        """Test target reference when only table is specified."""
        context = DetectorContext(table_name="orders")
        assert context.target_ref == "table:orders"

    def test_target_ref_unknown(self):
        """Test target reference when nothing is specified."""
        context = DetectorContext()
        assert context.target_ref == "unknown"

    def test_get_analysis(self, sample_detector_context: DetectorContext):
        """Test getting analysis results."""
        typing = sample_detector_context.get_analysis("typing")
        assert typing is not None
        assert typing["detected_type"] == "DECIMAL"

    def test_get_analysis_missing(self, sample_detector_context: DetectorContext):
        """Test getting missing analysis returns default."""
        result = sample_detector_context.get_analysis("nonexistent", {"default": True})
        assert result == {"default": True}


class TestDetectorRegistry:
    """Tests for DetectorRegistry."""

    def test_register_detector(self, empty_registry: DetectorRegistry):
        """Test registering a detector."""
        detector = MockDetector()
        empty_registry.register(detector)

        assert "mock_detector" in empty_registry.get_detector_ids()
        assert empty_registry.get_detector("mock_detector") is detector

    def test_unregister_detector(self, empty_registry: DetectorRegistry):
        """Test unregistering a detector."""
        detector = MockDetector()
        empty_registry.register(detector)
        empty_registry.unregister("mock_detector")

        assert "mock_detector" not in empty_registry.get_detector_ids()

    def test_get_detectors_for_layer(self, empty_registry: DetectorRegistry):
        """Test getting detectors by layer."""
        detector = MockDetector()
        empty_registry.register(detector)

        structural_detectors = empty_registry.get_detectors_for_layer("structural")
        assert len(structural_detectors) == 1
        assert structural_detectors[0].detector_id == "mock_detector"

        semantic_detectors = empty_registry.get_detectors_for_layer("semantic")
        assert len(semantic_detectors) == 0

    def test_get_runnable_detectors(
        self,
        empty_registry: DetectorRegistry,
        sample_detector_context: DetectorContext,
    ):
        """Test getting runnable detectors based on available analyses."""
        detector = MockDetector()
        empty_registry.register(detector)

        # Context has 'typing' analysis, so detector should be runnable
        runnable = empty_registry.get_runnable_detectors(sample_detector_context)
        assert len(runnable) == 1

        # Context without typing analysis
        empty_context = DetectorContext(table_name="test", column_name="col")
        runnable = empty_registry.get_runnable_detectors(empty_context)
        assert len(runnable) == 0

    def test_get_layers(self, empty_registry: DetectorRegistry):
        """Test getting unique layers."""
        detector = MockDetector()
        empty_registry.register(detector)

        layers = empty_registry.get_layers()
        assert "structural" in layers


class TestEntropyDetector:
    """Tests for EntropyDetector base class."""

    def test_can_run_with_required_analyses(self, sample_detector_context: DetectorContext):
        """Test can_run returns True when required analyses are available."""
        detector = MockDetector()
        assert detector.can_run(sample_detector_context) is True

    def test_cannot_run_without_required_analyses(self):
        """Test can_run returns False when required analyses are missing."""
        detector = MockDetector()
        context = DetectorContext(table_name="test", column_name="col")
        assert detector.can_run(context) is False

    def test_dimension_path(self):
        """Test dimension path property."""
        detector = MockDetector()
        assert detector.dimension_path == "structural.types.mock_sub"

    def test_detect(self, sample_detector_context: DetectorContext):
        """Test detect method returns entropy objects."""
        detector = MockDetector()
        results = detector.detect(sample_detector_context)

        assert len(results) == 1
        obj = results[0]
        assert obj.layer == "structural"
        assert obj.dimension == "types"
        assert obj.detector_id == "mock_detector"
        assert obj.score == pytest.approx(0.05, abs=0.01)  # 1.0 - 0.95

    def test_create_entropy_object(self, sample_detector_context: DetectorContext):
        """Test create_entropy_object helper."""
        detector = MockDetector()
        obj = detector.create_entropy_object(
            context=sample_detector_context,
            score=0.5,
            evidence=[{"test": True}],
        )

        assert obj.layer == "structural"
        assert obj.dimension == "types"
        assert obj.sub_dimension == "mock_sub"
        assert obj.target == "column:orders.amount"
        assert obj.score == 0.5
        assert obj.detector_id == "mock_detector"


class TestDefaultRegistry:
    """Tests for the default registry."""

    def test_get_default_registry(self):
        """Test getting the default registry."""
        registry = get_default_registry()
        assert registry is not None
        assert isinstance(registry, DetectorRegistry)

    def test_default_registry_singleton(self):
        """Test that default registry is a singleton."""
        registry1 = get_default_registry()
        registry2 = get_default_registry()
        assert registry1 is registry2
