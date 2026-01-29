"""Tests for built-in detector registration."""

import pytest

from dataraum.entropy.detectors import (
    BUILTIN_DETECTORS,
    BusinessMeaningDetector,
    DerivedValueDetector,
    DetectorRegistry,
    JoinPathDeterminismDetector,
    NullRatioDetector,
    OutlierRateDetector,
    RelationshipEntropyDetector,
    TemporalEntropyDetector,
    TypeFidelityDetector,
    UnitEntropyDetector,
    register_builtin_detectors,
)


class TestBuiltinDetectors:
    """Tests for BUILTIN_DETECTORS list and registration."""

    def test_builtin_detectors_list(self):
        """Test that all expected detectors are in BUILTIN_DETECTORS."""
        expected_detectors = [
            # Structural
            TypeFidelityDetector,
            JoinPathDeterminismDetector,
            RelationshipEntropyDetector,
            # Value
            NullRatioDetector,
            OutlierRateDetector,
            # Semantic
            BusinessMeaningDetector,
            UnitEntropyDetector,
            TemporalEntropyDetector,
            # Computational
            DerivedValueDetector,
        ]

        assert len(BUILTIN_DETECTORS) == len(expected_detectors)
        for detector_class in expected_detectors:
            assert detector_class in BUILTIN_DETECTORS

    def test_register_builtin_detectors(self):
        """Test registering all builtin detectors to a registry."""
        registry = DetectorRegistry()
        register_builtin_detectors(registry)

        # Check all detectors are registered
        detector_ids = registry.get_detector_ids()
        assert "type_fidelity" in detector_ids
        assert "join_path_determinism" in detector_ids
        assert "relationship_entropy" in detector_ids
        assert "null_ratio" in detector_ids
        assert "outlier_rate" in detector_ids
        assert "business_meaning" in detector_ids
        assert "unit_entropy" in detector_ids
        assert "temporal_entropy" in detector_ids
        assert "derived_value" in detector_ids

    def test_register_builtin_detectors_idempotent(self):
        """Test that registering builtin detectors is idempotent."""
        registry = DetectorRegistry()

        # Register twice
        register_builtin_detectors(registry)
        register_builtin_detectors(registry)

        # Should still have same number of detectors
        detector_ids = registry.get_detector_ids()
        assert len(detector_ids) == len(BUILTIN_DETECTORS)

    def test_layers_covered(self):
        """Test that all entropy layers are covered by builtin detectors."""
        registry = DetectorRegistry()
        register_builtin_detectors(registry)

        layers = registry.get_layers()
        assert "structural" in layers
        assert "value" in layers
        assert "semantic" in layers
        assert "computational" in layers

    def test_structural_detectors(self):
        """Test structural layer detectors."""
        registry = DetectorRegistry()
        register_builtin_detectors(registry)

        structural_detectors = registry.get_detectors_for_layer("structural")
        assert len(structural_detectors) == 3
        detector_ids = [d.detector_id for d in structural_detectors]
        assert "type_fidelity" in detector_ids
        assert "join_path_determinism" in detector_ids
        assert "relationship_entropy" in detector_ids

    def test_value_detectors(self):
        """Test value layer detectors."""
        registry = DetectorRegistry()
        register_builtin_detectors(registry)

        value_detectors = registry.get_detectors_for_layer("value")
        assert len(value_detectors) == 2
        detector_ids = [d.detector_id for d in value_detectors]
        assert "null_ratio" in detector_ids
        assert "outlier_rate" in detector_ids

    def test_semantic_detectors(self):
        """Test semantic layer detectors."""
        registry = DetectorRegistry()
        register_builtin_detectors(registry)

        semantic_detectors = registry.get_detectors_for_layer("semantic")
        assert len(semantic_detectors) == 3
        detector_ids = [d.detector_id for d in semantic_detectors]
        assert "business_meaning" in detector_ids
        assert "unit_entropy" in detector_ids
        assert "temporal_entropy" in detector_ids

    def test_computational_detectors(self):
        """Test computational layer detectors."""
        registry = DetectorRegistry()
        register_builtin_detectors(registry)

        computational_detectors = registry.get_detectors_for_layer("computational")
        assert len(computational_detectors) == 1
        assert computational_detectors[0].detector_id == "derived_value"


class TestDetectorRequirements:
    """Tests for detector requirements and can_run checks."""

    @pytest.fixture
    def registry(self) -> DetectorRegistry:
        """Create registry with all builtin detectors."""
        registry = DetectorRegistry()
        register_builtin_detectors(registry)
        return registry

    def test_type_fidelity_requires_typing(self, registry: DetectorRegistry):
        """Test TypeFidelityDetector requires typing analysis."""
        detector = registry.get_detector("type_fidelity")
        assert detector is not None
        assert "typing" in detector.required_analyses

    def test_null_ratio_requires_statistics(self, registry: DetectorRegistry):
        """Test NullRatioDetector requires statistics analysis."""
        detector = registry.get_detector("null_ratio")
        assert detector is not None
        assert "statistics" in detector.required_analyses

    def test_business_meaning_requires_semantic(self, registry: DetectorRegistry):
        """Test BusinessMeaningDetector requires semantic analysis."""
        detector = registry.get_detector("business_meaning")
        assert detector is not None
        assert "semantic" in detector.required_analyses

    def test_derived_value_requires_correlation(self, registry: DetectorRegistry):
        """Test DerivedValueDetector requires correlation analysis."""
        detector = registry.get_detector("derived_value")
        assert detector is not None
        assert "correlation" in detector.required_analyses

    def test_join_path_requires_relationships(self, registry: DetectorRegistry):
        """Test JoinPathDeterminismDetector requires relationships analysis."""
        detector = registry.get_detector("join_path_determinism")
        assert detector is not None
        assert "relationships" in detector.required_analyses
