"""Tests for detector registry and gate data model."""

from dataraum.entropy.detectors import (
    get_default_registry,
)
from dataraum.pipeline.base import PhaseStatus

ALL_DETECTOR_IDS = {
    "type_fidelity",
    "join_path_determinism",
    "relationship_entropy",
    "null_ratio",
    "outlier_rate",
    "benford",
    "temporal_drift",
    "derived_value",
    "business_meaning",
    "unit_entropy",
    "temporal_entropy",
    "dimensional_entropy",
    "column_quality",
    "dimension_coverage",
    "business_cycle_health",
    "cross_table_consistency",
}


class TestDetectorRegistry:
    """All expected detectors are registered."""

    def test_all_expected_detectors_registered(self):
        registry = get_default_registry()
        registered = {d.detector_id for d in registry.get_all_detectors()}
        assert registered == ALL_DETECTOR_IDS


# --- PhaseStatus ---


class TestPhaseStatus:
    def test_all_statuses(self):
        statuses = set(PhaseStatus)
        assert len(statuses) == 4  # pending, completed, failed, skipped
