"""Tests for detector trust levels and gate data model."""

from dataraum.entropy.detectors import (
    DetectorTrust,
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
}


class TestDetectorTrustLevels:
    """All detectors are machine-verifiable (HARD)."""

    def test_all_detectors_are_hard(self):
        registry = get_default_registry()
        for d in registry.get_all_detectors():
            assert d.trust_level == DetectorTrust.HARD, (
                f"{d.detector_id} should be HARD but is {d.trust_level}"
            )

    def test_all_detectors_are_verifiers(self):
        registry = get_default_registry()
        for d in registry.get_all_detectors():
            assert d.is_verifier, f"{d.detector_id} should be a verifier"

    def test_all_expected_detectors_registered(self):
        registry = get_default_registry()
        registered = {d.detector_id for d in registry.get_all_detectors()}
        assert registered == ALL_DETECTOR_IDS

    def test_no_soft_detectors(self):
        registry = get_default_registry()
        assert registry.get_soft_detectors() == []


# --- PhaseStatus ---


class TestPhaseStatus:
    def test_all_statuses(self):
        statuses = set(PhaseStatus)
        assert len(statuses) == 5  # pending, running, completed, failed, skipped
