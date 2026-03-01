"""Tests for detector trust levels and gate data model."""

from dataraum.entropy.detectors import (
    DetectorTrust,
    get_default_registry,
)
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.models import EntropyObject
from dataraum.pipeline.base import PhaseStatus
from dataraum.pipeline.entropy_state import PipelineEntropyState

# --- Trust Level Classification ---


EXPECTED_HARD = {
    "type_fidelity",
    "join_path_determinism",
    "relationship_entropy",
    "null_ratio",
    "outlier_rate",
    "benford",
    "temporal_drift",
    "derived_value",
}

EXPECTED_SOFT = {
    "business_meaning",
    "unit_entropy",
    "temporal_entropy",
    "dimensional_entropy",
}


class TestDetectorTrustLevels:
    """Verify each detector has the correct trust level."""

    def test_hard_detectors(self):
        registry = get_default_registry()
        hard_ids = {d.detector_id for d in registry.get_hard_detectors()}
        assert hard_ids == EXPECTED_HARD

    def test_soft_detectors(self):
        registry = get_default_registry()
        soft_ids = {d.detector_id for d in registry.get_soft_detectors()}
        assert soft_ids == EXPECTED_SOFT

    def test_all_detectors_classified(self):
        """Every registered detector must be either HARD or SOFT."""
        registry = get_default_registry()
        for d in registry.get_all_detectors():
            assert d.trust_level in (DetectorTrust.HARD, DetectorTrust.SOFT), (
                f"{d.detector_id} has unknown trust level: {d.trust_level}"
            )

    def test_is_verifier_matches_hard(self):
        registry = get_default_registry()
        for d in registry.get_all_detectors():
            assert d.is_verifier == (d.trust_level == DetectorTrust.HARD), (
                f"{d.detector_id}: is_verifier={d.is_verifier} "
                f"but trust_level={d.trust_level}"
            )

    def test_default_trust_is_soft(self):
        """A detector with no explicit trust_level defaults to SOFT."""

        class BareDetector(EntropyDetector):
            detector_id = "bare"
            layer = "test"
            dimension = "test"
            sub_dimension = "test"

            def detect(self, context: DetectorContext) -> list[EntropyObject]:
                return []

        d = BareDetector()
        assert d.trust_level == DetectorTrust.SOFT
        assert d.is_verifier is False


# --- PhaseStatus.GATE_BLOCKED ---


class TestPhaseStatusGateBlocked:
    def test_gate_blocked_exists(self):
        assert PhaseStatus.GATE_BLOCKED == "gate_blocked"

    def test_gate_blocked_is_distinct(self):
        statuses = set(PhaseStatus)
        assert PhaseStatus.GATE_BLOCKED in statuses
        assert len(statuses) == 6  # pending, running, completed, failed, skipped, gate_blocked


# --- PipelineEntropyState ---


class TestPipelineEntropyState:
    def test_update_and_get_score(self):
        state = PipelineEntropyState()
        state.update_score("type_fidelity", 0.4, target_count=5)
        assert state.get_score("type_fidelity") == 0.4

    def test_get_score_missing(self):
        state = PipelineEntropyState()
        assert state.get_score("nonexistent") is None

    def test_check_preconditions_pass(self):
        state = PipelineEntropyState()
        state.update_score("type_fidelity", 0.3)
        violations = state.check_preconditions({"type_fidelity": 0.5})
        assert violations == {}

    def test_check_preconditions_fail(self):
        state = PipelineEntropyState()
        state.update_score("type_fidelity", 0.6)
        violations = state.check_preconditions({"type_fidelity": 0.5})
        assert "type_fidelity" in violations
        assert violations["type_fidelity"] == (0.6, 0.5)

    def test_check_preconditions_unmeasured_passes(self):
        """Unmeasured dimensions should not block (no data yet)."""
        state = PipelineEntropyState()
        violations = state.check_preconditions({"type_fidelity": 0.5})
        assert violations == {}

    def test_take_snapshot(self):
        state = PipelineEntropyState()
        state.update_score("type_fidelity", 0.3)
        snap = state.take_snapshot()
        assert "type_fidelity" in snap
        assert len(state.snapshots) == 1

        # Mutating state after snapshot doesn't affect snapshot
        state.update_score("type_fidelity", 0.9)
        assert snap["type_fidelity"].score == 0.3

    def test_to_dict(self):
        state = PipelineEntropyState()
        state.update_score("type_fidelity", 0.3)
        state.update_score("null_ratio", 0.1)
        d = state.to_dict()
        assert d == {"type_fidelity": 0.3, "null_ratio": 0.1}
