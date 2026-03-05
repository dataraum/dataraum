"""Tests for hard_snapshot: take_hard_snapshot, HardSnapshot, load_column_analysis."""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from dataraum.entropy.detectors.base import (
    DetectorContext,
    DetectorRegistry,
    DetectorTrust,
    EntropyDetector,
)
from dataraum.entropy.hard_snapshot import HardSnapshot, take_hard_snapshot
from dataraum.entropy.models import EntropyObject

# --- HardSnapshot dataclass ---


class TestHardSnapshot:
    def test_score_for_existing(self):
        snap = HardSnapshot(
            scores={"type_fidelity": 0.3, "null_ratio": 0.1},
            detectors_run=["type_fidelity", "null_ratio"],
        )
        assert snap.score_for("type_fidelity") == 0.3
        assert snap.score_for("null_ratio") == 0.1

    def test_score_for_missing(self):
        snap = HardSnapshot(scores={"type_fidelity": 0.3}, detectors_run=[])
        assert snap.score_for("nonexistent") is None

    def test_empty_snapshot(self):
        snap = HardSnapshot(scores={}, detectors_run=[])
        assert snap.scores == {}
        assert snap.detectors_run == []
        assert snap.score_for("anything") is None

    def test_measured_at_default(self):
        before = datetime.now(UTC)
        snap = HardSnapshot(scores={}, detectors_run=[])
        after = datetime.now(UTC)
        assert before <= snap.measured_at <= after

    def test_frozen(self):
        snap = HardSnapshot(scores={"a": 1.0}, detectors_run=[])
        with pytest.raises(AttributeError):
            snap.scores = {}  # type: ignore[misc]


# --- Helpers for mocking ---


class StubHardDetector(EntropyDetector):
    """A stub hard detector for testing."""

    detector_id = "stub_hard"
    layer = "structural"
    dimension = "types"
    sub_dimension = "type_fidelity"
    trust_level = DetectorTrust.HARD
    required_analyses = ["typing"]

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        return [
            self.create_entropy_object(
                context,
                score=0.25,
                evidence=[{"detail": "stub"}],
            )
        ]


class StubSemanticDetector(EntropyDetector):
    """A stub semantic detector."""

    detector_id = "stub_semantic"
    layer = "semantic"
    dimension = "business_meaning"
    sub_dimension = "naming_clarity"
    trust_level = DetectorTrust.HARD
    required_analyses = ["semantic"]

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        return [self.create_entropy_object(context, score=0.5)]


class FailingDetector(EntropyDetector):
    """A hard detector that raises an exception."""

    detector_id = "failing_hard"
    layer = "value"
    dimension = "nulls"
    sub_dimension = "null_ratio"
    trust_level = DetectorTrust.HARD
    required_analyses = ["statistics"]

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        raise RuntimeError("Detector crashed")


class NullDetector(EntropyDetector):
    """A hard detector for null_ratio."""

    detector_id = "null_ratio"
    layer = "value"
    dimension = "nulls"
    sub_dimension = "null_ratio"
    trust_level = DetectorTrust.HARD
    required_analyses = ["statistics"]

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        return [self.create_entropy_object(context, score=0.1)]


# --- take_hard_snapshot ---


class TestTakeHardSnapshot:
    def test_runs_all_detectors(self):
        """All registered detectors are run."""
        registry = DetectorRegistry()
        registry.register(StubHardDetector())
        registry.register(StubSemanticDetector())

        analysis = {"typing": {"parse_success_rate": 0.95}, "semantic": {"role": "measure"}}

        with (
            patch(
                "dataraum.entropy.hard_snapshot._resolve_column_target",
                return_value=("tbl1", "col1", "orders", "amount"),
            ),
            patch(
                "dataraum.entropy.hard_snapshot.load_column_analysis",
                return_value=analysis,
            ),
            patch(
                "dataraum.entropy.hard_snapshot.get_default_registry",
                return_value=registry,
            ),
        ):
            snap = take_hard_snapshot("column:orders.amount", session=MagicMock())

        assert "type_fidelity" in snap.scores
        assert "naming_clarity" in snap.scores
        assert "stub_hard" in snap.detectors_run
        assert "stub_semantic" in snap.detectors_run

    def test_dimensions_filter(self):
        """When dimensions is specified, only matching detectors run."""
        registry = DetectorRegistry()
        registry.register(StubHardDetector())  # type_fidelity
        registry.register(NullDetector())  # null_ratio

        analysis = {"typing": {"parse_success_rate": 0.95}, "statistics": {"null_ratio": 0.1}}

        with (
            patch(
                "dataraum.entropy.hard_snapshot._resolve_column_target",
                return_value=("tbl1", "col1", "orders", "amount"),
            ),
            patch(
                "dataraum.entropy.hard_snapshot.load_column_analysis",
                return_value=analysis,
            ),
            patch(
                "dataraum.entropy.hard_snapshot.get_default_registry",
                return_value=registry,
            ),
        ):
            snap = take_hard_snapshot(
                "column:orders.amount",
                session=MagicMock(),
                dimensions=["type_fidelity"],
            )

        assert "type_fidelity" in snap.scores
        assert "null_ratio" not in snap.scores

    def test_unresolvable_target(self):
        """Unresolvable target returns empty snapshot."""
        with patch(
            "dataraum.entropy.hard_snapshot._resolve_column_target",
            return_value=None,
        ):
            snap = take_hard_snapshot("column:missing.col", session=MagicMock())

        assert snap.scores == {}
        assert snap.detectors_run == []

    def test_detector_failure_doesnt_crash(self):
        """A failing detector is logged and skipped, not propagated."""
        registry = DetectorRegistry()
        registry.register(StubHardDetector())
        registry.register(FailingDetector())

        analysis = {"typing": {"parse_success_rate": 0.95}, "statistics": {"null_ratio": 0.1}}

        with (
            patch(
                "dataraum.entropy.hard_snapshot._resolve_column_target",
                return_value=("tbl1", "col1", "orders", "amount"),
            ),
            patch(
                "dataraum.entropy.hard_snapshot.load_column_analysis",
                return_value=analysis,
            ),
            patch(
                "dataraum.entropy.hard_snapshot.get_default_registry",
                return_value=registry,
            ),
        ):
            snap = take_hard_snapshot("column:orders.amount", session=MagicMock())

        # StubHardDetector succeeded
        assert "type_fidelity" in snap.scores
        # FailingDetector was skipped
        assert "failing_hard" not in snap.detectors_run

    def test_detector_missing_analysis_skipped(self):
        """Detectors whose required analyses are missing are skipped."""
        registry = DetectorRegistry()
        registry.register(StubHardDetector())  # requires "typing"

        analysis = {"statistics": {"null_ratio": 0.1}}  # no "typing"

        with (
            patch(
                "dataraum.entropy.hard_snapshot._resolve_column_target",
                return_value=("tbl1", "col1", "orders", "amount"),
            ),
            patch(
                "dataraum.entropy.hard_snapshot.load_column_analysis",
                return_value=analysis,
            ),
            patch(
                "dataraum.entropy.hard_snapshot.get_default_registry",
                return_value=registry,
            ),
        ):
            snap = take_hard_snapshot("column:orders.amount", session=MagicMock())

        assert snap.scores == {}
        assert snap.detectors_run == []
