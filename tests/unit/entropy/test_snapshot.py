"""Tests for snapshot: take_snapshot, Snapshot, load_column_analysis."""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from dataraum.entropy.detectors.base import (
    DetectorContext,
    DetectorRegistry,
    EntropyDetector,
)
from dataraum.entropy.models import EntropyObject
from dataraum.entropy.snapshot import Snapshot, take_snapshot

# --- Snapshot dataclass ---


class TestSnapshot:
    def test_score_for_existing(self):
        snap = Snapshot(
            scores={"type_fidelity": 0.3, "null_ratio": 0.1},
            detectors_run=["type_fidelity", "null_ratio"],
        )
        assert snap.score_for("type_fidelity") == 0.3
        assert snap.score_for("null_ratio") == 0.1

    def test_score_for_missing(self):
        snap = Snapshot(scores={"type_fidelity": 0.3}, detectors_run=[])
        assert snap.score_for("nonexistent") is None

    def test_empty_snapshot(self):
        snap = Snapshot(scores={}, detectors_run=[])
        assert snap.scores == {}
        assert snap.detectors_run == []
        assert snap.score_for("anything") is None

    def test_measured_at_default(self):
        before = datetime.now(UTC)
        snap = Snapshot(scores={}, detectors_run=[])
        after = datetime.now(UTC)
        assert before <= snap.measured_at <= after

    def test_objects_default_empty(self):
        snap = Snapshot(scores={}, detectors_run=[])
        assert snap.objects == ()

    def test_frozen(self):
        snap = Snapshot(scores={"a": 1.0}, detectors_run=[])
        with pytest.raises(AttributeError):
            snap.scores = {}  # type: ignore[misc]


# --- Helpers for mocking ---


class StubTypingDetector(EntropyDetector):
    """A stub typing detector for testing."""

    detector_id = "stub_typing"
    layer = "structural"
    dimension = "types"
    sub_dimension = "type_fidelity"

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

    required_analyses = ["semantic"]

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        return [self.create_entropy_object(context, score=0.5)]


class FailingDetector(EntropyDetector):
    """A detector that raises an exception."""

    detector_id = "failing_detector"
    layer = "value"
    dimension = "nulls"
    sub_dimension = "null_ratio"

    required_analyses = ["statistics"]

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        raise RuntimeError("Detector crashed")


class NullDetector(EntropyDetector):
    """A detector for null_ratio."""

    detector_id = "null_ratio"
    layer = "value"
    dimension = "nulls"
    sub_dimension = "null_ratio"

    required_analyses = ["statistics"]

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        return [self.create_entropy_object(context, score=0.1)]


# --- take_snapshot ---


class TestTakeSnapshot:
    def test_runs_all_detectors(self):
        """All registered detectors are run."""
        registry = DetectorRegistry()
        registry.register(StubTypingDetector())
        registry.register(StubSemanticDetector())

        analysis = {"typing": {"parse_success_rate": 0.95}, "semantic": {"role": "measure"}}

        with (
            patch(
                "dataraum.entropy.snapshot._resolve_column_target",
                return_value=("tbl1", "col1", "orders", "amount"),
            ),
            patch(
                "dataraum.entropy.snapshot.load_column_analysis",
                return_value=analysis,
            ),
            patch(
                "dataraum.entropy.snapshot.get_default_registry",
                return_value=registry,
            ),
        ):
            snap = take_snapshot("column:orders.amount", session=MagicMock())

        assert "type_fidelity" in snap.scores
        assert "naming_clarity" in snap.scores
        assert "stub_typing" in snap.detectors_run
        assert "stub_semantic" in snap.detectors_run

    def test_dimensions_filter(self):
        """When dimensions is specified, only matching detectors run."""
        registry = DetectorRegistry()
        registry.register(StubTypingDetector())  # type_fidelity
        registry.register(NullDetector())  # null_ratio

        analysis = {"typing": {"parse_success_rate": 0.95}, "statistics": {"null_ratio": 0.1}}

        with (
            patch(
                "dataraum.entropy.snapshot._resolve_column_target",
                return_value=("tbl1", "col1", "orders", "amount"),
            ),
            patch(
                "dataraum.entropy.snapshot.load_column_analysis",
                return_value=analysis,
            ),
            patch(
                "dataraum.entropy.snapshot.get_default_registry",
                return_value=registry,
            ),
        ):
            snap = take_snapshot(
                "column:orders.amount",
                session=MagicMock(),
                dimensions=["type_fidelity"],
            )

        assert "type_fidelity" in snap.scores
        assert "null_ratio" not in snap.scores

    def test_unresolvable_target(self):
        """Unresolvable target returns empty snapshot."""
        with patch(
            "dataraum.entropy.snapshot._resolve_column_target",
            return_value=None,
        ):
            snap = take_snapshot("column:missing.col", session=MagicMock())

        assert snap.scores == {}
        assert snap.detectors_run == []

    def test_detector_failure_doesnt_crash(self):
        """A failing detector is logged and skipped, not propagated."""
        registry = DetectorRegistry()
        registry.register(StubTypingDetector())
        registry.register(FailingDetector())

        analysis = {"typing": {"parse_success_rate": 0.95}, "statistics": {"null_ratio": 0.1}}

        with (
            patch(
                "dataraum.entropy.snapshot._resolve_column_target",
                return_value=("tbl1", "col1", "orders", "amount"),
            ),
            patch(
                "dataraum.entropy.snapshot.load_column_analysis",
                return_value=analysis,
            ),
            patch(
                "dataraum.entropy.snapshot.get_default_registry",
                return_value=registry,
            ),
        ):
            snap = take_snapshot("column:orders.amount", session=MagicMock())

        # StubTypingDetector succeeded
        assert "type_fidelity" in snap.scores
        # FailingDetector was skipped
        assert "failing_detector" not in snap.detectors_run

    def test_take_snapshot_returns_objects(self):
        """Snapshot.objects contains the full EntropyObject instances."""
        registry = DetectorRegistry()
        registry.register(StubTypingDetector())
        registry.register(StubSemanticDetector())

        analysis = {"typing": {"parse_success_rate": 0.95}, "semantic": {"role": "measure"}}

        with (
            patch(
                "dataraum.entropy.snapshot._resolve_column_target",
                return_value=("tbl1", "col1", "orders", "amount"),
            ),
            patch(
                "dataraum.entropy.snapshot.load_column_analysis",
                return_value=analysis,
            ),
            patch(
                "dataraum.entropy.snapshot.get_default_registry",
                return_value=registry,
            ),
        ):
            snap = take_snapshot("column:orders.amount", session=MagicMock())

        assert len(snap.objects) == 2
        sub_dims = {obj.sub_dimension for obj in snap.objects}
        assert sub_dims == {"type_fidelity", "naming_clarity"}
        # Each object is a full EntropyObject
        for obj in snap.objects:
            assert isinstance(obj, EntropyObject)

    def test_detector_missing_analysis_skipped(self):
        """Detectors whose required analyses are missing are skipped."""
        registry = DetectorRegistry()
        registry.register(StubTypingDetector())  # requires "typing"

        analysis = {"statistics": {"null_ratio": 0.1}}  # no "typing"

        with (
            patch(
                "dataraum.entropy.snapshot._resolve_column_target",
                return_value=("tbl1", "col1", "orders", "amount"),
            ),
            patch(
                "dataraum.entropy.snapshot.load_column_analysis",
                return_value=analysis,
            ),
            patch(
                "dataraum.entropy.snapshot.get_default_registry",
                return_value=registry,
            ),
        ):
            snap = take_snapshot("column:orders.amount", session=MagicMock())

        assert snap.scores == {}
        assert snap.detectors_run == []


# --- Table-scope stub ---


class StubTableDetector(EntropyDetector):
    """A stub table-scoped detector for testing."""

    detector_id = "stub_table"
    layer = "semantic"
    dimension = "dimensional"
    sub_dimension = "cross_column_patterns"
    scope = "table"

    required_analyses = ["slice_variance"]

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        return [self.create_entropy_object(context, score=0.6)]


# --- Table-scope take_snapshot ---


class TestTakeSnapshotTableScope:
    def test_table_target_resolves(self):
        """Table target runs table-scoped detectors."""
        registry = DetectorRegistry()
        registry.register(StubTableDetector())
        registry.register(StubTypingDetector())  # column-scoped

        analysis = {"slice_variance": {"columns": {}, "slice_data": {}}}

        with (
            patch(
                "dataraum.entropy.snapshot._resolve_table_target",
                return_value=("tbl1", "orders"),
            ),
            patch(
                "dataraum.entropy.snapshot.load_table_analysis",
                return_value=analysis,
            ),
            patch(
                "dataraum.entropy.snapshot.get_default_registry",
                return_value=registry,
            ),
        ):
            snap = take_snapshot("table:orders", session=MagicMock())

        assert "cross_column_patterns" in snap.scores
        assert "stub_table" in snap.detectors_run
        # Column-scoped detector should NOT have run
        assert "stub_typing" not in snap.detectors_run

    def test_table_target_runs_table_detectors_only(self):
        """Table targets only run scope='table' detectors."""
        registry = DetectorRegistry()
        registry.register(StubTableDetector())
        registry.register(NullDetector())  # column-scoped

        analysis = {"slice_variance": {"columns": {}, "slice_data": {}}}

        with (
            patch(
                "dataraum.entropy.snapshot._resolve_table_target",
                return_value=("tbl1", "orders"),
            ),
            patch(
                "dataraum.entropy.snapshot.load_table_analysis",
                return_value=analysis,
            ),
            patch(
                "dataraum.entropy.snapshot.get_default_registry",
                return_value=registry,
            ),
        ):
            snap = take_snapshot("table:orders", session=MagicMock())

        assert "stub_table" in snap.detectors_run
        assert "null_ratio" not in snap.detectors_run

    def test_column_target_skips_table_detectors(self):
        """Column targets skip scope='table' detectors."""
        registry = DetectorRegistry()
        registry.register(StubTypingDetector())
        registry.register(StubTableDetector())

        analysis = {"typing": {"parse_success_rate": 0.95}, "slice_variance": {}}

        with (
            patch(
                "dataraum.entropy.snapshot._resolve_column_target",
                return_value=("tbl1", "col1", "orders", "amount"),
            ),
            patch(
                "dataraum.entropy.snapshot.load_column_analysis",
                return_value=analysis,
            ),
            patch(
                "dataraum.entropy.snapshot.get_default_registry",
                return_value=registry,
            ),
        ):
            snap = take_snapshot("column:orders.amount", session=MagicMock())

        assert "type_fidelity" in snap.scores
        assert "stub_typing" in snap.detectors_run
        assert "stub_table" not in snap.detectors_run

    def test_unresolvable_table_target(self):
        """Unresolvable table target returns empty snapshot."""
        with patch(
            "dataraum.entropy.snapshot._resolve_table_target",
            return_value=None,
        ):
            snap = take_snapshot("table:missing", session=MagicMock())

        assert snap.scores == {}
        assert snap.detectors_run == []
