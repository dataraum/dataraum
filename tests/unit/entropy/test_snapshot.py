"""Tests for snapshot: take_snapshot, Snapshot."""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from dataraum.entropy.detectors.base import (
    DetectorContext,
    DetectorRegistry,
    EntropyDetector,
)
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
from dataraum.entropy.models import EntropyObject
from dataraum.entropy.snapshot import Snapshot, take_snapshot

# --- Snapshot dataclass ---


class TestSnapshot:
    def test_empty_snapshot(self):
        snap = Snapshot(scores={}, detectors_run=[])
        assert snap.scores == {}
        assert snap.detectors_run == []

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
    layer = Layer.STRUCTURAL
    dimension = Dimension.TYPES
    sub_dimension = SubDimension.TYPE_FIDELITY

    required_analyses = [AnalysisKey.TYPING]

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
    layer = Layer.SEMANTIC
    dimension = Dimension.BUSINESS_MEANING
    sub_dimension = SubDimension.NAMING_CLARITY

    required_analyses = [AnalysisKey.SEMANTIC]

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        return [self.create_entropy_object(context, score=0.5)]


class FailingDetector(EntropyDetector):
    """A detector that raises an exception."""

    detector_id = "failing_detector"
    layer = Layer.VALUE
    dimension = Dimension.NULLS
    sub_dimension = SubDimension.NULL_RATIO

    required_analyses = [AnalysisKey.STATISTICS]

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        raise RuntimeError("Detector crashed")


class NullDetector(EntropyDetector):
    """A detector for null_ratio."""

    detector_id = "null_ratio"
    layer = Layer.VALUE
    dimension = Dimension.NULLS
    sub_dimension = SubDimension.NULL_RATIO

    required_analyses = [AnalysisKey.STATISTICS]

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        return [self.create_entropy_object(context, score=0.1)]


# --- Stub that pre-populates analysis_results via load_data ---


class PreloadingStubTypingDetector(StubTypingDetector):
    """Stub typing detector that populates analysis_results in load_data."""

    def load_data(self, context: DetectorContext) -> None:
        context.analysis_results["typing"] = {"parse_success_rate": 0.95}


class PreloadingStubSemanticDetector(StubSemanticDetector):
    """Stub semantic detector that populates analysis_results in load_data."""

    def load_data(self, context: DetectorContext) -> None:
        context.analysis_results["semantic"] = {"role": "measure"}


class PreloadingNullDetector(NullDetector):
    """Stub null detector that populates analysis_results in load_data."""

    def load_data(self, context: DetectorContext) -> None:
        context.analysis_results["statistics"] = {"null_ratio": 0.1}


class PreloadingFailingDetector(FailingDetector):
    """Failing detector that still loads data."""

    def load_data(self, context: DetectorContext) -> None:
        context.analysis_results["statistics"] = {"null_ratio": 0.1}


# --- take_snapshot ---


class TestTakeSnapshot:
    def test_runs_all_detectors(self):
        """All registered detectors are run."""
        registry = DetectorRegistry()
        registry.register(PreloadingStubTypingDetector())
        registry.register(PreloadingStubSemanticDetector())

        with (
            patch(
                "dataraum.entropy.snapshot._resolve_column_target",
                return_value=("tbl1", "col1", "orders", "amount"),
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
        registry.register(PreloadingStubTypingDetector())  # type_fidelity
        registry.register(PreloadingNullDetector())  # null_ratio

        with (
            patch(
                "dataraum.entropy.snapshot._resolve_column_target",
                return_value=("tbl1", "col1", "orders", "amount"),
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
        registry.register(PreloadingStubTypingDetector())
        registry.register(PreloadingFailingDetector())

        with (
            patch(
                "dataraum.entropy.snapshot._resolve_column_target",
                return_value=("tbl1", "col1", "orders", "amount"),
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
        registry.register(PreloadingStubTypingDetector())
        registry.register(PreloadingStubSemanticDetector())

        with (
            patch(
                "dataraum.entropy.snapshot._resolve_column_target",
                return_value=("tbl1", "col1", "orders", "amount"),
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
        registry.register(StubTypingDetector())  # requires "typing", no load_data override

        with (
            patch(
                "dataraum.entropy.snapshot._resolve_column_target",
                return_value=("tbl1", "col1", "orders", "amount"),
            ),
            patch(
                "dataraum.entropy.snapshot.get_default_registry",
                return_value=registry,
            ),
        ):
            snap = take_snapshot("column:orders.amount", session=MagicMock())

        assert snap.scores == {}
        assert snap.detectors_run == []

    def test_pre_populated_context_still_works(self):
        """Detectors with pre-populated analysis_results (no load_data) still work.

        This tests backward compatibility — if analysis_results are already
        present on the context (e.g., in tests), detectors don't need load_data.
        """
        registry = DetectorRegistry()

        # Stub that pre-populates typing in analysis_results
        class TypingPreloader(StubTypingDetector):
            def load_data(self, context: DetectorContext) -> None:
                context.analysis_results["typing"] = {"parse_success_rate": 0.95}

        registry.register(TypingPreloader())

        with (
            patch(
                "dataraum.entropy.snapshot._resolve_column_target",
                return_value=("tbl1", "col1", "orders", "amount"),
            ),
            patch(
                "dataraum.entropy.snapshot.get_default_registry",
                return_value=registry,
            ),
        ):
            snap = take_snapshot("column:orders.amount", session=MagicMock())

        assert "type_fidelity" in snap.scores


# --- Table-scope stub ---


class StubTableDetector(EntropyDetector):
    """A stub table-scoped detector for testing."""

    detector_id = "stub_table"
    layer = Layer.SEMANTIC
    dimension = Dimension.DIMENSIONAL
    sub_dimension = SubDimension.CROSS_COLUMN_PATTERNS
    scope = "table"

    required_analyses = [AnalysisKey.SLICE_VARIANCE]

    def load_data(self, context: DetectorContext) -> None:
        context.analysis_results["slice_variance"] = {"columns": {}, "slice_data": {}}

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        return [self.create_entropy_object(context, score=0.6)]


# --- Table-scope take_snapshot ---


class TestTakeSnapshotTableScope:
    def test_table_target_resolves(self):
        """Table target runs table-scoped detectors."""
        registry = DetectorRegistry()
        registry.register(StubTableDetector())
        registry.register(PreloadingStubTypingDetector())  # column-scoped

        with (
            patch(
                "dataraum.entropy.snapshot._resolve_table_target",
                return_value=("tbl1", "orders"),
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
        registry.register(PreloadingNullDetector())  # column-scoped

        with (
            patch(
                "dataraum.entropy.snapshot._resolve_table_target",
                return_value=("tbl1", "orders"),
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
        registry.register(PreloadingStubTypingDetector())
        registry.register(StubTableDetector())

        with (
            patch(
                "dataraum.entropy.snapshot._resolve_column_target",
                return_value=("tbl1", "col1", "orders", "amount"),
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


# --- View-scope stub ---


class StubViewDetector(EntropyDetector):
    """A stub view-scoped detector for testing."""

    detector_id = "stub_view"
    layer = Layer.SEMANTIC
    dimension = Dimension.COVERAGE
    sub_dimension = SubDimension.DIMENSION_COVERAGE
    scope = "view"

    required_analyses = [AnalysisKey.ENRICHED_VIEW]

    def load_data(self, context: DetectorContext) -> None:
        context.analysis_results["enriched_view"] = MagicMock(
            dimension_columns=["dim__col"], view_name="enriched_orders"
        )

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        return [self.create_entropy_object(context, score=0.4)]


# --- View-scope take_snapshot ---


class TestTakeSnapshotViewScope:
    def test_view_target_resolves(self):
        """View target runs view-scoped detectors."""
        registry = DetectorRegistry()
        registry.register(StubViewDetector())
        registry.register(PreloadingStubTypingDetector())  # column-scoped

        with (
            patch(
                "dataraum.entropy.snapshot._resolve_view_target",
                return_value=("v1", "enriched_orders", "tbl1"),
            ),
            patch(
                "dataraum.entropy.snapshot.get_default_registry",
                return_value=registry,
            ),
        ):
            snap = take_snapshot("view:enriched_orders", session=MagicMock())

        assert "dimension_coverage" in snap.scores
        assert "stub_view" in snap.detectors_run

    def test_view_target_skips_column_detectors(self):
        """View targets only run scope='view' detectors, not column."""
        registry = DetectorRegistry()
        registry.register(StubViewDetector())
        registry.register(PreloadingStubTypingDetector())  # column-scoped

        with (
            patch(
                "dataraum.entropy.snapshot._resolve_view_target",
                return_value=("v1", "enriched_orders", "tbl1"),
            ),
            patch(
                "dataraum.entropy.snapshot.get_default_registry",
                return_value=registry,
            ),
        ):
            snap = take_snapshot("view:enriched_orders", session=MagicMock())

        assert "stub_typing" not in snap.detectors_run

    def test_column_target_skips_view_detectors(self):
        """Column targets skip scope='view' detectors."""
        registry = DetectorRegistry()
        registry.register(PreloadingStubTypingDetector())
        registry.register(StubViewDetector())

        with (
            patch(
                "dataraum.entropy.snapshot._resolve_column_target",
                return_value=("tbl1", "col1", "orders", "amount"),
            ),
            patch(
                "dataraum.entropy.snapshot.get_default_registry",
                return_value=registry,
            ),
        ):
            snap = take_snapshot("column:orders.amount", session=MagicMock())

        assert "stub_typing" in snap.detectors_run
        assert "stub_view" not in snap.detectors_run

    def test_unresolvable_view_target(self):
        """Unresolvable view target returns empty snapshot."""
        with patch(
            "dataraum.entropy.snapshot._resolve_view_target",
            return_value=None,
        ):
            snap = take_snapshot("view:missing", session=MagicMock())

        assert snap.scores == {}
        assert snap.detectors_run == []
