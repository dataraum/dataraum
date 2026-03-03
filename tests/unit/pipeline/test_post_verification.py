"""Tests for post-verification: phases declare post_verification, orchestrator runs detectors."""

from unittest.mock import MagicMock, patch

from dataraum.entropy.hard_snapshot import HardSnapshot
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.entropy_state import PipelineEntropyState
from dataraum.pipeline.orchestrator import Pipeline
from dataraum.pipeline.phases.base import BasePhase

# --- Test helpers ---


class StubPhaseWithPostVerification(BasePhase):
    """Phase that declares post_verification dimensions."""

    def __init__(
        self,
        phase_name: str = "typing",
        deps: list[str] | None = None,
        post_verif: list[str] | None = None,
    ):
        self._name = phase_name
        self._deps = deps or []
        self._post_verif = post_verif or []

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return f"Stub: {self._name}"

    @property
    def dependencies(self) -> list[str]:
        return self._deps

    @property
    def outputs(self) -> list[str]:
        return []

    @property
    def post_verification(self) -> list[str]:
        return self._post_verif

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        return PhaseResult.success()


# --- Phase annotations ---


class TestPhasePostVerificationAnnotations:
    def test_typing_phase_declares_type_fidelity(self):
        from dataraum.pipeline.registry import get_phase_class

        cls = get_phase_class("typing")
        assert cls is not None
        phase = cls()
        assert phase.post_verification == ["type_fidelity"]

    def test_statistics_phase_declares_null_and_outlier(self):
        from dataraum.pipeline.registry import get_phase_class

        cls = get_phase_class("statistics")
        assert cls is not None
        phase = cls()
        assert "null_ratio" in phase.post_verification
        assert "outlier_rate" in phase.post_verification

    def test_relationships_phase_declares_join_quality(self):
        from dataraum.pipeline.registry import get_phase_class

        cls = get_phase_class("relationships")
        assert cls is not None
        phase = cls()
        assert "join_path_determinism" in phase.post_verification
        assert "relationship_quality" in phase.post_verification

    def test_semantic_phase_declares_naming_unit(self):
        from dataraum.pipeline.registry import get_phase_class

        cls = get_phase_class("semantic")
        assert cls is not None
        phase = cls()
        assert "naming_clarity" in phase.post_verification
        assert "unit_declaration" in phase.post_verification

    def test_import_phase_has_no_post_verification(self):
        from dataraum.pipeline.registry import get_phase_class

        cls = get_phase_class("import")
        assert cls is not None
        phase = cls()
        assert phase.post_verification == []


# --- _run_post_verification ---


class TestRunPostVerification:
    def test_post_verification_updates_entropy_state(self):
        """After post-verification, entropy_state should have scores."""
        pipeline = Pipeline()
        phase = StubPhaseWithPostVerification("typing", post_verif=["type_fidelity"])
        pipeline.register(phase)
        pipeline._entropy_state = PipelineEntropyState()

        mock_manager = MagicMock()
        mock_session = MagicMock()
        mock_manager.session_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_manager.session_scope.return_value.__exit__ = MagicMock(return_value=False)

        # Mock: no tables found (empty source)
        mock_session.execute.return_value.scalars.return_value.all.return_value = []

        scores = pipeline._run_post_verification(phase, mock_manager, "src1", [])
        assert scores == {}

    def test_post_verification_aggregates_scores(self):
        """Post-verification should aggregate scores across columns."""
        pipeline = Pipeline()
        phase = StubPhaseWithPostVerification("typing", post_verif=["type_fidelity"])
        pipeline.register(phase)

        # Create mock table and columns
        mock_table = MagicMock()
        mock_table.table_id = "tbl1"
        mock_table.table_name = "orders"

        mock_col1 = MagicMock()
        mock_col1.column_id = "col1"
        mock_col1.column_name = "amount"

        mock_col2 = MagicMock()
        mock_col2.column_id = "col2"
        mock_col2.column_name = "quantity"

        mock_manager = MagicMock()
        mock_session = MagicMock()
        mock_manager.session_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_manager.session_scope.return_value.__exit__ = MagicMock(return_value=False)

        # First query returns tables, second returns columns
        call_count = 0

        def mock_execute(stmt):
            nonlocal call_count
            result = MagicMock()
            if call_count == 0:
                result.scalars.return_value.all.return_value = [mock_table]
            else:
                result.scalars.return_value.all.return_value = [mock_col1, mock_col2]
            call_count += 1
            return result

        mock_session.execute = mock_execute

        snap1 = HardSnapshot(scores={"type_fidelity": 0.2}, detectors_run=["type_fidelity"])
        snap2 = HardSnapshot(scores={"type_fidelity": 0.4}, detectors_run=["type_fidelity"])

        with patch(
            "dataraum.entropy.hard_snapshot.take_hard_snapshot",
            side_effect=[snap1, snap2],
        ):
            scores = pipeline._run_post_verification(phase, mock_manager, "src1", ["tbl1"])

        assert "type_fidelity" in scores
        # Mean of 0.2 and 0.4 = 0.3
        assert abs(scores["type_fidelity"] - 0.3) < 0.001

    def test_post_verification_failure_returns_empty(self):
        """Post-verification errors should not crash the pipeline."""
        pipeline = Pipeline()
        phase = StubPhaseWithPostVerification("typing", post_verif=["type_fidelity"])
        pipeline.register(phase)

        mock_manager = MagicMock()
        mock_manager.session_scope.side_effect = RuntimeError("DB error")

        scores = pipeline._run_post_verification(phase, mock_manager, "src1", [])
        assert scores == {}

    def test_no_post_verification_returns_empty(self):
        """Phase without post_verification returns empty dict."""
        pipeline = Pipeline()
        phase = StubPhaseWithPostVerification("import", post_verif=[])
        pipeline.register(phase)

        scores = pipeline._run_post_verification(phase, MagicMock(), "src1", [])
        assert scores == {}
