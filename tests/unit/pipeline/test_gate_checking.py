"""Tests for gate checking in the orchestrator."""

from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.entropy_state import PipelineEntropyState
from dataraum.pipeline.orchestrator import Pipeline, PipelineConfig
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.runner import GateMode, RunConfig

# --- Test helpers ---


class StubPhase(BasePhase):
    """Minimal phase for testing."""

    def __init__(
        self,
        phase_name: str,
        deps: list[str] | None = None,
        preconditions: dict[str, float] | None = None,
    ):
        self._name = phase_name
        self._deps = deps or []
        self._preconditions = preconditions or {}

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return f"Stub phase: {self._name}"

    @property
    def dependencies(self) -> list[str]:
        return self._deps

    @property
    def outputs(self) -> list[str]:
        return []

    @property
    def entropy_preconditions(self) -> dict[str, float]:
        return self._preconditions

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        return PhaseResult.success()


# --- GateMode enum ---


class TestGateMode:
    def test_skip_is_default(self):
        config = RunConfig()
        assert config.gate_mode == GateMode.SKIP

    def test_gate_mode_values(self):
        assert GateMode.SKIP.value == "skip"
        assert GateMode.PAUSE.value == "pause"
        assert GateMode.FAIL.value == "fail"

    def test_contract_default_none(self):
        config = RunConfig()
        assert config.contract is None

    def test_max_fix_attempts_default(self):
        config = RunConfig()
        assert config.max_fix_attempts == 3


# --- Pipeline._check_gate ---


class TestCheckGate:
    def test_no_preconditions_passes(self):
        pipeline = Pipeline()
        phase = StubPhase("test")
        pipeline.register(phase)
        pipeline._entropy_state = PipelineEntropyState()
        passed, reason = pipeline._check_gate("test")
        assert passed is True
        assert reason == ""

    def test_preconditions_met_passes(self):
        pipeline = Pipeline()
        phase = StubPhase("test", preconditions={"type_fidelity": 0.5})
        pipeline.register(phase)
        pipeline._entropy_state = PipelineEntropyState()
        pipeline._entropy_state.update_score("type_fidelity", 0.3)
        passed, reason = pipeline._check_gate("test")
        assert passed is True

    def test_preconditions_violated_blocks(self):
        pipeline = Pipeline()
        phase = StubPhase("test", preconditions={"type_fidelity": 0.5})
        pipeline.register(phase)
        pipeline._entropy_state = PipelineEntropyState()
        pipeline._entropy_state.update_score("type_fidelity", 0.7)
        passed, reason = pipeline._check_gate("test")
        assert passed is False
        assert "type_fidelity" in reason
        assert "0.70" in reason
        assert "0.50" in reason

    def test_unmeasured_dimension_passes(self):
        """Dimensions not yet measured should not block."""
        pipeline = Pipeline()
        phase = StubPhase("test", preconditions={"type_fidelity": 0.5})
        pipeline.register(phase)
        pipeline._entropy_state = PipelineEntropyState()
        # No score set for type_fidelity
        passed, reason = pipeline._check_gate("test")
        assert passed is True

    def test_unknown_phase_passes(self):
        pipeline = Pipeline()
        pipeline._entropy_state = PipelineEntropyState()
        passed, reason = pipeline._check_gate("nonexistent")
        assert passed is True


# --- Phase entropy_preconditions annotations ---


class TestPhaseAnnotations:
    def test_statistics_phase_preconditions(self):
        from dataraum.pipeline.registry import get_phase_class

        cls = get_phase_class("statistics")
        assert cls is not None
        phase = cls()
        assert phase.entropy_preconditions == {"type_fidelity": 0.5}

    def test_semantic_phase_preconditions(self):
        from dataraum.pipeline.registry import get_phase_class

        cls = get_phase_class("semantic")
        assert cls is not None
        phase = cls()
        assert phase.entropy_preconditions == {
            "type_fidelity": 0.3,
            "join_path_determinism": 0.5,
        }

    def test_graph_execution_phase_preconditions(self):
        from dataraum.pipeline.registry import get_phase_class

        cls = get_phase_class("graph_execution")
        assert cls is not None
        phase = cls()
        assert phase.entropy_preconditions == {
            "type_fidelity": 0.3,
            "naming_clarity": 0.4,
        }

    def test_import_phase_no_preconditions(self):
        from dataraum.pipeline.registry import get_phase_class

        cls = get_phase_class("import")
        assert cls is not None
        phase = cls()
        assert phase.entropy_preconditions == {}


# --- PipelineConfig.gate_mode ---


class TestPipelineConfigGateMode:
    def test_default_is_skip(self):
        config = PipelineConfig()
        assert config.gate_mode == "skip"
