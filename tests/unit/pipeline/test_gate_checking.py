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
        post_verify: list[str] | None = None,
    ):
        self._name = phase_name
        self._deps = deps or []
        self._preconditions = preconditions or {}
        self._post_verify = post_verify or []

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

    @property
    def post_verification(self) -> list[str]:
        return self._post_verify

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

    def test_unmeasured_dimension_no_producer_passes(self):
        """Dimensions not yet measured pass when no phase produces them."""
        pipeline = Pipeline()
        phase = StubPhase("test", preconditions={"type_fidelity": 0.5})
        pipeline.register(phase)
        pipeline._entropy_state = PipelineEntropyState()
        # No other phase has post_verification for type_fidelity
        passed, reason = pipeline._check_gate("test")
        assert passed is True

    def test_unmeasured_dimension_with_producer_blocks(self):
        """Dimensions not yet measured block when a not-yet-completed phase produces them."""
        pipeline = Pipeline()
        # "producer" phase produces type_fidelity via post_verification
        producer = StubPhase("producer", post_verify=["type_fidelity"])
        pipeline.register(producer)
        # "test" phase has a precondition on type_fidelity
        phase = StubPhase("test", preconditions={"type_fidelity": 0.5})
        pipeline.register(phase)
        pipeline._entropy_state = PipelineEntropyState()
        # producer not yet completed → should block
        passed, reason = pipeline._check_gate("test")
        assert passed is False
        assert "type_fidelity" in reason
        assert "not yet measured" in reason

    def test_completed_producer_does_not_block(self):
        """After producer completes, unmeasured dimension doesn't block (uses actual score)."""
        pipeline = Pipeline()
        # "producer" produces type_fidelity
        producer = StubPhase("producer", post_verify=["type_fidelity"])
        pipeline.register(producer)
        # "test" has a precondition on type_fidelity
        phase = StubPhase("test", preconditions={"type_fidelity": 0.5})
        pipeline.register(phase)
        pipeline._entropy_state = PipelineEntropyState()
        # Mark producer as completed
        pipeline._completed = {"producer"}
        # type_fidelity not measured, but producer is complete → passes
        # (no active producer = dimension treated as unmeasured-and-ok)
        passed, reason = pipeline._check_gate("test")
        assert passed is True

    def test_completed_producer_with_bad_score_blocks(self):
        """After producer completes with a bad score, gate blocks on the actual score."""
        pipeline = Pipeline()
        producer = StubPhase("producer", post_verify=["type_fidelity"])
        pipeline.register(producer)
        phase = StubPhase("test", preconditions={"type_fidelity": 0.5})
        pipeline.register(phase)
        pipeline._entropy_state = PipelineEntropyState()
        pipeline._completed = {"producer"}
        # Producer ran and set a bad score
        pipeline._entropy_state.update_score("type_fidelity", 0.7)
        passed, reason = pipeline._check_gate("test")
        assert passed is False
        assert "0.70" in reason

    def test_unmeasured_sentinel_value(self):
        """Unmeasured + producible dimension uses -1.0 sentinel in violations."""
        state = PipelineEntropyState()
        violations = state.check_preconditions(
            {"type_fidelity": 0.5},
            producible_dimensions={"type_fidelity"},
        )
        assert "type_fidelity" in violations
        current, threshold = violations["type_fidelity"]
        assert current == -1.0
        assert threshold == 0.5

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
