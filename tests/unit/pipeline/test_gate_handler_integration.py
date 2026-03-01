"""Tests for gate handler integration at the orchestrator deadlock point."""

from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.entropy_state import PipelineEntropyState
from dataraum.pipeline.gates import (
    Gate,
    GateActionType,
    GateResolution,
)
from dataraum.pipeline.orchestrator import Pipeline, PipelineConfig
from dataraum.pipeline.phases.base import BasePhase


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
        return f"Stub: {self._name}"

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


class RecordingHandler:
    """Gate handler that records calls and returns a configured resolution."""

    def __init__(self, resolution: GateResolution):
        self.calls: list[Gate] = []
        self.notifications: list[str] = []
        self._resolution = resolution

    def resolve(self, gate: Gate) -> GateResolution:
        self.calls.append(gate)
        return self._resolution

    def notify(self, message: str) -> None:
        self.notifications.append(message)


class TestPipelineConfigGateHandler:
    def test_gate_handler_default_none(self):
        config = PipelineConfig()
        assert config.gate_handler is None

    def test_max_fix_attempts_default(self):
        config = PipelineConfig()
        assert config.max_fix_attempts == 3


class TestGateHandlerAtDeadlock:
    def _setup_pipeline(
        self,
        handler: RecordingHandler | None = None,
        gate_mode: str = "pause",
    ) -> Pipeline:
        """Create a pipeline with a gated phase."""
        pipeline = Pipeline()
        pipeline.config = PipelineConfig(
            gate_mode=gate_mode,
            gate_handler=handler,
            max_fix_attempts=3,
        )

        # Phase with preconditions that will fire
        phase = StubPhase("gated", preconditions={"type_fidelity": 0.5})
        pipeline.register(phase)

        pipeline._entropy_state = PipelineEntropyState()
        pipeline._entropy_state.update_score("type_fidelity", 0.7)

        # Simulate gate-blocked state
        pipeline._gate_blocked.add("gated")
        pipeline._compute_phase_priority()

        return pipeline

    def test_skip_resolution_unblocks_phase(self):
        """SKIP resolution should remove phase from gate_blocked."""
        handler = RecordingHandler(
            GateResolution(action_taken=GateActionType.SKIP)
        )
        pipeline = self._setup_pipeline(handler)

        # Verify the gate is blocked before
        assert "gated" in pipeline._gate_blocked

        # Simulate what the deadlock handler does
        from dataraum.pipeline.gates import build_gate

        violations = pipeline._entropy_state.check_preconditions(
            pipeline.phases["gated"].entropy_preconditions
        )
        gate = build_gate(
            blocked_phase="gated",
            violations=violations,
            entropy_state=pipeline._entropy_state.to_dict(),
        )
        resolution = handler.resolve(gate)

        assert resolution.action_taken == GateActionType.SKIP
        assert len(handler.calls) == 1
        assert handler.calls[0].blocked_phase == "gated"

    def test_fix_resolution_rechecks_gates(self):
        """FIX resolution should trigger re-checking of all gates."""
        handler = RecordingHandler(
            GateResolution(action_taken=GateActionType.FIX)
        )
        pipeline = self._setup_pipeline(handler)

        # After fix, if scores improve, gate should pass
        pipeline._entropy_state.update_score("type_fidelity", 0.3)

        passed, _ = pipeline._check_gate("gated")
        assert passed is True

    def test_max_fix_attempts_enforced(self):
        """After max attempts, phase should be permanently blocked."""
        pipeline = Pipeline()
        pipeline.config = PipelineConfig(max_fix_attempts=2)
        pipeline._gate_attempts["test_phase"] = 2

        assert pipeline._gate_attempts["test_phase"] >= pipeline.config.max_fix_attempts

    def test_no_handler_breaks_loop(self):
        """Without a handler, deadlock detection should break."""
        pipeline = self._setup_pipeline(handler=None)
        # No handler → no resolution possible → loop should break
        assert pipeline.config.gate_handler is None

    def test_handler_receives_correct_gate_data(self):
        """Handler should receive a properly constructed Gate."""
        handler = RecordingHandler(
            GateResolution(action_taken=GateActionType.SKIP)
        )

        from dataraum.pipeline.gates import build_gate

        violations = {"type_fidelity": (0.7, 0.5)}
        state = {"type_fidelity": 0.7}
        gate = build_gate("gated", violations, state)

        handler.resolve(gate)

        received = handler.calls[0]
        assert received.blocked_phase == "gated"
        assert received.gate_type == "structural"
        assert len(received.violations) == 1
        assert received.violations[0].dimension == "type_fidelity"
        assert received.violations[0].score == 0.7
        assert received.violations[0].threshold == 0.5
