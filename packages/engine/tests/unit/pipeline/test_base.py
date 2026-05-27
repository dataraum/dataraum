"""Tests for pipeline base types and phase registry."""

from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.pipeline_config import load_phase_declarations
from dataraum.pipeline.registry import get_registry


class TestPhaseRegistry:
    """Tests for the phase registry."""

    def test_registry_not_empty(self):
        registry = get_registry()
        assert len(registry) > 0

    def test_all_phases_have_names(self):
        registry = get_registry()
        for name, cls in registry.items():
            instance = cls()
            assert instance.name == name
            assert isinstance(instance.name, str)

    def test_all_declared_phases_have_descriptions(self):
        declarations = load_phase_declarations()
        for name, decl in declarations.items():
            assert decl.description, f"Phase {name!r} has no description"
            assert isinstance(decl.description, str)


class TestBasePhaseProperties:
    """Tests for BasePhase runtime behavior."""

    def test_run_measures_duration(self):
        """BasePhase.run() sets duration_seconds on the result."""

        class SlowPhase(BasePhase):
            name = "slow"

            def _run(self, ctx: PhaseContext) -> PhaseResult:
                return PhaseResult.success(records_processed=1)

        from unittest.mock import MagicMock

        ctx = MagicMock(spec=PhaseContext)
        result = SlowPhase().run(ctx)
        assert result.duration_seconds > 0

    def test_run_measures_duration_on_failure(self):
        """BasePhase.run() sets duration even when _run raises."""

        class CrashPhase(BasePhase):
            name = "crash"

            def _run(self, ctx: PhaseContext) -> PhaseResult:
                raise RuntimeError("boom")

        from unittest.mock import MagicMock

        ctx = MagicMock(spec=PhaseContext)
        result = CrashPhase().run(ctx)
        assert result.status.value == "failed"
        assert result.duration_seconds > 0
