"""Tests for pipeline base types and phase registry."""

from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.pipeline_config import load_phase_declarations
from dataraum.pipeline.registry import get_all_dependencies, get_phase_class, get_registry


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

    def test_import_phase_has_no_dependencies(self):
        declarations = load_phase_declarations()
        assert declarations["import"].dependencies == []

    def test_typing_depends_on_import(self):
        declarations = load_phase_declarations()
        assert "import" in declarations["typing"].dependencies

    def test_graph_execution_phase_exists(self):
        cls = get_phase_class("graph_execution")
        assert cls is not None


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


class TestDependencyResolution:
    """Tests for dependency resolution."""

    def test_get_all_dependencies_for_import(self):
        deps = get_all_dependencies("import")
        assert deps == set()

    def test_get_all_dependencies_for_typing(self):
        deps = get_all_dependencies("typing")
        assert deps == {"import"}

    def test_get_all_dependencies_for_statistics(self):
        deps = get_all_dependencies("statistics")
        assert "import" in deps
        assert "typing" in deps

    def test_get_all_dependencies_for_entropy_interpretation(self):
        deps = get_all_dependencies("entropy_interpretation")
        assert "import" in deps
        assert "typing" in deps
        assert "semantic" in deps
        assert "computation_review" in deps

    def test_unknown_phase_returns_empty(self):
        deps = get_all_dependencies("nonexistent")
        assert deps == set()
