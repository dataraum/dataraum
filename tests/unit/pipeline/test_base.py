"""Tests for pipeline base types and phase registry."""

from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
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

    def test_all_phases_have_descriptions(self):
        registry = get_registry()
        for _name, cls in registry.items():
            instance = cls()
            assert instance.description
            assert isinstance(instance.description, str)

    def test_import_phase_has_no_dependencies(self):
        cls = get_phase_class("import")
        assert cls is not None
        assert cls().dependencies == []

    def test_typing_depends_on_import(self):
        cls = get_phase_class("typing")
        assert cls is not None
        assert "import" in cls().dependencies

    def test_entropy_phase_exists(self):
        cls = get_phase_class("entropy")
        assert cls is not None
        instance = cls()
        assert "semantic" in instance.dependencies
        assert "computation_review" in instance.dependencies

    def test_graph_execution_phase_exists(self):
        cls = get_phase_class("graph_execution")
        assert cls is not None


class TestBasePhaseProperties:
    """Tests for produces_analyses defaults and timing."""

    def test_default_produces_analyses_empty(self):
        class DummyPhase(BasePhase):
            name = "dummy"
            description = "test"
            dependencies: list[str] = []
            outputs: list[str] = []

            def _run(self, ctx: PhaseContext) -> PhaseResult:
                return PhaseResult.success()

        phase = DummyPhase()
        assert phase.produces_analyses == set()

    def test_run_measures_duration(self):
        """BasePhase.run() sets duration_seconds on the result."""

        class SlowPhase(BasePhase):
            name = "slow"
            description = "test"
            dependencies: list[str] = []

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
            description = "test"
            dependencies: list[str] = []

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

    def test_get_all_dependencies_for_entropy(self):
        deps = get_all_dependencies("entropy")
        # Entropy depends on statistics, semantic, relationships, correlations
        # Each of those has their own dependencies
        assert "import" in deps
        assert "typing" in deps
        assert "statistics" in deps
        assert "semantic" in deps
        assert "relationships" in deps
        assert "correlations" in deps

    def test_unknown_phase_returns_empty(self):
        deps = get_all_dependencies("nonexistent")
        assert deps == set()
