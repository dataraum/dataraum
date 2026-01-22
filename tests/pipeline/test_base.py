"""Tests for pipeline base types."""

from dataraum_context.pipeline.base import (
    PIPELINE_DAG,
    PhaseResult,
    PhaseStatus,
    get_all_dependencies,
    get_phase_definition,
)


class TestPhaseResult:
    """Tests for PhaseResult."""

    def test_success(self):
        result = PhaseResult.success(
            outputs={"key": "value"},
            duration=1.5,
            records_processed=100,
            records_created=50,
        )
        assert result.status == PhaseStatus.COMPLETED
        assert result.outputs == {"key": "value"}
        assert result.duration_seconds == 1.5
        assert result.records_processed == 100
        assert result.records_created == 50
        assert result.error is None

    def test_failed(self):
        result = PhaseResult.failed("Something went wrong", duration=0.5)
        assert result.status == PhaseStatus.FAILED
        assert result.error == "Something went wrong"
        assert result.duration_seconds == 0.5

    def test_skipped(self):
        result = PhaseResult.skipped("Already done")
        assert result.status == PhaseStatus.SKIPPED
        assert result.error == "Already done"


class TestPipelineDAG:
    """Tests for the pipeline DAG definition."""

    def test_dag_not_empty(self):
        assert len(PIPELINE_DAG) > 0

    def test_all_phases_have_names(self):
        for phase in PIPELINE_DAG:
            assert phase.name
            assert isinstance(phase.name, str)

    def test_all_phases_have_descriptions(self):
        for phase in PIPELINE_DAG:
            assert phase.description
            assert isinstance(phase.description, str)

    def test_import_phase_has_no_dependencies(self):
        import_phase = get_phase_definition("import")
        assert import_phase is not None
        assert import_phase.dependencies == []

    def test_typing_depends_on_import(self):
        typing_phase = get_phase_definition("typing")
        assert typing_phase is not None
        assert "import" in typing_phase.dependencies

    def test_entropy_phase_exists(self):
        entropy_phase = get_phase_definition("entropy")
        assert entropy_phase is not None
        assert "statistics" in entropy_phase.dependencies
        assert "semantic" in entropy_phase.dependencies

    def test_graph_execution_phase_is_last(self):
        """Graph execution is now the final phase after context was merged into it."""
        graph_phase = get_phase_definition("graph_execution")
        assert graph_phase is not None
        assert PIPELINE_DAG[-1].name == "graph_execution"


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
