"""Tests for the quality_review checkpoint phase."""

from unittest.mock import MagicMock

from dataraum.pipeline.phases.quality_review_phase import QualityReviewPhase


class TestQualityReviewPhase:
    """Tests for QualityReviewPhase."""

    def test_name(self) -> None:
        phase = QualityReviewPhase()
        assert phase.name == "quality_review"

    def test_depends_on_semantic_and_statistical_quality(self) -> None:
        phase = QualityReviewPhase()
        assert "semantic" in phase.dependencies
        assert "statistical_quality" in phase.dependencies

    def test_is_quality_gate(self) -> None:
        phase = QualityReviewPhase()
        assert phase.is_quality_gate is True

    def test_produces_no_analyses(self) -> None:
        phase = QualityReviewPhase()
        assert phase.produces_analyses == set()

    def test_run_is_noop(self) -> None:
        phase = QualityReviewPhase()
        ctx = MagicMock()
        result = phase._run(ctx)
        assert result.status.value == "completed"
        assert result.summary == "Quality review checkpoint"

    def test_never_skips(self) -> None:
        phase = QualityReviewPhase()
        ctx = MagicMock()
        assert phase.should_skip(ctx) is None

    def test_registered_in_registry(self) -> None:
        from dataraum.pipeline.registry import get_registry

        registry = get_registry()
        assert "quality_review" in registry
