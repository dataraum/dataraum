"""Tests for the quality_review checkpoint phase."""

from unittest.mock import MagicMock

from dataraum.pipeline.phases.quality_review_phase import QualityReviewPhase


class TestQualityReviewPhase:
    """Tests for QualityReviewPhase."""

    def test_name(self) -> None:
        phase = QualityReviewPhase()
        assert phase.name == "quality_review"

    def test_depends_on_semantic(self) -> None:
        phase = QualityReviewPhase()
        assert phase.dependencies == ["semantic"]

    def test_post_verification_lists_foundation_detectors(self) -> None:
        phase = QualityReviewPhase()
        pv = phase.post_verification
        # Structural
        assert "type_fidelity" in pv
        assert "join_path_determinism" in pv
        assert "relationship_quality" in pv
        # Value
        assert "null_ratio" in pv
        assert "outlier_rate" in pv
        # Semantic
        assert "naming_clarity" in pv
        assert "unit_declaration" in pv
        assert "time_role" in pv

    def test_post_verification_excludes_later_detectors(self) -> None:
        phase = QualityReviewPhase()
        pv = phase.post_verification
        # These need data from phases after quality_review
        assert "dimension_coverage" not in pv
        assert "formula_match" not in pv
        assert "temporal_drift" not in pv
        assert "cross_column_patterns" not in pv
        assert "column_quality" not in pv

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

    def test_fix_handlers_empty_by_default(self) -> None:
        phase = QualityReviewPhase()
        assert phase.fix_handlers == {}

    def test_registered_in_registry(self) -> None:
        from dataraum.pipeline.registry import get_registry

        registry = get_registry()
        assert "quality_review" in registry
