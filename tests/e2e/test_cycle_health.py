"""E2E tests: verify cycle health scoring against real pipeline output.

Runs compute_cycle_health on the clean pipeline output (which has both
detected business cycles and validation results) and checks that the
health report is structurally sound and produces reasonable scores.

GROUND TRUTH: Do not modify assertions to fix failures — fix the production code instead.
"""

from __future__ import annotations

import pytest
from sqlalchemy.orm import Session

from dataraum.analysis.cycles.health import compute_cycle_health
from dataraum.pipeline.runner import RunResult

pytestmark = pytest.mark.e2e


class TestCycleHealth:
    """Verify cycle health scoring on real pipeline output."""

    def test_health_report_has_scores(
        self, pipeline_run: RunResult, metadata_session: Session
    ) -> None:
        """Health report should contain scores for detected cycles."""
        report = compute_cycle_health(
            metadata_session, pipeline_run.source_id, vertical="finance"
        )

        assert report.source_id == pipeline_run.source_id
        assert len(report.cycle_scores) > 0, "No cycle health scores produced"

    def test_each_cycle_has_composite_or_fallback(
        self, pipeline_run: RunResult, metadata_session: Session
    ) -> None:
        """Each cycle should have a composite score (or at least one signal)."""
        report = compute_cycle_health(
            metadata_session, pipeline_run.source_id, vertical="finance"
        )

        for score in report.cycle_scores:
            has_signal = (
                score.completion_rate is not None
                or score.validation_pass_rate is not None
            )
            assert has_signal, (
                f"Cycle '{score.cycle_name}' ({score.canonical_type}) "
                f"has neither completion_rate nor validation_pass_rate"
            )
            if has_signal:
                assert score.composite_score is not None

    def test_composite_scores_in_range(
        self, pipeline_run: RunResult, metadata_session: Session
    ) -> None:
        """Composite scores should be between 0 and 1."""
        report = compute_cycle_health(
            metadata_session, pipeline_run.source_id, vertical="finance"
        )

        for score in report.cycle_scores:
            if score.composite_score is not None:
                assert 0.0 <= score.composite_score <= 1.0, (
                    f"Cycle '{score.cycle_name}': composite_score={score.composite_score} out of range"
                )

    def test_overall_health_in_range(
        self, pipeline_run: RunResult, metadata_session: Session
    ) -> None:
        """Overall health should be between 0 and 1 when cycles exist."""
        report = compute_cycle_health(
            metadata_session, pipeline_run.source_id, vertical="finance"
        )

        if report.cycle_scores:
            assert report.overall_health is not None
            assert 0.0 <= report.overall_health <= 1.0
