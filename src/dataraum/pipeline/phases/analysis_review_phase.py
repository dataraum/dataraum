"""Analysis review checkpoint phase (Gate 2).

A no-op phase that sits after enrichment phases (correlations, quality_summary,
temporal_slice_analysis). Its sole purpose is to act as a quality gate for
Zone 2 (enrichment) detectors — those requiring CORRELATION, SLICE_VARIANCE,
DRIFT_SUMMARIES, COLUMN_QUALITY_REPORTS, or ENRICHED_VIEW analyses.

Mirrors quality_review (Gate 1) which checks foundation detectors after
typing/statistics/relationships/semantic.
"""

from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase


@analysis_phase
class AnalysisReviewPhase(BasePhase):
    """Quality checkpoint — runs enrichment entropy detectors (Gate 2)."""

    @property
    def name(self) -> str:
        return "analysis_review"

    @property
    def description(self) -> str:
        return "Quality checkpoint after enrichment — runs Zone 2 entropy detectors"

    @property
    def dependencies(self) -> list[str]:
        return ["correlations", "quality_summary", "temporal_slice_analysis"]

    @property
    def is_quality_gate(self) -> bool:
        """Quality gates assess ALL accumulated scores against contracts."""
        return True

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """No-op — the checkpoint's value is in being a quality gate."""
        return PhaseResult.success(summary="Analysis review checkpoint")

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Never skip — the checkpoint must always run to evaluate entropy."""
        return None
