"""Quality review checkpoint phase.

A no-op phase that sits after semantic and before enriched_views.
Its sole purpose is to act as a quality gate — the scheduler auto-derives
which detectors to run, and this phase triggers contract assessment on all
accumulated scores.

In interactive mode (``dataraum fix``), this is where the pipeline pauses to
show all foundation entropy violations and allows the user to apply fixes
before proceeding.
"""

from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase


@analysis_phase
class QualityReviewPhase(BasePhase):
    """Quality checkpoint — runs foundation entropy detectors."""

    @property
    def name(self) -> str:
        return "quality_review"

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """No-op — the checkpoint's value is in being a quality gate."""
        return PhaseResult.success(summary="Quality review checkpoint")

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Never skip — the checkpoint must always run to evaluate entropy."""
        return None
