"""Computation review checkpoint phase (Gate 3).

A no-op phase that sits after Zone 3 analysis phases (business_cycles,
validation). Its sole purpose is to act as a quality gate for Zone 3
detectors — those requiring VALIDATION or BUSINESS_CYCLES analyses.

Ensures cross-table inconsistencies and cycle-health problems are caught
and fixable before the Bayesian network incorporates them and before
metrics are computed.
"""

from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase


@analysis_phase
class ComputationReviewPhase(BasePhase):
    """Quality checkpoint — runs Zone 3 entropy detectors (Gate 3)."""

    @property
    def name(self) -> str:
        return "computation_review"

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """No-op — the checkpoint's value is in being a quality gate."""
        return PhaseResult.success(summary="Computation review checkpoint")

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Never skip — the checkpoint must always run to evaluate entropy."""
        return None
