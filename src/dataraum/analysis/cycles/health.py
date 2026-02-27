"""Cycle health scoring.

Computes per-cycle health scores by combining cycle completion rates
with validation pass rates for cycle-relevant validations.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sqlalchemy import select

from dataraum.analysis.cycles.db_models import DetectedBusinessCycle
from dataraum.analysis.validation.config import get_validation_specs_for_cycles
from dataraum.analysis.validation.db_models import ValidationResultRecord

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@dataclass
class CycleHealthScore:
    """Health score for a single detected cycle."""

    cycle_id: str
    cycle_name: str
    canonical_type: str | None
    completion_rate: float | None
    validation_pass_rate: float | None
    validations_run: int
    validations_passed: int
    composite_score: float | None


@dataclass
class HealthReport:
    """Aggregate health report for all cycles in a source."""

    source_id: str
    cycle_scores: list[CycleHealthScore] = field(default_factory=list)
    overall_health: float | None = None


def compute_cycle_health(
    session: Session, source_id: str, *, vertical: str
) -> HealthReport:
    """Compute health scores for all detected cycles in a source.

    Combines cycle completion rates (from LLM detection) with validation
    pass rates (from validation results) into a weighted composite score.

    Args:
        session: SQLAlchemy session
        source_id: Source to compute health for
        vertical: Vertical name (e.g. 'finance')

    Returns:
        HealthReport with per-cycle scores and overall health
    """
    # 1. Query detected cycles for this source
    cycles = session.scalars(
        select(DetectedBusinessCycle).where(DetectedBusinessCycle.source_id == source_id)
    ).all()

    if not cycles:
        return HealthReport(source_id=source_id)

    # 2. Collect all table_ids across cycles for validation lookup
    all_table_ids: set[str] = set()
    for cycle in cycles:
        all_table_ids.update(cycle.tables_involved or [])

    # 3. Query validation results for those tables
    validation_results: list[ValidationResultRecord] = []
    if all_table_ids:
        validation_results = list(
            session.scalars(select(ValidationResultRecord)).all()
        )
        # Filter to results that share table_ids with our cycles
        validation_results = [
            vr
            for vr in validation_results
            if set(vr.table_ids or []) & all_table_ids
        ]

    # 4. Compute per-cycle health scores
    scores: list[CycleHealthScore] = []
    for cycle in cycles:
        cycle_table_ids = set(cycle.tables_involved or [])
        canonical = cycle.canonical_type

        # Find relevant validation spec IDs for this cycle type.
        # For known types, matches type-specific + universal validations.
        # For LLM-detected types not in vocabulary, matches universal validations only.
        relevant_spec_ids: set[str] = set()
        if canonical:
            relevant_specs = get_validation_specs_for_cycles([canonical], vertical)
            relevant_spec_ids = {s.validation_id for s in relevant_specs}

        # Match validation results: must overlap on table_ids AND be a relevant spec
        matched_results = [
            vr
            for vr in validation_results
            if vr.validation_id in relevant_spec_ids
            and set(vr.table_ids or []) & cycle_table_ids
        ]

        validations_run = len(matched_results)
        validations_passed = sum(1 for vr in matched_results if vr.passed)

        validation_pass_rate: float | None = None
        if validations_run > 0:
            validation_pass_rate = validations_passed / validations_run

        # Use LLM-provided completion_rate, or fall back to confidence
        # for cycles without transactional completion signals (e.g., reporting
        # cycles where the LLM couldn't derive a completion metric).
        effective_completion = cycle.completion_rate
        if effective_completion is None and validation_pass_rate is None:
            effective_completion = cycle.confidence

        composite = _compute_composite(effective_completion, validation_pass_rate)

        scores.append(
            CycleHealthScore(
                cycle_id=cycle.cycle_id,
                cycle_name=cycle.cycle_name,
                canonical_type=canonical,
                completion_rate=effective_completion,
                validation_pass_rate=validation_pass_rate,
                validations_run=validations_run,
                validations_passed=validations_passed,
                composite_score=composite,
            )
        )

    # 5. Overall health = mean of non-None composite scores
    composites = [s.composite_score for s in scores if s.composite_score is not None]
    overall = sum(composites) / len(composites) if composites else None

    return HealthReport(
        source_id=source_id,
        cycle_scores=scores,
        overall_health=overall,
    )


def _compute_composite(
    completion_rate: float | None,
    validation_pass_rate: float | None,
) -> float | None:
    """Weighted composite of completion rate and validation pass rate.

    Weights: 0.6 completion, 0.4 validation. Falls back to whichever
    signal is available, or None if neither.

    Note: the caller (compute_cycle_health) ensures at least one signal
    is present by falling back to detection confidence for completion_rate
    when both signals would otherwise be None.
    """
    if completion_rate is not None and validation_pass_rate is not None:
        return 0.6 * completion_rate + 0.4 * validation_pass_rate
    if completion_rate is not None:
        return completion_rate
    if validation_pass_rate is not None:
        return validation_pass_rate
    return None


__all__ = [
    "CycleHealthScore",
    "HealthReport",
    "compute_cycle_health",
]
