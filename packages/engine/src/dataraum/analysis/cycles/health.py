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
from dataraum.analysis.validation.evaluate import ValidationVerdict, evaluate_validation
from dataraum.analysis.validation.models import ValidationSpec, ValidationStatus
from dataraum.storage import Table

if TYPE_CHECKING:
    import duckdb
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
    """Aggregate health report for all cycles in a session run."""

    cycle_scores: list[CycleHealthScore] = field(default_factory=list)
    overall_health: float | None = None


def compute_cycle_health(
    session: Session,
    *,
    duckdb_conn: duckdb.DuckDBPyConnection | None,
    vertical: str,
    run_id: str | None,
) -> HealthReport:
    """Compute health scores for all detected cycles in a session run (DAT-455).

    Combines cycle completion rates (from LLM detection) with validation
    pass rates into a weighted composite score. The pass rate is computed ON
    DEMAND (DAT-617): each matched check's run-versioned ``sql_used`` is
    re-run against current data and judged fresh, never read from a stored
    verdict that goes stale on re-import.

    Args:
        session: SQLAlchemy session
        duckdb_conn: Connection scoped to the current typed lake — re-runs the
            validation SQL for the pass rate. ``None`` ⇒ no recompute; the
            pass rate stays absent and health falls back to completion alone.
        vertical: Vertical name (e.g. 'finance')
        run_id: The promoted operating_model run to read BOTH the detected
            cycles AND the validation results at. Both are run-versioned
            (DAT-455 / DAT-438); ``None`` (no promoted run) reads NOTHING —
            fail-closed, never a cross-run read that would mix superseded runs
            into this report.

    Returns:
        HealthReport with per-cycle scores and overall health.
    """
    # 1. Query detected cycles for this run. Both cycles and validation
    # results are run-versioned and coexist across runs (DAT-455) — fail-closed
    # on a missing run, never a cross-run read.
    if run_id is None:
        return HealthReport()

    cycles = session.scalars(
        select(DetectedBusinessCycle).where(
            DetectedBusinessCycle.run_id == run_id,
        )
    ).all()

    if not cycles:
        return HealthReport()

    # 2. Load this run's validation results (run-scoped, same operating_model run
    # as the cycles — their evidence describes one run, never a mix). Cycles
    # reference their tables by NAME (``tables_involved`` is the LLM's
    # ``table_name`` form — the same the per-table detector matches on), while
    # validation results store table_ids (UUIDs). Resolve the results' table_ids
    # → names by PK (bounded + unambiguous) so cycles and results are matched in
    # ONE namespace; comparing names against ids never intersects and would
    # silently null every validation_pass_rate.
    validation_results = list(
        session.scalars(
            select(ValidationResultRecord).where(ValidationResultRecord.run_id == run_id)
        ).all()
    )
    result_table_ids = {tid for vr in validation_results for tid in (vr.table_ids or [])}
    id_to_name: dict[str, str] = {}
    if result_table_ids:
        id_to_name = {
            t.table_id: t.table_name
            for t in session.scalars(
                select(Table).where(Table.table_id.in_(result_table_ids))
            ).all()
        }

    # 3. Compute per-cycle health scores
    scores: list[CycleHealthScore] = []
    for cycle in cycles:
        cycle_table_names = set(cycle.tables_involved or [])
        canonical = cycle.canonical_type

        # Find relevant validation spec IDs for this cycle type.
        # For known types, matches type-specific + universal validations.
        # For LLM-detected types not in vocabulary, matches universal validations only.
        relevant_spec_ids: set[str] = set()
        spec_by_id: dict[str, ValidationSpec] = {}
        if canonical:
            relevant_specs = get_validation_specs_for_cycles([canonical], vertical)
            relevant_spec_ids = {s.validation_id for s in relevant_specs}
            spec_by_id = {s.validation_id: s for s in relevant_specs}

        # Match validation results: must share a table with the cycle (compared
        # in NAME space via the id→name map) AND be a relevant spec.
        matched_results = [
            vr
            for vr in validation_results
            if vr.validation_id in relevant_spec_ids
            and _result_table_names(vr, id_to_name) & cycle_table_names
        ]

        # Pass rate is recomputed ON DEMAND (DAT-617): a stored verdict goes
        # stale on re-import, so re-run each matched check's run-versioned
        # ``sql_used`` against current data and judge it fresh. Counts JUDGED
        # measurements only (DAT-439): an unbound check (no ``sql_used`` —
        # skipped/generation-error) or an inconclusive re-run is ignorance, not
        # an assessment, so it stays out of numerator AND denominator (counting
        # it as passed=False would silently deflate cycle health). No
        # connection ⇒ no recompute; the pass rate stays absent.
        assessed: list[ValidationVerdict] = []
        if duckdb_conn is not None:
            for vr in matched_results:
                spec = spec_by_id.get(vr.validation_id)
                if spec is None or not vr.sql_used:
                    continue
                verdict = evaluate_validation(duckdb_conn, vr.sql_used, spec)
                if verdict.status == ValidationStatus.ERROR:
                    continue
                assessed.append(verdict)
        validations_run = len(assessed)
        validations_passed = sum(1 for v in assessed if v.passed)

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

    # 4. Overall health = mean of non-None composite scores
    composites = [s.composite_score for s in scores if s.composite_score is not None]
    overall = sum(composites) / len(composites) if composites else None

    return HealthReport(
        cycle_scores=scores,
        overall_health=overall,
    )


def _result_table_names(vr: ValidationResultRecord, id_to_name: dict[str, str]) -> set[str]:
    """Resolve a validation result's table_ids (UUIDs) to the table NAMES it touches.

    Uses ``id_to_name`` so results (id-keyed) can be matched against cycles
    (name-keyed); ids absent from the map are dropped.
    """
    return {id_to_name[tid] for tid in (vr.table_ids or []) if tid in id_to_name}


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
