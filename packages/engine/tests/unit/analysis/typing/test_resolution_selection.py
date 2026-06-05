"""Tests for run-aware best-candidate selection in type resolution.

Pins the fix for the replay-poison bug: resolution persists its own
``automatic``/``fallback`` TypeDecision every run, and the old selection
honored ANY pre-existing decision as a human override — freezing the first
run's outcome forever (a fallback-VARCHAR row blocked taught patterns; an
automatic DATE row re-applied WITHOUT its standardization expr, plain-
TRY_CASTing DD.MM.YYYY columns to all-NULL). Selection now scopes candidates
to the current run and honors only ``manual`` decisions — keeping the
standardization expr for the decided type.
"""

from __future__ import annotations

from datetime import UTC, datetime

from dataraum.analysis.typing.db_models import TypeCandidate, TypeDecision
from dataraum.analysis.typing.resolution import _select_best_candidates
from dataraum.core.models.base import DataType
from dataraum.pipeline.registry import import_all_phase_models
from dataraum.storage import Column

# Constructing detached ORM objects triggers mapper configuration, which needs
# every string-annotated relationship target (TableEntity, ...) imported.
import_all_phase_models()

MIN_CONFIDENCE = 0.85


def _column(name: str = "tag_datum") -> Column:
    return Column(column_id="col-1", table_id="tbl-1", column_name=name, column_position=0)


def _candidate(
    run_id: str,
    data_type: str,
    confidence: float,
    pattern: str | None = None,
) -> TypeCandidate:
    return TypeCandidate(
        session_id="sess-1",
        column_id="col-1",
        run_id=run_id,
        data_type=data_type,
        confidence=confidence,
        detected_pattern=pattern,
    )


def _decision(run_id: str, source: str, decided_type: str) -> TypeDecision:
    return TypeDecision(
        session_id="sess-1",
        column_id="col-1",
        run_id=run_id,
        decided_type=decided_type,
        decision_source=source,
        decided_at=datetime.now(UTC),
    )


def test_prior_automatic_decision_does_not_freeze_the_column() -> None:
    """Run B re-decides from its own candidates despite run A's automatic row.

    The user-visible failure: run A fell back to VARCHAR, the taught date
    pattern produced a DATE candidate on run B — and the old code returned
    VARCHAR/"manual" off run A's decision, so teaching could never work.
    """
    col = _column()
    col.type_decisions.append(_decision("run-A", "fallback", "VARCHAR"))
    col.type_candidates.append(_candidate("run-A", "VARCHAR", 1.0))
    col.type_candidates.append(_candidate("run-B", "DATE", 0.95, pattern="eu_date"))

    (spec,) = _select_best_candidates([col], MIN_CONFIDENCE, run_id="run-B")

    assert spec.data_type == DataType.DATE
    assert spec.decision_source == "automatic"
    assert spec.pattern is not None
    assert spec.pattern.standardization_expr is not None
    assert "TRY_STRPTIME" in spec.pattern.standardization_expr


def test_prior_run_candidates_do_not_compete() -> None:
    """Run A's confidence-1.0 VARCHAR fallback must not outcompete run B's DATE."""
    col = _column()
    col.type_candidates.append(_candidate("run-A", "VARCHAR", 1.0))
    col.type_candidates.append(_candidate("run-B", "DATE", 0.9, pattern="eu_date"))

    (spec,) = _select_best_candidates([col], MIN_CONFIDENCE, run_id="run-B")

    assert spec.data_type == DataType.DATE


def test_manual_decision_pins_type_and_keeps_standardization_expr() -> None:
    """A human override pins the TYPE but the same-type candidate's pattern
    still supplies the standardization expr — honoring the type while dropping
    the expr would plain-TRY_CAST DD.MM.YYYY to an all-NULL column.

    The DATE candidate sits BELOW min_confidence (0.7 < 0.85) on purpose: a
    manual pin bypasses the threshold — even a weak same-type candidate's expr
    beats a plain cast.
    """
    col = _column()
    col.type_decisions.append(_decision("run-A", "manual", "DATE"))
    col.type_candidates.append(_candidate("run-B", "DATE", 0.7, pattern="eu_date"))
    col.type_candidates.append(_candidate("run-B", "VARCHAR", 1.0))

    (spec,) = _select_best_candidates([col], MIN_CONFIDENCE, run_id="run-B")

    assert spec.decision_source == "manual"
    assert spec.data_type == DataType.DATE
    assert spec.pattern is not None
    assert spec.pattern.standardization_expr is not None
    assert "TRY_STRPTIME" in spec.pattern.standardization_expr


def test_manual_decision_without_matching_candidate_plain_casts_and_warns() -> None:
    """No same-type candidate this run → the manual type is honored with a
    plain TRY_CAST (pattern None) and the degradation is logged loudly — for
    string-parsed types that cast can NULL every value, and silence here was
    part of the original destruction path."""
    from structlog.testing import capture_logs

    col = _column()
    col.type_decisions.append(_decision("run-A", "manual", "DATE"))
    col.type_candidates.append(_candidate("run-B", "VARCHAR", 1.0))

    with capture_logs() as logs:
        (spec,) = _select_best_candidates([col], MIN_CONFIDENCE, run_id="run-B")

    assert spec.decision_source == "manual"
    assert spec.data_type == DataType.DATE
    assert spec.pattern is None
    assert any(e["event"] == "manual_override_no_matching_candidate" for e in logs)


def test_no_run_candidates_falls_back_to_varchar() -> None:
    """A run with no candidates of its own falls back — it never borrows a
    prior run's."""
    col = _column()
    col.type_candidates.append(_candidate("run-A", "DATE", 0.95, pattern="eu_date"))

    (spec,) = _select_best_candidates([col], MIN_CONFIDENCE, run_id="run-B")

    assert spec.data_type == DataType.VARCHAR
    assert spec.decision_source == "fallback"
