"""Evaluate the ``reconciles_with`` assertion (DAT-739).

:mod:`dataraum.analysis.semantic.reconciles_with` DERIVES the assertion that two
computations of one quantity must tie out. This module EXECUTES it: it re-runs
the groundings the assertion covers, compares the numbers, and records what it
observed in ``concept_reconciliation``. Until this ran, "must tie out" was a
claim three surfaces rendered and nothing checked.

**What gets paired.** An edge's endpoints are concept names. The self-loop
(``from == to``, the only shape any live producer writes) pairs a concept's own
groundings against each other — the second angle the multi-grounding producer
counted. A partner edge pairs each of one concept's groundings against each of
the other's. Both reduce to: two sets of groundings, one set of unordered pairs.

**Comparability is checked before anything is subtracted.** Two numbers are only
a tie-out if they measure the same quantity in the same frame:

* Different ``aggregation`` (a SUM against a COUNT) computes different
  quantities. Abstain.
* Different bound reporting instants (DAT-887) measure different moments — a
  stock at Q1 close against a stock at Q2 close differs for calendar reasons
  that have nothing to do with agreement. Abstain, and record BOTH instants so
  the row says why. This case is abstained WITHOUT executing: the two numbers'
  only use would be the subtraction being refused.
* Different row RESTRICTIONS (``predicate``) are NOT a barrier. Reaching one
  quantity through two different populations — the general ledger's AP accounts
  against the subledger's whole AP table — is precisely what a second angle is.

**Nothing is re-authored.** Each grounding executes as the statement already
stored on it, through the same primitive the metric path uses
(:func:`~dataraum.query.execution.execute_sql_steps`) against the same
connection. The stored SQL is never inspected, parsed, or rewritten to work out
what it means: everything this module needs — the relation, the aggregation, the
bound instant — is read from typed columns and the persisted clause parts.

**No tolerance is invented.** Where the edge declares no band the row records
the observed delta under ``no_tolerance_declared``; the graded verdicts appear
only when a band actually exists. See
:mod:`dataraum.analysis.semantic.reconciliation_db_models` for why the database
enforces that rather than trusting this module to.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from itertools import combinations, product
from typing import Any

import duckdb
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.semantic.db_models import ConceptEdge, ConceptEdgePredicate
from dataraum.analysis.semantic.reconciliation_db_models import (
    PAIR_KEY_UNPAIRED,
    ConceptReconciliation,
    ReconciliationAbstainReason,
    ReconciliationStatus,
    ReconciliationVerdict,
)
from dataraum.core.logging import get_logger
from dataraum.query.execution import execute_sql_steps
from dataraum.query.snippet_models import SQLSnippetRecord
from dataraum.storage.upsert import upsert

logger = get_logger(__name__)

#: Upper bound on pairs evaluated for ONE assertion. A concept grounded many
#: ways would otherwise cost a quadratic number of comparisons; the pairs are
#: taken in a deterministic order and the remainder is disclosed as truncated,
#: never dropped silently. A resource guard on the work, not a threshold on the
#: data — nothing about a number's correctness depends on it.
MAX_PAIRS_PER_ASSERTION = 24

_UPSERT_KEY = ["vertical", "from_concept", "to_concept", "pair_key", "run_id"]


@dataclass(frozen=True)
class _Grounding:
    """One executable grounding, resolved from its snippet's typed fields."""

    snippet_id: str
    sql: str
    relation: str | None
    aggregation: str | None
    #: The resolved reporting instant, when this is a point-in-time extract.
    as_of: str | None


@dataclass
class ReconciliationOutcome:
    """What the evaluation produced, for the phase to disclose.

    Three channels, and an assertion appears in at most one. ``observed`` is the
    normal, informative outcome — a measured delta with no declared band to
    grade it against — and is structured OUTPUT, not a warning: disclosing a
    number nobody asked to be within a bound is not an alarm. ``breached`` and
    ``failures`` are WARNINGS: a declared band was exceeded, or a stored
    grounding that should still execute did not.
    """

    rows: int = 0
    evaluated: int = 0
    abstained: int = 0
    #: concept-pair → the observed delta, where no band is declared.
    observed: dict[str, str] = field(default_factory=dict)
    #: concept-pair → the observed delta, where a declared band was exceeded.
    breached: dict[str, str] = field(default_factory=dict)
    #: concept-pair → why an assertion could not be evaluated at all.
    withheld: dict[str, str] = field(default_factory=dict)
    #: concept-pair → a grounding that failed to execute.
    failures: dict[str, str] = field(default_factory=dict)
    #: concept-pair → how many pairs went un-evaluated under the bound.
    truncated: dict[str, str] = field(default_factory=dict)


def evaluate_reconciliations(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    *,
    vertical: str,
    run_id: str,
) -> ReconciliationOutcome:
    """Evaluate every active ``reconciles_with`` assertion for one vertical.

    Args:
        session: Workspace session (in-run — no ``current_*`` reads). Must
            already hold this run's derived edges.
        duckdb_conn: The connection the groundings were composed against; each
            stored statement re-executes against it verbatim.
        vertical: The workspace's vertical; concept names resolve within it.
        run_id: The operating_model run this evaluation is versioned by.

    Returns:
        The outcome, for the phase to disclose. Rows are written for every
        assertion — an unevaluable one records a typed abstention rather than
        nothing, so a silent table can only ever mean "nothing was asserted".
    """
    # Production sessions run autoflush=False, and the edges this evaluates were
    # written (and superseded) moments ago in the same session.
    session.flush()

    edges = list(
        session.execute(
            select(ConceptEdge)
            .where(
                ConceptEdge.vertical == vertical,
                ConceptEdge.predicate == ConceptEdgePredicate.RECONCILES_WITH.value,
                ConceptEdge.superseded_at.is_(None),
            )
            .order_by(ConceptEdge.from_concept, ConceptEdge.to_concept)
        ).scalars()
    )
    outcome = ReconciliationOutcome()
    if not edges:
        return outcome

    groundings = _groundings_by_concept(session)
    executed: dict[str, Decimal | str] = {}
    rows: list[dict[str, Any]] = []

    for edge in edges:
        rows.extend(
            _evaluate_edge(
                edge,
                groundings=groundings,
                executed=executed,
                duckdb_conn=duckdb_conn,
                run_id=run_id,
                outcome=outcome,
            )
        )

    if rows:
        upsert(session, ConceptReconciliation, rows, index_elements=_UPSERT_KEY)
        outcome.rows = len(rows)

    logger.info(
        "concept_reconciliation_evaluated",
        vertical=vertical,
        assertions=len(edges),
        rows=outcome.rows,
        evaluated=outcome.evaluated,
        abstained=outcome.abstained,
        breached=len(outcome.breached),
    )
    return outcome


def _groundings_by_concept(session: Session) -> dict[str, list[_Grounding]]:
    """Every healthy graph-authored extract, grouped by the concept it grounds.

    The membership test is the one ``reconciles_with`` derivation itself uses —
    a healthy EXTRACT minted by the graph agent, keyed to a concept by
    ``standard_field`` — so the executor evaluates exactly the grounding set the
    assertion was derived from. Diverging here would abstain on assertions whose
    support demonstrably exists.
    """
    snippets = session.execute(
        select(SQLSnippetRecord)
        .where(
            SQLSnippetRecord.snippet_type == "extract",
            SQLSnippetRecord.source.like("graph:%"),
            SQLSnippetRecord.failure_count == 0,
        )
        .order_by(SQLSnippetRecord.snippet_id)
    ).scalars()

    by_concept: dict[str, list[_Grounding]] = {}
    for snippet in snippets:
        concept = snippet.standard_field
        if not concept or not snippet.sql:
            continue
        parts = snippet.parts or {}
        relations = parts.get("from") or []
        binding = parts.get("period_binding") or {}
        by_concept.setdefault(concept, []).append(
            _Grounding(
                snippet_id=snippet.snippet_id,
                sql=snippet.sql,
                relation=str(relations[0]) if relations else None,
                aggregation=snippet.aggregation,
                as_of=binding.get("as_of") if isinstance(binding, dict) else None,
            )
        )
    return by_concept


def _evaluate_edge(
    edge: ConceptEdge,
    *,
    groundings: dict[str, list[_Grounding]],
    executed: dict[str, Decimal | str],
    duckdb_conn: duckdb.DuckDBPyConnection,
    run_id: str,
    outcome: ReconciliationOutcome,
) -> list[dict[str, Any]]:
    """One assertion's rows — one per evaluated pair, or one abstention."""
    label = _label(edge)
    pairs = _pairs(edge, groundings)

    if not pairs:
        # The witness producer's assertion with no second SQL angle, or a
        # partner concept nothing grounded. Asserted and unevaluable is a fact
        # worth recording; silence would be indistinguishable from unasserted.
        outcome.abstained += 1
        outcome.withheld[label] = "no second grounding to compare"
        return [_abstained_row(edge, run_id, ReconciliationAbstainReason.NO_EVALUABLE_PAIR)]

    if len(pairs) > MAX_PAIRS_PER_ASSERTION:
        outcome.truncated[label] = (
            f"{len(pairs) - MAX_PAIRS_PER_ASSERTION} of {len(pairs)} pairs not evaluated"
        )
        pairs = pairs[:MAX_PAIRS_PER_ASSERTION]

    return [
        _evaluate_pair(
            edge,
            left,
            right,
            executed=executed,
            duckdb_conn=duckdb_conn,
            run_id=run_id,
            outcome=outcome,
            label=label,
        )
        for left, right in pairs
    ]


def _pairs(
    edge: ConceptEdge, groundings: dict[str, list[_Grounding]]
) -> list[tuple[_Grounding, _Grounding]]:
    """The grounding pairs one assertion covers, in a deterministic order.

    The self-loop takes unordered pairs within one concept's groundings; a
    partner edge takes the cross product of the two sides. The assertion is
    SYMMETRIC, so each pair is emitted once, ordered by snippet id — which also
    makes the truncation bound cut the same pairs on every run.
    """
    left_side = groundings.get(edge.from_concept, [])
    if edge.from_concept == edge.to_concept:
        return list(combinations(left_side, 2))
    right_side = groundings.get(edge.to_concept, [])
    return [(a, b) for a, b in product(left_side, right_side) if a.snippet_id != b.snippet_id]


def _evaluate_pair(
    edge: ConceptEdge,
    left: _Grounding,
    right: _Grounding,
    *,
    executed: dict[str, Decimal | str],
    duckdb_conn: duckdb.DuckDBPyConnection,
    run_id: str,
    outcome: ReconciliationOutcome,
    label: str,
) -> dict[str, Any]:
    """Compare one pair of groundings, or record why it could not be compared."""
    # Canonical order: the assertion is symmetric, so the pair identity must not
    # depend on which side the enumeration happened to reach first.
    if right.snippet_id < left.snippet_id:
        left, right = right, left
    row = _pair_row(edge, left, right, run_id)

    if left.aggregation != right.aggregation:
        return _abstain(row, ReconciliationAbstainReason.DIFFERENT_AGGREGATIONS, outcome, label)
    if left.as_of != right.as_of:
        # Deliberately before execution — see the module note.
        return _abstain(
            row, ReconciliationAbstainReason.DIFFERENT_REPORTING_INSTANTS, outcome, label
        )
    if not left.relation or not right.relation:
        return _abstain(row, ReconciliationAbstainReason.UNRESOLVED_GROUNDING, outcome, label)

    left_value = _value_of(left, executed, duckdb_conn)
    if isinstance(left_value, str):
        return _abstain_for_observation(row, left, left_value, outcome, label)
    right_value = _value_of(right, executed, duckdb_conn)
    if isinstance(right_value, str):
        return _abstain_for_observation(row, right, right_value, outcome, label)

    delta = left_value - right_value
    relative = _relative_delta(left_value, right_value)
    tolerance = edge.tolerance

    if tolerance is None:
        verdict = ReconciliationVerdict.NO_TOLERANCE_DECLARED
        outcome.observed[label] = _delta_text(delta, relative)
    elif relative <= Decimal(str(tolerance)):
        verdict = ReconciliationVerdict.WITHIN_TOLERANCE
    else:
        verdict = ReconciliationVerdict.BEYOND_TOLERANCE
        outcome.breached[label] = f"{_delta_text(delta, relative)} exceeds tolerance {tolerance:g}"

    outcome.evaluated += 1
    row.update(
        status=ReconciliationStatus.EVALUATED.value,
        verdict=verdict.value,
        tolerance=tolerance,
        left_value=left_value,
        right_value=right_value,
        delta=delta,
        relative_delta=relative,
    )
    return row


#: Sentinel distinguishing "executed, no measured support" from an error string.
_NO_VALUE = "\x00no_value"


def _value_of(
    grounding: _Grounding,
    executed: dict[str, Decimal | str],
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> Decimal | str:
    """This grounding's number, or a failure string. Executed once per run.

    The stored statement runs verbatim through the metric path's own primitive.
    A grounding shared by several pairs executes ONCE — the value is a property
    of the grounding, not of the comparison.
    """
    cached = executed.get(grounding.snippet_id)
    if cached is not None:
        return cached

    result = execute_sql_steps([], grounding.sql, duckdb_conn)
    outcome: Decimal | str
    if not result.success or result.value is None:
        outcome = result.error or "execution returned no result"
    else:
        outcome = _as_decimal(result.value.final_value)
    executed[grounding.snippet_id] = outcome
    return outcome


def _as_decimal(value: Any) -> Decimal | str:
    """An executed scalar as an exact Decimal, or the no-value sentinel.

    NULL is NOT zero: an aggregate with no support measured nothing, and reading
    it as a zero would manufacture agreement (or a break) out of missing data.
    Floats convert through their string form so a binary artefact never shows up
    as a spurious delta.
    """
    if value is None or isinstance(value, bool):
        return _NO_VALUE
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except InvalidOperation, ValueError:
        return _NO_VALUE


def _relative_delta(left: Decimal, right: Decimal) -> Decimal:
    """``|left - right| / max(|left|, |right|)`` — what a declared band grades.

    A tolerance is declared as a fraction, so the comparison needs the delta on
    that scale. The denominator can only vanish when both sides are zero, which
    is exact agreement and reports as zero divergence.
    """
    scale = max(abs(left), abs(right))
    if scale == 0:
        return Decimal(0)
    return abs(left - right) / scale


def _delta_text(delta: Decimal, relative: Decimal) -> str:
    """The observed divergence, phrased as a measurement and nothing more."""
    return f"observed delta {delta:g} ({relative:.4g} relative)"


def _label(edge: ConceptEdge) -> str:
    """How one assertion is named in the phase's disclosure."""
    if edge.from_concept == edge.to_concept:
        return edge.from_concept
    return f"{edge.from_concept}↔{edge.to_concept}"


def _pair_row(
    edge: ConceptEdge, left: _Grounding, right: _Grounding, run_id: str
) -> dict[str, Any]:
    """The identity and provenance columns shared by every pair row."""
    return {
        "run_id": run_id,
        "vertical": edge.vertical,
        "from_concept": edge.from_concept,
        "to_concept": edge.to_concept,
        "pair_key": f"{left.snippet_id}|{right.snippet_id}",
        "left_snippet_id": left.snippet_id,
        "right_snippet_id": right.snippet_id,
        "left_relation": left.relation,
        "right_relation": right.relation,
        "left_as_of": left.as_of,
        "right_as_of": right.as_of,
    }


def _abstain(
    row: dict[str, Any],
    reason: ReconciliationAbstainReason,
    outcome: ReconciliationOutcome,
    label: str,
    *,
    detail: str | None = None,
) -> dict[str, Any]:
    """Complete a pair row as a typed abstention, on exactly one channel.

    An EXECUTION_FAILED abstention is the one that should not have happened — a
    grounding this run composed no longer runs — so it rides the warning
    channel. Every other reason is a correct refusal to compare and is
    disclosed as structured output.
    """
    outcome.abstained += 1
    if reason is ReconciliationAbstainReason.EXECUTION_FAILED:
        outcome.failures[label] = detail or reason.value
    else:
        outcome.withheld[label] = detail or reason.value
    row.update(status=ReconciliationStatus.ABSTAINED.value, abstain_reason=reason.value)
    return row


def _abstain_for_observation(
    row: dict[str, Any],
    side: _Grounding,
    observed: str,
    outcome: ReconciliationOutcome,
    label: str,
) -> dict[str, Any]:
    """Abstain because one side yielded no comparable number."""
    if observed == _NO_VALUE:
        return _abstain(
            row,
            ReconciliationAbstainReason.NO_VALUE,
            outcome,
            label,
            detail=f"{side.snippet_id}: no measured support",
        )
    return _abstain(
        row,
        ReconciliationAbstainReason.EXECUTION_FAILED,
        outcome,
        label,
        detail=f"{side.snippet_id}: {observed}",
    )


def _abstained_row(
    edge: ConceptEdge, run_id: str, reason: ReconciliationAbstainReason
) -> dict[str, Any]:
    """The assertion-level row for an assertion that formed no pair at all."""
    return {
        "run_id": run_id,
        "vertical": edge.vertical,
        "from_concept": edge.from_concept,
        "to_concept": edge.to_concept,
        "pair_key": PAIR_KEY_UNPAIRED,
        "status": ReconciliationStatus.ABSTAINED.value,
        "abstain_reason": reason.value,
    }


__all__ = ["MAX_PAIRS_PER_ASSERTION", "ReconciliationOutcome", "evaluate_reconciliations"]
