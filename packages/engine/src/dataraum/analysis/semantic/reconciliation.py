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

    ``observed`` is the normal, informative outcome — a measured delta with no
    declared band to grade it against — and is structured OUTPUT, not a warning:
    disclosing a number nobody asked to be within a bound is not an alarm.
    ``breached`` and ``failures`` are WARNINGS: a declared band was exceeded, or
    a grounding malfunctioned (it no longer executes, or it returned something
    that is not a quantity).

    Each channel maps an ASSERTION to its detail, and one assertion can cover
    several pairs with different outcomes — so an assertion may appear on more
    than one channel, and entries ACCUMULATE within a channel rather than
    overwrite (see :func:`_disclose`). The counts are per pair.
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

    assertions = _canonical_assertions(edges)
    groundings = _groundings_by_concept(session)
    executed: dict[str, Decimal | str] = {}
    rows: list[dict[str, Any]] = []

    for (frm, to), tolerance in sorted(assertions.items()):
        rows.extend(
            _evaluate_assertion(
                frm,
                to,
                tolerance=tolerance,
                vertical=vertical,
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
        assertions=len(assertions),
        rows=outcome.rows,
        evaluated=outcome.evaluated,
        abstained=outcome.abstained,
        breached=len(outcome.breached),
    )
    return outcome


def _canonical_assertions(edges: list[ConceptEdge]) -> dict[tuple[str, str], float | None]:
    """One entry per asserted concept pair, name-ordered, with its declared band.

    ``reconciles_with`` is SYMMETRIC and a partner assertion is stored in BOTH
    directions (the ``concept_edges`` contract), so reading the rows as they come
    would evaluate one comparison twice and record it under two keys — the same
    fact in two homes, and a doubled count in the phase's disclosure. Ordering
    the endpoints collapses the mirror pair to the single assertion it is.

    A band declared on either direction is the assertion's band; the mirror
    rows are two spellings of one statement, so taking the declared one over a
    NULL loses nothing. Should both directions ever declare DIFFERENT bands —
    unreachable today, since no producer writes a partner edge at all — the
    name-ordered direction wins, deterministically: ``edges`` arrives ordered by
    ``(from_concept, to_concept)`` and the first declared band is kept, so the
    outcome never depends on row order. Self-loops are already canonical.
    """
    out: dict[tuple[str, str], float | None] = {}
    for edge in edges:
        key = (
            (edge.from_concept, edge.to_concept)
            if edge.from_concept <= edge.to_concept
            else (edge.to_concept, edge.from_concept)
        )
        if key not in out or (out[key] is None and edge.tolerance is not None):
            out[key] = edge.tolerance
    return out


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


def _evaluate_assertion(
    frm: str,
    to: str,
    *,
    tolerance: float | None,
    vertical: str,
    groundings: dict[str, list[_Grounding]],
    executed: dict[str, Decimal | str],
    duckdb_conn: duckdb.DuckDBPyConnection,
    run_id: str,
    outcome: ReconciliationOutcome,
) -> list[dict[str, Any]]:
    """One assertion's rows — one per evaluated pair, or one abstention."""
    label = _label(frm, to)
    pairs = _pairs(frm, to, groundings)

    if not pairs:
        # The witness producer's assertion with no second SQL angle, or a
        # partner concept nothing grounded. Asserted and unevaluable is a fact
        # worth recording; silence would be indistinguishable from unasserted.
        outcome.abstained += 1
        _disclose(outcome.withheld, label, "no second grounding to compare")
        return [
            _abstained_row(frm, to, vertical, run_id, ReconciliationAbstainReason.NO_EVALUABLE_PAIR)
        ]

    if len(pairs) > MAX_PAIRS_PER_ASSERTION:
        outcome.truncated[label] = (
            f"{len(pairs) - MAX_PAIRS_PER_ASSERTION} of {len(pairs)} pairs not evaluated"
        )
        pairs = pairs[:MAX_PAIRS_PER_ASSERTION]

    return [
        _evaluate_pair(
            frm,
            to,
            left,
            right,
            tolerance=tolerance,
            vertical=vertical,
            executed=executed,
            duckdb_conn=duckdb_conn,
            run_id=run_id,
            outcome=outcome,
            label=label,
        )
        for left, right in pairs
    ]


def _pairs(
    frm: str, to: str, groundings: dict[str, list[_Grounding]]
) -> list[tuple[_Grounding, _Grounding]]:
    """The grounding pairs one assertion covers, in a deterministic order.

    The self-loop takes unordered pairs within one concept's groundings; a
    partner assertion takes the cross product of the two sides. The assertion is
    SYMMETRIC, so each pair is emitted once, ordered by snippet id — which also
    makes the truncation bound cut the same pairs on every run.
    """
    left_side = groundings.get(frm, [])
    if frm == to:
        return list(combinations(left_side, 2))
    right_side = groundings.get(to, [])
    return [(a, b) for a, b in product(left_side, right_side) if a.snippet_id != b.snippet_id]


def _evaluate_pair(
    frm: str,
    to: str,
    left: _Grounding,
    right: _Grounding,
    *,
    tolerance: float | None,
    vertical: str,
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
    row = _pair_row(frm, to, vertical, left, right, run_id)

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

    if tolerance is None:
        verdict = ReconciliationVerdict.NO_TOLERANCE_DECLARED
        _disclose(outcome.observed, label, _delta_text(delta, relative))
    elif relative <= Decimal(str(tolerance)):
        verdict = ReconciliationVerdict.WITHIN_TOLERANCE
    else:
        verdict = ReconciliationVerdict.BEYOND_TOLERANCE
        _disclose(
            outcome.breached,
            label,
            f"{_delta_text(delta, relative)} exceeds tolerance {tolerance:g}",
        )

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
#: Sentinel for a scalar that came back but is not a quantity (a VARCHAR, a date,
#: a boolean). Distinct from _NO_VALUE because it is a different fact: the
#: grounding measured SOMETHING, and that something cannot be reconciled.
_NON_NUMERIC = "\x00non_numeric"


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
    """An executed scalar as an exact Decimal, or a typed sentinel.

    NULL is NOT zero: an aggregate with no support measured nothing, and reading
    it as a zero would manufacture agreement (or a break) out of missing data.
    Floats convert through their string form so a binary artefact never shows up
    as a spurious delta.

    Anything that came back but is not a quantity — a VARCHAR, a date, a boolean
    — is NOT the same fact as no support, and collapsing the two would file a
    grounding that measures the wrong KIND of thing under "nothing to measure".
    It gets its own sentinel and is reported loudly.
    """
    if value is None:
        return _NO_VALUE
    if isinstance(value, bool):
        return _NON_NUMERIC
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except InvalidOperation, ValueError:
        return _NON_NUMERIC


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


def _label(frm: str, to: str) -> str:
    """How one assertion is named in the phase's disclosure."""
    return frm if frm == to else f"{frm}↔{to}"


def _pair_row(
    frm: str, to: str, vertical: str, left: _Grounding, right: _Grounding, run_id: str
) -> dict[str, Any]:
    """The identity and provenance columns shared by every pair row."""
    return {
        "run_id": run_id,
        "vertical": vertical,
        "from_concept": frm,
        "to_concept": to,
        "pair_key": f"{left.snippet_id}|{right.snippet_id}",
        "left_snippet_id": left.snippet_id,
        "right_snippet_id": right.snippet_id,
        "left_relation": left.relation,
        "right_relation": right.relation,
        "left_as_of": left.as_of,
        "right_as_of": right.as_of,
    }


#: Abstentions that mean something MALFUNCTIONED rather than that the executor
#: correctly declined to compare — these ride the phase's warning channel.
_LOUD_REASONS = frozenset(
    {
        ReconciliationAbstainReason.EXECUTION_FAILED,
        ReconciliationAbstainReason.NON_NUMERIC_VALUE,
    }
)


def _disclose(channel: dict[str, str], label: str, detail: str) -> None:
    """Record one pair's detail under its assertion, without losing the others.

    An assertion covering several pairs produces several details, and assigning
    them to the same key would keep only the last — three broken groundings
    would surface as one. The channel is a disclosure; dropping most of it
    defeats the point.
    """
    prior = channel.get(label)
    channel[label] = f"{prior}; {detail}" if prior else detail


def _abstain(
    row: dict[str, Any],
    reason: ReconciliationAbstainReason,
    outcome: ReconciliationOutcome,
    label: str,
    *,
    detail: str | None = None,
) -> dict[str, Any]:
    """Complete a pair row as a typed abstention, on exactly one channel.

    A malfunction — a grounding this run composed that no longer runs, or one
    returning something that is not a quantity — rides the warning channel.
    Every other reason is a correct refusal to compare and is disclosed as
    structured output.
    """
    outcome.abstained += 1
    channel = outcome.failures if reason in _LOUD_REASONS else outcome.withheld
    _disclose(channel, label, detail or reason.value)
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
    if observed == _NON_NUMERIC:
        return _abstain(
            row,
            ReconciliationAbstainReason.NON_NUMERIC_VALUE,
            outcome,
            label,
            detail=f"{side.snippet_id}: returned a non-numeric value",
        )
    return _abstain(
        row,
        ReconciliationAbstainReason.EXECUTION_FAILED,
        outcome,
        label,
        detail=f"{side.snippet_id}: {observed}",
    )


def _abstained_row(
    frm: str, to: str, vertical: str, run_id: str, reason: ReconciliationAbstainReason
) -> dict[str, Any]:
    """The assertion-level row for an assertion that formed no pair at all."""
    return {
        "run_id": run_id,
        "vertical": vertical,
        "from_concept": frm,
        "to_concept": to,
        "pair_key": PAIR_KEY_UNPAIRED,
        "status": ReconciliationStatus.ABSTAINED.value,
        "abstain_reason": reason.value,
    }


__all__ = ["MAX_PAIRS_PER_ASSERTION", "ReconciliationOutcome", "evaluate_reconciliations"]
