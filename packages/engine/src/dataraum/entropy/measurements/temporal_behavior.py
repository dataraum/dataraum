"""Temporal-behaviour adjudication — stock vs flow, teach-first (ADR-0009, DAT-445).

Is a measure column a STOCK (a carried-forward point-in-time level, like a balance —
must NOT be summed across periods) or a FLOW (a per-period movement, like a
transaction amount — summable)? Up to two pooled witnesses over the claim space
{stock, flow}; the pooling engine returns the posterior plus conflict ``C`` and
ignorance ``U``:

* **LLM claim** — the LLM's INDEPENDENT stock/flow read of the column (name + table
  context + sample values), produced in ``semantic_per_column``. Abstains on
  ``unsure``/absent.
* **structural reconciliation (DAT-491)** — the DATA-GROUNDED witness: the
  ``aggregation_lineage`` session phase discovers whether the column aggregates an
  event table, and the deterministic R_flow/R_stock residual statistic says HOW it
  reconciles (``per_period`` → flow, ``cumulative`` → stock), with the match rate
  as confidence. Abstains when no lineage reconciled (including every add_source
  detect — lineage rows are exact-run, written only by begin_session). This is the
  witness whose input is the data, not the name: the only one that can dissent
  when both name-readers are wrong together (the ambiguous-name regime where
  measured accuracy of prior+claim falls to ~chance, correlated).

Stock/flow is DATA-DETERMINED — the ontology cannot declare it (DAT-657): the same
concept (e.g. ``account_balance``) materializes as a FLOW (per-period movement) or a
STOCK (period-end level), a modelling choice the ontology can't know. So the concept
no longer votes; the live ``debit_balance`` case — the LLM reads the periodic
``trial_balance`` movement column as flow and the structural reconciliation agrees —
resolves to flow with no manufactured conflict. A lone or weak witness routes to
``U`` (ignorance about the column's behaviour), not low entropy (the doc-trap) — an
opaque column whose behaviour can't be determined is surfaced as ignorance, not
silently resolved. There is no teach for stock/flow itself (data-determined, DAT-657);
a mis-grounding is corrected on the grounding path, not by teaching a format here.

There is deliberately NO data-trajectory witness: the DAT-459 spike falsified the
time-series persistence statistic, and the DAT-445 kill-gate showed an LLM reading a
column's own trajectory is confidently WRONG on ambiguous shapes (trending flow,
mean-reverting stock) — stock/flow is not determinable from a column's own values.
The structural witness reconciles against the INDEPENDENT per-period movements of
the event table instead, which is robust exactly where the trajectory statistics
broke (see ``analysis/lineage/reconcile.py``).

Pure module: no DB, no LLM, no config. Reliabilities are documented placeholder
priors, calibrated later from generative families (DAT-450) — not tuned to a metric.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from dataraum.entropy.pooling import PoolResult, Witness, pool

# The canonical claim space. Order fixes the tuple layout passed to the pool.
CLAIM_SPACE: tuple[str, str] = ("stock", "flow")

# A witness within this of uniform is ABSTAINING — it has no opinion. Abstention is
# ignorance, not disagreement, so an abstaining witness is dropped before pooling
# rather than manufacturing conflict against a confident one (and pool([]) → U=1).
_OPINION_EPS = 1e-6

# A resolved label is CONTESTED only above a meaningful conflict level — aligned
# with the readiness low band (risk <= 0.3 is "ready"). Witnesses that agree on
# the label but differ in confidence produce small positive JSD (~0.02); reusing
# the abstention epsilon here made the flag fire on nearly every column and
# degraded the query agent's caveat signal to noise.
CONTESTED_MIN_CONFLICT = 0.3

# Default confidence when a present signal carries none (a declared behaviour / claim
# with no confidence still leans, just not at full strength).
_DEFAULT_CONFIDENCE = 0.7

# LLM claim label → P(stock) extreme. "unsure"/None → abstain.
_CLAIM_PSTOCK: dict[str, float] = {"stock": 1.0, "flow": 0.0}
# Reconciliation pattern → P(stock) extreme (DAT-491). Unknown/None → abstain.
_PATTERN_PSTOCK: dict[str, float] = {"cumulative": 1.0, "per_period": 0.0}

# Neutral uncalibrated FALLBACK — used only when no reliabilities are threaded in
# (direct/test callers). The SHIPPED, calibrated values live in the artifact
# dataraum-config/entropy/reliabilities.yaml (measured by the eval rig, DAT-450) and
# are passed via ``reliabilities=``. Per ADR-0009 the shipped r are
# estimated-with-provenance, never inline constants.
DEFAULT_RELIABILITIES: dict[str, float] = {
    "llm_claim": 0.6,
    "structural_reconciliation": 0.8,
}


@dataclass(frozen=True)
class ColumnTemporalAdjudication:
    """The pooled stock/flow verdict for one column + the witnesses behind it."""

    table: str
    column: str
    claim_field: str  # "temporal_behavior:{table}.{column}" — the claim-slot identity
    # ALL opinionated witnesses (EntropyObject provenance). NOTE (DAT-764): unlike
    # every other adjudication detector, this may be a SUPERSET of what feeds
    # ``result`` — when the data-grounded structural witness overrules a disagreeing
    # name-based claim, only the authoritative subset is pooled. Do not assume
    # ``result``'s (C, U) is recomputable from ``witnesses`` for this detector.
    witnesses: tuple[Witness, ...]
    result: PoolResult
    # The structural witness fired and pooled OUT a disagreeing ``llm_claim`` (DAT-764).
    # Carried so the resolved layer flags ``contested`` without recomputing the test.
    overruled: bool = False


def _distribution(p_stock: float) -> dict[str, float]:
    """A claim-space distribution from P(stock), clamped to [0, 1]."""
    p = min(1.0, max(0.0, p_stock))
    return {"stock": p, "flow": 1.0 - p}


def _witness(witness_id: str, distribution: Mapping[str, float], reliability: float) -> Witness:
    return Witness(
        witness_id=witness_id,
        distribution=tuple(distribution[label] for label in CLAIM_SPACE),
        reliability=reliability,
    )


def _has_opinion(witness: Witness) -> bool:
    """A witness has an opinion when its distribution is not (≈) uniform."""
    uniform = 1.0 / len(witness.distribution)
    return any(abs(p - uniform) > _OPINION_EPS for p in witness.distribution)


def _p_stock(witness: Witness) -> float:
    """The witness's P(stock) — its position on the stock(≥0.5)/flow(<0.5) line."""
    return witness.distribution[CLAIM_SPACE.index("stock")]


def _find(witnesses: tuple[Witness, ...], witness_id: str) -> Witness | None:
    """The opinionated witness with this id, or ``None`` if it abstained/absent."""
    return next((w for w in witnesses if w.witness_id == witness_id), None)


def _leaning(p_stock_extreme: float | None, confidence: float | None) -> dict[str, float]:
    """Lean toward an extreme P(stock), scaled by confidence; ``None`` → abstain.

    ``0.5 + (extreme − 0.5)·conf`` — at conf→0 the witness collapses to ``0.5``
    (abstains), at conf→1 it asserts the extreme. This is the grounding-conditional
    mechanism: a weak/contested grounding (low confidence) cannot confidently assert.
    """
    if p_stock_extreme is None:
        return _distribution(0.5)
    conf = _DEFAULT_CONFIDENCE if confidence is None else min(1.0, max(0.0, float(confidence)))
    return _distribution(0.5 + (p_stock_extreme - 0.5) * conf)


def llm_claim_distribution(claim: str | None, confidence: float | None) -> dict[str, float]:
    """The LLM's independent stock/flow read as a claim-space distribution."""
    return _leaning(_CLAIM_PSTOCK.get((claim or "").strip()), confidence)


def structural_reconciliation_distribution(
    pattern: str | None, match_rate: float | None
) -> dict[str, float]:
    """The reconciled aggregation pattern as a claim-space distribution (DAT-491).

    ``cumulative`` → stock, ``per_period`` → flow, scaled by the reconciliation's
    match rate (voting-entity fraction × agreement); absent lineage abstains.
    """
    return _leaning(_PATTERN_PSTOCK.get((pattern or "").strip()), match_rate)


def resolved_behaviour(adj: ColumnTemporalAdjudication) -> tuple[str | None, bool]:
    """The resolved temporal behaviour + a contested flag, from an adjudication.

    ``(label, contested)`` where label ∈ {"point_in_time", "additive", None} — None
    when no witness took a position (total ignorance). Inverse of the ontology
    vocabulary so the resolve write round-trips onto ColumnConcept (DAT-637).

    The label follows the AUTHORITATIVE posterior — which is the structural
    reconciliation alone whenever it fired and the name-based ``llm_claim`` disagreed
    (DAT-764), so the data decides stock/flow. ``contested`` records whether the two
    reads landed on opposite sides of the stock/flow line: pure observability for the
    teach/readiness lane (deliberately NOT rendered to SQL agents — resolve.py), and
    the readiness/loss lane keys on the pooled conflict SCORE, not this flag.
    """
    result = adj.result
    if not result.posterior:
        return None, False
    p_stock = result.posterior[CLAIM_SPACE.index("stock")]
    if abs(p_stock - 0.5) < _OPINION_EPS:
        # Exactly-uniform posterior (e.g. the zero-reliability fallback):
        # nobody was trusted — do not resolve a label, let alone an
        # "uncontested stock" via the >= tie-break.
        return None, False
    label = "point_in_time" if p_stock >= 0.5 else "additive"
    structural = _find(adj.witnesses, "structural_reconciliation")
    llm = _find(adj.witnesses, "llm_claim")
    if structural is not None and llm is not None:
        # Both independent reads present → they contest iff they land on opposite
        # sides of the stock/flow line. That is exactly the overrule condition
        # ``measure_temporal_behavior`` already computed — reuse it (single source
        # of truth), never recompute, so the two can't silently drift.
        contested = adj.overruled
    else:
        # A lone witness (or none) — no second read to contest it; fall back to the
        # pooled conflict for the llm-only / structural-only cases.
        contested = result.conflict > CONTESTED_MIN_CONFLICT
    return label, contested


def measure_temporal_behavior(
    table: str,
    column: str,
    *,
    llm_claim: str | None = None,
    llm_confidence: float | None = None,
    structural_pattern: str | None = None,
    structural_match_rate: float | None = None,
    reliabilities: Mapping[str, float] | None = None,
) -> ColumnTemporalAdjudication:
    """Adjudicate one column into ``(C, U)`` + a stock/flow posterior.

    Args:
        table, column: identity for the claim slot.
        llm_claim: the LLM's independent read (``stock`` / ``flow`` / ``unsure`` / None).
        llm_confidence: the LLM's confidence in that read.
        structural_pattern: the reconciled aggregation pattern (``per_period`` /
            ``cumulative`` / None — DAT-491; None = no lineage, witness abstains).
        structural_match_rate: the reconciliation's match rate as confidence.
        reliabilities: per-witness reliability overrides; defaults to
            :data:`DEFAULT_RELIABILITIES`.

    Returns:
        A :class:`ColumnTemporalAdjudication`. High ``result.conflict`` means the LLM
        read and the data-grounded structural reconciliation disagree; high
        ``ignorance`` means the column's behaviour is undetermined (→ ``investigate``
        + teach). Stock/flow is data-determined — the ontology no longer votes
        (DAT-657).
    """
    rel = reliabilities or DEFAULT_RELIABILITIES
    candidates = (
        _witness(
            "llm_claim",
            llm_claim_distribution(llm_claim, llm_confidence),
            rel["llm_claim"],
        ),
        _witness(
            "structural_reconciliation",
            structural_reconciliation_distribution(structural_pattern, structural_match_rate),
            rel.get(
                "structural_reconciliation", DEFAULT_RELIABILITIES["structural_reconciliation"]
            ),
        ),
    )
    # Only witnesses that take a position are pooled: an abstaining witness is
    # ignorance, not a conflicting party. Both abstain → pool([]) → C=0, U=1.
    opinionated = tuple(w for w in candidates if _has_opinion(w))
    # DAT-764 / DAT-657: stock/flow is DATA-DETERMINED, so the structural
    # reconciliation is authoritative when it fired a gated verdict this run. A
    # name-anchored ``llm_claim`` that DISAGREES with it is OVERRULED — pooled out,
    # not against it. The pool's conflict ``C`` is weight-robust (a reliability edge
    # cannot damp it — reliabilities.py), so a symmetric pool let a confident LLM
    # "balance"→stock read both flip the label on a moderate-match verdict AND
    # manufacture a readiness-blocking disagreement (``trial_balance.debit_balance``,
    # per_period@0.75 → stock). An AGREEING claim is KEPT — it corroborates (lower
    # ignorance); when structural abstained, the ``llm_claim`` stands alone. This is
    # the docstring's "the structural witness already wins" made literal.
    structural = _find(opinionated, "structural_reconciliation")
    llm = _find(opinionated, "llm_claim")
    overruled = (
        structural is not None
        and llm is not None
        and (_p_stock(llm) >= 0.5) != (_p_stock(structural) >= 0.5)
    )
    pooled = (structural,) if overruled and structural is not None else opinionated
    return ColumnTemporalAdjudication(
        table=table,
        column=column,
        claim_field=f"temporal_behavior:{table}.{column}",
        # Both opinionated reads are recorded (EntropyObject provenance); only the
        # authoritative set is POOLED into the (C, U) + posterior.
        witnesses=opinionated,
        result=pool(pooled),
        overruled=overruled,
    )
