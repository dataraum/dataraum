"""Temporal-behaviour adjudication — stock vs flow, teach-first (ADR-0009, DAT-445).

Is a measure column a STOCK (a carried-forward point-in-time level, like a balance —
must NOT be summed across periods) or a FLOW (a per-period movement, like a
transaction amount — summable)? Up to three pooled witnesses over the claim space
{stock, flow}; the pooling engine returns the posterior plus conflict ``C`` and
ignorance ``U``:

* **ontology prior (grounding-conditional)** — the column's concept temporal_behavior
  (``point_in_time`` → stock, ``additive`` → flow), with strength scaled by the
  GROUNDING CONFIDENCE: a contested or weak grounding collapses the prior toward
  ``0.5`` (→ ``U``; contests propagate, never hide). Abstains when no behaviour is
  declared.
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

The live ``debit_balance`` case is the disagreement: the concept claims a balance
(stock) but the LLM, reading the periodic ``trial_balance`` context + values, reads
flow → ``C`` rises → ``investigate`` + a ranked ``concept_property`` / ``rebind``
teach suggestion. A lone or weak witness routes to ``U`` (ignorance about the
column's behaviour), not low entropy (the doc-trap) — an opaque column whose
behaviour can't be determined is surfaced for teach, not silently resolved. The
teach enters as the dominant prior → next run → ``C`` → ε.

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
# degraded the query agent's caveat signal to noise (review finding C1).
CONTESTED_MIN_CONFLICT = 0.3

# Default confidence when a present signal carries none (a declared behaviour / claim
# with no confidence still leans, just not at full strength).
_DEFAULT_CONFIDENCE = 0.7

# Ontology temporal_behavior vocabulary → P(stock) extreme. Unknown/None → abstain.
_BEHAVIOUR_PSTOCK: dict[str, float] = {"point_in_time": 1.0, "additive": 0.0}
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
    "ontology_prior": 0.7,
    "llm_claim": 0.6,
    "structural_reconciliation": 0.8,
}


@dataclass(frozen=True)
class ColumnTemporalAdjudication:
    """The pooled stock/flow verdict for one column + the witnesses behind it."""

    table: str
    column: str
    claim_field: str  # "temporal_behavior:{table}.{column}" — the claim-slot identity
    witnesses: tuple[Witness, ...]
    result: PoolResult


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


def ontology_prior_distribution(
    temporal_behavior: str | None, grounding_confidence: float | None
) -> dict[str, float]:
    """The concept's declared temporal behaviour as a grounding-conditional claim.

    ``point_in_time`` → stock, ``additive`` → flow, scaled by how confidently the
    column is grounded to that concept; an unrecognised/absent behaviour abstains.
    """
    return _leaning(_BEHAVIOUR_PSTOCK.get((temporal_behavior or "").strip()), grounding_confidence)


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


def resolved_behaviour(result: PoolResult) -> tuple[str | None, bool]:
    """The resolved temporal behaviour + a contested flag, from a pooled result.

    ``(label, contested)`` where label ∈ {"point_in_time", "additive", None} — None
    when no witness took a position (total ignorance). ``contested`` is True when the
    pooled conflict is non-trivial: the resolved layer writes the label but flags it
    so a downstream SQL agent treats a contested stock with caution. Inverse of the
    ontology vocabulary so the resolve write round-trips onto ColumnConcept (DAT-637).
    """
    if not result.posterior:
        return None, False
    p_stock = result.posterior[CLAIM_SPACE.index("stock")]
    if abs(p_stock - 0.5) < _OPINION_EPS:
        # Exactly-uniform posterior (e.g. the zero-reliability fallback):
        # nobody was trusted — do not resolve a label, let alone an
        # "uncontested stock" via the >= tie-break.
        return None, False
    label = "point_in_time" if p_stock >= 0.5 else "additive"
    contested = result.conflict > CONTESTED_MIN_CONFLICT
    return label, contested


def measure_temporal_behavior(
    table: str,
    column: str,
    *,
    ontology_behaviour: str | None,
    grounding_confidence: float | None = None,
    llm_claim: str | None = None,
    llm_confidence: float | None = None,
    structural_pattern: str | None = None,
    structural_match_rate: float | None = None,
    reliabilities: Mapping[str, float] | None = None,
) -> ColumnTemporalAdjudication:
    """Adjudicate one column into ``(C, U)`` + a stock/flow posterior.

    Args:
        table, column: identity for the claim slot.
        ontology_behaviour: the concept's ``temporal_behavior`` (``point_in_time`` /
            ``additive`` / None).
        grounding_confidence: how confidently the column is grounded to that concept
            (weakens the prior; None → default lean).
        llm_claim: the LLM's independent read (``stock`` / ``flow`` / ``unsure`` / None).
        llm_confidence: the LLM's confidence in that read.
        structural_pattern: the reconciled aggregation pattern (``per_period`` /
            ``cumulative`` / None — DAT-491; None = no lineage, witness abstains).
        structural_match_rate: the reconciliation's match rate as confidence.
        reliabilities: per-witness reliability overrides; defaults to
            :data:`DEFAULT_RELIABILITIES`.

    Returns:
        A :class:`ColumnTemporalAdjudication`. High ``result.conflict`` means the
        declared behaviour and the LLM read disagree (the live ``debit_balance``
        case); high ``ignorance`` means the column's behaviour is undetermined
        (→ ``investigate`` + teach).
    """
    rel = reliabilities or DEFAULT_RELIABILITIES
    candidates = (
        _witness(
            "ontology_prior",
            ontology_prior_distribution(ontology_behaviour, grounding_confidence),
            rel["ontology_prior"],
        ),
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
    witnesses = tuple(w for w in candidates if _has_opinion(w))
    return ColumnTemporalAdjudication(
        table=table,
        column=column,
        claim_field=f"temporal_behavior:{table}.{column}",
        witnesses=witnesses,
        result=pool(witnesses),
    )
