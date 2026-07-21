"""Relationship-discovery adjudication — is this relationship genuine? (ADR-0009).

For one directional column pair in the defined relationship catalog, the
witness classes already exist as ROWS on the pair
(``analysis/relationships`` ``detection_method``):

* **value overlap (the data witness)** — the structural ``candidate`` row's
  measured statistics: ``join_confidence`` = max(Jaccard, containment) over the
  actual column values (``analysis/relationships/joins.py``), damped toward
  uniform by its own ``statistical_confidence`` (exact = 1.0; sampled/minhash
  carry their estimation confidence). This witness reads the DATA, never the
  column names — the v6 entry criterion. Abstains when no candidate row was
  derived for the pair this run.
* **LLM judgment** — the ``llm`` row written by ``semantic_per_table``: the
  selector confirmed the pair as genuine, at its own measured ``confidence``.
  Abstains when there is no llm row (the pair entered the catalog by teach
  only; the decline signal is carried in evidence, not manufactured into an
  opinion — its strength is uncalibrated).
* **manual curation** — the ``manual`` row materialized from an ``add`` teach
  overlay: the user explicitly asserted the relationship, at the row's
  confidence. Abstains when absent.
* **keeper retention** — the ``keeper`` row materialized from a ``keep``
  overlay (silent acceptance, DAT-409): a promoted run's llm relationship
  the current run did not reproduce and the user never rejected. Weaker
  provenance than an explicit add — its own reliability ``r`` carries that,
  not an inline constant. Abstains when absent.

The detection-v1 case is the point: a 20%-orphan FK the LLM confirms by name
has a lukewarm data witness (containment broken → ``join_confidence`` ≈ 0.59)
against a confident llm witness → conflict ``C`` rises → the relationship is
NOT banded genuine-and-quiet. Probe (2026-06-10, real pooling engine, observed
live values): injected-confirmed C = 0.147 vs clean-confirmed C = 0.013
(margin +0.134); keeper-vs-weak-overlap C = 0.333 vs clean keeper C = 0.000.

Pure module: no DB, no LLM, no config. Reliabilities are cold-start fallbacks
only; the shipped values live in ``dataraum-config/entropy/reliabilities.yaml``
(placeholder priors until the eval rig measures them).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from dataraum.entropy.pooling import PoolResult, Witness, pool

# The canonical claim space. Order fixes the tuple layout passed to the pool.
CLAIM_SPACE: tuple[str, str] = ("genuine", "spurious")

# A witness within this of uniform is ABSTAINING — dropped before pooling
# (abstention is ignorance, not disagreement; same convention as null_semantics
# and temporal_behavior).
_OPINION_EPS = 1e-6

# Neutral uncalibrated FALLBACK — used only when no reliabilities are threaded
# in (direct/test callers). The SHIPPED values live in the artifact
# dataraum-config/entropy/reliabilities.yaml (placeholder priors, calibrated
# later by the eval rig, DAT-450). Per ADR-0009 the shipped r are
# estimated-with-provenance, never inline constants.
DEFAULT_RELIABILITIES: dict[str, float] = {
    "value_overlap": 0.7,
    "llm_judgment": 0.7,
    "manual_curation": 0.9,
    "keeper_retention": 0.5,
}


@dataclass(frozen=True)
class RelationshipAdjudication:
    """The pooled genuine/spurious verdict for one pair + the witnesses behind it."""

    claim_field: str  # "relationship_genuine" — the claim-slot identity
    witnesses: tuple[Witness, ...]
    result: PoolResult


def _distribution(p_genuine: float) -> dict[str, float]:
    """A claim-space distribution from P(genuine), clamped to [0, 1]."""
    p = min(1.0, max(0.0, p_genuine))
    return {"genuine": p, "spurious": 1.0 - p}


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


def _leaning(p_genuine_extreme: float | None, confidence: float | None) -> dict[str, float]:
    """Lean toward an extreme P(genuine), scaled by confidence; ``None`` → abstain.

    ``0.5 + (extreme − 0.5)·conf`` — the same grounding-conditional mechanism as
    temporal_behavior: at conf→0 the witness collapses to uniform, at conf→1 it
    asserts the extreme. Both inputs are MEASURED row values, never tuned.
    """
    if p_genuine_extreme is None or confidence is None:
        return _distribution(0.5)
    conf = min(1.0, max(0.0, float(confidence)))
    return _distribution(0.5 + (p_genuine_extreme - 0.5) * conf)


def value_overlap_distribution(
    join_confidence: float | None, statistical_confidence: float | None
) -> dict[str, float]:
    """The measured value-overlap statistic as a claim-space distribution.

    ``join_confidence`` (max of Jaccard and containment over actual values) IS
    the position over {genuine, spurious}; ``statistical_confidence`` (1.0 for
    exact computation, lower for sampled/minhash estimates) damps it toward
    uniform. No candidate row → abstain.
    """
    return _leaning(join_confidence, statistical_confidence)


def llm_judgment_distribution(llm_confidence: float | None) -> dict[str, float]:
    """The LLM selector's confirmation as a claim-space distribution.

    An llm row asserts genuine at the row's own measured confidence. No llm row
    → abstain (a decline's strength is uncalibrated; it rides in evidence).
    """
    return _leaning(None if llm_confidence is None else 1.0, llm_confidence)


def curation_distribution(row_confidence: float | None) -> dict[str, float]:
    """A teach-materialized row (manual add / keeper) as a claim-space distribution.

    The user's assertion (explicit or by silence) leans genuine at the row's
    confidence; how much to TRUST each kind is the per-witness reliability.
    Absent row → abstain.
    """
    return _leaning(None if row_confidence is None else 1.0, row_confidence)


def measure_relationship_discovery(
    *,
    join_confidence: float | None = None,
    statistical_confidence: float | None = None,
    llm_confidence: float | None = None,
    manual_confidence: float | None = None,
    keeper_confidence: float | None = None,
    reliabilities: Mapping[str, float] | None = None,
) -> RelationshipAdjudication:
    """Adjudicate one relationship pair into ``(C, U)`` + a genuine/spurious posterior.

    Args:
        join_confidence: the candidate row's measured value-overlap statistic
            (max(Jaccard, containment)); ``None`` = no candidate row this run.
        statistical_confidence: the overlap estimate's own confidence (exact =
            1.0; sampled/minhash lower); ``None`` = no candidate row.
        llm_confidence: the llm row's confidence; ``None`` = no llm row.
        manual_confidence: the manual row's confidence; ``None`` = no manual row.
        keeper_confidence: the keeper row's confidence; ``None`` = no keeper row.
        reliabilities: per-witness reliability overrides; defaults to
            :data:`DEFAULT_RELIABILITIES`.

    Returns:
        A :class:`RelationshipAdjudication`. High ``result.conflict`` means the
        witnesses disagree about whether the relationship is genuine (the
        orphan-broken-but-confirmed case); high ``ignorance`` means nobody
        qualified weighed in.
    """
    rel = reliabilities or DEFAULT_RELIABILITIES
    candidates = (
        _witness(
            "value_overlap",
            value_overlap_distribution(join_confidence, statistical_confidence),
            rel.get("value_overlap", DEFAULT_RELIABILITIES["value_overlap"]),
        ),
        _witness(
            "llm_judgment",
            llm_judgment_distribution(llm_confidence),
            rel.get("llm_judgment", DEFAULT_RELIABILITIES["llm_judgment"]),
        ),
        _witness(
            "manual_curation",
            curation_distribution(manual_confidence),
            rel.get("manual_curation", DEFAULT_RELIABILITIES["manual_curation"]),
        ),
        _witness(
            "keeper_retention",
            curation_distribution(keeper_confidence),
            rel.get("keeper_retention", DEFAULT_RELIABILITIES["keeper_retention"]),
        ),
    )
    # Only witnesses that take a position are pooled: an abstaining witness is
    # ignorance, not a conflicting party. All abstain → pool([]) → C=0, U=1.
    witnesses = tuple(w for w in candidates if _has_opinion(w))
    return RelationshipAdjudication(
        claim_field="relationship_genuine",
        witnesses=witnesses,
        result=pool(witnesses),
    )
