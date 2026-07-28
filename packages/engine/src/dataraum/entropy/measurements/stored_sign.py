"""Stored-sign adjudication — how a monetary balance's values are STORED (DAT-875).

Does a balance column carry each account family's natural direction already
(``natural_balance`` — a liability reads positive), or one raw ledger direction for
every family (``ledger_signed`` — a liability reads negative)? Up to two pooled
witnesses over the claim space {natural_balance, ledger_signed}; the pooling engine
returns the posterior plus conflict ``C`` and ignorance ``U``:

* **LLM claim** — the catalogue agent's INDEPENDENT read, authored in
  ``catalogue_semantics`` from the column's own range/sample, its table's axes and
  the ontology concept it binds. Abstains on ``unsure``/absent. This witness is
  structurally a NAME-AND-MARGINAL read: the agent's evidence is per column, never
  row-aligned, so it cannot observe a value's sign together with the account class
  of the same row — the very joint fact the question is about.
* **sign partition** — the DATA-GROUNDED witness. The ``aggregation_lineage`` phase
  reconciles the measure against an event table's per-period movements under a
  signed convention ``C``; the same entities are re-classified under ``-C`` and the
  two winning-pattern voter sets counted (``analysis/lineage/processor.py``). One
  set explaining every reconciling entity means one ledger direction fits all
  account families ⇒ ``ledger_signed``. Two DISJOINT sets each explaining a family
  means the stored values already absorbed the family's sign flip ⇒
  ``natural_balance``. Abstains when no lineage reconciled (every add_source detect
  included — lineage rows are exact-run).

Why this needed a typed fact at all: ``sign_natural_balance`` (the finance vertical's
convention) says how a measure is EXPRESSED, and explicitly not how it is STORED.
Three prompt revisions failed to stop a validation check from normalizing the GL side
to natural while trusting a raw stored ``ending_balance``, because no instruction can
substitute for a fact the model cannot see. Serving the fact is the fix; the SQL is
never rewritten (that would be a deterministic shadow over an agent's judgment).

The separation is ARITHMETIC, not tuned. A carried-forward series matched against a
sign-flipped anchor lands at residual ``Σ|-m - m| / Σ|m| = 2``, against the
reconciliation's wrong-anchor gate of 0.5 — a factor-4 margin that no draw of the
corpus was consulted to pick. The only judgment constant here is what counts as a
FAMILY rather than a stray voter, and it inherits the reconciliation's own
"a lone entity is not a verdict" minimum.

TWO LABEL-HONESTY CAVEATS — both are calibration watch items, and this is where the
next reader is meant to find them. What the partition measures with certainty is the
PARTITION (uniform vs split); mapping it onto the two NAMES rests on assumptions that
can fail:

1. **The convention is measured RELATIVE to the event table.** The witness compares a
   measure against event amounts under a signed convention and its negation, so it
   reads the measure's sign relative to THOSE amounts. The mapping split⇒
   ``natural_balance`` / uniform⇒``ledger_signed`` assumes the event side is
   family-BLIND — a raw ``debit``/``credit`` pair, which is how a journal stores its
   two sides. Were an event amount column itself family-normalized, both labels
   INVERT: a genuinely ``natural_balance`` measure would reconcile uniformly against
   it and be labelled ``ledger_signed``. Nothing in the data says which side is raw,
   so this assumption is load-bearing and unverified by the witness itself.
2. **A single-family population cannot distinguish the two.** Where every account in
   the reconciling population is debit-normal (an assets-only balance sheet, a
   P&L-only extract), the natural and ledger directions COINCIDE — no split can
   appear, so the witness mints a confident ``ledger_signed`` for a column the data
   cannot actually discriminate. Harmless for a comparison (no family needs flipping,
   so either reading yields the same SQL), but it is a confident label over an
   undetermined fact, and it will read as measured in any eval that counts labels.

Neither is defended against here, because both defences would need the account-class
axis — which is not machine-readable anywhere today (the vertical's ``concept_groups``
discard their labels at seed time, ``concept_edge_store``). Revisit if that changes.

Pure module: no DB, no LLM, no config. Reliabilities are documented placeholder
priors — the shipped calibrated values are measured by the eval rig (DAT-450) and
threaded in via ``reliabilities=``; there is no measured entry for this detector yet,
so it runs on the fallback below until one is calibrated.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from dataraum.entropy.pooling import PoolResult, Witness, pool

# The canonical claim space. Order fixes the tuple layout passed to the pool. The
# labels are IDENTICAL to the persisted vocabulary (``catalogue.models.STORED_SIGNS``,
# its single home) so the resolve write round-trips onto ColumnConcept with no
# translation table — deliberately unlike temporal_behavior's stock/flow →
# point_in_time/additive seam, which exists only because its two vocabularies were
# fixed independently. ``test_measurement_stored_sign`` pins the two sets equal.
NATURAL_BALANCE = "natural_balance"
LEDGER_SIGNED = "ledger_signed"
CLAIM_SPACE: tuple[str, str] = (NATURAL_BALANCE, LEDGER_SIGNED)

# A witness within this of uniform is ABSTAINING — it has no opinion. Abstention is
# ignorance, not disagreement, so an abstaining witness is dropped before pooling
# rather than manufacturing conflict against a confident one (and pool([]) → U=1).
_OPINION_EPS = 1e-6

# A resolved label is CONTESTED only above a meaningful conflict level — aligned with
# the readiness low band (risk <= 0.3 is "ready"), as for temporal_behavior.
CONTESTED_MIN_CONFLICT = 0.3

# Default confidence when a present claim carries none.
_DEFAULT_CONFIDENCE = 0.7

# An account FAMILY is a set of entities that reconcile together under one sign. A
# single entity is not a family — it is the same "a lone voter is ignorance, not a
# verdict" minimum the reconciliation itself applies (reconcile.MIN_ENTITIES_FIRED),
# restated here so this module stays pure. Below it, a stray mirror voter cannot
# turn a uniform partition into a split one, and a stray primary voter cannot claim
# the column reconciles at all.
MIN_FAMILY_ENTITIES = 2

# LLM claim label → P(natural_balance) extreme. "unsure"/None → abstain.
_CLAIM_PNATURAL: dict[str, float] = {NATURAL_BALANCE: 1.0, LEDGER_SIGNED: 0.0}

# Neutral uncalibrated FALLBACK — used when no reliabilities are threaded in. The
# claim is deliberately the weaker of the two: it is a name read on a question whose
# answer is not in its evidence (module docstring), where the partition reads the
# data directly. Both are placeholders pending DAT-450 measurement.
DEFAULT_RELIABILITIES: dict[str, float] = {
    "llm_claim": 0.5,
    "sign_partition": 0.8,
}


@dataclass(frozen=True)
class ColumnStoredSignAdjudication:
    """The pooled storage-convention verdict for one column + its witnesses."""

    table: str
    column: str
    claim_field: str  # "stored_sign:{table}.{column}" — the claim-slot identity
    # ALL opinionated witnesses (EntropyObject provenance). As for temporal_behavior
    # (DAT-764) this may be a SUPERSET of what feeds ``result``: when the data-grounded
    # partition overrules a disagreeing name-based claim, only the authoritative
    # subset is pooled. Do not assume ``result``'s (C, U) is recomputable from
    # ``witnesses``.
    witnesses: tuple[Witness, ...]
    result: PoolResult
    # The partition fired and pooled OUT a disagreeing ``llm_claim``.
    overruled: bool = False


def _distribution(p_natural: float) -> dict[str, float]:
    """A claim-space distribution from P(natural_balance), clamped to [0, 1]."""
    p = min(1.0, max(0.0, p_natural))
    return {NATURAL_BALANCE: p, LEDGER_SIGNED: 1.0 - p}


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


def _p_natural(witness: Witness) -> float:
    """The witness's P(natural_balance) — its side of the storage-convention line."""
    return witness.distribution[CLAIM_SPACE.index(NATURAL_BALANCE)]


def _find(witnesses: tuple[Witness, ...], witness_id: str) -> Witness | None:
    """The opinionated witness with this id, or ``None`` if it abstained/absent."""
    return next((w for w in witnesses if w.witness_id == witness_id), None)


def _leaning(p_natural_extreme: float | None, confidence: float | None) -> dict[str, float]:
    """Lean toward an extreme P(natural), scaled by confidence; ``None`` → abstain.

    ``0.5 + (extreme − 0.5)·conf`` — at conf→0 the witness collapses to ``0.5``
    (abstains), at conf→1 it asserts the extreme.
    """
    if p_natural_extreme is None:
        return _distribution(0.5)
    conf = _DEFAULT_CONFIDENCE if confidence is None else min(1.0, max(0.0, float(confidence)))
    return _distribution(0.5 + (p_natural_extreme - 0.5) * conf)


def llm_claim_distribution(claim: str | None, confidence: float | None) -> dict[str, float]:
    """The catalogue agent's independent storage-convention read as a distribution."""
    return _leaning(_CLAIM_PNATURAL.get((claim or "").strip()), confidence)


def sign_partition_distribution(
    n_entities: int | None,
    fired_primary: int | None,
    fired_mirror: int | None,
    fired_both: int | None,
) -> dict[str, float]:
    """The measured voter partition as a claim-space distribution.

    Args:
        n_entities: the pairing's entity population (the confidence denominator).
        fired_primary: entities voting the winning pattern under the convention.
        fired_mirror: entities voting it under the convention's NEGATION.
        fired_both: entities counted on both sides — degenerate (a near-dead anchor
            fits either sign), so they are removed from both rather than allowed to
            evidence a partition they cannot speak to.

    Returns:
        A distribution leaning ``natural_balance`` when two families of at least
        :data:`MIN_FAMILY_ENTITIES` split across the two signs, ``ledger_signed``
        when one such family absorbs every reconciling entity, and uniform
        (abstaining) otherwise — including when no partition was measured at all.

    Confidence is COVERAGE: the fraction of the entity population whose series
    actually reconciled under one sign or the other. Entities that reconciled under
    neither are unobserved, not evidence, so a partition read off a small corner of
    the population asserts weakly rather than confidently. That scaling is the whole
    confidence model — no coverage threshold is imposed on top of it, because a
    threshold would discard a weak-but-honest read that pooling already discounts.
    """
    if not n_entities or fired_primary is None or fired_mirror is None:
        return _distribution(0.5)
    both = fired_both or 0
    primary_only = max(0, fired_primary - both)
    mirror_only = max(0, fired_mirror - both)
    covered = primary_only + mirror_only
    if covered <= 0:
        return _distribution(0.5)
    coverage = min(1.0, covered / n_entities)
    major, minor = max(primary_only, mirror_only), min(primary_only, mirror_only)
    if major < MIN_FAMILY_ENTITIES:
        # Not even one family reconciles — a lone voter is ignorance, not a verdict.
        return _distribution(0.5)
    if minor >= MIN_FAMILY_ENTITIES:
        # Two disjoint families, opposite signs: the stored values already carry each
        # family's own direction.
        return _leaning(_CLAIM_PNATURAL[NATURAL_BALANCE], coverage)
    if minor == 0:
        # One sign explains every reconciling entity: the ledger's own direction was
        # stored, uniformly, across families.
        return _leaning(_CLAIM_PNATURAL[LEDGER_SIGNED], coverage)
    # A single dissenting entity is neither a family nor noise we can name — it is
    # exactly the ambiguity this witness must not resolve by rounding.
    return _distribution(0.5)


def resolved_stored_sign(adj: ColumnStoredSignAdjudication) -> tuple[str | None, bool]:
    """The resolved storage convention + a contested flag, from an adjudication.

    ``(label, contested)`` where label ∈ ``STORED_SIGNS`` ∪ {None} — None when no
    witness took a position (total ignorance), which the resolve pass writes THROUGH
    as NULL so a stale label cannot outlive the run that lost its evidence.

    The label follows the AUTHORITATIVE posterior — the sign partition alone whenever
    it fired and the name-based ``llm_claim`` disagreed, so the data decides. That is
    the temporal_behavior precedent (DAT-764) applied to a question where the case for
    it is stronger still: the claim's evidence provably does not contain the answer.
    """
    result = adj.result
    if not result.posterior:
        return None, False
    p_natural = result.posterior[CLAIM_SPACE.index(NATURAL_BALANCE)]
    if abs(p_natural - 0.5) < _OPINION_EPS:
        # Exactly-uniform posterior (e.g. the zero-reliability fallback): nobody was
        # trusted — do not resolve a label via a tie-break.
        return None, False
    label = NATURAL_BALANCE if p_natural > 0.5 else LEDGER_SIGNED
    partition = _find(adj.witnesses, "sign_partition")
    llm = _find(adj.witnesses, "llm_claim")
    if partition is not None and llm is not None:
        # Both reads present → they contest iff they land on opposite sides. That is
        # the overrule condition ``measure_stored_sign`` already computed; reuse it
        # (single source of truth) so the two cannot silently drift.
        contested = adj.overruled
    else:
        contested = result.conflict > CONTESTED_MIN_CONFLICT
    return label, contested


def measure_stored_sign(
    table: str,
    column: str,
    *,
    llm_claim: str | None = None,
    llm_confidence: float | None = None,
    n_entities: int | None = None,
    fired_primary: int | None = None,
    fired_mirror: int | None = None,
    fired_both: int | None = None,
    reliabilities: Mapping[str, float] | None = None,
) -> ColumnStoredSignAdjudication:
    """Adjudicate one column into ``(C, U)`` + a storage-convention posterior.

    Args:
        table, column: identity for the claim slot.
        llm_claim: the catalogue agent's read (a ``STORED_SIGNS`` value / ``unsure``
            / None).
        llm_confidence: the agent's confidence in that read.
        n_entities, fired_primary, fired_mirror, fired_both: the measured sign
            partition (``MeasureAggregationLineage``); absent ⇒ the witness abstains.
        reliabilities: per-witness reliability overrides; defaults to
            :data:`DEFAULT_RELIABILITIES`.

    Returns:
        A :class:`ColumnStoredSignAdjudication`. High ``result.conflict`` means the
        agent's read and the measured partition disagree; high ``ignorance`` means the
        storage convention is undetermined this run.
    """
    rel = reliabilities or DEFAULT_RELIABILITIES
    candidates = (
        _witness(
            "llm_claim",
            llm_claim_distribution(llm_claim, llm_confidence),
            rel.get("llm_claim", DEFAULT_RELIABILITIES["llm_claim"]),
        ),
        _witness(
            "sign_partition",
            sign_partition_distribution(n_entities, fired_primary, fired_mirror, fired_both),
            rel.get("sign_partition", DEFAULT_RELIABILITIES["sign_partition"]),
        ),
    )
    # Only witnesses that take a position are pooled: an abstaining witness is
    # ignorance, not a conflicting party. Both abstain → pool([]) → C=0, U=1.
    opinionated = tuple(w for w in candidates if _has_opinion(w))
    # The storage convention is DATA-DETERMINED, so the measured partition is
    # authoritative when it fired. A name-anchored ``llm_claim`` that DISAGREES is
    # OVERRULED — pooled out, not against it — because the pool's conflict is
    # weight-robust (a reliability edge cannot damp it), so a symmetric pool would let
    # a confident wrong claim both flip the label and manufacture a readiness-blocking
    # disagreement. An AGREEING claim is KEPT: it corroborates, lowering ignorance.
    partition = _find(opinionated, "sign_partition")
    llm = _find(opinionated, "llm_claim")
    overruled = (
        partition is not None
        and llm is not None
        and (_p_natural(llm) > 0.5) != (_p_natural(partition) > 0.5)
    )
    pooled = (partition,) if overruled and partition is not None else opinionated
    return ColumnStoredSignAdjudication(
        table=table,
        column=column,
        claim_field=f"stored_sign:{table}.{column}",
        witnesses=opinionated,
        result=pool(pooled),
        overruled=overruled,
    )
