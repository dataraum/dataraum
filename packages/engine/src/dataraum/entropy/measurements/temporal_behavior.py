"""Temporal-behaviour adjudication — stock vs flow (ADR-0009, DAT-445/DAT-459).

Is a period-keyed numeric column a STOCK (a carried-forward point-in-time level,
like a balance) or a FLOW (a per-period movement, like a transaction amount)? Two
independent witnesses each emit a distribution over that binary claim space; the
pooling engine (:mod:`dataraum.entropy.pooling`) returns the posterior plus
conflict ``C`` and ignorance ``U``:

* **structural reconciliation** — the data's own answer, reconciled against the
  INDEPENDENT per-period movements of the same accounts (the line-item table). A
  column whose VALUE equals the period movement is a flow; one whose CHANGE equals
  the period movement carries forward — a stock. This is robust exactly where a
  time-series persistence statistic (lag-1 autocorrelation / variance ratio) is
  not: a trending or seasonal flow still equals its per-period movement, and a
  mean-reverting stock still carries forward, so both — the cases that FALSIFIED
  the persistence signature (DAT-459 spike) — classify correctly here. It abstains
  when neither hypothesis reconciles (a wrong or missing movement anchor): a true
  reconciliation drives the winning residual to ~0, a misaligned anchor never does.
* **semantic claim** — the column's DECLARED temporal behaviour: the ontology's
  ``temporal_behavior`` property for the business concept the column maps to (a
  "balance" concept claims stock; a "movement/amount" concept claims flow),
  supplied by the detector. Abstains when no concept/behaviour is declared.

The live bug is the disagreement: a column NAMED like a balance (``debit_balance``)
whose data is actually a per-period flow — semantic says stock, structure says
flow → conflict ``C`` rises → ``investigate`` + a ``temporal_behavior`` teach
suggestion. No hard-coded name list; the disagreement *is* the signal.

Pure module: no DB, no config, no LLM. The loaders that produce the per-account
series + movement anchor and the column's semantic claim, and the persistence of
witnesses + the pooled ``EntropyObject``, are the detector slice. Reliabilities are
documented placeholder priors, calibrated later from generative families (DAT-450)
— not constants tuned to a metric.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from dataraum.entropy.pooling import PoolResult, Witness, pool

# The canonical claim space. Order fixes the tuple layout passed to the pool.
CLAIM_SPACE: tuple[str, str] = ("stock", "flow")

# A witness within this of uniform is ABSTAINING — it has no opinion. Abstention is
# ignorance, not disagreement, so an abstaining witness is dropped before pooling
# rather than manufacturing conflict against a confident one. A claim-vs-data
# conflict needs two parties that actually take a position.
_OPINION_EPS = 1e-6

# Min periods for an account series to be reconcilable (a 3-point diff is the
# floor at which a carry-forward vs per-period shape is distinguishable).
MIN_PERIODS = 4

# Residual scale at which a reconciliation still "counts" as a fit. Grounded, not
# tuned to a metric: on the real month-end-close corpus a correct reconciliation
# drives the winning residual to ~0 (and holds < ~0.25 under 25% reconciliation
# noise), while a wrong/misaligned movement anchor leaves the winning residual
# ~0.53 (scripts/dat459_structural_reconciliation.py, DAT-459). A tolerance of
# 0.45 sits in that empty gap: fit→0 (the account abstains) before the wrong-anchor
# regime, so a misaligned anchor never asserts stock or flow.
RECONCILE_TOLERANCE = 0.45

# Neutral uncalibrated FALLBACK — used only when no reliabilities are threaded in
# (direct/test callers). The SHIPPED, calibrated values live in the artifact
# dataraum-config/entropy/reliabilities.yaml (measured by the eval rig, DAT-450)
# and are passed via ``reliabilities=``. Per ADR-0009 the shipped r are
# estimated-with-provenance, never inline constants; these match the artifact's
# placeholder priors so direct callers behave identically until the rig has run.
DEFAULT_RELIABILITIES: dict[str, float] = {
    "structural_reconciliation": 0.8,
    "semantic_claim": 0.6,
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


def _reconcile_residuals(
    values: Sequence[float], movements: Sequence[float]
) -> tuple[float, float]:
    """Scale-free L1 residuals of the FLOW and STOCK hypotheses.

    * ``R_flow``  = Σ|value − movement|        / Σ|movement|        (column IS the movement)
    * ``R_stock`` = Σ|Δvalue − movement[1:]|   / Σ|movement[1:]|    (column CARRIES FORWARD)

    A genuine flow drives ``R_flow``→0, a genuine stock drives ``R_stock``→0; a
    wrong anchor leaves both large (→ the caller abstains).
    """
    denom = sum(abs(x) for x in movements) or 1.0
    r_flow = sum(abs(v - m) for v, m in zip(values, movements, strict=True)) / denom
    deltas = [values[i + 1] - values[i] for i in range(len(values) - 1)]
    tail = movements[1:]
    tail_denom = sum(abs(x) for x in tail) or 1.0
    r_stock = sum(abs(d - m) for d, m in zip(deltas, tail, strict=True)) / tail_denom
    return r_flow, r_stock


def _account_anchors(series: Mapping[str, Any]) -> list[list[float]]:
    """Candidate per-period movement anchors for one account.

    A column reconciles against the period aggregate it corresponds to — a gross
    debit column matches ``Σdebit``, a net balance matches ``Σ(debit−credit)`` — so
    the detector supplies several candidates under ``"anchors"`` and the best-fitting
    one is used (per hypothesis). ``"movements"`` is sugar for a single anchor.
    """
    anchors = series.get("anchors")
    if anchors is not None:
        return [[float(x) for x in a] for a in anchors]
    movements = series.get("movements")
    return [[float(x) for x in movements]] if movements is not None else []


def reconciliation_distribution(
    series_by_account: Mapping[str, Mapping[str, Any]],
    *,
    min_periods: int = MIN_PERIODS,
    tolerance: float = RECONCILE_TOLERANCE,
) -> dict[str, float]:
    """How the data reconciles — stock vs flow — against the movement anchors.

    Each account votes for the hypothesis (stock/flow) that reconciles, weighted by
    goodness-of-fit (1 when the winning residual is ~0, →0 as it approaches
    ``tolerance``). Each hypothesis is reconciled against its best-fitting candidate
    anchor (gross debit / gross credit / net …). An account whose best residual
    exceeds ``tolerance`` reconciles under neither hypothesis (wrong/missing anchor)
    and abstains. The column leans stock to the extent its accounts confidently
    reconcile as stock; a column whose accounts reconcile weakly, or split, stays at
    ``0.5`` (abstain).
    """
    direction_num = 0.0  # Σ p_stock_account · fit
    weight = 0.0  # Σ fit
    n_voting = 0
    for series in series_by_account.values():
        values = [float(v) for v in series["values"]]
        anchors = [
            a
            for a in _account_anchors(series)
            if len(a) == len(values) and sum(abs(x) for x in a) > 0
        ]
        if len(values) < min_periods or not anchors:
            continue
        best_flow = min(_reconcile_residuals(values, a)[0] for a in anchors)
        best_stock = min(_reconcile_residuals(values, a)[1] for a in anchors)
        best = min(best_flow, best_stock)
        fit = max(0.0, 1.0 - best / tolerance)
        if fit <= 0.0:
            continue  # nothing reconciles for this account → no vote
        p_stock_account = 1.0 if best_stock < best_flow else 0.0
        direction_num += p_stock_account * fit
        weight += fit
        n_voting += 1
    if weight == 0.0 or n_voting == 0:
        return _distribution(0.5)  # no account reconciled → abstain
    direction = direction_num / weight  # 1 = all stock, 0 = all flow
    confidence = weight / n_voting  # mean fit = how cleanly accounts reconcile
    return _distribution(0.5 + (direction - 0.5) * confidence)


def semantic_distribution(semantic_claim: Mapping[str, float] | None) -> dict[str, float]:
    """The column's declared temporal behaviour as a claim-space distribution.

    ``semantic_claim`` is the detector's read of the ontology ``temporal_behavior``
    for the column's concept, e.g. ``{"stock": 0.9}``. ``None`` (no concept or no
    declared behaviour) abstains at ``0.5`` — without a claim there is nothing for
    the structure to disagree with.
    """
    if not semantic_claim:
        return _distribution(0.5)
    if "stock" in semantic_claim:
        return _distribution(float(semantic_claim["stock"]))
    if "flow" in semantic_claim:
        return _distribution(1.0 - float(semantic_claim["flow"]))
    return _distribution(0.5)


def measure_temporal_behavior(
    table: str,
    column: str,
    series_by_account: Mapping[str, Mapping[str, Sequence[float]]],
    semantic_claim: Mapping[str, float] | None = None,
    *,
    reliabilities: Mapping[str, float] | None = None,
) -> ColumnTemporalAdjudication:
    """Adjudicate one period-keyed column into ``(C, U)`` + a stock/flow posterior.

    Args:
        table: the focal table name (for the claim-slot identity).
        column: the column name.
        series_by_account: ``{account: {"values": [...], "movements": [...]}}`` —
            the column's per-account period series and the INDEPENDENT per-period
            net movement anchor (from the line-item table), period-aligned.
        semantic_claim: the column's declared temporal behaviour (ontology
            ``temporal_behavior`` of its concept), e.g. ``{"stock": 0.9}``; ``None``
            abstains.
        reliabilities: per-witness reliability overrides; defaults to
            :data:`DEFAULT_RELIABILITIES`.

    Returns:
        A :class:`ColumnTemporalAdjudication`. High ``result.conflict`` means the
        declared behaviour and the data disagree (the live ``debit_balance`` case).
    """
    rel = reliabilities or DEFAULT_RELIABILITIES
    candidates = (
        _witness(
            "structural_reconciliation",
            reconciliation_distribution(series_by_account),
            rel["structural_reconciliation"],
        ),
        _witness(
            "semantic_claim",
            semantic_distribution(semantic_claim),
            rel["semantic_claim"],
        ),
    )
    # Only witnesses that actually take a position are pooled: an abstaining witness
    # is ignorance, not a conflicting party (a structural read with no declared
    # concept, or a claim with no reconcilable data, must not manufacture conflict).
    witnesses = tuple(w for w in candidates if _has_opinion(w))
    return ColumnTemporalAdjudication(
        table=table,
        column=column,
        claim_field=f"temporal_behavior:{table}.{column}",
        witnesses=witnesses,
        result=pool(witnesses),
    )
