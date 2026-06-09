"""Null-semantics adjudication — the first pooled measurement (ADR-0009, DAT-457).

For each token the parser *rejected* in a column, is it a null marker
(``is-null``) or a genuine value (``is-value``)? Three witnesses each emit a
distribution over that binary claim space; the pooling engine
(:mod:`dataraum.entropy.pooling`) returns the posterior plus conflict ``C`` and
ignorance ``U``:

* **quarantine clustering** — a token that dominates the column's quarantine
  leans ``is-null`` (a sentinel rejected en masse), damped when the quarantine
  is thin.
* **type claim** — when the column resolved to a strict type and the token is
  among the cast failures, the cleaner the rest parsed the more this token reads
  as a sentinel; otherwise the witness abstains (``0.5``), since absence from
  the truncated ``failed_examples`` list is not evidence it parsed.
* **null vocabulary** — the vertical's curated null tokens (incl. ``null_value``
  teaches): a hit is strong ``is-null``; a miss leans mildly ``is-value``.

The novel-sentinel case is the whole point: a token quarantine + type call a
null marker but the vocabulary has never seen makes the vocabulary witness
disagree → **conflict** ``C`` rises → ``investigate`` + a ``null_value`` teach
suggestion. No hard-coded token list; the disagreement *is* the signal.

Pure module: no DB, no config, no LLM. The loaders that produce these input
shapes, and the persistence of witnesses + the pooled ``EntropyObject``, are the
detector slice (4b). Reliabilities are documented placeholder priors, calibrated
later from generative families (DAT-450) — not constants tuned to a metric.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from dataraum.entropy.pooling import PoolResult, Witness, pool

# The canonical claim space (identity comparison, ADR-0009 v4). Order fixes the
# tuple layout passed to the pooling engine.
CLAIM_SPACE: tuple[str, str] = ("is-null", "is-value")

# Column types that mean "no type was inferred" — the type witness has no signal.
_TEXT_TYPES = frozenset({"", "VARCHAR", "TEXT", "STRING", "CHAR"})

# Neutral uncalibrated FALLBACK — used only when no reliabilities are threaded in
# (direct/test callers). The SHIPPED, calibrated values live in the artifact
# dataraum-config/entropy/reliabilities.yaml (measured by the eval rig, DAT-450)
# and are loaded by the detector and passed via ``reliabilities=``. Per ADR-0009
# the shipped r are estimated-with-provenance, never inline constants; these
# match the artifact's placeholder priors so direct callers behave identically
# until the rig has run.
DEFAULT_RELIABILITIES: dict[str, float] = {
    "quarantine_clustering": 0.8,
    "type_claim": 0.7,
    "null_vocabulary": 0.6,
}

# Resolved-layer threshold: a token is a column null marker when its pooled
# posterior leans is-null past this. NOT a calibrated detection threshold — it
# reads the resolved BELIEF (conflict is carried separately as the open-conflict
# signal; a contested token still belongs here when the pool believes is-null).
RESOLVED_IS_NULL_THRESHOLD = 0.7


def resolved_null_tokens(
    evidence: Sequence[Mapping[str, Any]],
    *,
    is_null_threshold: float = RESOLVED_IS_NULL_THRESHOLD,
) -> list[str]:
    """The rejected tokens an adjudication resolved to is-null (ADR-0009 resolved layer).

    From a null_semantics EntropyObject's per-token ``evidence`` (each entry a
    ``{token, posterior: {is-null, is-value}, conflict, ignorance}`` summary),
    return the tokens whose pooled posterior leans is-null — the column's null
    markers the query agent should treat as NULL. Order-preserving; deduped.
    """
    tokens: list[str] = []
    seen: set[str] = set()
    for entry in evidence:
        posterior = entry.get("posterior") or {}
        token = entry.get("token")
        if token is None or float(posterior.get("is-null", 0.0)) <= is_null_threshold:
            continue
        token_str = str(token)
        if token_str not in seen:
            seen.add(token_str)
            tokens.append(token_str)
    return tokens


@dataclass(frozen=True)
class TokenAdjudication:
    """The pooled verdict for one rejected token + the witnesses behind it."""

    token: str
    claim_field: str  # "null_token:{token}" — the claim-slot identity
    witnesses: tuple[Witness, ...]
    result: PoolResult


def _distribution(p_is_null: float) -> dict[str, float]:
    """A claim-space distribution from P(is-null), clamped to [0, 1]."""
    p = min(1.0, max(0.0, p_is_null))
    return {"is-null": p, "is-value": 1.0 - p}


def _witness(witness_id: str, distribution: Mapping[str, float], reliability: float) -> Witness:
    return Witness(
        witness_id=witness_id,
        distribution=tuple(distribution[label] for label in CLAIM_SPACE),
        reliability=reliability,
    )


def quarantine_distribution(
    token_count: int,
    total_rejected: int,
    unique_tokens: int,
    *,
    volume_floor: float = 5.0,
) -> dict[str, float]:
    """How strongly the quarantine implies a token is a null marker — by clustering.

    A column's rejects cluster into a FEW distinct, repeated tokens when those
    are sentinels (``#ERR``, ``TBD``), and smear across MANY one-off values when
    they are just corruption. So ``is-null`` rises with:

    * ``concentration = 1 - unique_tokens/total_rejected`` — few distinct tokens
      dominating. NOT per-token *share*, which dilutes when several sentinels
      co-occur (5 sentinels at ~20% share each read as weak — the live-run
      finding, DAT-457); the cluster is the signal, not one token's slice of it.
    * ``repetition = 1 - 1/token_count`` — this token recurs (a one-off is a
      typo, not a sentinel).
    * ``volume_confidence`` — enough rejections to trust the shape.

    The witness never argues ``is-value``: being rejected is one-directional
    evidence; absent a cluster it abstains at ``0.5``.
    """
    if total_rejected <= 0 or unique_tokens <= 0 or token_count <= 0:
        return _distribution(0.5)
    concentration = max(0.0, 1.0 - unique_tokens / total_rejected)
    repetition = 1.0 - 1.0 / token_count
    volume_confidence = total_rejected / (total_rejected + volume_floor)
    return _distribution(0.5 + 0.5 * concentration * repetition * volume_confidence)


def type_distribution(token: str, typing_data: Mapping[str, Any]) -> dict[str, float]:
    """The type decision's read on a token — it votes only with evidence.

    When the column resolved to a strict type and the token is among the cast
    failures, ``is-null`` scales with how cleanly the rest parsed. Otherwise the
    witness abstains (``0.5``): a VARCHAR column inferred no type, and a token
    absent from the truncated ``failed_examples`` list may simply not be listed.
    """
    resolved_type = str(typing_data.get("resolved_type") or "").upper()
    if resolved_type in _TEXT_TYPES:
        return _distribution(0.5)
    failed = {str(value).strip() for value in (typing_data.get("failed_examples") or [])}
    if token.strip() not in failed:
        return _distribution(0.5)
    parse_success_rate = float(typing_data.get("parse_success_rate") or 0.0)
    return _distribution(0.5 + 0.5 * parse_success_rate)


def vocabulary_distribution(
    token: str,
    null_tokens: Sequence[str],
    *,
    hit_is_null: float = 0.9,
    miss_is_null: float = 0.3,
) -> dict[str, float]:
    """Whether the token is a curated null marker (case/whitespace-insensitive).

    A hit is strong ``is-null``; a miss leans mildly ``is-value`` (the list is
    curated but not exhaustive — a novel sentinel is a miss here, which is what
    makes this witness disagree with quarantine and surface conflict).
    """
    known = {str(value).strip().casefold() for value in null_tokens}
    is_known = token.strip().casefold() in known
    return _distribution(hit_is_null if is_known else miss_is_null)


def measure_null_semantics(
    quarantine_data: Mapping[str, Any],
    typing_data: Mapping[str, Any],
    null_tokens: Sequence[str],
    *,
    reliabilities: Mapping[str, float] | None = None,
) -> list[TokenAdjudication]:
    """Adjudicate every rejected token in a column into ``(C, U)`` + posterior.

    Args:
        quarantine_data: ``{"rejected_tokens": [{"token", "count"}], "total_rejected": int}``.
        typing_data: the ``load_typing`` shape (``resolved_type``,
            ``parse_success_rate``, ``failed_examples``, …).
        null_tokens: the vertical null vocabulary (``get_null_strings()``),
            already including any ``null_value`` teach overlays.
        reliabilities: per-witness reliability overrides; defaults to
            :data:`DEFAULT_RELIABILITIES`.

    Returns:
        One :class:`TokenAdjudication` per distinct rejected token.
    """
    rel = reliabilities or DEFAULT_RELIABILITIES
    rejected = quarantine_data.get("rejected_tokens") or []
    total_rejected = int(
        quarantine_data.get("total_rejected") or sum(int(t.get("count", 0)) for t in rejected)
    )
    unique_tokens = len(rejected)

    adjudications: list[TokenAdjudication] = []
    for entry in rejected:
        token = str(entry["token"])
        count = int(entry.get("count", 0))
        witnesses = (
            _witness(
                "quarantine_clustering",
                quarantine_distribution(count, total_rejected, unique_tokens),
                rel["quarantine_clustering"],
            ),
            _witness("type_claim", type_distribution(token, typing_data), rel["type_claim"]),
            _witness(
                "null_vocabulary",
                vocabulary_distribution(token, null_tokens),
                rel["null_vocabulary"],
            ),
        )
        adjudications.append(
            TokenAdjudication(
                token=token,
                claim_field=f"null_token:{token}",
                witnesses=witnesses,
                result=pool(witnesses),
            )
        )
    return adjudications
