"""Witness and result types for the generic pooling engine (ADR-0009, DAT-457).

A *witness* is one opinion about a single canonical claim: a probability
distribution over the (small, fixed) claim space plus a measured reliability.
Pooling several witnesses yields a posterior and two *orthogonal* entropy
outputs — conflict and ignorance. See :mod:`dataraum.entropy.pooling.pool`.

This is adjudication-entropy substrate only. The statistical detectors
(``null_ratio``/``outlier_rate``/``benford``/``temporal_drift``/``slice_variance``)
are surprise entropy (``D_KL(observed || reference)``) and never enter the pool.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Witness:
    """One witness's opinion over a shared claim space.

    Attributes:
        witness_id: Stable identifier of the witness (e.g. ``"semantic_claim"``,
            ``"temporal_signature"``, ``"teach"``). Provenance; not used in math.
        distribution: Probabilities over the claim space. Need not be normalized
            on input; :func:`~dataraum.entropy.pooling.pool.pool` normalizes.
        reliability: Measured trust in ``[0, 1]``. Used as the log-linear pooling
            exponent and as the evidence-mass weight for ignorance. ``0`` drops
            the witness from the posterior without removing its row.
    """

    witness_id: str
    distribution: tuple[float, ...]
    reliability: float = 1.0


@dataclass(frozen=True)
class PoolResult:
    """Output of pooling: the posterior plus the ``(conflict, ignorance)`` split.

    Attributes:
        posterior: The pooled belief ``q`` over the claim space (sums to 1).
            Empty when there were no witnesses.
        conflict: ``C`` in ``[0, 1]`` — witnesses are individually confident but
            disagree with each other. Routes to ``investigate`` + teach.
        ignorance: ``U`` in ``[0, 1]`` — thin/uninformative evidence; nobody
            qualified has weighed in. Routes to "collect more evidence".
        n_witnesses: Number of witnesses pooled.
        evidence_mass: Effective informative evidence ``Σ rᵢ·certaintyᵢ`` that
            drives ``U``. Exposed for provenance / debugging (loud, not silent).
    """

    posterior: tuple[float, ...]
    conflict: float
    ignorance: float
    n_witnesses: int
    evidence_mass: float
