"""Generic opinion-pooling engine for adjudication entropy (ADR-0009, DAT-457).

Witnessed canonical claims pool into a posterior plus two orthogonal entropy
outputs: conflict ``C`` (witnesses disagree) and ignorance ``U`` (thin
evidence). Statistical/surprise detectors do not use this engine.
"""

from __future__ import annotations

from dataraum.entropy.pooling.models import PoolResult, Witness
from dataraum.entropy.pooling.pool import (
    jensen_shannon_divergence,
    log_linear_pool,
    pool,
    shannon_entropy,
)

__all__ = [
    "PoolResult",
    "Witness",
    "jensen_shannon_divergence",
    "log_linear_pool",
    "pool",
    "shannon_entropy",
]
