"""Entropy views module - caller-specific context builders.

Layer 3 of the entropy framework - provides stable APIs for different consumers:
- build_for_query(): For query agent (query/agent.py)
- build_for_readiness(): For the readiness + evidence view (graphs/context.py)

Each builder returns a view tailored to the caller's needs,
ensuring typed tables enforcement and appropriate data structure.
"""

from dataraum.entropy.views.query_context import (
    EntropyForQuery,
    build_for_query,
)
from dataraum.entropy.views.readiness_context import (
    ColumnNodeEvidence,
    ColumnReadinessResult,
    EntropyForReadiness,
    build_for_readiness,
)

__all__ = [
    # Query context
    "EntropyForQuery",
    "build_for_query",
    # Readiness context
    "ColumnReadinessResult",
    "ColumnNodeEvidence",
    "EntropyForReadiness",
    "build_for_readiness",
]
