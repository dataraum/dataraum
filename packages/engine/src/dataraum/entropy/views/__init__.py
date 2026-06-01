"""Entropy views module - caller-specific context builders.

Layer 3 of the entropy framework - provides stable APIs for different consumers:
- build_for_query(): contract gate for the query agent.
- load_persisted_readiness(): query-time readiness band — reads the snapshot the
  terminal detect step persisted (the single source of truth, DAT-399 slice D).
- build_column_evidence(): rollup-free raw evidence for the contract gate.
- build_for_readiness(): the full noisy-OR rollup — the detect step's computation
  (via persist_readiness), NOT a query-time path.

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
    build_column_evidence,
    build_for_readiness,
    load_persisted_readiness,
)

__all__ = [
    # Query context
    "EntropyForQuery",
    "build_for_query",
    # Readiness context
    "ColumnReadinessResult",
    "ColumnNodeEvidence",
    "EntropyForReadiness",
    "build_column_evidence",
    "build_for_readiness",
    "load_persisted_readiness",
]
