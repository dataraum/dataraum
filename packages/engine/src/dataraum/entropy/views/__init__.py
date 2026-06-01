"""Entropy views module - caller-specific context builders.

Layer 3 of the entropy framework - provides stable APIs for different consumers:
- build_for_query(): For query agent (query/agent.py)
- build_for_network(): For network inference + evidence view (mcp/server.py, graphs/context.py)

Each builder returns a view tailored to the caller's needs,
ensuring typed tables enforcement and appropriate data structure.
"""

from dataraum.entropy.views.network_context import (
    ColumnNetworkResult,
    ColumnNodeEvidence,
    EntropyForNetwork,
    build_for_network,
)
from dataraum.entropy.views.query_context import (
    EntropyForQuery,
    build_for_query,
)

__all__ = [
    # Query context
    "EntropyForQuery",
    "build_for_query",
    # Network context
    "ColumnNetworkResult",
    "ColumnNodeEvidence",
    "EntropyForNetwork",
    "build_for_network",
]
