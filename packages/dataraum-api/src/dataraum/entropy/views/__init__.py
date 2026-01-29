"""Entropy views module - caller-specific context builders.

Layer 3 of the entropy framework - provides stable APIs for different consumers:
- build_for_graph(): For graph execution context (graphs/context.py)
- build_for_query(): For query agent (query/agent.py)
- build_for_dashboard(): For API endpoints (api/routers/entropy.py)

Each builder returns a view tailored to the caller's needs,
ensuring typed tables enforcement and appropriate data structure.
"""

from dataraum.entropy.views.dashboard_context import (
    EntropyForDashboard,
    build_for_dashboard,
)
from dataraum.entropy.views.graph_context import (
    EntropyForGraph,
    build_for_graph,
    format_entropy_for_prompt,
)
from dataraum.entropy.views.query_context import (
    EntropyForQuery,
    build_for_query,
)

__all__ = [
    # Graph context
    "EntropyForGraph",
    "build_for_graph",
    "format_entropy_for_prompt",
    # Query context
    "EntropyForQuery",
    "build_for_query",
    # Dashboard context
    "EntropyForDashboard",
    "build_for_dashboard",
]
