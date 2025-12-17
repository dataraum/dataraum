"""Relationship models.

Re-exports from analysis/relationships for backward compatibility.
The enrichment package will be removed in the future.
"""

from pydantic import BaseModel

# Re-export from canonical location
from dataraum_context.analysis.correlation.models import EnrichedRelationship

__all__ = ["EnrichedRelationship", "GraphAnalysisResult"]


class GraphAnalysisResult(BaseModel):
    """Result of relationship graph analysis.

    Contains cycle detection and connectivity metrics.
    """

    cycles: list[list[str]]
    """List of cycles, each cycle is a list of table IDs."""

    betti_0: int
    """Number of connected components (Betti-0 number)."""

    cycle_count: int
    """Total number of cycles detected."""

    node_count: int
    """Number of nodes (tables) in the graph."""

    edge_count: int
    """Number of edges (relationships) in the graph."""
