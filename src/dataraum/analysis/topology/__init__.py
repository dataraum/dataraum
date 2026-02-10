"""Topological Data Analysis (TDA) Module.

This module provides domain-agnostic topological analysis for tabular data:
- Betti numbers extraction (connected components, cycles, voids)
- Persistence diagrams and entropy
- Homological stability assessment (bottleneck distance)

For cross-table schema analysis, see relationships/graph_topology.py.
"""

from dataraum.analysis.topology.analyzer import analyze_topological_quality
from dataraum.analysis.topology.extraction import compute_persistent_entropy
from dataraum.analysis.topology.stability import compute_bottleneck_distance
from dataraum.analysis.topology.tda.extractor import TableTopologyExtractor

__all__ = [
    "analyze_topological_quality",
    "compute_bottleneck_distance",
    "compute_persistent_entropy",
    "TableTopologyExtractor",
]
