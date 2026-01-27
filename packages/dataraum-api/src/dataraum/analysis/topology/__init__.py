"""Topological Data Analysis (TDA) Module.

This module provides domain-agnostic topological analysis for tabular data:
- Betti numbers extraction (connected components, cycles, voids)
- Persistence diagrams and entropy
- Homological stability assessment
- Cycle detection

For cross-table schema analysis, see relationships/graph_topology.py.

Usage:
    from dataraum.analysis.topology import (
        analyze_topological_quality,
        TableTopologyExtractor,
        BettiNumbers,
        TopologicalQualityResult,
    )
"""

# Analysis functions
from dataraum.analysis.topology.analyzer import analyze_topological_quality

# Pydantic models
from dataraum.analysis.topology.models import (
    BettiNumbers,
    CycleDetection,
    PersistenceDiagram,
    PersistencePoint,
    StabilityAnalysis,
    TopologicalAnomaly,
    TopologicalQualityResult,
)

# Stability functions (for temporal topology)
from dataraum.analysis.topology.stability import compute_bottleneck_distance

# TDA extraction
from dataraum.analysis.topology.tda.extractor import TableTopologyExtractor

__all__ = [
    # Analysis functions
    "analyze_topological_quality",
    # Pydantic models
    "BettiNumbers",
    "CycleDetection",
    "PersistenceDiagram",
    "PersistencePoint",
    "StabilityAnalysis",
    "TopologicalAnomaly",
    "TopologicalQualityResult",
    # Stability
    "compute_bottleneck_distance",
    # TDA extraction
    "TableTopologyExtractor",
]
