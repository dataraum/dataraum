"""Topological Data Analysis (TDA) Module.

This module provides domain-agnostic topological analysis for tabular data:
- Betti numbers extraction (connected components, cycles, voids)
- Persistence diagrams and entropy
- Homological stability assessment
- Cycle detection

For cross-table schema analysis, see relationships/graph_topology.py.

Usage:
    from dataraum_context.analysis.topology import (
        analyze_topological_quality,
        TableTopologyExtractor,
        BettiNumbers,
        TopologicalQualityResult,
        TopologicalQualityMetrics,
    )
"""

# Analysis functions
from dataraum_context.analysis.topology.analyzer import analyze_topological_quality

# DB models
from dataraum_context.analysis.topology.db_models import TopologicalQualityMetrics

# Pydantic models
from dataraum_context.analysis.topology.models import (
    BettiNumbers,
    CycleDetection,
    PersistenceDiagram,
    PersistencePoint,
    StabilityAnalysis,
    TopologicalAnomaly,
    TopologicalQualityResult,
)

# Stability functions (for temporal topology)
from dataraum_context.analysis.topology.stability import compute_bottleneck_distance

# TDA extraction
from dataraum_context.analysis.topology.tda.extractor import TableTopologyExtractor

__all__ = [
    # Analysis functions
    "analyze_topological_quality",
    # DB models
    "TopologicalQualityMetrics",
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
