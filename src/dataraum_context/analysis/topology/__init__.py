"""Topological Data Analysis (TDA) Module.

This module provides domain-agnostic topological analysis for tabular data:
- Betti numbers extraction (connected components, cycles, voids)
- Persistence diagrams and entropy
- Homological stability assessment
- Cycle detection

For domain-specific analysis (e.g., financial cycle classification),
see domains/financial/cycles/.

Usage:
    from dataraum_context.analysis.topology import (
        analyze_topological_quality,
        analyze_topological_quality_multi_table,
        TableTopologyExtractor,
        BettiNumbers,
        TopologicalQualityResult,
        TopologicalQualityMetrics,
    )
"""

# Analysis functions
from dataraum_context.analysis.topology.analyzer import (
    analyze_topological_quality,
    analyze_topological_quality_multi_table,
)

# DB models
from dataraum_context.analysis.topology.db_models import (
    BusinessCycleClassification,
    MultiTableTopologyMetrics,
    TopologicalQualityMetrics,
)

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

# TDA extraction
from dataraum_context.analysis.topology.tda.extractor import TableTopologyExtractor

__all__ = [
    # Analysis functions
    "analyze_topological_quality",
    "analyze_topological_quality_multi_table",
    # DB models
    "BusinessCycleClassification",
    "MultiTableTopologyMetrics",
    "TopologicalQualityMetrics",
    # Pydantic models
    "BettiNumbers",
    "CycleDetection",
    "PersistenceDiagram",
    "PersistencePoint",
    "StabilityAnalysis",
    "TopologicalAnomaly",
    "TopologicalQualityResult",
    # TDA extraction
    "TableTopologyExtractor",
]
