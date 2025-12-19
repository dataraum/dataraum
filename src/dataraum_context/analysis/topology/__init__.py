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
    )
"""

# TDA extraction
from dataraum_context.analysis.topology.tda.extractor import TableTopologyExtractor

# NOTE: Other imports will be added as files are migrated
# - analyzer.py: analyze_topological_quality, analyze_topological_quality_multi_table
# - models.py: BettiNumbers, CycleDetection, etc.
# - db_models.py: TopologicalQualityMetrics, etc.

__all__ = [
    # TDA extraction
    "TableTopologyExtractor",
]
