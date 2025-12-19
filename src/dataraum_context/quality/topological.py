"""Topological Quality Analysis - Re-exports from analysis.topology.

This module is a backward compatibility shim. The actual implementation
has been moved to analysis/topology/.

TODO: Update all imports to use analysis.topology directly, then remove this file.
"""

# Re-export analysis functions from new location
from dataraum_context.analysis.topology.analyzer import (
    analyze_topological_quality,
    analyze_topological_quality_multi_table,
)
from dataraum_context.analysis.topology.extraction import (
    compute_persistent_entropy,
    detect_persistent_cycles,
    extract_betti_numbers,
    process_persistence_diagrams,
)
from dataraum_context.analysis.topology.stability import (
    assess_homological_stability,
    compute_bottleneck_distance,
    compute_historical_complexity,
    get_previous_topology,
)

# Re-export from relationships package (was already a re-export)
from dataraum_context.enrichment.relationships.graph_analysis import (
    analyze_relationship_graph,
)

__all__ = [
    # Analysis functions
    "analyze_topological_quality",
    "analyze_topological_quality_multi_table",
    # Extraction functions
    "extract_betti_numbers",
    "process_persistence_diagrams",
    "compute_persistent_entropy",
    "detect_persistent_cycles",
    # Stability functions
    "assess_homological_stability",
    "compute_bottleneck_distance",
    "compute_historical_complexity",
    "get_previous_topology",
    # Relationship analysis (from relationships package)
    "analyze_relationship_graph",
]
