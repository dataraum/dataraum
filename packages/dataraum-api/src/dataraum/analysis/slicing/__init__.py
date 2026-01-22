"""Slicing analysis module.

LLM-powered analysis to identify optimal data slices for subset analysis.
Uses outputs from semantic, statistics, and correlation phases to recommend
the best categorical dimensions for slicing the data.
"""

from dataraum.analysis.slicing.agent import SlicingAgent
from dataraum.analysis.slicing.db_models import (
    SliceDefinition,
    SlicingAnalysisRun,
)
from dataraum.analysis.slicing.models import (
    SliceRecommendation,
    SliceSQL,
    SlicingAnalysisResult,
)
from dataraum.analysis.slicing.processor import (
    analyze_slices,
    execute_slices_from_definitions,
)
from dataraum.analysis.slicing.slice_runner import (
    SliceAnalysisResult,
    SliceTableInfo,
    TemporalSlicesResult,
    TopologySlicesResult,
    register_slice_tables,
    run_analysis_on_slices,
    run_temporal_analysis_on_slices,
    run_topology_on_slices,
)
from dataraum.analysis.slicing.utils import load_slicing_context

__all__ = [
    # Main entry points
    "analyze_slices",
    "execute_slices_from_definitions",
    "SlicingAgent",
    # Slice runner
    "register_slice_tables",
    "run_analysis_on_slices",
    "run_temporal_analysis_on_slices",
    "run_topology_on_slices",
    "SliceTableInfo",
    "SliceAnalysisResult",
    "TemporalSlicesResult",
    "TopologySlicesResult",
    # Utils
    "load_slicing_context",
    # Models
    "SliceRecommendation",
    "SliceSQL",
    "SlicingAnalysisResult",
    # DB Models
    "SliceDefinition",
    "SlicingAnalysisRun",
]
