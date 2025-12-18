"""Slicing analysis module.

LLM-powered analysis to identify optimal data slices for subset analysis.
Uses outputs from semantic, statistics, and correlation phases to recommend
the best categorical dimensions for slicing the data.
"""

from dataraum_context.analysis.slicing.agent import SlicingAgent
from dataraum_context.analysis.slicing.db_models import (
    SliceDefinition,
    SlicingAnalysisRun,
)
from dataraum_context.analysis.slicing.models import (
    SliceRecommendation,
    SliceSQL,
    SlicingAnalysisResult,
)
from dataraum_context.analysis.slicing.processor import (
    analyze_slices,
    execute_slices_from_definitions,
)
from dataraum_context.analysis.slicing.utils import load_slicing_context

__all__ = [
    # Main entry points
    "analyze_slices",
    "execute_slices_from_definitions",
    "SlicingAgent",
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
