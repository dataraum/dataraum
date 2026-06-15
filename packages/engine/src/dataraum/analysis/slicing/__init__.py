"""Slicing analysis module.

LLM-powered analysis to identify optimal data slices for subset analysis.
Uses outputs from semantic, statistics, and correlation phases to recommend
the best categorical dimensions for slicing the data.
"""

from dataraum.analysis.slicing.agent import SlicingAgent
from dataraum.analysis.slicing.db_models import (
    SliceDefinition,
)
from dataraum.analysis.slicing.models import (
    SliceRecommendation,
    SliceSQL,
    SlicingAnalysisResult,
)
from dataraum.analysis.slicing.slice_runner import (
    SliceTableInfo,
    register_slice_tables,
)

__all__ = [
    # Main entry points
    "SlicingAgent",
    # Slice runner
    "register_slice_tables",
    "SliceTableInfo",
    # Models
    "SliceRecommendation",
    "SliceSQL",
    "SlicingAnalysisResult",
    # DB Models
    "SliceDefinition",
]
