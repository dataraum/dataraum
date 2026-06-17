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
    SlicingAnalysisResult,
)

__all__ = [
    # Main entry points
    "SlicingAgent",
    # Models
    "SliceRecommendation",
    "SlicingAnalysisResult",
    # DB Models
    "SliceDefinition",
]
