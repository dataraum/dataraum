"""Quality summary module.

LLM-powered analysis to generate data quality summaries per column,
aggregating findings across all slices of that column.
"""

from dataraum.analysis.quality_summary.agent import QualitySummaryAgent
from dataraum.analysis.quality_summary.db_models import (
    ColumnQualityReport,
    ColumnSliceProfile,
)
from dataraum.analysis.quality_summary.processor import summarize_quality

__all__ = [
    # Main entry point
    "summarize_quality",
    "QualitySummaryAgent",
    # DB Models
    "ColumnQualityReport",
    "ColumnSliceProfile",
]
