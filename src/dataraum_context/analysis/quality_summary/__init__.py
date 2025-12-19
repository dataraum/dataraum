"""Quality summary module.

LLM-powered analysis to generate data quality summaries per column,
aggregating findings across all slices of that column.
"""

from dataraum_context.analysis.quality_summary.agent import QualitySummaryAgent
from dataraum_context.analysis.quality_summary.db_models import (
    ColumnQualityReport,
    QualitySummaryRun,
)
from dataraum_context.analysis.quality_summary.models import (
    ColumnQualitySummary,
    QualitySummaryResult,
    SliceComparison,
)
from dataraum_context.analysis.quality_summary.processor import (
    aggregate_slice_results,
    summarize_quality,
)

__all__ = [
    # Main entry points
    "summarize_quality",
    "aggregate_slice_results",
    "QualitySummaryAgent",
    # Models
    "ColumnQualitySummary",
    "SliceComparison",
    "QualitySummaryResult",
    # DB Models
    "ColumnQualityReport",
    "QualitySummaryRun",
]
