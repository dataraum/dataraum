"""Quality summary module.

LLM-powered analysis to generate data quality summaries per column,
aggregating findings across all slices of that column.
"""

from dataraum.analysis.quality_summary.agent import QualitySummaryAgent
from dataraum.analysis.quality_summary.db_models import (
    ColumnQualityReport,
    ColumnSliceProfile,
    QualitySummaryRun,
)
from dataraum.analysis.quality_summary.models import (
    ColumnQualitySummary,
    QualitySummaryResult,
    SliceColumnMatrix,
    SliceComparison,
    SliceQualityCell,
)
from dataraum.analysis.quality_summary.processor import (
    aggregate_slice_results,
    summarize_quality,
)
from dataraum.analysis.quality_summary.variance import (
    ColumnClassification,
    SliceFilterConfig,
    SliceVarianceMetrics,
    compute_slice_variance,
    filter_interesting_columns,
    get_filter_config,
)

__all__ = [
    # Main entry points
    "summarize_quality",
    "aggregate_slice_results",
    "QualitySummaryAgent",
    # Categorical variance filtering
    "ColumnClassification",
    "SliceVarianceMetrics",
    "SliceFilterConfig",
    "compute_slice_variance",
    "filter_interesting_columns",
    "get_filter_config",
    # Models
    "ColumnQualitySummary",
    "SliceComparison",
    "QualitySummaryResult",
    "SliceColumnMatrix",
    "SliceQualityCell",
    # DB Models
    "ColumnQualityReport",
    "ColumnSliceProfile",
    "QualitySummaryRun",
]
