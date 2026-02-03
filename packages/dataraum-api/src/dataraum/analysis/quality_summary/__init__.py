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
    build_quality_matrix,
    summarize_quality,
)
from dataraum.analysis.quality_summary.variance import (
    # Categorical slice filtering
    ColumnClassification,
    SliceFilterConfig,
    SliceVarianceMetrics,
    # Temporal filtering configs
    TemporalColumnFilterConfig,
    # Temporal filtering results
    TemporalColumnResult,
    TemporalDriftFilterConfig,
    TemporalDriftResult,
    TemporalSliceFilterConfig,
    TemporalSliceResult,
    classify_column,
    compute_slice_variance,
    filter_interesting_columns,
    # Temporal filtering functions
    filter_interesting_drift,
    filter_interesting_temporal_columns,
    filter_interesting_temporal_slices,
    get_filter_config,
    is_interesting_drift,
    is_interesting_temporal_column,
    is_interesting_temporal_slice,
)

__all__ = [
    # Main entry points
    "summarize_quality",
    "aggregate_slice_results",
    "build_quality_matrix",
    "QualitySummaryAgent",
    # Categorical variance filtering
    "ColumnClassification",
    "SliceVarianceMetrics",
    "SliceFilterConfig",
    "compute_slice_variance",
    "classify_column",
    "filter_interesting_columns",
    "get_filter_config",
    # Temporal filtering configs
    "TemporalSliceFilterConfig",
    "TemporalColumnFilterConfig",
    "TemporalDriftFilterConfig",
    # Temporal filtering results
    "TemporalSliceResult",
    "TemporalColumnResult",
    "TemporalDriftResult",
    # Temporal filtering functions
    "is_interesting_temporal_slice",
    "is_interesting_temporal_column",
    "is_interesting_drift",
    "filter_interesting_temporal_slices",
    "filter_interesting_temporal_columns",
    "filter_interesting_drift",
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
