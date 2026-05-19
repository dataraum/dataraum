"""Temporal slicing analysis module.

Provides drift detection and period-level completeness/anomaly analysis
for slice data using Jensen-Shannon divergence and z-score methods.
"""

from dataraum.analysis.temporal_slicing.analyzer import (
    analyze_column_drift,
    analyze_period_metrics,
    persist_drift_results,
    persist_period_results,
)
from dataraum.analysis.temporal_slicing.db_models import (
    ColumnDriftSummary,
    TemporalSliceAnalysis,
)
from dataraum.analysis.temporal_slicing.models import (
    ColumnDriftResult,
    CompletenessResult,
    DriftEvidence,
    PeriodAnalysisResult,
    PeriodMetrics,
    TemporalSliceConfig,
    TimeGrain,
    VolumeAnomalyResult,
)

__all__ = [
    # Entry points
    "analyze_column_drift",
    "analyze_period_metrics",
    "persist_drift_results",
    "persist_period_results",
    # Config
    "TemporalSliceConfig",
    "TimeGrain",
    # Result models
    "ColumnDriftResult",
    "DriftEvidence",
    "PeriodMetrics",
    "CompletenessResult",
    "VolumeAnomalyResult",
    "PeriodAnalysisResult",
    # DB Models
    "ColumnDriftSummary",
    "TemporalSliceAnalysis",
]
