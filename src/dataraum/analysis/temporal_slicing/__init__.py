"""Temporal slicing analysis module.

Provides drift detection for slice data using Jensen-Shannon divergence.
"""

from dataraum.analysis.temporal_slicing.analyzer import (
    analyze_column_drift,
    persist_drift_results,
)
from dataraum.analysis.temporal_slicing.db_models import (
    ColumnDriftSummary,
)
from dataraum.analysis.temporal_slicing.models import (
    ColumnDriftResult,
    DriftEvidence,
    TemporalSliceConfig,
    TimeGrain,
)

__all__ = [
    # Entry points
    "analyze_column_drift",
    "persist_drift_results",
    # Config
    "TemporalSliceConfig",
    "TimeGrain",
    # Result models
    "ColumnDriftResult",
    "DriftEvidence",
    # DB Models
    "ColumnDriftSummary",
]
