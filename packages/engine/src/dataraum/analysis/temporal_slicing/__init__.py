"""Temporal slicing analysis module.

Per-(slice table, period) row counts and numeric-column sums — the
aggregation-lineage reconciliation substrate (DAT-491). One ``GROUP BY`` over a
slice table's time column; periods come from the data.
"""

from dataraum.analysis.temporal_slicing.analyzer import (
    compute_period_sums,
    persist_period_sums,
)
from dataraum.analysis.temporal_slicing.db_models import (
    TemporalSliceAnalysis,
)
from dataraum.analysis.temporal_slicing.models import (
    PeriodSums,
    TimeGrain,
)

__all__ = [
    # Entry points
    "compute_period_sums",
    "persist_period_sums",
    # Models
    "TimeGrain",
    "PeriodSums",
    # DB Models
    "TemporalSliceAnalysis",
]
