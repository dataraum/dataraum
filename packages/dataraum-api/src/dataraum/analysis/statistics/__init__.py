"""Statistical profiling module.

Computes column-level statistics on typed data:
- Basic counts (total, null, distinct, cardinality)
- String stats (min/max/avg length)
- Top values (frequency analysis)
- Numeric stats (min, max, mean, stddev, skewness, kurtosis, cv)
- Percentiles
- Histograms

Statistical Quality (moved from quality/ in Phase 9A):
- Benford's Law compliance (fraud detection)
- Outlier detection (IQR + Isolation Forest)
"""

from dataraum.analysis.statistics.db_models import (
    StatisticalProfile,
    StatisticalQualityMetrics,
)
from dataraum.analysis.statistics.models import (
    BenfordAnalysis,
    ColumnProfile,
    DetectedPattern,
    HistogramBucket,
    NumericStats,
    OutlierDetection,
    StatisticalQualityResult,
    StatisticsProfileResult,
    StringStats,
    ValueCount,
)
from dataraum.analysis.statistics.profiler import profile_statistics
from dataraum.analysis.statistics.quality import (
    assess_statistical_quality,
    check_benford_law,
    detect_outliers_iqr,
    detect_outliers_isolation_forest,
)

__all__ = [
    # Main entry points
    "profile_statistics",
    "assess_statistical_quality",
    # Quality functions
    "check_benford_law",
    "detect_outliers_iqr",
    "detect_outliers_isolation_forest",
    # DB Models
    "StatisticalProfile",
    "StatisticalQualityMetrics",
    # Pydantic Models - Statistics
    "ColumnProfile",
    "NumericStats",
    "StringStats",
    "HistogramBucket",
    "ValueCount",
    "DetectedPattern",
    "StatisticsProfileResult",
    # Pydantic Models - Quality
    "BenfordAnalysis",
    "OutlierDetection",
    "StatisticalQualityResult",
]
