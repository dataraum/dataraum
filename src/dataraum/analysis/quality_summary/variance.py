"""Quantitative slice variance analysis.

Computes variance metrics across slices to identify columns with
interesting patterns. Filters out EMPTY, CONSTANT, and STABLE columns
so only INTERESTING columns go to LLM processing.

Based on the principle: if a metric is the same across all slices,
it tells you nothing about slice-specific behavior. The variance IS the signal.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from dataraum.analysis.quality_summary.models import AggregatedColumnData
from dataraum.core.logging import get_logger

logger = get_logger(__name__)


class ColumnClassification(str, Enum):
    """Classification of column based on slice variance analysis."""

    EMPTY = "empty"  # 100% NULL across all slices - no information
    CONSTANT = "constant"  # Single distinct value - configuration value
    INTERESTING = "interesting"  # Variance exceeds thresholds - business rules hidden
    STABLE = "stable"  # No interesting variance - uniform behavior


@dataclass
class SliceVarianceMetrics:
    """Computed variance metrics for a column across slices."""

    column_name: str
    classification: ColumnClassification

    # Null ratio variance
    null_spread: float = 0.0  # max - min null ratio
    null_min: float | None = None
    null_max: float | None = None

    # Distinct count variance
    distinct_ratio: float = 1.0  # max / min distinct count
    distinct_min: int | None = None
    distinct_max: int | None = None

    # Outlier variance
    outlier_spread: float = 0.0  # max - min outlier ratio
    outlier_min: float | None = None
    outlier_max: float | None = None

    # Benford p-value variance (fraud/manipulation detection)
    benford_spread: float = 0.0  # max - min benford p-value
    benford_min: float | None = None
    benford_max: float | None = None

    # Row count variance (for context)
    row_ratio: float = 1.0  # max / min row count
    row_min: int | None = None
    row_max: int | None = None

    # Which thresholds were exceeded
    exceeded_thresholds: list[str] = field(default_factory=list)

    # Insights for INTERESTING columns
    insights: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class SliceFilterConfig:
    """Configuration for slice variance thresholds.

    Loaded from config/pipeline.yaml under quality_summary.variance_filter
    """

    # Null ratio spread threshold (10% = field is conditionally populated)
    null_spread_threshold: float = 0.10

    # Distinct count ratio threshold (2x = cardinality depends on context)
    distinct_ratio_threshold: float = 2.0

    # Outlier ratio spread threshold (5% = slice-specific quality issue)
    outlier_spread_threshold: float = 0.05

    # Benford p-value spread threshold (compliance varies by slice = potential manipulation)
    # 0.3 = if some slices pass Benford (p>0.05) and others fail badly (p<0.01)
    benford_spread_threshold: float = 0.30

    # Row count ratio threshold (10x = very uneven slice coverage)
    row_ratio_threshold: float = 10.0

    # Null ratio threshold for EMPTY classification
    empty_null_threshold: float = 0.99

    # Enable/disable filtering (False = send all columns to LLM)
    enabled: bool = True


def get_filter_config() -> SliceFilterConfig:
    """Load filter config from pipeline.yaml or use defaults."""
    try:
        from dataraum.core.config import get_settings

        settings = get_settings()
        if hasattr(settings, "pipeline") and settings.pipeline:
            cfg = settings.pipeline.get("quality_summary", {}).get("variance_filter", {})
            return SliceFilterConfig(
                null_spread_threshold=cfg.get("null_spread_threshold", 0.10),
                distinct_ratio_threshold=cfg.get("distinct_ratio_threshold", 2.0),
                outlier_spread_threshold=cfg.get("outlier_spread_threshold", 0.05),
                benford_spread_threshold=cfg.get("benford_spread_threshold", 0.30),
                row_ratio_threshold=cfg.get("row_ratio_threshold", 10.0),
                empty_null_threshold=cfg.get("empty_null_threshold", 0.99),
                enabled=cfg.get("enabled", True),
            )
    except Exception:
        pass
    return SliceFilterConfig()


def compute_slice_variance(col_data: AggregatedColumnData) -> SliceVarianceMetrics:
    """Compute variance metrics for a column across its slices.

    Args:
        col_data: Aggregated column data with slice_data list

    Returns:
        SliceVarianceMetrics with computed values and classification
    """
    slices = col_data.slice_data
    config = get_filter_config()

    metrics = SliceVarianceMetrics(
        column_name=col_data.column_name,
        classification=ColumnClassification.STABLE,  # Default
    )

    if not slices:
        metrics.classification = ColumnClassification.EMPTY
        return metrics

    # Extract values from slices (with proper type narrowing)
    null_ratios: list[float] = [
        float(nr) for s in slices if (nr := s.get("null_ratio")) is not None
    ]
    distinct_counts: list[int] = [
        int(dc) for s in slices if (dc := s.get("distinct_count")) is not None
    ]
    outlier_ratios: list[float] = [
        float(or_) for s in slices if (or_ := s.get("outlier_ratio")) is not None
    ]
    benford_pvalues: list[float] = [
        float(bp) for s in slices if (bp := s.get("benford_p_value")) is not None
    ]
    row_counts: list[int] = [int(rc) for s in slices if (rc := s.get("row_count")) is not None]

    # Check for EMPTY classification (all nulls)
    if null_ratios and all(nr > config.empty_null_threshold for nr in null_ratios):
        metrics.classification = ColumnClassification.EMPTY
        metrics.null_min = min(null_ratios)
        metrics.null_max = max(null_ratios)
        return metrics

    # Check for CONSTANT classification (single distinct value everywhere)
    if distinct_counts and all(dc == 1 for dc in distinct_counts):
        metrics.classification = ColumnClassification.CONSTANT
        metrics.distinct_min = 1
        metrics.distinct_max = 1
        return metrics

    # Compute null spread
    if null_ratios:
        metrics.null_min = min(null_ratios)
        metrics.null_max = max(null_ratios)
        metrics.null_spread = metrics.null_max - metrics.null_min
        if metrics.null_spread > config.null_spread_threshold:
            metrics.exceeded_thresholds.append("null_spread")
            metrics.insights.append(
                {
                    "pattern": "null_ratio varies",
                    "detail": f"min={metrics.null_min:.1%}, max={metrics.null_max:.1%}",
                    "hypothesis": "Field is conditionally populated based on slice context",
                }
            )

    # Compute distinct ratio
    if distinct_counts:
        metrics.distinct_min = min(distinct_counts)
        metrics.distinct_max = max(distinct_counts)
        if metrics.distinct_min > 0:
            metrics.distinct_ratio = metrics.distinct_max / metrics.distinct_min
            if metrics.distinct_ratio > config.distinct_ratio_threshold:
                metrics.exceeded_thresholds.append("distinct_ratio")
                metrics.insights.append(
                    {
                        "pattern": f"distinct_count varies {metrics.distinct_ratio:.1f}x",
                        "detail": f"min={metrics.distinct_min}, max={metrics.distinct_max}",
                        "hypothesis": "Field meaning or usage varies by slice context",
                    }
                )

    # Compute outlier spread
    if outlier_ratios:
        metrics.outlier_min = min(outlier_ratios)
        metrics.outlier_max = max(outlier_ratios)
        metrics.outlier_spread = metrics.outlier_max - metrics.outlier_min
        if metrics.outlier_spread > config.outlier_spread_threshold:
            metrics.exceeded_thresholds.append("outlier_spread")
            metrics.insights.append(
                {
                    "pattern": "outlier_ratio varies",
                    "detail": f"min={metrics.outlier_min:.1%}, max={metrics.outlier_max:.1%}",
                    "hypothesis": "Slice-specific data quality issue or different value distributions",
                }
            )

    # Compute benford p-value spread (fraud/manipulation detection)
    if benford_pvalues and len(benford_pvalues) >= 2:
        metrics.benford_min = min(benford_pvalues)
        metrics.benford_max = max(benford_pvalues)
        metrics.benford_spread = metrics.benford_max - metrics.benford_min
        if metrics.benford_spread > config.benford_spread_threshold:
            metrics.exceeded_thresholds.append("benford_spread")
            # Determine which slices pass/fail Benford's law
            passing = [p for p in benford_pvalues if p > 0.05]
            failing = [p for p in benford_pvalues if p < 0.01]
            metrics.insights.append(
                {
                    "pattern": "benford_compliance varies",
                    "detail": f"p-value range: {metrics.benford_min:.3f} to {metrics.benford_max:.3f}",
                    "hypothesis": (
                        f"Benford's law compliance varies by slice ({len(passing)} pass, {len(failing)} fail). "
                        "This may indicate data manipulation or synthetic data in specific slices."
                    ),
                }
            )

    # Compute row ratio (informational, not a filter criterion by default)
    if row_counts and min(row_counts) > 0:
        metrics.row_min = min(row_counts)
        metrics.row_max = max(row_counts)
        metrics.row_ratio = metrics.row_max / metrics.row_min
        if metrics.row_ratio > config.row_ratio_threshold:
            metrics.exceeded_thresholds.append("row_ratio")
            metrics.insights.append(
                {
                    "pattern": f"row_count varies {metrics.row_ratio:.1f}x",
                    "detail": f"min={metrics.row_min}, max={metrics.row_max}",
                    "hypothesis": "Very uneven slice coverage - some slices may be edge cases",
                }
            )

    # Final classification
    if metrics.exceeded_thresholds:
        metrics.classification = ColumnClassification.INTERESTING
    else:
        metrics.classification = ColumnClassification.STABLE

    return metrics


def filter_interesting_columns(
    columns_data: list[AggregatedColumnData],
) -> tuple[list[AggregatedColumnData], dict[str, SliceVarianceMetrics]]:
    """Filter columns to only include INTERESTING ones for LLM processing.

    Computes variance metrics for each column and returns only those
    classified as INTERESTING. Also returns the full classification
    dict for reporting purposes.

    Args:
        columns_data: List of aggregated column data

    Returns:
        Tuple of:
        - Filtered list (only INTERESTING columns)
        - Dict mapping column_name to SliceVarianceMetrics (all columns)
    """
    config = get_filter_config()

    if not config.enabled:
        # Filtering disabled - return all columns
        logger.info("Slice variance filtering disabled, processing all columns")
        return columns_data, {}

    all_metrics: dict[str, SliceVarianceMetrics] = {}
    interesting_columns: list[AggregatedColumnData] = []

    # Counters for logging
    counts = {
        ColumnClassification.EMPTY: 0,
        ColumnClassification.CONSTANT: 0,
        ColumnClassification.INTERESTING: 0,
        ColumnClassification.STABLE: 0,
    }

    for col_data in columns_data:
        metrics = compute_slice_variance(col_data)
        all_metrics[col_data.column_name] = metrics
        counts[metrics.classification] += 1

        if metrics.classification == ColumnClassification.INTERESTING:
            interesting_columns.append(col_data)

    logger.info(
        "slice_variance_filter_applied",
        total_columns=len(columns_data),
        empty=counts[ColumnClassification.EMPTY],
        constant=counts[ColumnClassification.CONSTANT],
        stable=counts[ColumnClassification.STABLE],
        interesting=counts[ColumnClassification.INTERESTING],
    )

    # Log interesting columns with their patterns
    for col_data in interesting_columns:
        metrics = all_metrics[col_data.column_name]
        logger.debug(
            "interesting_column",
            column=col_data.column_name,
            exceeded=metrics.exceeded_thresholds,
            null_spread=f"{metrics.null_spread:.1%}" if metrics.null_spread else None,
            distinct_ratio=f"{metrics.distinct_ratio:.1f}x" if metrics.distinct_ratio > 1 else None,
        )

    return interesting_columns, all_metrics


__all__ = [
    "ColumnClassification",
    "SliceVarianceMetrics",
    "SliceFilterConfig",
    "compute_slice_variance",
    "filter_interesting_columns",
    "get_filter_config",
]
