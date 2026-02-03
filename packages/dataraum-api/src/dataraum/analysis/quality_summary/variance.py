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


def classify_column(col_data: AggregatedColumnData) -> ColumnClassification:
    """Classify a column based on its slice variance.

    Convenience wrapper around compute_slice_variance().

    Args:
        col_data: Aggregated column data

    Returns:
        ColumnClassification enum value
    """
    return compute_slice_variance(col_data).classification


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


# =============================================================================
# TEMPORAL VARIANCE FILTERING
# =============================================================================
# Filters for temporal analysis outputs:
# - Temporal Slice Analyses (volume/coverage patterns over time)
# - Temporal Column Profiles (date/timestamp characteristics)
# - Temporal Drift Analyses (distribution changes between periods)


@dataclass
class TemporalSliceFilterConfig:
    """Configuration for temporal slice variance thresholds."""

    # Row count ratio threshold across periods (2x = volume volatility)
    row_ratio_threshold: float = 2.0

    # Coverage ratio spread threshold (10% = inconsistent data collection)
    coverage_spread_threshold: float = 0.10

    # Period-over-period change threshold (30% = significant swing)
    pop_change_threshold: float = 0.30

    # Minimum coverage to not be considered sparse
    sparse_coverage_threshold: float = 0.10


@dataclass
class TemporalColumnFilterConfig:
    """Configuration for temporal column profile thresholds."""

    # Completeness ratio for major gaps
    major_gap_threshold: float = 0.50

    # Period-end spike ratio for fiscal behavior
    period_end_spike_threshold: float = 1.5

    # Largest gap in days to be significant
    significant_gap_days: int = 7


@dataclass
class TemporalDriftFilterConfig:
    """Configuration for temporal drift thresholds."""

    # JS divergence threshold for significant drift
    js_divergence_threshold: float = 0.30

    # JS divergence value indicating complete replacement (ln(2))
    js_complete_replacement: float = 0.693

    # Tolerance for detecting complete replacement
    js_replacement_tolerance: float = 0.01

    # Column names where complete replacement is expected (e.g., batch numbers)
    expected_replacement_columns: list[str] = field(default_factory=lambda: ["Stapelnummer"])


@dataclass
class TemporalSliceResult:
    """Result of temporal slice analysis filtering."""

    slice_name: str
    is_interesting: bool
    reasons: list[str] = field(default_factory=list)

    # Metrics
    row_ratio: float | None = None
    coverage_spread: float | None = None
    max_pop_change: float | None = None
    has_volume_anomaly: bool = False

    # Insights
    insights: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class TemporalColumnResult:
    """Result of temporal column profile filtering."""

    column_name: str
    is_interesting: bool
    reasons: list[str] = field(default_factory=list)

    # Metrics
    completeness_ratio: float | None = None
    period_end_spike_ratio: float | None = None
    gap_count: int = 0
    largest_gap_days: int = 0
    is_constant: bool = False

    # Insights
    insights: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class TemporalDriftResult:
    """Result of temporal drift analysis filtering."""

    column_name: str
    period_label: str
    slice_name: str
    is_interesting: bool
    reasons: list[str] = field(default_factory=list)

    # Metrics
    js_divergence: float | None = None
    has_significant_drift: bool = False
    has_category_changes: bool = False
    new_categories: list[str] = field(default_factory=list)
    missing_categories: list[str] = field(default_factory=list)

    # Insights
    insights: list[dict[str, Any]] = field(default_factory=list)


def is_interesting_temporal_slice(
    slice_data: list[dict[str, Any]],
    slice_name: str,
    config: TemporalSliceFilterConfig | None = None,
) -> TemporalSliceResult:
    """Determine if a temporal slice has interesting patterns across periods.

    Args:
        slice_data: List of period records for this slice (from temporal_slice_analyses)
            Expected keys: row_count, coverage_ratio, period_over_period_change,
                          is_volume_anomaly, period_label
        slice_name: Name of the slice (e.g., "herkunftskennzeichen_re")
        config: Optional filter configuration

    Returns:
        TemporalSliceResult with is_interesting flag and reasons
    """
    if config is None:
        config = TemporalSliceFilterConfig()

    result = TemporalSliceResult(slice_name=slice_name, is_interesting=False)

    if not slice_data:
        return result

    # Extract metrics across periods
    row_counts: list[int] = [int(rc) for d in slice_data if (rc := d.get("row_count")) is not None]
    coverages: list[float] = [
        float(cv) for d in slice_data if (cv := d.get("coverage_ratio")) is not None
    ]
    pop_changes: list[float] = [
        float(pc) for d in slice_data if (pc := d.get("period_over_period_change")) is not None
    ]
    anomalies: list[bool | None] = [d.get("is_volume_anomaly") for d in slice_data]

    # 1. Volume volatility (row_ratio > threshold)
    if row_counts and len(row_counts) >= 2:
        min_rows = min(r for r in row_counts if r > 0) if any(r > 0 for r in row_counts) else 0
        max_rows = max(row_counts)
        if min_rows > 0:
            result.row_ratio = max_rows / min_rows
            if result.row_ratio > config.row_ratio_threshold:
                result.is_interesting = True
                result.reasons.append("row_ratio")
                result.insights.append(
                    {
                        "pattern": f"volume varies {result.row_ratio:.1f}x across periods",
                        "detail": f"min={min_rows}, max={max_rows}",
                        "hypothesis": "Business seasonality or data collection issue",
                    }
                )
        elif max_rows > 0:
            # Goes from 0 to something = infinite ratio
            result.row_ratio = float("inf")
            result.is_interesting = True
            result.reasons.append("row_ratio_infinite")
            result.insights.append(
                {
                    "pattern": "slice appears/disappears across periods",
                    "detail": f"Some periods have 0 rows, max={max_rows}",
                    "hypothesis": "New transaction type introduced or discontinued",
                }
            )

    # 2. Coverage inconsistency
    if coverages and len(coverages) >= 2:
        result.coverage_spread = max(coverages) - min(coverages)
        if result.coverage_spread > config.coverage_spread_threshold:
            result.is_interesting = True
            result.reasons.append("coverage_spread")
            result.insights.append(
                {
                    "pattern": f"coverage varies {result.coverage_spread:.1%} across periods",
                    "detail": f"min={min(coverages):.1%}, max={max(coverages):.1%}",
                    "hypothesis": "Data collection process maturing or inconsistent",
                }
            )

        # Also flag sparse slices
        if min(coverages) < config.sparse_coverage_threshold:
            result.is_interesting = True
            if "sparse_coverage" not in result.reasons:
                result.reasons.append("sparse_coverage")
                result.insights.append(
                    {
                        "pattern": f"sparse coverage in some periods ({min(coverages):.1%})",
                        "hypothesis": "May be specialized use case or data quality issue",
                    }
                )

    # 3. Significant period-over-period change
    if pop_changes:
        result.max_pop_change = max(abs(c) for c in pop_changes)
        if result.max_pop_change > config.pop_change_threshold:
            result.is_interesting = True
            result.reasons.append("pop_change")
            result.insights.append(
                {
                    "pattern": f"period-over-period change of {result.max_pop_change:.1%}",
                    "hypothesis": "Significant business event or data migration",
                }
            )

    # 4. Pre-flagged volume anomaly
    if any(a for a in anomalies if a):
        result.has_volume_anomaly = True
        result.is_interesting = True
        result.reasons.append("volume_anomaly")
        result.insights.append(
            {
                "pattern": "pre-flagged volume anomaly",
                "hypothesis": "Statistical outlier in transaction volume",
            }
        )

    return result


def is_interesting_temporal_column(
    profile: dict[str, Any],
    config: TemporalColumnFilterConfig | None = None,
) -> TemporalColumnResult:
    """Determine if a temporal column profile is interesting.

    Args:
        profile: Column profile record (from temporal_column_profiles)
            Expected keys: column_name, completeness_ratio, min_timestamp, max_timestamp,
                          profile_data (JSON with fiscal_calendar, completeness, etc.)
        config: Optional filter configuration

    Returns:
        TemporalColumnResult with is_interesting flag and reasons
    """
    if config is None:
        config = TemporalColumnFilterConfig()

    column_name = profile.get("column_name", "unknown")
    result = TemporalColumnResult(column_name=column_name, is_interesting=False)

    # Extract core metrics
    result.completeness_ratio = profile.get("completeness_ratio")
    min_ts = profile.get("min_timestamp")
    max_ts = profile.get("max_timestamp")

    # Parse nested profile_data
    profile_data = profile.get("profile_data", {}) or {}
    fiscal_calendar = profile_data.get("fiscal_calendar", {}) or {}
    completeness = profile_data.get("completeness", {}) or {}

    result.period_end_spike_ratio = fiscal_calendar.get("period_end_spike_ratio")
    result.gap_count = completeness.get("gap_count", 0) or 0
    result.largest_gap_days = completeness.get("largest_gap_days", 0) or 0

    # 1. Major gaps (completeness < 50%)
    if (
        result.completeness_ratio is not None
        and result.completeness_ratio < config.major_gap_threshold
    ):
        result.is_interesting = True
        result.reasons.append("major_gaps")
        result.insights.append(
            {
                "pattern": f"completeness only {result.completeness_ratio:.1%}",
                "hypothesis": "Field is rarely used or optional in this workflow",
            }
        )

    # 2. Fiscal calendar behavior (period-end spike)
    if (
        result.period_end_spike_ratio is not None
        and result.period_end_spike_ratio > config.period_end_spike_threshold
    ):
        result.is_interesting = True
        result.reasons.append("period_end_spike")
        result.insights.append(
            {
                "pattern": f"month-end concentration {result.period_end_spike_ratio:.1f}x",
                "detail": "Transactions cluster at period boundaries",
                "hypothesis": "Fiscal calendar behavior - month-end posting concentration",
            }
        )

    # 3. Has temporal discontinuities (gaps)
    if result.gap_count > 0:
        result.is_interesting = True
        result.reasons.append("has_gaps")
        result.insights.append(
            {
                "pattern": f"{result.gap_count} gap(s) in temporal coverage",
                "detail": f"largest gap: {result.largest_gap_days} days",
                "hypothesis": "Data collection interruptions or system downtime",
            }
        )

    # 4. Significant gap size
    if result.largest_gap_days > config.significant_gap_days:
        if "has_gaps" not in result.reasons:
            result.is_interesting = True
            result.reasons.append("significant_gap")
        result.insights.append(
            {
                "pattern": f"gap of {result.largest_gap_days} days",
                "hypothesis": "Extended data collection gap - holiday, migration, or system issue",
            }
        )

    # 5. Effectively constant (single date)
    if min_ts is not None and max_ts is not None and min_ts == max_ts:
        result.is_constant = True
        result.is_interesting = True
        result.reasons.append("constant")
        result.insights.append(
            {
                "pattern": "single timestamp value",
                "detail": f"all records have timestamp: {min_ts}",
                "hypothesis": "Default value or batch import artifact",
            }
        )

    return result


def is_interesting_drift(
    drift: dict[str, Any],
    config: TemporalDriftFilterConfig | None = None,
) -> TemporalDriftResult:
    """Determine if a temporal drift analysis is interesting.

    Pre-filter: Automatically returns not interesting if both
    has_significant_drift=0 AND has_category_changes=0.

    Args:
        drift: Drift analysis record (from temporal_drift_analyses)
            Expected keys: column_name, period_label, slice_table_name,
                          js_divergence, has_significant_drift, has_category_changes,
                          new_categories_json, missing_categories_json
        config: Optional filter configuration

    Returns:
        TemporalDriftResult with is_interesting flag and reasons
    """
    if config is None:
        config = TemporalDriftFilterConfig()

    column_name = drift.get("column_name", "unknown")
    period_label = drift.get("period_label", "unknown")
    slice_name = drift.get("slice_table_name", "unknown")

    result = TemporalDriftResult(
        column_name=column_name,
        period_label=period_label,
        slice_name=slice_name,
        is_interesting=False,
    )

    # Extract metrics
    result.js_divergence = drift.get("js_divergence")
    result.has_significant_drift = bool(drift.get("has_significant_drift"))
    result.has_category_changes = bool(drift.get("has_category_changes"))
    result.new_categories = drift.get("new_categories_json") or []
    result.missing_categories = drift.get("missing_categories_json") or []

    # PRE-FILTER: Skip if both flags are 0
    if not result.has_significant_drift and not result.has_category_changes:
        return result  # Not interesting

    # SPECIAL CASE: Expected complete replacement (e.g., Stapelnummer)
    # JS divergence ≈ 0.693 (ln(2)) means complete distribution replacement
    if result.js_divergence is not None:
        is_complete_replacement = (
            abs(result.js_divergence - config.js_complete_replacement)
            < config.js_replacement_tolerance
        )

        if is_complete_replacement and column_name in config.expected_replacement_columns:
            # This is EXPECTED, not interesting
            result.insights.append(
                {
                    "pattern": "complete monthly replacement (expected)",
                    "detail": f"JS divergence = {result.js_divergence:.3f} (ln(2))",
                    "hypothesis": f"Column '{column_name}' values are expected to change completely each period (e.g., batch numbers)",
                }
            )
            return result  # Not interesting

    # 1. Significant JS divergence
    if result.js_divergence is not None and result.js_divergence > config.js_divergence_threshold:
        result.is_interesting = True
        result.reasons.append("js_divergence")

        # Determine severity
        if (
            abs(result.js_divergence - config.js_complete_replacement)
            < config.js_replacement_tolerance
        ):
            result.insights.append(
                {
                    "pattern": "complete distribution replacement",
                    "detail": f"JS divergence = {result.js_divergence:.3f} (≈ln(2))",
                    "hypothesis": "Values completely changed between periods - data migration or business rule change",
                }
            )
        else:
            result.insights.append(
                {
                    "pattern": f"distribution shift ({result.js_divergence:.1%} divergence)",
                    "hypothesis": "Value distribution changed between periods",
                }
            )

    # 2. Category changes (new values appeared)
    if result.new_categories:
        result.is_interesting = True
        result.reasons.append("new_categories")
        result.insights.append(
            {
                "pattern": f"new values appeared: {result.new_categories[:5]}{'...' if len(result.new_categories) > 5 else ''}",
                "hypothesis": "New options/codes introduced in this period",
            }
        )

    # 3. Category changes (values disappeared)
    if result.missing_categories:
        result.is_interesting = True
        result.reasons.append("missing_categories")
        result.insights.append(
            {
                "pattern": f"values disappeared: {result.missing_categories[:5]}{'...' if len(result.missing_categories) > 5 else ''}",
                "hypothesis": "Options/codes discontinued or data issue",
            }
        )

    return result


def filter_interesting_temporal_slices(
    slice_analyses: dict[str, list[dict[str, Any]]],
    config: TemporalSliceFilterConfig | None = None,
) -> tuple[list[str], dict[str, TemporalSliceResult]]:
    """Filter temporal slices to only interesting ones.

    Args:
        slice_analyses: Dict mapping slice_name -> list of period records
        config: Optional filter configuration

    Returns:
        Tuple of:
        - List of interesting slice names
        - Dict mapping all slice names to their TemporalSliceResult
    """
    all_results: dict[str, TemporalSliceResult] = {}
    interesting_slices: list[str] = []

    for slice_name, periods in slice_analyses.items():
        result = is_interesting_temporal_slice(periods, slice_name, config)
        all_results[slice_name] = result

        if result.is_interesting:
            interesting_slices.append(slice_name)

    logger.info(
        "temporal_slice_filter_applied",
        total_slices=len(slice_analyses),
        interesting=len(interesting_slices),
        filtered_out=len(slice_analyses) - len(interesting_slices),
    )

    return interesting_slices, all_results


def filter_interesting_temporal_columns(
    column_profiles: list[dict[str, Any]],
    config: TemporalColumnFilterConfig | None = None,
) -> tuple[list[dict[str, Any]], dict[str, TemporalColumnResult]]:
    """Filter temporal column profiles to only interesting ones.

    Args:
        column_profiles: List of column profile records
        config: Optional filter configuration

    Returns:
        Tuple of:
        - List of interesting profile records
        - Dict mapping column names to their TemporalColumnResult
    """
    all_results: dict[str, TemporalColumnResult] = {}
    interesting_profiles: list[dict[str, Any]] = []

    for profile in column_profiles:
        result = is_interesting_temporal_column(profile, config)
        all_results[result.column_name] = result

        if result.is_interesting:
            interesting_profiles.append(profile)

    logger.info(
        "temporal_column_filter_applied",
        total_columns=len(column_profiles),
        interesting=len(interesting_profiles),
        filtered_out=len(column_profiles) - len(interesting_profiles),
    )

    return interesting_profiles, all_results


def filter_interesting_drift(
    drift_analyses: list[dict[str, Any]],
    config: TemporalDriftFilterConfig | None = None,
) -> tuple[list[dict[str, Any]], list[TemporalDriftResult]]:
    """Filter temporal drift analyses to only interesting ones.

    Applies pre-filter (both flags=0 → skip) and expected replacement filter.

    Args:
        drift_analyses: List of drift analysis records
        config: Optional filter configuration

    Returns:
        Tuple of:
        - List of interesting drift records
        - List of all TemporalDriftResult objects
    """
    all_results: list[TemporalDriftResult] = []
    interesting_drifts: list[dict[str, Any]] = []

    for drift in drift_analyses:
        result = is_interesting_drift(drift, config)
        all_results.append(result)

        if result.is_interesting:
            interesting_drifts.append(drift)

    # Count pre-filtered
    pre_filtered = sum(
        1 for r in all_results if not r.has_significant_drift and not r.has_category_changes
    )

    logger.info(
        "temporal_drift_filter_applied",
        total_drifts=len(drift_analyses),
        pre_filtered=pre_filtered,
        interesting=len(interesting_drifts),
        filtered_out=len(drift_analyses) - len(interesting_drifts),
    )

    return interesting_drifts, all_results


__all__ = [
    # Categorical slice filtering
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
]
