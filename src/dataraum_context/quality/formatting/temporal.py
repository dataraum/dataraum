"""Temporal quality formatter.

Formats temporal quality metrics into contextualized output for LLM consumption.
Groups related metrics into interpretation units:
- Freshness: staleness, data freshness, update regularity
- Completeness: coverage ratio, gaps, expected vs actual periods
- Patterns: seasonality, trends, change points
- Stability: distribution shifts over time

Usage:
    from dataraum_context.quality.formatting.temporal import (
        format_temporal_quality,
        format_freshness_group,
        format_temporal_completeness_group,
        format_patterns_group,
        format_stability_group,
    )

    result = format_temporal_quality(
        staleness_days=5,
        completeness_ratio=0.98,
        has_seasonality=True,
        seasonality_strength=0.75,
        config=formatter_config,
    )
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from dataraum_context.quality.formatting.config import FormatterConfig, get_default_config


@dataclass
class MetricContext:
    """Contextualized metric with severity and interpretation."""

    value: Any
    severity: str
    interpretation: str
    details: dict[str, Any] | None = None


@dataclass
class GroupContext:
    """Contextualized metric group."""

    group_name: str
    overall_severity: str
    interpretation: str
    metrics: dict[str, MetricContext]
    samples: list[Any] | None = None


# =============================================================================
# Interpretation Templates
# =============================================================================

FRESHNESS_INTERPRETATIONS = {
    "none": "Data is fresh and regularly updated",
    "low": "Data is slightly stale but still acceptable",
    "moderate": "Data showing age, update may be needed",
    "high": "Data is stale, freshness concern",
    "severe": "Data is significantly outdated",
    "critical": "Data critically stale, immediate refresh required",
}

TEMPORAL_COMPLETENESS_INTERPRETATIONS = {
    "none": "Complete temporal coverage with no significant gaps",
    "low": "Minor gaps in temporal coverage, minimal impact",
    "moderate": "Moderate gaps detected, some periods missing",
    "high": "Significant temporal gaps affecting continuity",
    "severe": "Severe gaps in temporal coverage, major periods missing",
    "critical": "Critical temporal gaps, data continuity broken",
}

PATTERN_INTERPRETATIONS = {
    "none": "Temporal patterns detected and stable",
    "low": "Minor temporal pattern irregularities",
    "moderate": "Moderate temporal pattern deviations",
    "high": "Significant temporal anomalies detected",
    "severe": "Severe temporal pattern disruptions",
}

STABILITY_INTERPRETATIONS = {
    "none": "Distribution is stable over time",
    "low": "Minor distribution fluctuations detected",
    "moderate": "Moderate distribution shifts between periods",
    "high": "Significant distribution changes detected",
    "severe": "Severe distribution instability, major shifts detected",
    "critical": "Critical instability, distribution fundamentally changed",
}


# =============================================================================
# Group Formatters
# =============================================================================


def format_freshness_group(
    staleness_days: float | None = None,
    data_freshness_days: float | None = None,
    last_update: datetime | None = None,
    is_stale: bool | None = None,
    update_frequency_score: float | None = None,
    median_interval_seconds: float | None = None,
    config: FormatterConfig | None = None,
    column_name: str | None = None,
) -> GroupContext:
    """Format freshness metrics into contextualized group.

    Args:
        staleness_days: Days since last expected update
        data_freshness_days: Days since most recent data point
        last_update: Timestamp of last update
        is_stale: Whether data is considered stale
        update_frequency_score: Regularity score (0-1)
        median_interval_seconds: Median time between updates
        config: Formatter configuration for thresholds
        column_name: Column name for pattern-based thresholds

    Returns:
        GroupContext with severity, interpretation, and metric details
    """
    config = config or get_default_config()
    metrics: dict[str, MetricContext] = {}
    severities: list[str] = []

    # Staleness
    if staleness_days is not None:
        severity = config.get_severity("temporal", "staleness_days", staleness_days, column_name)
        severities.append(severity)

        if staleness_days < 1:
            interp = f"Data updated within the last day ({staleness_days:.1f} days old)"
        elif staleness_days < 7:
            interp = f"Data is {staleness_days:.1f} days old"
        else:
            interp = f"Data is {staleness_days:.0f} days stale"

        metrics["staleness_days"] = MetricContext(
            value=staleness_days,
            severity=severity,
            interpretation=interp,
            details={"last_update": last_update.isoformat() if last_update else None},
        )

    # Data freshness (alternative to staleness)
    if data_freshness_days is not None and staleness_days is None:
        severity = config.get_severity(
            "temporal", "staleness_days", data_freshness_days, column_name
        )
        severities.append(severity)

        metrics["data_freshness_days"] = MetricContext(
            value=data_freshness_days,
            severity=severity,
            interpretation=f"Most recent data point is {data_freshness_days:.1f} days old",
            details={"is_stale": is_stale},
        )

    # Update frequency score (informational)
    if update_frequency_score is not None:
        if update_frequency_score > 0.9:
            freq_interp = "Updates are highly regular"
        elif update_frequency_score > 0.7:
            freq_interp = "Updates are reasonably regular"
        elif update_frequency_score > 0.5:
            freq_interp = "Updates show some irregularity"
        else:
            freq_interp = "Updates are irregular"

        # Convert median interval to human-readable
        interval_str = None
        if median_interval_seconds is not None:
            if median_interval_seconds < 60:
                interval_str = f"{median_interval_seconds:.0f} seconds"
            elif median_interval_seconds < 3600:
                interval_str = f"{median_interval_seconds / 60:.0f} minutes"
            elif median_interval_seconds < 86400:
                interval_str = f"{median_interval_seconds / 3600:.1f} hours"
            else:
                interval_str = f"{median_interval_seconds / 86400:.1f} days"

        metrics["update_frequency_score"] = MetricContext(
            value=update_frequency_score,
            severity="none",  # Informational
            interpretation=freq_interp,
            details={"median_interval": interval_str},
        )

    # Determine overall severity
    severity_order = ["none", "low", "moderate", "high", "severe", "critical"]
    overall_severity = "none"
    for sev in severities:
        if sev in severity_order and severity_order.index(sev) > severity_order.index(
            overall_severity
        ):
            overall_severity = sev

    return GroupContext(
        group_name="freshness",
        overall_severity=overall_severity,
        interpretation=FRESHNESS_INTERPRETATIONS.get(overall_severity, "Unknown freshness status"),
        metrics=metrics,
    )


def format_temporal_completeness_group(
    completeness_ratio: float | None = None,
    expected_periods: int | None = None,
    actual_periods: int | None = None,
    gap_count: int | None = None,
    largest_gap_days: float | None = None,
    gaps: list[dict[str, Any]] | None = None,
    config: FormatterConfig | None = None,
    column_name: str | None = None,
) -> GroupContext:
    """Format temporal completeness metrics into contextualized group.

    Args:
        completeness_ratio: Ratio of actual to expected periods (0-1)
        expected_periods: Number of periods expected
        actual_periods: Number of periods actually present
        gap_count: Number of gaps detected
        largest_gap_days: Size of largest gap in days
        gaps: List of gap details
        config: Formatter configuration
        column_name: Column name for pattern-based thresholds

    Returns:
        GroupContext with severity, interpretation, and gap details
    """
    config = config or get_default_config()
    metrics: dict[str, MetricContext] = {}
    severities: list[str] = []

    # Completeness ratio
    if completeness_ratio is not None:
        severity = config.get_severity(
            "temporal", "temporal_completeness", completeness_ratio, column_name
        )
        severities.append(severity)

        pct = completeness_ratio * 100
        if expected_periods is not None and actual_periods is not None:
            missing = expected_periods - actual_periods
            interp = f"{pct:.1f}% temporal coverage ({actual_periods:,} of {expected_periods:,} periods, {missing:,} missing)"
        else:
            interp = f"{pct:.1f}% temporal coverage"

        metrics["completeness_ratio"] = MetricContext(
            value=completeness_ratio,
            severity=severity,
            interpretation=interp,
            details={
                "expected_periods": expected_periods,
                "actual_periods": actual_periods,
            },
        )

    # Gap count
    if gap_count is not None:
        severity = config.get_severity("temporal", "gap_count", gap_count, column_name)
        severities.append(severity)

        if gap_count == 0:
            interp = "No temporal gaps detected"
        elif gap_count == 1:
            interp = "1 temporal gap detected"
        else:
            interp = f"{gap_count} temporal gaps detected"

        metrics["gap_count"] = MetricContext(
            value=gap_count,
            severity=severity,
            interpretation=interp,
            details=None,
        )

    # Largest gap
    if largest_gap_days is not None:
        severity = config.get_severity(
            "temporal", "largest_gap_days", largest_gap_days, column_name
        )
        severities.append(severity)

        if largest_gap_days < 1:
            interp = f"Largest gap is {largest_gap_days * 24:.1f} hours"
        elif largest_gap_days < 7:
            interp = f"Largest gap is {largest_gap_days:.1f} days"
        elif largest_gap_days < 30:
            interp = f"Largest gap is {largest_gap_days / 7:.1f} weeks"
        else:
            interp = (
                f"Largest gap is {largest_gap_days:.0f} days ({largest_gap_days / 30:.1f} months)"
            )

        metrics["largest_gap_days"] = MetricContext(
            value=largest_gap_days,
            severity=severity,
            interpretation=interp,
            details=None,
        )

    # Determine overall severity
    severity_order = ["none", "low", "moderate", "high", "severe", "critical"]
    overall_severity = "none"
    for sev in severities:
        if sev in severity_order and severity_order.index(sev) > severity_order.index(
            overall_severity
        ):
            overall_severity = sev

    # Include gap samples (up to 3)
    gap_samples = None
    if gaps:
        gap_samples = [
            {
                "start": g.get("gap_start"),
                "end": g.get("gap_end"),
                "days": g.get("gap_length_days"),
                "severity": g.get("severity"),
            }
            for g in gaps[:3]
        ]

    return GroupContext(
        group_name="temporal_completeness",
        overall_severity=overall_severity,
        interpretation=TEMPORAL_COMPLETENESS_INTERPRETATIONS.get(
            overall_severity, "Unknown completeness status"
        ),
        metrics=metrics,
        samples=gap_samples,
    )


def format_patterns_group(
    has_seasonality: bool | None = None,
    seasonality_strength: float | None = None,
    seasonality_period: str | None = None,
    seasonality_peaks: dict[str, Any] | None = None,
    has_trend: bool | None = None,
    trend_strength: float | None = None,
    trend_direction: str | None = None,
    trend_slope: float | None = None,
    change_point_count: int | None = None,
    change_points: list[dict[str, Any]] | None = None,
    config: FormatterConfig | None = None,
    column_name: str | None = None,
) -> GroupContext:
    """Format temporal pattern metrics into contextualized group.

    Args:
        has_seasonality: Whether seasonality detected
        seasonality_strength: Strength of seasonal component (0-1)
        seasonality_period: Period type (daily, weekly, etc.)
        seasonality_peaks: Peak periods (month, day_of_week, etc.)
        has_trend: Whether trend detected
        trend_strength: Strength of trend (0-1)
        trend_direction: 'increasing', 'decreasing', 'stable'
        trend_slope: Slope coefficient
        change_point_count: Number of change points detected
        change_points: Change point details
        config: Formatter configuration
        column_name: Column name for pattern-based thresholds

    Returns:
        GroupContext with pattern analysis
    """
    config = config or get_default_config()
    metrics: dict[str, MetricContext] = {}
    severities: list[str] = []

    # Seasonality
    if has_seasonality is not None:
        if has_seasonality and seasonality_strength is not None:
            if seasonality_strength > 0.8:
                strength_desc = "Strong"
            elif seasonality_strength > 0.5:
                strength_desc = "Moderate"
            else:
                strength_desc = "Weak"

            period_desc = f" ({seasonality_period})" if seasonality_period else ""
            interp = f"{strength_desc} seasonality detected{period_desc}"

            # Build peak info
            peak_info = None
            if seasonality_peaks:
                peak_parts = []
                if "month" in seasonality_peaks:
                    month_names = [
                        "Jan",
                        "Feb",
                        "Mar",
                        "Apr",
                        "May",
                        "Jun",
                        "Jul",
                        "Aug",
                        "Sep",
                        "Oct",
                        "Nov",
                        "Dec",
                    ]
                    month_idx = seasonality_peaks["month"]
                    if isinstance(month_idx, int) and 1 <= month_idx <= 12:
                        peak_parts.append(f"peak in {month_names[month_idx - 1]}")
                if "day_of_week" in seasonality_peaks:
                    day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
                    day_idx = seasonality_peaks["day_of_week"]
                    if isinstance(day_idx, int) and 0 <= day_idx <= 6:
                        peak_parts.append(f"peak on {day_names[day_idx]}")
                peak_info = ", ".join(peak_parts) if peak_parts else None
        else:
            interp = "No significant seasonality detected"
            peak_info = None

        metrics["seasonality"] = MetricContext(
            value=has_seasonality,
            severity="none",  # Seasonality is informational
            interpretation=interp,
            details={
                "strength": seasonality_strength,
                "period": seasonality_period,
                "peaks": peak_info,
            },
        )

    # Trend
    if has_trend is not None:
        if has_trend and trend_strength is not None:
            if trend_strength > 0.8:
                strength_desc = "Strong"
            elif trend_strength > 0.5:
                strength_desc = "Moderate"
            else:
                strength_desc = "Weak"

            direction = trend_direction or "unknown"
            interp = f"{strength_desc} {direction} trend detected"

            if trend_slope is not None:
                slope_sign = "+" if trend_slope > 0 else ""
                interp += f" (slope: {slope_sign}{trend_slope:.4f})"
        else:
            interp = "No significant trend detected"

        metrics["trend"] = MetricContext(
            value=has_trend,
            severity="none",  # Trend is informational
            interpretation=interp,
            details={
                "strength": trend_strength,
                "direction": trend_direction,
                "slope": trend_slope,
            },
        )

    # Change points
    if change_point_count is not None:
        # More change points = potential concern
        if change_point_count == 0:
            severity = "none"
            interp = "No structural breaks detected"
        elif change_point_count <= 2:
            severity = "low"
            interp = f"{change_point_count} change point(s) detected"
        elif change_point_count <= 5:
            severity = "moderate"
            interp = f"{change_point_count} change points detected - multiple structural breaks"
        else:
            severity = "high"
            interp = f"{change_point_count} change points detected - frequent structural changes"

        severities.append(severity)

        # Format change point samples
        cp_details = None
        if change_points:
            cp_details = [
                {
                    "date": cp.get("detected_at"),
                    "type": cp.get("change_type"),
                    "magnitude": cp.get("magnitude"),
                }
                for cp in change_points[:3]
            ]

        metrics["change_points"] = MetricContext(
            value=change_point_count,
            severity=severity,
            interpretation=interp,
            details={"points": cp_details},
        )

    # Determine overall severity (change points can indicate issues)
    severity_order = ["none", "low", "moderate", "high", "severe", "critical"]
    overall_severity = "none"
    for sev in severities:
        if sev in severity_order and severity_order.index(sev) > severity_order.index(
            overall_severity
        ):
            overall_severity = sev

    return GroupContext(
        group_name="patterns",
        overall_severity=overall_severity,
        interpretation=PATTERN_INTERPRETATIONS.get(overall_severity, "Unknown pattern status"),
        metrics=metrics,
    )


def format_stability_group(
    stability_score: float | None = None,
    shift_count: int | None = None,
    max_ks_statistic: float | None = None,
    shifts: list[dict[str, Any]] | None = None,
    config: FormatterConfig | None = None,
    column_name: str | None = None,
) -> GroupContext:
    """Format distribution stability metrics into contextualized group.

    Args:
        stability_score: Overall stability (0-1, higher is more stable)
        shift_count: Number of distribution shifts detected
        max_ks_statistic: Maximum KS statistic across comparisons
        shifts: Shift details
        config: Formatter configuration
        column_name: Column name for pattern-based thresholds

    Returns:
        GroupContext with stability analysis
    """
    config = config or get_default_config()
    metrics: dict[str, MetricContext] = {}
    severities: list[str] = []

    # Stability score (descending - lower is worse)
    if stability_score is not None:
        # Map stability score to severity (inverted - high score = good)
        if stability_score >= 0.9:
            severity = "none"
        elif stability_score >= 0.8:
            severity = "low"
        elif stability_score >= 0.6:
            severity = "moderate"
        elif stability_score >= 0.4:
            severity = "high"
        else:
            severity = "severe"

        severities.append(severity)

        pct = stability_score * 100
        if stability_score >= 0.9:
            interp = f"Distribution highly stable ({pct:.0f}% stability)"
        elif stability_score >= 0.7:
            interp = f"Distribution reasonably stable ({pct:.0f}% stability)"
        else:
            interp = f"Distribution showing instability ({pct:.0f}% stability)"

        metrics["stability_score"] = MetricContext(
            value=stability_score,
            severity=severity,
            interpretation=interp,
            details={"max_ks_statistic": max_ks_statistic},
        )

    # Shift count
    if shift_count is not None:
        if shift_count == 0:
            severity = "none"
            interp = "No significant distribution shifts detected"
        elif shift_count <= 2:
            severity = "low"
            interp = f"{shift_count} distribution shift(s) detected"
        elif shift_count <= 5:
            severity = "moderate"
            interp = f"{shift_count} distribution shifts detected"
        else:
            severity = "high"
            interp = f"{shift_count} distribution shifts - significant instability"

        severities.append(severity)

        # Format shift samples
        shift_details = None
        if shifts:
            shift_details = [
                {
                    "period": f"{s.get('period1_start')} to {s.get('period2_end')}",
                    "direction": s.get("shift_direction"),
                    "magnitude": s.get("shift_magnitude"),
                }
                for s in shifts[:3]
            ]

        metrics["shift_count"] = MetricContext(
            value=shift_count,
            severity=severity,
            interpretation=interp,
            details={"shifts": shift_details},
        )

    # Determine overall severity
    severity_order = ["none", "low", "moderate", "high", "severe", "critical"]
    overall_severity = "none"
    for sev in severities:
        if sev in severity_order and severity_order.index(sev) > severity_order.index(
            overall_severity
        ):
            overall_severity = sev

    return GroupContext(
        group_name="stability",
        overall_severity=overall_severity,
        interpretation=STABILITY_INTERPRETATIONS.get(overall_severity, "Unknown stability status"),
        metrics=metrics,
    )


# =============================================================================
# Main Formatter
# =============================================================================


def format_temporal_quality(
    # Freshness metrics
    staleness_days: float | None = None,
    data_freshness_days: float | None = None,
    last_update: datetime | None = None,
    is_stale: bool | None = None,
    update_frequency_score: float | None = None,
    median_interval_seconds: float | None = None,
    # Completeness metrics
    completeness_ratio: float | None = None,
    expected_periods: int | None = None,
    actual_periods: int | None = None,
    gap_count: int | None = None,
    largest_gap_days: float | None = None,
    gaps: list[dict[str, Any]] | None = None,
    # Pattern metrics
    has_seasonality: bool | None = None,
    seasonality_strength: float | None = None,
    seasonality_period: str | None = None,
    seasonality_peaks: dict[str, Any] | None = None,
    has_trend: bool | None = None,
    trend_strength: float | None = None,
    trend_direction: str | None = None,
    trend_slope: float | None = None,
    change_point_count: int | None = None,
    change_points: list[dict[str, Any]] | None = None,
    # Stability metrics
    stability_score: float | None = None,
    shift_count: int | None = None,
    max_ks_statistic: float | None = None,
    shifts: list[dict[str, Any]] | None = None,
    # Configuration
    config: FormatterConfig | None = None,
    column_name: str | None = None,
) -> dict[str, Any]:
    """Format all temporal quality metrics into contextualized output.

    This is the main entry point for temporal formatting. It groups
    related metrics and produces a structured output suitable for LLM
    consumption or human review.

    Args:
        staleness_days: Days since last expected update
        data_freshness_days: Days since most recent data point
        last_update: Timestamp of last update
        is_stale: Whether data is considered stale
        update_frequency_score: Regularity score (0-1)
        median_interval_seconds: Median time between updates
        completeness_ratio: Temporal coverage ratio
        expected_periods: Expected period count
        actual_periods: Actual period count
        gap_count: Number of gaps
        largest_gap_days: Largest gap size
        gaps: Gap details
        has_seasonality: Seasonality detected
        seasonality_strength: Seasonal strength
        seasonality_period: Period type
        seasonality_peaks: Peak periods
        has_trend: Trend detected
        trend_strength: Trend strength
        trend_direction: Trend direction
        trend_slope: Trend slope
        change_point_count: Change point count
        change_points: Change point details
        stability_score: Distribution stability
        shift_count: Number of shifts
        max_ks_statistic: Max KS statistic
        shifts: Shift details
        config: Formatter configuration
        column_name: Column name for thresholds

    Returns:
        Dict with contextualized temporal quality assessment
    """
    config = config or get_default_config()

    # Format each group
    freshness = format_freshness_group(
        staleness_days=staleness_days,
        data_freshness_days=data_freshness_days,
        last_update=last_update,
        is_stale=is_stale,
        update_frequency_score=update_frequency_score,
        median_interval_seconds=median_interval_seconds,
        config=config,
        column_name=column_name,
    )

    completeness = format_temporal_completeness_group(
        completeness_ratio=completeness_ratio,
        expected_periods=expected_periods,
        actual_periods=actual_periods,
        gap_count=gap_count,
        largest_gap_days=largest_gap_days,
        gaps=gaps,
        config=config,
        column_name=column_name,
    )

    patterns = format_patterns_group(
        has_seasonality=has_seasonality,
        seasonality_strength=seasonality_strength,
        seasonality_period=seasonality_period,
        seasonality_peaks=seasonality_peaks,
        has_trend=has_trend,
        trend_strength=trend_strength,
        trend_direction=trend_direction,
        trend_slope=trend_slope,
        change_point_count=change_point_count,
        change_points=change_points,
        config=config,
        column_name=column_name,
    )

    stability = format_stability_group(
        stability_score=stability_score,
        shift_count=shift_count,
        max_ks_statistic=max_ks_statistic,
        shifts=shifts,
        config=config,
        column_name=column_name,
    )

    # Determine overall severity
    severity_order = ["none", "low", "moderate", "high", "severe", "critical"]
    all_severities = [
        freshness.overall_severity,
        completeness.overall_severity,
        patterns.overall_severity,
        stability.overall_severity,
    ]
    overall_severity = max(
        all_severities, key=lambda s: severity_order.index(s) if s in severity_order else 0
    )

    # Build output
    return {
        "temporal_quality": {
            "overall_severity": overall_severity,
            "groups": {
                "freshness": _group_to_dict(freshness),
                "completeness": _group_to_dict(completeness),
                "patterns": _group_to_dict(patterns),
                "stability": _group_to_dict(stability),
            },
            "column_name": column_name,
        }
    }


def _group_to_dict(group: GroupContext) -> dict[str, Any]:
    """Convert GroupContext to dictionary."""
    result: dict[str, Any] = {
        "severity": group.overall_severity,
        "interpretation": group.interpretation,
        "metrics": {},
    }

    for name, metric in group.metrics.items():
        result["metrics"][name] = {
            "value": metric.value,
            "severity": metric.severity,
            "interpretation": metric.interpretation,
        }
        if metric.details:
            result["metrics"][name]["details"] = metric.details

    if group.samples:
        result["samples"] = group.samples

    return result
