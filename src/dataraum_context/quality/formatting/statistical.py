"""Statistical quality formatter.

Formats statistical quality metrics into contextualized output for LLM consumption.
Groups related metrics into interpretation units:
- Completeness: null_ratio, cardinality_ratio
- Outliers: iqr_outlier_ratio, isolation_forest_ratio, samples
- Benford: is_compliant, chi_square, p_value

Usage:
    from dataraum_context.quality.formatting.statistical import (
        format_statistical_quality,
        format_completeness_group,
        format_outliers_group,
        format_benford_group,
    )

    result = format_statistical_quality(
        profile=column_profile,
        quality_result=stat_quality_result,
        config=formatter_config,
        column_name="revenue",
    )
"""

from dataclasses import dataclass
from typing import Any

from dataraum_context.core.formatting.config import FormatterConfig, get_default_config


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

COMPLETENESS_INTERPRETATIONS = {
    "none": "Data is complete with minimal missing values",
    "low": "Low proportion of missing values, generally acceptable",
    "moderate": "Moderate missing values detected, may affect analysis accuracy",
    "high": "High proportion of missing values, significant data gaps",
    "severe": "Severe completeness issues, majority of data is missing",
    "critical": "Critical completeness failure, data unusable without addressing",
}

OUTLIER_INTERPRETATIONS = {
    "none": "No significant outliers detected, distribution appears normal",
    "low": "Few statistical outliers, within expected variation",
    "moderate": "Moderate outliers detected, review recommended",
    "high": "High proportion of outliers, data quality concern",
    "severe": "Severe outlier contamination, values significantly outside expected range",
}

BENFORD_INTERPRETATIONS = {
    "none": "Conforms to Benford's Law - first-digit distribution as expected",
    "low": "Minor deviation from Benford's Law, borderline compliance",
    "moderate": "Moderate deviation from Benford's Law, warrants investigation",
    "high": "Significant deviation from Benford's Law, potential data anomaly",
    "severe": "Severe Benford's Law violation, possible data manipulation or systematic error",
    "critical": "Critical Benford's Law violation, immediate investigation required",
}


# =============================================================================
# Group Formatters
# =============================================================================


def format_completeness_group(
    null_ratio: float | None,
    null_count: int | None = None,
    total_count: int | None = None,
    cardinality_ratio: float | None = None,
    distinct_count: int | None = None,
    config: FormatterConfig | None = None,
    column_name: str | None = None,
) -> GroupContext:
    """Format completeness metrics into contextualized group.

    Args:
        null_ratio: Proportion of null values (0.0 to 1.0)
        null_count: Absolute count of nulls
        total_count: Total row count
        cardinality_ratio: Proportion of distinct values
        distinct_count: Absolute distinct count
        config: Formatter configuration for thresholds
        column_name: Column name for pattern-based thresholds

    Returns:
        GroupContext with severity, interpretation, and metric details
    """
    config = config or get_default_config()
    metrics: dict[str, MetricContext] = {}

    # Determine overall severity (worst of the metrics)
    severities: list[str] = []

    # Null ratio
    if null_ratio is not None:
        severity = config.get_severity("completeness", "null_ratio", null_ratio, column_name)
        severities.append(severity)

        pct = null_ratio * 100
        if null_count is not None and total_count is not None:
            interp = f"{pct:.1f}% of values are missing ({null_count:,} of {total_count:,})"
        else:
            interp = f"{pct:.1f}% of values are missing"

        metrics["null_ratio"] = MetricContext(
            value=null_ratio,
            severity=severity,
            interpretation=interp,
            details={"null_count": null_count, "total_count": total_count},
        )

    # Cardinality ratio
    if cardinality_ratio is not None:
        # Cardinality doesn't have direct severity, just provide context
        if cardinality_ratio > 0.99:
            card_interp = "Near-unique values (potential identifier)"
        elif cardinality_ratio < 0.01:
            card_interp = "Very low cardinality (potential categorical)"
        else:
            card_interp = f"{cardinality_ratio * 100:.1f}% unique values"

        metrics["cardinality_ratio"] = MetricContext(
            value=cardinality_ratio,
            severity="none",  # Cardinality is informational
            interpretation=card_interp,
            details={"distinct_count": distinct_count},
        )

    # Determine overall severity
    severity_order = ["none", "low", "moderate", "high", "severe", "critical"]
    overall_severity = "none"
    for sev in severities:
        if severity_order.index(sev) > severity_order.index(overall_severity):
            overall_severity = sev

    return GroupContext(
        group_name="completeness",
        overall_severity=overall_severity,
        interpretation=COMPLETENESS_INTERPRETATIONS.get(
            overall_severity, "Unknown completeness status"
        ),
        metrics=metrics,
    )


def format_outliers_group(
    iqr_outlier_ratio: float | None = None,
    iqr_outlier_count: int | None = None,
    iqr_lower_fence: float | None = None,
    iqr_upper_fence: float | None = None,
    isolation_forest_ratio: float | None = None,
    isolation_forest_count: int | None = None,
    outlier_samples: list[dict[str, Any]] | None = None,
    config: FormatterConfig | None = None,
    column_name: str | None = None,
) -> GroupContext:
    """Format outlier detection metrics into contextualized group.

    Args:
        iqr_outlier_ratio: Proportion of IQR outliers
        iqr_outlier_count: Count of IQR outliers
        iqr_lower_fence: IQR lower bound
        iqr_upper_fence: IQR upper bound
        isolation_forest_ratio: Proportion of IF anomalies
        isolation_forest_count: Count of IF anomalies
        outlier_samples: Sample outlier values
        config: Formatter configuration
        column_name: Column name for pattern-based thresholds

    Returns:
        GroupContext with severity, interpretation, and samples
    """
    config = config or get_default_config()
    metrics: dict[str, MetricContext] = {}
    severities: list[str] = []

    # IQR outliers
    if iqr_outlier_ratio is not None:
        severity = config.get_severity(
            "outliers", "iqr_outlier_ratio", iqr_outlier_ratio, column_name
        )
        severities.append(severity)

        pct = iqr_outlier_ratio * 100
        if iqr_outlier_count is not None:
            interp = f"{pct:.1f}% statistical outliers ({iqr_outlier_count:,} values)"
        else:
            interp = f"{pct:.1f}% statistical outliers"

        metrics["iqr_outlier_ratio"] = MetricContext(
            value=iqr_outlier_ratio,
            severity=severity,
            interpretation=interp,
            details={
                "count": iqr_outlier_count,
                "lower_fence": iqr_lower_fence,
                "upper_fence": iqr_upper_fence,
            },
        )

    # Isolation Forest
    if isolation_forest_ratio is not None:
        severity = config.get_severity(
            "outliers", "isolation_forest_ratio", isolation_forest_ratio, column_name
        )
        severities.append(severity)

        pct = isolation_forest_ratio * 100
        interp = f"{pct:.1f}% anomalies detected by Isolation Forest"

        metrics["isolation_forest_ratio"] = MetricContext(
            value=isolation_forest_ratio,
            severity=severity,
            interpretation=interp,
            details={"count": isolation_forest_count},
        )

    # Determine overall severity
    severity_order = ["none", "low", "moderate", "high", "severe", "critical"]
    overall_severity = "none"
    for sev in severities:
        if sev in severity_order and severity_order.index(sev) > severity_order.index(
            overall_severity
        ):
            overall_severity = sev

    # Format samples for display
    formatted_samples = None
    if outlier_samples:
        formatted_samples = [
            sample.get("value") if isinstance(sample, dict) else sample
            for sample in outlier_samples[:5]  # Limit to 5 samples
        ]

    return GroupContext(
        group_name="outliers",
        overall_severity=overall_severity,
        interpretation=OUTLIER_INTERPRETATIONS.get(overall_severity, "Unknown outlier status"),
        metrics=metrics,
        samples=formatted_samples,
    )


def format_benford_group(
    is_compliant: bool | None = None,
    chi_square: float | None = None,
    p_value: float | None = None,
    digit_distribution: dict[str, float] | None = None,
    config: FormatterConfig | None = None,
    column_name: str | None = None,
) -> GroupContext:
    """Format Benford's Law analysis into contextualized group.

    Args:
        is_compliant: Whether data conforms to Benford's Law
        chi_square: Chi-square test statistic
        p_value: Statistical significance
        digit_distribution: First-digit frequency distribution
        config: Formatter configuration
        column_name: Column name for pattern-based thresholds

    Returns:
        GroupContext with severity, interpretation, and test details
    """
    config = config or get_default_config()
    metrics: dict[str, MetricContext] = {}

    # If no Benford analysis available
    if is_compliant is None and p_value is None:
        return GroupContext(
            group_name="benford",
            overall_severity="none",
            interpretation="Benford's Law analysis not applicable or not computed",
            metrics={},
        )

    # Determine severity from p_value
    if p_value is not None:
        severity = config.get_severity("benford", "p_value", p_value, column_name)
    elif is_compliant is not None:
        severity = "none" if is_compliant else "high"
    else:
        severity = "none"

    # Build interpretation
    if p_value is not None:
        if severity == "none":
            interp = f"Conforms to Benford's Law (p={p_value:.4f})"
        else:
            if chi_square is not None:
                interp = f"Deviates from Benford's Law (p={p_value:.4f}, χ²={chi_square:.2f})"
            else:
                interp = f"Deviates from Benford's Law (p={p_value:.4f})"

        metrics["p_value"] = MetricContext(
            value=p_value,
            severity=severity,
            interpretation=interp,
            details={
                "chi_square": chi_square,
                "is_compliant": is_compliant,
            },
        )

    # Add digit distribution if available
    if digit_distribution:
        # Expected Benford distribution for reference
        expected = {
            "1": 0.301,
            "2": 0.176,
            "3": 0.125,
            "4": 0.097,
            "5": 0.079,
            "6": 0.067,
            "7": 0.058,
            "8": 0.051,
            "9": 0.046,
        }
        max_deviation = 0.0
        for digit, freq in digit_distribution.items():
            if digit in expected:
                deviation = abs(freq - expected[digit])
                max_deviation = max(max_deviation, deviation)

        metrics["digit_distribution"] = MetricContext(
            value=digit_distribution,
            severity="none",  # Informational
            interpretation=f"Max deviation from expected: {max_deviation:.1%}",
            details={"expected": expected},
        )

    return GroupContext(
        group_name="benford",
        overall_severity=severity,
        interpretation=BENFORD_INTERPRETATIONS.get(severity, "Unknown Benford status"),
        metrics=metrics,
    )


# =============================================================================
# Main Formatter
# =============================================================================


def format_statistical_quality(
    null_ratio: float | None = None,
    null_count: int | None = None,
    total_count: int | None = None,
    cardinality_ratio: float | None = None,
    distinct_count: int | None = None,
    iqr_outlier_ratio: float | None = None,
    iqr_outlier_count: int | None = None,
    iqr_lower_fence: float | None = None,
    iqr_upper_fence: float | None = None,
    isolation_forest_ratio: float | None = None,
    isolation_forest_count: int | None = None,
    outlier_samples: list[dict[str, Any]] | None = None,
    benford_compliant: bool | None = None,
    benford_chi_square: float | None = None,
    benford_p_value: float | None = None,
    benford_digit_distribution: dict[str, float] | None = None,
    config: FormatterConfig | None = None,
    column_name: str | None = None,
) -> dict[str, Any]:
    """Format all statistical quality metrics into contextualized output.

    This is the main entry point for statistical formatting. It groups
    related metrics and produces a structured output suitable for LLM
    consumption or human review.

    Args:
        null_ratio: Proportion of null values
        null_count: Count of null values
        total_count: Total row count
        cardinality_ratio: Proportion of distinct values
        distinct_count: Count of distinct values
        iqr_outlier_ratio: IQR outlier proportion
        iqr_outlier_count: IQR outlier count
        iqr_lower_fence: IQR lower boundary
        iqr_upper_fence: IQR upper boundary
        isolation_forest_ratio: IF anomaly proportion
        isolation_forest_count: IF anomaly count
        outlier_samples: Sample outlier values
        benford_compliant: Benford's Law compliance
        benford_chi_square: Benford chi-square statistic
        benford_p_value: Benford p-value
        benford_digit_distribution: First-digit distribution
        config: Formatter configuration
        column_name: Column name for pattern-based thresholds

    Returns:
        Dict with contextualized statistical quality assessment
    """
    config = config or get_default_config()

    # Format each group
    completeness = format_completeness_group(
        null_ratio=null_ratio,
        null_count=null_count,
        total_count=total_count,
        cardinality_ratio=cardinality_ratio,
        distinct_count=distinct_count,
        config=config,
        column_name=column_name,
    )

    outliers = format_outliers_group(
        iqr_outlier_ratio=iqr_outlier_ratio,
        iqr_outlier_count=iqr_outlier_count,
        iqr_lower_fence=iqr_lower_fence,
        iqr_upper_fence=iqr_upper_fence,
        isolation_forest_ratio=isolation_forest_ratio,
        isolation_forest_count=isolation_forest_count,
        outlier_samples=outlier_samples,
        config=config,
        column_name=column_name,
    )

    benford = format_benford_group(
        is_compliant=benford_compliant,
        chi_square=benford_chi_square,
        p_value=benford_p_value,
        digit_distribution=benford_digit_distribution,
        config=config,
        column_name=column_name,
    )

    # Determine overall severity
    severity_order = ["none", "low", "moderate", "high", "severe", "critical"]
    all_severities = [
        completeness.overall_severity,
        outliers.overall_severity,
        benford.overall_severity,
    ]
    overall_severity = max(
        all_severities, key=lambda s: severity_order.index(s) if s in severity_order else 0
    )

    # Build output
    return {
        "statistical_quality": {
            "overall_severity": overall_severity,
            "groups": {
                "completeness": _group_to_dict(completeness),
                "outliers": _group_to_dict(outliers),
                "benford": _group_to_dict(benford),
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
