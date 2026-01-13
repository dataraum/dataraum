"""Base formatting utilities for quality and entropy context.

Provides reusable patterns for transforming raw metrics into
structured, interpretable context for LLM consumption.

This module was moved from quality/formatting/base.py to core/formatting/
to support shared use by both quality and entropy layers.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class SeverityLevel(str, Enum):
    """Standard severity levels used across quality formatters."""

    NONE = "none"
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    SEVERE = "severe"
    CRITICAL = "critical"


# Standard emoji mapping for severity levels
_SEVERITY_EMOJIS: dict[str, str] = {
    "none": "",
    "low": "",
    "moderate": "",
    "high": "",
    "severe": "",
    "critical": "",
}


def severity_emoji(severity: str, include_emoji: bool = True) -> str:
    """Get emoji prefix for a severity level.

    Args:
        severity: Severity level string (none, low, moderate, high, severe, critical)
        include_emoji: Whether to include the emoji (False returns empty string)

    Returns:
        Emoji string with trailing space, or empty string
    """
    if not include_emoji:
        return ""
    emoji = _SEVERITY_EMOJIS.get(severity.lower(), "")
    return f"{emoji} " if emoji else ""


@dataclass
class ThresholdConfig:
    """Configuration for mapping numeric values to severity levels.

    Thresholds define the boundaries between severity levels.
    Values are inclusive at the lower bound, exclusive at the upper.

    Example for VIF thresholds:
        ThresholdConfig(
            thresholds={"none": 1.0, "moderate": 5.0, "high": 10.0},
            default_severity="severe"
        )
        - value <= 1.0 -> "none"
        - 1.0 < value <= 5.0 -> "moderate"
        - 5.0 < value <= 10.0 -> "high"
        - value > 10.0 -> "severe" (default)
    """

    thresholds: dict[str, float]
    default_severity: str = "severe"
    ascending: bool = True  # True = higher values are more severe

    def get_severity(self, value: float) -> str:
        """Map a numeric value to a severity level.

        Args:
            value: Numeric metric value

        Returns:
            Severity level string
        """
        if self.ascending:
            # Higher values = more severe
            sorted_thresholds = sorted(self.thresholds.items(), key=lambda x: x[1])
            for severity, threshold in sorted_thresholds:
                if value <= threshold:
                    return severity
            return self.default_severity
        else:
            # Lower values = more severe (e.g., completeness ratio)
            sorted_thresholds = sorted(self.thresholds.items(), key=lambda x: x[1], reverse=True)
            for severity, threshold in sorted_thresholds:
                if value >= threshold:
                    return severity
            return self.default_severity


def map_to_severity(value: float, thresholds: dict[str, float], default: str = "severe") -> str:
    """Map a numeric value to a severity level using thresholds.

    Convenience function for simple threshold mapping without creating
    a ThresholdConfig object.

    Args:
        value: Numeric metric value
        thresholds: Dict mapping severity names to upper bounds (ascending)
        default: Default severity if value exceeds all thresholds

    Returns:
        Severity level string

    Example:
        >>> map_to_severity(7.5, {"none": 1.0, "moderate": 5.0, "high": 10.0})
        "high"
    """
    config = ThresholdConfig(thresholds=thresholds, default_severity=default)
    return config.get_severity(value)


@dataclass
class InterpretationTemplate:
    """Templates for generating natural language interpretations.

    Each severity level has a template string with optional placeholders
    for value, metric_name, and other context.
    """

    templates: dict[str, str]
    default_template: str = "{metric_name} shows unexpected value: {value}"

    def format(self, severity: str, **kwargs: Any) -> str:
        """Generate interpretation text.

        Args:
            severity: Severity level to look up template
            **kwargs: Values to substitute in template (value, metric_name, etc.)

        Returns:
            Formatted interpretation string
        """
        template = self.templates.get(severity.lower(), self.default_template)
        return template.format(**kwargs)


def generate_interpretation(
    severity: str,
    templates: dict[str, str],
    default_template: str = "{metric_name}: {value}",
    **kwargs: Any,
) -> str:
    """Generate natural language interpretation based on severity.

    Args:
        severity: Severity level string
        templates: Dict mapping severity to template strings
        default_template: Fallback template if severity not found
        **kwargs: Values to substitute in template

    Returns:
        Formatted interpretation string

    Example:
        >>> templates = {
        ...     "none": "No {metric_name} issues detected",
        ...     "high": "{metric_name} is elevated at {value:.1f}",
        ... }
        >>> generate_interpretation("high", templates, metric_name="VIF", value=7.5)
        "VIF is elevated at 7.5"
    """
    template = templates.get(severity.lower(), default_template)
    return template.format(**kwargs)


@dataclass
class RecommendationConfig:
    """Configuration for generating recommendations.

    Maps severity levels to action templates and optionally includes
    severity-appropriate emojis.
    """

    templates: dict[str, str]
    default_template: str = "Review {entity} for potential issues"
    include_emoji: bool = True

    def format(self, severity: str, **kwargs: Any) -> str:
        """Generate recommendation text.

        Args:
            severity: Severity level
            **kwargs: Values to substitute (entity, metric_name, etc.)

        Returns:
            Formatted recommendation with optional emoji prefix
        """
        template = self.templates.get(severity.lower(), self.default_template)
        emoji = severity_emoji(severity, self.include_emoji)
        return f"{emoji}{template.format(**kwargs)}".strip()


def generate_recommendation(
    severity: str,
    templates: dict[str, str],
    default_template: str = "Review for potential issues",
    include_emoji: bool = True,
    **kwargs: Any,
) -> str:
    """Generate actionable recommendation based on severity.

    Args:
        severity: Severity level string
        templates: Dict mapping severity to recommendation templates
        default_template: Fallback template if severity not found
        include_emoji: Whether to prefix with severity emoji
        **kwargs: Values to substitute in template

    Returns:
        Formatted recommendation string with optional emoji

    Example:
        >>> templates = {
        ...     "high": "Investigate {column} for redundancy",
        ...     "severe": "Remove or consolidate {column}",
        ... }
        >>> generate_recommendation("severe", templates, column="price_usd")
        "Remove or consolidate price_usd"
    """
    template = templates.get(severity.lower(), default_template)
    emoji = severity_emoji(severity, include_emoji)
    return f"{emoji}{template.format(**kwargs)}".strip()


def format_list_with_overflow(
    items: list[str], max_display: int = 3, conjunction: str = "and"
) -> str:
    """Format a list of items with overflow indicator.

    Args:
        items: List of item strings
        max_display: Maximum items to display before truncating
        conjunction: Word to use before overflow count

    Returns:
        Formatted string like "A, B, C, and 5 others"

    Example:
        >>> format_list_with_overflow(["a", "b", "c", "d", "e"], max_display=3)
        "a, b, c, and 2 others"
    """
    if not items:
        return ""

    if len(items) <= max_display:
        return ", ".join(items)

    displayed = items[:max_display]
    overflow = len(items) - max_display
    return f"{', '.join(displayed)}, {conjunction} {overflow} others"


# === Common Threshold Configurations ===


@dataclass
class CommonThresholds:
    """Common threshold configurations used across formatters."""

    # VIF (Variance Inflation Factor) thresholds
    VIF: ThresholdConfig = field(
        default_factory=lambda: ThresholdConfig(
            thresholds={"none": 1.0, "low": 2.5, "moderate": 5.0, "high": 10.0},
            default_severity="severe",
        )
    )

    # Condition Index thresholds
    CONDITION_INDEX: ThresholdConfig = field(
        default_factory=lambda: ThresholdConfig(
            thresholds={"none": 10.0, "moderate": 30.0},
            default_severity="severe",
        )
    )

    # Correlation coefficient thresholds (absolute value)
    CORRELATION: ThresholdConfig = field(
        default_factory=lambda: ThresholdConfig(
            thresholds={"none": 0.3, "low": 0.5, "moderate": 0.7, "high": 0.9},
            default_severity="severe",
        )
    )

    # Completeness ratio thresholds (descending - lower is worse)
    COMPLETENESS: ThresholdConfig = field(
        default_factory=lambda: ThresholdConfig(
            thresholds={"none": 0.99, "low": 0.95, "moderate": 0.8, "high": 0.5},
            default_severity="severe",
            ascending=False,
        )
    )

    # Null ratio thresholds
    NULL_RATIO: ThresholdConfig = field(
        default_factory=lambda: ThresholdConfig(
            thresholds={"none": 0.01, "low": 0.05, "moderate": 0.2, "high": 0.5},
            default_severity="severe",
        )
    )

    # Outlier ratio thresholds
    OUTLIER_RATIO: ThresholdConfig = field(
        default_factory=lambda: ThresholdConfig(
            thresholds={"none": 0.01, "low": 0.05, "moderate": 0.1, "high": 0.2},
            default_severity="severe",
        )
    )


# Singleton instance for convenience
THRESHOLDS = CommonThresholds()
