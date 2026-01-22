"""Null ratio entropy detector.

Measures uncertainty from null values.
High null ratio indicates missing data that affects aggregation reliability.
"""

from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.models import EntropyObject, ResolutionOption


class NullRatioDetector(EntropyDetector):
    """Detector for null value uncertainty.

    Uses null_ratio from statistics profiling to measure data completeness.
    Higher null ratios mean more uncertainty in aggregations.

    Source: statistics/ColumnProfile.null_ratio
    Formula: entropy = min(1.0, null_ratio * multiplier)

    Multiplier is configurable in config/entropy/thresholds.yaml.
    Default: 2x (50% nulls = max entropy).
    """

    detector_id = "null_ratio"
    layer = "value"
    dimension = "nulls"
    sub_dimension = "null_ratio"
    required_analyses = ["statistics"]
    description = "Measures uncertainty from null/missing values"

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Detect null ratio entropy.

        Args:
            context: Detector context with statistics analysis results

        Returns:
            List with single EntropyObject for null ratio
        """
        # Load configuration
        config = get_entropy_config()
        detector_config = config.detector("null_ratio")

        # Get configurable thresholds
        multiplier = detector_config.get("multiplier", 2.0)
        impact_minimal = detector_config.get("impact_minimal", 0.05)
        impact_moderate = detector_config.get("impact_moderate", 0.20)
        impact_significant = detector_config.get("impact_significant", 0.50)
        suggest_declare = detector_config.get("suggest_declare_threshold", 0.1)
        suggest_filter = detector_config.get("suggest_filter_threshold", 0.4)
        reduction_declare = detector_config.get("reduction_declare", 0.3)
        reduction_filter = detector_config.get("reduction_filter", 0.8)
        reduction_impute = detector_config.get("reduction_impute", 0.6)

        stats = context.get_analysis("statistics", {})

        # Extract null ratio
        # Can come as ColumnProfile or dict
        if hasattr(stats, "null_ratio"):
            null_ratio = stats.null_ratio
            null_count = getattr(stats, "null_count", 0)
            total_count = getattr(stats, "total_count", 0)
        else:
            null_ratio = stats.get("null_ratio", 0.0)
            null_count = stats.get("null_count", 0)
            total_count = stats.get("total_count", 0)

        # Calculate entropy using configurable multiplier
        score = min(1.0, null_ratio * multiplier)

        # Classify null impact using configurable thresholds
        if null_ratio == 0:
            null_impact = "none"
        elif null_ratio < impact_minimal:
            null_impact = "minimal"
        elif null_ratio < impact_moderate:
            null_impact = "moderate"
        elif null_ratio < impact_significant:
            null_impact = "significant"
        else:
            null_impact = "critical"

        # Build evidence
        evidence = [
            {
                "null_ratio": null_ratio,
                "null_count": null_count,
                "total_count": total_count,
                "null_impact": null_impact,
            }
        ]

        # Build resolution options using configurable thresholds
        resolution_options: list[ResolutionOption] = []

        if score > suggest_declare:
            # Some nulls - suggest null semantics declaration
            resolution_options.append(
                ResolutionOption(
                    action="declare_null_meaning",
                    parameters={
                        "column": context.column_name,
                        "meanings": ["not_applicable", "unknown", "not_yet_set"],
                    },
                    expected_entropy_reduction=score * reduction_declare,
                    effort="low",
                    description="Declare what null values mean in this context",
                    cascade_dimensions=["semantic.business_meaning"],
                )
            )

        if score > suggest_filter:
            # High nulls - suggest imputation or filtering
            resolution_options.append(
                ResolutionOption(
                    action="filter_nulls",
                    parameters={
                        "column": context.column_name,
                        "strategy": "exclude",
                    },
                    expected_entropy_reduction=score * reduction_filter,
                    effort="low",
                    description="Exclude null values from aggregations",
                )
            )
            resolution_options.append(
                ResolutionOption(
                    action="impute_values",
                    parameters={
                        "column": context.column_name,
                        "strategy": "mean",  # or median, mode, etc.
                    },
                    expected_entropy_reduction=score * reduction_impute,
                    effort="medium",
                    description="Impute missing values using statistical methods",
                )
            )

        return [
            self.create_entropy_object(
                context=context,
                score=score,
                evidence=evidence,
                resolution_options=resolution_options,
            )
        ]
