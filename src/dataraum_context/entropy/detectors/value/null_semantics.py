"""Null ratio entropy detector.

Measures uncertainty from null values.
High null ratio indicates missing data that affects aggregation reliability.
"""

from dataraum_context.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum_context.entropy.models import EntropyObject, ResolutionOption


class NullRatioDetector(EntropyDetector):
    """Detector for null value uncertainty.

    Uses null_ratio from statistics profiling to measure data completeness.
    Higher null ratios mean more uncertainty in aggregations.

    Source: statistics/ColumnProfile.null_ratio
    Formula: entropy = min(1.0, null_ratio * 2)

    The 2x multiplier amplifies the impact: 50% nulls = max entropy.
    """

    detector_id = "null_ratio"
    layer = "value"
    dimension = "nulls"
    sub_dimension = "null_ratio"
    required_analyses = ["statistics"]
    description = "Measures uncertainty from null/missing values"

    async def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Detect null ratio entropy.

        Args:
            context: Detector context with statistics analysis results

        Returns:
            List with single EntropyObject for null ratio
        """
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

        # Calculate entropy: amplify null ratio (50% nulls = max entropy)
        score = min(1.0, null_ratio * 2)

        # Classify null impact
        if null_ratio == 0:
            null_impact = "none"
        elif null_ratio < 0.05:
            null_impact = "minimal"
        elif null_ratio < 0.20:
            null_impact = "moderate"
        elif null_ratio < 0.50:
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

        # Build resolution options
        resolution_options: list[ResolutionOption] = []

        if score > 0.1:
            # Some nulls - suggest null semantics declaration
            resolution_options.append(
                ResolutionOption(
                    action="declare_null_meaning",
                    parameters={
                        "column": context.column_name,
                        "meanings": ["not_applicable", "unknown", "not_yet_set"],
                    },
                    expected_entropy_reduction=score * 0.3,
                    effort="low",
                    description="Declare what null values mean in this context",
                    cascade_dimensions=["semantic.business_meaning"],
                )
            )

        if score > 0.4:
            # High nulls - suggest imputation or filtering
            resolution_options.append(
                ResolutionOption(
                    action="filter_nulls",
                    parameters={
                        "column": context.column_name,
                        "strategy": "exclude",
                    },
                    expected_entropy_reduction=score * 0.8,
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
                    expected_entropy_reduction=score * 0.6,
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
