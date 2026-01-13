"""Outlier rate entropy detector.

Measures uncertainty from outlier values.
High outlier rate indicates data quality issues that affect aggregations.
"""

from dataraum_context.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum_context.entropy.models import EntropyObject, ResolutionOption


class OutlierRateDetector(EntropyDetector):
    """Detector for outlier-based uncertainty.

    Uses IQR outlier detection from statistical quality analysis.
    High outlier rates suggest data quality issues or extreme values
    that can skew aggregations.

    Source: statistics/quality.iqr_outlier_ratio
    Formula: entropy = min(1.0, outlier_ratio * 10)

    The 10x multiplier amplifies the impact: 10% outliers = max entropy.
    """

    detector_id = "outlier_rate"
    layer = "value"
    dimension = "outliers"
    sub_dimension = "outlier_rate"
    required_analyses = ["statistics"]
    description = "Measures uncertainty from outlier values"

    async def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Detect outlier rate entropy.

        Args:
            context: Detector context with statistics/quality analysis

        Returns:
            List with single EntropyObject for outlier rate
        """
        stats = context.get_analysis("statistics", {})

        # Extract outlier information
        # Look for quality sub-object or direct outlier fields
        quality = stats.get("quality", stats)
        outlier_detection = None

        if hasattr(quality, "outlier_detection"):
            outlier_detection = quality.outlier_detection
        elif isinstance(quality, dict):
            outlier_detection = quality.get("outlier_detection")

        # Extract IQR outlier ratio
        if outlier_detection:
            if hasattr(outlier_detection, "iqr_outlier_ratio"):
                outlier_ratio = outlier_detection.iqr_outlier_ratio
                outlier_count = getattr(outlier_detection, "iqr_outlier_count", 0)
                lower_fence = getattr(outlier_detection, "iqr_lower_fence", None)
                upper_fence = getattr(outlier_detection, "iqr_upper_fence", None)
            else:
                outlier_ratio = outlier_detection.get("iqr_outlier_ratio", 0.0)
                outlier_count = outlier_detection.get("iqr_outlier_count", 0)
                lower_fence = outlier_detection.get("iqr_lower_fence")
                upper_fence = outlier_detection.get("iqr_upper_fence")
        else:
            # No outlier detection available - check for direct ratio
            outlier_ratio = stats.get("iqr_outlier_ratio", 0.0)
            outlier_count = stats.get("iqr_outlier_count", 0)
            lower_fence = stats.get("iqr_lower_fence")
            upper_fence = stats.get("iqr_upper_fence")

        # Calculate entropy: amplify outlier ratio (10% outliers = max entropy)
        score = min(1.0, outlier_ratio * 10)

        # Classify outlier impact
        if outlier_ratio == 0:
            outlier_impact = "none"
        elif outlier_ratio < 0.01:
            outlier_impact = "minimal"
        elif outlier_ratio < 0.05:
            outlier_impact = "moderate"
        elif outlier_ratio < 0.10:
            outlier_impact = "significant"
        else:
            outlier_impact = "critical"

        # Build evidence
        evidence = [
            {
                "outlier_ratio": outlier_ratio,
                "outlier_count": outlier_count,
                "outlier_impact": outlier_impact,
                "iqr_lower_fence": lower_fence,
                "iqr_upper_fence": upper_fence,
            }
        ]

        # Build resolution options
        resolution_options: list[ResolutionOption] = []

        if score > 0.2:
            # Some outliers - suggest capping or exclusion
            resolution_options.append(
                ResolutionOption(
                    action="winsorize",
                    parameters={
                        "column": context.column_name,
                        "lower_percentile": 1,
                        "upper_percentile": 99,
                    },
                    expected_entropy_reduction=score * 0.7,
                    effort="low",
                    description="Cap extreme values at specified percentiles",
                )
            )

        if score > 0.5:
            # High outliers - suggest review or removal
            resolution_options.append(
                ResolutionOption(
                    action="exclude_outliers",
                    parameters={
                        "column": context.column_name,
                        "method": "iqr",
                        "multiplier": 1.5,
                    },
                    expected_entropy_reduction=score * 0.9,
                    effort="low",
                    description="Exclude IQR-based outliers from aggregations",
                )
            )
            resolution_options.append(
                ResolutionOption(
                    action="investigate_outliers",
                    parameters={
                        "column": context.column_name,
                    },
                    expected_entropy_reduction=score * 0.5,
                    effort="high",
                    description="Manual review of outlier values for data quality issues",
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
