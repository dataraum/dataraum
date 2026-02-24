"""Outlier rate entropy detector.

Measures uncertainty from outlier values.
High outlier rate indicates data quality issues that affect aggregations.
"""

from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.models import EntropyObject, ResolutionOption


class OutlierRateDetector(EntropyDetector):
    """Detector for outlier-based uncertainty.

    Uses IQR outlier detection from statistical quality analysis.
    High outlier rates suggest data quality issues or extreme values
    that can skew aggregations.

    Source: statistics/quality.iqr_outlier_ratio
    Formula: piecewise-linear mapping aligned with impact thresholds.
    0% → 0.0, 1% → 0.15, 5% → 0.4, 10% → 0.65, 20%+ → 1.0

    Impact thresholds configurable in config/entropy/thresholds.yaml.
    """

    detector_id = "outlier_rate"
    layer = "value"
    dimension = "outliers"
    sub_dimension = "outlier_rate"
    required_analyses = ["statistics", "semantic"]
    description = "Measures uncertainty from outlier values"

    # Semantic roles where outlier detection is meaningless
    _SKIP_ROLES = frozenset({"key", "foreign_key"})

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Detect outlier rate entropy.

        Skips columns with semantic_role key/foreign_key (outliers are
        meaningless for identifiers).

        Args:
            context: Detector context with statistics and semantic analysis

        Returns:
            List with single EntropyObject for outlier rate,
            or empty list if not applicable
        """
        # Skip identifier columns — outliers are meaningless for keys
        semantic = context.get_analysis("semantic", {})
        if hasattr(semantic, "semantic_role"):
            role = semantic.semantic_role
        else:
            role = semantic.get("semantic_role")
        if role in self._SKIP_ROLES:
            return []

        # Load configuration
        config = get_entropy_config()
        detector_config = config.detector("outlier_rate")

        # Get configurable thresholds
        impact_minimal = detector_config.get("impact_minimal", 0.01)
        impact_moderate = detector_config.get("impact_moderate", 0.05)
        impact_significant = detector_config.get("impact_significant", 0.10)
        score_at_minimal = detector_config.get("score_at_minimal", 0.15)
        score_at_moderate = detector_config.get("score_at_moderate", 0.40)
        score_at_significant = detector_config.get("score_at_significant", 0.65)
        suggest_winsorize = detector_config.get("suggest_winsorize_threshold", 0.2)
        suggest_exclude = detector_config.get("suggest_exclude_threshold", 0.5)
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

        # Calculate entropy using piecewise-linear mapping aligned with impact thresholds
        # 0% → 0.0, impact_minimal → score_at_minimal, impact_moderate → score_at_moderate,
        # impact_significant → score_at_significant, 2×impact_significant → 1.0
        if outlier_ratio == 0:
            score = 0.0
        elif outlier_ratio < impact_minimal:
            score = (outlier_ratio / impact_minimal) * score_at_minimal
        elif outlier_ratio < impact_moderate:
            score = (
                score_at_minimal
                + (outlier_ratio - impact_minimal)
                / (impact_moderate - impact_minimal)
                * (score_at_moderate - score_at_minimal)
            )
        elif outlier_ratio < impact_significant:
            score = (
                score_at_moderate
                + (outlier_ratio - impact_moderate)
                / (impact_significant - impact_moderate)
                * (score_at_significant - score_at_moderate)
            )
        else:
            score = min(
                1.0,
                score_at_significant
                + (outlier_ratio - impact_significant)
                / impact_significant
                * (1.0 - score_at_significant),
            )

        # Classify outlier impact using configurable thresholds
        if outlier_ratio == 0:
            outlier_impact = "none"
        elif outlier_ratio < impact_minimal:
            outlier_impact = "minimal"
        elif outlier_ratio < impact_moderate:
            outlier_impact = "moderate"
        elif outlier_ratio < impact_significant:
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

        # Build resolution options using configurable thresholds
        resolution_options: list[ResolutionOption] = []

        if score > suggest_winsorize:
            # Some outliers - suggest capping or exclusion
            resolution_options.append(
                ResolutionOption(
                    action="transform_winsorize",
                    parameters={
                        "column": context.column_name,
                        "lower_percentile": 1,
                        "upper_percentile": 99,
                    },
                    effort="low",
                    description="Cap extreme values at specified percentiles",
                )
            )

        if score > suggest_exclude:
            # High outliers - suggest review or removal
            resolution_options.append(
                ResolutionOption(
                    action="transform_exclude_outliers",
                    parameters={
                        "column": context.column_name,
                        "method": "iqr",
                        "multiplier": 1.5,
                    },
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
