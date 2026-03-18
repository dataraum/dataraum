"""Null ratio entropy detector.

Measures uncertainty from null values.
High null ratio indicates missing data that affects aggregation reliability.
"""

from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
from dataraum.entropy.models import EntropyObject, ResolutionOption


class NullRatioDetector(EntropyDetector):
    """Detector for null value uncertainty.

    Uses null_ratio from statistics profiling to measure data completeness.
    Higher null ratios mean more uncertainty in aggregations.

    Source: statistics/ColumnProfile.null_ratio
    Score equals null_ratio directly (already 0.0–1.0).
    """

    detector_id = "null_ratio"
    layer = Layer.VALUE
    dimension = Dimension.NULLS
    sub_dimension = SubDimension.NULL_RATIO
    required_analyses = [AnalysisKey.STATISTICS]
    description = "Measures uncertainty from null/missing values"

    def load_data(self, context: DetectorContext) -> None:
        """Load statistical profile for this column."""
        if context.session is None or context.column_id is None:
            return
        from dataraum.entropy.detectors.loaders import load_statistics

        result = load_statistics(context.session, context.column_id)
        if result is not None:
            context.analysis_results["statistics"] = result

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
        impact_minimal = detector_config.get("impact_minimal", 0.05)
        impact_moderate = detector_config.get("impact_moderate", 0.20)
        impact_significant = detector_config.get("impact_significant", 0.50)
        suggest_declare = detector_config.get("suggest_declare_threshold", 0.1)
        suggest_filter = detector_config.get("suggest_filter_threshold", 0.4)
        accepted_columns: list[str] = self.config.get("accepted_columns") or detector_config.get(
            "accepted_columns", []
        )
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

        # Score equals null_ratio directly (already 0.0–1.0)
        score = null_ratio

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
                    action="document_null_semantics",
                    parameters={
                        "column": context.column_name,
                        "meanings": ["not_applicable", "unknown", "not_yet_set"],
                    },
                    effort="low",
                    description="Declare what null values mean in this context",
                )
            )

        if score > suggest_filter:
            # High nulls - suggest imputation or filtering
            resolution_options.append(
                ResolutionOption(
                    action="transform_filter_nulls",
                    parameters={
                        "column": context.column_name,
                        "strategy": "exclude",
                    },
                    effort="low",
                    description="Exclude null values from aggregations",
                )
            )
            resolution_options.append(
                ResolutionOption(
                    action="transform_impute_values",
                    parameters={
                        "column": context.column_name,
                        "strategy": "mean",  # or median, mode, etc.
                    },
                    effort="medium",
                    description="Impute missing values using statistical methods",
                )
            )

        if score > 0:
            # Accept: nulls are structurally expected (tree FK, optional dim)
            resolution_options.append(
                ResolutionOption(
                    action="accept_finding",
                    parameters={
                        "column": context.column_name,
                        "detector_id": self.detector_id,
                    },
                    effort="low",
                    description="Accept nulls as structurally expected for this column",
                )
            )

        # Mark as accepted (score stays honest, contract overrule handles gate)
        target_key = f"{context.table_name}.{context.column_name}"
        if target_key in accepted_columns:
            evidence[0]["accepted"] = True

        return [
            self.create_entropy_object(
                context=context,
                score=score,
                evidence=evidence,
                resolution_options=resolution_options,
            )
        ]
