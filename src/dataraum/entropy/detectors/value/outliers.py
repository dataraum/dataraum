"""Outlier rate entropy detector.

Measures uncertainty from outlier values.
High outlier rate indicates data quality issues that affect aggregations.
"""

from typing import Any

from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
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
    layer = Layer.VALUE
    dimension = Dimension.OUTLIERS
    sub_dimension = SubDimension.OUTLIER_RATE
    required_analyses = [AnalysisKey.STATISTICS, AnalysisKey.SEMANTIC]
    description = "Measures uncertainty from outlier values"

    # Semantic roles where outlier detection is meaningless
    _SKIP_ROLES = frozenset({"key", "foreign_key"})

    def load_data(self, context: DetectorContext) -> None:
        """Load statistics and semantic annotation for this column."""
        if context.session is None or context.column_id is None:
            return
        from dataraum.entropy.detectors.loaders import load_semantic, load_statistics

        stats = load_statistics(context.session, context.column_id)
        if stats is not None:
            context.analysis_results["statistics"] = stats
        sem = load_semantic(context.session, context.column_id)
        if sem is not None:
            context.analysis_results["semantic"] = sem

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
        cv_attenuation_threshold = detector_config.get("cv_attenuation_threshold", 2.0)
        accepted_columns: list[str] = self.config.get("accepted_columns") or detector_config.get(
            "accepted_columns", []
        )
        stats = context.get_analysis("statistics", {})

        # Extract outlier information
        # Look for quality sub-object or direct outlier fields
        quality = stats.get("quality", stats)
        outlier_detection = None

        if hasattr(quality, "outlier_detection"):
            outlier_detection = quality.outlier_detection
        elif isinstance(quality, dict):
            outlier_detection = quality.get("outlier_detection")

        # Extract IQR and Z-score outlier ratios
        zscore_ratio: float = 0.0
        if outlier_detection:
            if hasattr(outlier_detection, "iqr_outlier_ratio"):
                outlier_ratio = outlier_detection.iqr_outlier_ratio
                outlier_count = getattr(outlier_detection, "iqr_outlier_count", 0)
                lower_fence = getattr(outlier_detection, "iqr_lower_fence", None)
                upper_fence = getattr(outlier_detection, "iqr_upper_fence", None)
                zscore_ratio = getattr(outlier_detection, "zscore_outlier_ratio", 0.0)
            else:
                outlier_ratio = outlier_detection.get("iqr_outlier_ratio", 0.0)
                outlier_count = outlier_detection.get("iqr_outlier_count", 0)
                lower_fence = outlier_detection.get("iqr_lower_fence")
                upper_fence = outlier_detection.get("iqr_upper_fence")
                zscore_ratio = outlier_detection.get("zscore_outlier_ratio", 0.0)
        else:
            # No nested outlier_detection — check for direct ratio fields
            outlier_ratio = stats.get("iqr_outlier_ratio", 0.0)
            if not outlier_ratio:
                # Column was not assessed (e.g. non-numeric). Return empty
                # to avoid diluting the dimension average with false zeros.
                return []
            outlier_count = stats.get("iqr_outlier_count", 0)
            lower_fence = stats.get("iqr_lower_fence")
            upper_fence = stats.get("iqr_upper_fence")

        # Use the worse of IQR and modified Z-score (MAD-based) outlier ratios.
        # Both are percentages on the same scale; the modified Z-score is more
        # robust for non-normal data (Iglewicz & Hoaglin 1993, Leys et al. 2013).
        outlier_ratio = max(outlier_ratio, zscore_ratio or 0.0)

        # Calculate entropy using piecewise-linear mapping aligned with impact thresholds
        # 0% → 0.0, impact_minimal → score_at_minimal, impact_moderate → score_at_moderate,
        # impact_significant → score_at_significant, 2×impact_significant → 1.0
        if outlier_ratio == 0:
            score = 0.0
        elif outlier_ratio < impact_minimal:
            score = (outlier_ratio / impact_minimal) * score_at_minimal
        elif outlier_ratio < impact_moderate:
            score = score_at_minimal + (outlier_ratio - impact_minimal) / (
                impact_moderate - impact_minimal
            ) * (score_at_moderate - score_at_minimal)
        elif outlier_ratio < impact_significant:
            score = score_at_moderate + (outlier_ratio - impact_moderate) / (
                impact_significant - impact_moderate
            ) * (score_at_significant - score_at_moderate)
        else:
            score = min(
                1.0,
                score_at_significant
                + (outlier_ratio - impact_significant)
                / impact_significant
                * (1.0 - score_at_significant),
            )

        # Attenuate score for high-CV columns where IQR outlier detection is unreliable.
        # Columns with high coefficient of variation (e.g., FX rates spanning 0.7 to 150)
        # naturally have wide ranges — IQR "outliers" are legitimate values, not quality issues.
        # Attenuate using robust_cv (MAD/|median|) which is not inflated by the outliers
        # being detected, avoiding the self-defeating attenuation loop that stddev-based CV caused.
        # No fallback to classical cv — if robust_cv is absent, skip attenuation entirely.
        cv_attenuated = False
        profile_data = stats.get("profile_data", {})
        if isinstance(profile_data, dict):
            numeric_stats = profile_data.get("numeric_stats", {})
            if isinstance(numeric_stats, dict):
                cv = numeric_stats.get("robust_cv")
                if cv is not None and cv > cv_attenuation_threshold:
                    dampen = cv_attenuation_threshold / cv
                    score = score * dampen
                    cv_attenuated = True

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
        evidence_dict: dict[str, Any] = {
            "outlier_ratio": outlier_ratio,
            "outlier_count": outlier_count,
            "outlier_impact": outlier_impact,
            "iqr_lower_fence": lower_fence,
            "iqr_upper_fence": upper_fence,
        }
        if cv_attenuated:
            evidence_dict["cv_attenuated"] = True
            evidence_dict["robust_cv"] = cv  # type: ignore[possibly-undefined]

        # Add Z-score cross-method evidence
        if zscore_ratio > 0:
            evidence_dict["zscore_outlier_ratio"] = zscore_ratio
            iqr = outlier_ratio
            if iqr > 0 and zscore_ratio > 0:
                evidence_dict["method_agreement"] = min(iqr, zscore_ratio) / max(iqr, zscore_ratio)

        evidence = [evidence_dict]

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
                    action="investigate_outliers",
                    parameters={
                        "column": context.column_name,
                    },
                    effort="high",
                    description="Manual review of outlier values for data quality issues",
                )
            )

        if score > 0:
            # Accept finding: user reviewed, outliers are expected for this column
            resolution_options.append(
                ResolutionOption(
                    action="accept_finding",
                    parameters={
                        "column": context.column_name,
                        "detector_id": self.detector_id,
                    },
                    effort="low",
                    description="Accept outlier findings as expected for this column",
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
