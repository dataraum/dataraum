"""Outlier rate entropy detector.

Measures uncertainty from outlier values.
High outlier rate indicates data quality issues that affect aggregations.

Distribution-aware: adapts to the data shape using semantic and statistical
context from the pipeline.

- **Zero-inflated columns** (e.g. debit/credit mutual exclusivity): IQR is
  compressed by the zero mass, producing false outlier ratios. Detected via
  zero_ratio > 30%. The detector excludes zeros before computing outlier ratio.
- **Right-skewed distributions** (e.g. invoice amounts): IQR on linear values
  flags the natural long tail as outliers. Detected via skewness > 1.5. The
  detector uses log-IQR instead (outlier detection in log-space).
- **Dimensionless columns** (rates, ratios): skipped entirely — value ranges
  are structurally determined, not quality signals.
"""

import math
from typing import Any

from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
from dataraum.entropy.models import EntropyObject, ResolutionOption

# Thresholds for distribution shape detection
_ZERO_INFLATED_THRESHOLD = 0.30  # > 30% zeros → exclude zeros before IQR
_SKEWNESS_THRESHOLD = 1.5  # skewness > 1.5 → use log-IQR


class OutlierRateDetector(EntropyDetector):
    """Detector for outlier-based uncertainty.

    Uses IQR outlier detection from statistical quality analysis, adapted
    to the distribution shape:

    - Zero-inflated: excludes structural zeros before computing IQR
    - Right-skewed (log-normal): uses log-IQR instead of linear IQR
    - Normal/symmetric: uses standard IQR

    Source: statistics/quality.iqr_outlier_ratio (linear) or recomputed
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

    # Only measure columns benefit from outlier analysis.
    # Dimensions, attributes, timestamps, keys — outliers are meaningless.
    _APPLICABLE_ROLES = frozenset({"measure"})

    # Columns where the unit is "dimensionless" are rates, ratios, indices,
    # or percentages — not quantities. IQR/zscore outlier detection on
    # exchange rates (0.006 for JPY, 1.2 for EUR) is structurally wrong,
    # not a data quality signal.
    _SKIP_UNIT_SOURCES = frozenset({"dimensionless"})

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
        # Only apply to measure columns — outliers are meaningless for
        # keys, dimensions, attributes, timestamps
        semantic = context.get_analysis("semantic", {})
        if hasattr(semantic, "semantic_role"):
            role = semantic.semantic_role
        else:
            role = semantic.get("semantic_role")
        if role not in self._APPLICABLE_ROLES:
            return []

        # Skip dimensionless columns (rates, ratios, indices) — their
        # value ranges are structurally determined, not quality signals
        if hasattr(semantic, "unit_source_column"):
            unit_src = semantic.unit_source_column
        else:
            unit_src = semantic.get("unit_source_column")
        if unit_src in self._SKIP_UNIT_SOURCES:
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

        # --- Distribution shape adaptation ---
        # IQR/zscore outlier detection fails on two common financial data shapes:
        #
        # 1. Zero-inflated (e.g. debit column where 50% are zero because those
        #    rows are credit entries): the zero mass compresses IQR, making
        #    normal values look like outliers. Fix: exclude zeros, recompute.
        #
        # 2. Right-skewed / log-normal (e.g. invoice amounts ranging 100–50k):
        #    IQR on linear values flags the natural long tail. Fix: compute
        #    IQR in log-space where the distribution is approximately normal.
        #
        # Both use statistics already computed by the profiler.
        shape_adjusted = False
        profile_data = stats.get("profile_data", {})
        numeric_stats = (
            profile_data.get("numeric_stats", {}) if isinstance(profile_data, dict) else {}
        )

        if isinstance(numeric_stats, dict):
            skewness = numeric_stats.get("skewness")
            percentiles = numeric_stats.get("percentiles", {})
            p25 = percentiles.get("p25")
            p75 = percentiles.get("p75")
            min_val = numeric_stats.get("min_value")
            max_val = numeric_stats.get("max_value")

            # Detect zero-inflation: check if >30% of values are at zero
            top_values = (
                profile_data.get("top_values", []) if isinstance(profile_data, dict) else []
            )
            zero_pct = 0.0
            for tv in top_values:
                if isinstance(tv, dict) and tv.get("value") in (0, 0.0):
                    zero_pct = tv.get("percentage", 0.0) / 100.0
                    break

            # For log-IQR we need positive Q1 and Q3. Zero-inflated columns
            # have Q1=0 (and sometimes median=0), so find the first positive
            # percentile to use as Q1.
            effective_q1 = p25 if (p25 is not None and p25 > 0) else None
            p50 = percentiles.get("p50")
            if effective_q1 is None and p50 is not None and p50 > 0:
                effective_q1 = p50  # Median is positive — use as Q1 proxy
            if effective_q1 is None and zero_pct > 0.5:
                # More than 50% zeros with median at zero — IQR is structurally
                # unreliable. These are typically mutual exclusivity columns
                # (debit/credit, where one is always zero) handled by the
                # dimensional_entropy detector. Skip outlier scoring entirely.
                return []

            # Determine if the column is right-skewed (log-normal shaped).
            # Zero-inflated columns are also right-skewed in their non-zero part,
            # so both cases use the same log-IQR path.
            is_right_skewed = skewness is not None and skewness > _SKEWNESS_THRESHOLD
            is_zero_inflated = zero_pct > _ZERO_INFLATED_THRESHOLD
            needs_log_iqr = (is_right_skewed or is_zero_inflated) and (
                effective_q1 is not None
                and effective_q1 > 0
                and p75 is not None
                and p75 > 0
                and p75 > effective_q1
            )

            if needs_log_iqr:
                # Log-IQR: compute outlier fences in log-space where the
                # distribution is approximately normal. This correctly handles
                # both pure right-skew (invoice amounts) and zero-inflated
                # columns (debit/credit mutual exclusivity).
                log_q1 = math.log(effective_q1)
                log_q3 = math.log(p75)
                log_iqr = log_q3 - log_q1
                log_upper = log_q3 + 1.5 * log_iqr
                log_lower = log_q1 - 1.5 * log_iqr

                upper_threshold = math.exp(log_upper)
                lower_threshold = math.exp(log_lower)

                # Estimate outlier ratio from percentiles
                p01 = percentiles.get("p01", min_val or 0)
                p99 = percentiles.get("p99", max_val)

                # Fraction below lower threshold (only among non-zero values)
                lower_outliers = 0.0
                if p01 > 0 and p01 < lower_threshold and effective_q1 > lower_threshold:
                    lower_outliers = (
                        0.01 * (lower_threshold - p01) / (effective_q1 - p01)
                        if effective_q1 > p01
                        else 0.0
                    )

                # Fraction above upper threshold
                upper_outliers = 0.0
                if p99 is not None and p99 > upper_threshold:
                    if p99 > p75:
                        upper_outliers = 0.25 * (p99 - upper_threshold) / (p99 - p75)
                    else:
                        upper_outliers = 0.0

                outlier_ratio = max(0.0, min(lower_outliers + upper_outliers, 1.0))
                shape_adjusted = True

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
        if shape_adjusted:
            evidence_dict["shape_adjusted"] = True
            reasons = []
            if zero_pct > _ZERO_INFLATED_THRESHOLD:
                reasons.append("zero_inflated")
                evidence_dict["zero_pct"] = zero_pct
            if skewness is not None and skewness > _SKEWNESS_THRESHOLD:
                reasons.append("log_normal")
                evidence_dict["skewness"] = skewness
            evidence_dict["adjustment_reason"] = "+".join(reasons) if reasons else "log_iqr"
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
                    action="document_accepted_outlier_rate",
                    parameters={
                        "column": context.column_name,
                        "detector_id": self.detector_id,
                    },
                    effort="low",
                    description="Accept outlier findings as expected for this column",
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
