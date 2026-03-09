"""Benford's Law compliance entropy detector.

Measures deviation from Benford's Law for numeric columns.
Non-compliance in financial/accounting data can indicate data quality
issues, fabrication, or systematic rounding.

For non-compliant columns, severity is computed using Cramér's V
(chi-square normalized by sample size) instead of raw chi-square.
This prevents large datasets from inflating scores — a chi_sq of 1917
with n=8000 is a small effect (V=0.17), not a catastrophic one.

Source: statistics/quality.benford_analysis (via quality_data JSON)
"""

import math

from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import AnalysisKey, Dimension, FixAction, Layer, SubDimension
from dataraum.entropy.models import EntropyObject, ResolutionOption

# Benford's Law uses digits 1-9, so df = k - 1 = 8
_BENFORD_DF = 8


class BenfordDetector(EntropyDetector):
    """Detector for Benford's Law compliance entropy.

    Only applies to numeric columns with semantic_role = "measure".
    Uses chi-square test results from statistical quality analysis
    to determine if the digit distribution follows Benford's Law.

    Severity scaling uses Cramér's V (effect size) rather than raw
    chi-square, so scores reflect practical significance, not just
    statistical significance inflated by sample size.

    Source: statistics/quality.benford_analysis
    Scores configurable in config/entropy/thresholds.yaml.
    """

    detector_id = "benford"
    layer = Layer.VALUE
    dimension = Dimension.DISTRIBUTION
    sub_dimension = SubDimension.BENFORD_COMPLIANCE
    required_analyses = [AnalysisKey.STATISTICS, AnalysisKey.SEMANTIC]
    description = "Measures deviation from Benford's Law for numeric columns"

    # Only measure columns benefit from Benford analysis
    _APPLICABLE_ROLES = frozenset({"measure"})

    @property
    def fixable_actions(self) -> set[FixAction]:
        """Accept-finding lowers the score for reviewed Benford deviations."""
        return {FixAction.ACCEPT_FINDING}

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
        """Detect Benford's Law compliance entropy.

        Skips columns that are not numeric measures. Uses pre-computed
        Benford analysis from the statistics quality phase.

        Args:
            context: Detector context with statistics and semantic analysis

        Returns:
            List with single EntropyObject for Benford compliance,
            or empty list if not applicable
        """
        # Only apply to measure columns
        semantic = context.get_analysis("semantic", {})
        if hasattr(semantic, "semantic_role"):
            role = semantic.semantic_role
        else:
            role = semantic.get("semantic_role")
        if role not in self._APPLICABLE_ROLES:
            return []

        # Load configuration
        config = get_entropy_config()
        detector_config = config.detector("benford")

        score_compliant = detector_config.get("score_compliant", 0.1)
        score_non_compliant = detector_config.get("score_non_compliant", 0.7)
        chi_sq_escalation_threshold = detector_config.get("chi_sq_escalation_threshold", 0.01)
        v_max = detector_config.get("cramers_v_max", 0.5)
        min_sample_size = detector_config.get("min_sample_size", 100)
        score_accepted = self.config.get("score_accepted") or detector_config.get("score_accepted", 0.2)
        accepted_columns: list[str] = (
            self.config.get("accepted_columns") or detector_config.get("accepted_columns", [])
        )

        stats = context.get_analysis("statistics", {})
        n_values = stats.get("total_count", 0) or 0
        quality = stats.get("quality", stats)

        # Try to get Benford analysis from quality data
        benford_analysis = None
        benford_compliant = None

        if isinstance(quality, dict):
            benford_analysis = quality.get("benford_analysis")
            benford_compliant = quality.get("benford_compliant")

        # If no Benford data available, skip
        if benford_compliant is None and benford_analysis is None:
            return []

        # Skip unreliable small samples
        if n_values < min_sample_size:
            return []

        # Determine compliance
        if benford_analysis and isinstance(benford_analysis, dict):
            is_compliant = benford_analysis.get("is_compliant", benford_compliant)
            chi_square = benford_analysis.get("chi_square")
            p_value = benford_analysis.get("p_value")
            digit_distribution = benford_analysis.get("digit_distribution")
            interpretation = benford_analysis.get("interpretation", "")
        else:
            is_compliant = benford_compliant
            chi_square = None
            p_value = None
            digit_distribution = None
            interpretation = ""

        # Calculate Cramér's V (effect size normalized for sample size)
        cramers_v: float | None = None
        if chi_square and chi_square > 0 and n_values > 0:
            cramers_v = math.sqrt(chi_square / (n_values * _BENFORD_DF))

        # Calculate score
        if p_value is not None and isinstance(p_value, (int, float)):
            if p_value > chi_sq_escalation_threshold:
                # Mild: p-value gradient maps to [score_compliant, score_non_compliant]
                score = score_compliant + (score_non_compliant - score_compliant) * (1.0 - p_value)
                score = max(score_compliant, min(score_non_compliant, score))
            else:
                # Non-compliant: use Cramér's V for practical significance
                # V < 0.1 = negligible, 0.1-0.3 = small, 0.3-0.5 = medium, >0.5 = large
                if cramers_v is not None:
                    severity = min(1.0, cramers_v / v_max)
                    score = score_non_compliant + (1.0 - score_non_compliant) * severity
                else:
                    score = score_non_compliant
                score = max(score_compliant, min(1.0, score))
        elif is_compliant:
            score = score_compliant
        else:
            score = score_non_compliant

        # Classify effect size
        if cramers_v is not None:
            if cramers_v < 0.1:
                effect_size = "negligible"
            elif cramers_v < 0.3:
                effect_size = "small"
            elif cramers_v < 0.5:
                effect_size = "medium"
            else:
                effect_size = "large"
        else:
            effect_size = None

        # Build evidence
        evidence = [
            {
                "is_compliant": is_compliant,
                "chi_square": chi_square,
                "p_value": p_value,
                "n_values": n_values,
                "cramers_v": round(cramers_v, 4) if cramers_v is not None else None,
                "effect_size": effect_size,
                "digit_distribution": digit_distribution,
                "interpretation": interpretation,
            }
        ]

        # Resolution options for non-compliant columns
        resolution_options: list[ResolutionOption] = []
        if not is_compliant:
            resolution_options.append(
                ResolutionOption(
                    action="investigate_benford_deviation",
                    parameters={
                        "column": context.column_name,
                        "chi_square": chi_square,
                        "p_value": p_value,
                        "cramers_v": round(cramers_v, 4) if cramers_v else None,
                        "effect_size": effect_size,
                    },
                    effort="medium",
                    description="Investigate Benford's Law deviation — may indicate data quality issues or systematic rounding",
                )
            )
            resolution_options.append(
                ResolutionOption(
                    action="accept_finding",
                    parameters={
                        "column": context.column_name,
                        "detector_id": self.detector_id,
                    },
                    effort="low",
                    description="Accept Benford deviation as expected for this data",
                )
            )

        # Apply acceptance floor if this column was previously accepted
        target_key = f"{context.table_name}.{context.column_name}"
        if target_key in accepted_columns:
            score = score_accepted
            evidence[0]["accepted"] = True

        return [
            self.create_entropy_object(
                context=context,
                score=score,
                evidence=evidence,
                resolution_options=resolution_options,
            )
        ]
