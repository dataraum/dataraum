"""Benford's Law compliance entropy detector.

Measures deviation from Benford's Law for numeric columns.
Non-compliance in financial/accounting data can indicate data quality
issues, fabrication, or systematic rounding.

Source: statistics/quality.benford_analysis (via quality_data JSON)
"""

from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.models import EntropyObject, ResolutionOption


class BenfordDetector(EntropyDetector):
    """Detector for Benford's Law compliance entropy.

    Only applies to numeric columns with semantic_role = "measure".
    Uses chi-square test results from statistical quality analysis
    to determine if the digit distribution follows Benford's Law.

    Source: statistics/quality.benford_analysis
    Scores configurable in config/entropy/thresholds.yaml.
    """

    detector_id = "benford"
    layer = "value"
    dimension = "distribution"
    sub_dimension = "benford_compliance"
    required_analyses = ["statistics", "semantic"]
    description = "Measures deviation from Benford's Law for numeric columns"

    # Only measure columns benefit from Benford analysis
    _APPLICABLE_ROLES = frozenset({"measure"})

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
        stats = context.get_analysis("statistics", {})
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

        # Calculate score
        if is_compliant:
            score = score_compliant
        else:
            score = score_non_compliant

        # Build evidence
        evidence = [
            {
                "is_compliant": is_compliant,
                "chi_square": chi_square,
                "p_value": p_value,
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
                    },
                    effort="medium",
                    description="Investigate Benford's Law deviation — may indicate data quality issues or systematic rounding",
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
