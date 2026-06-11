"""Benford's Law surprise detector.

Measures how far a numeric measure column's leading-digit distribution sits from
Benford's Law — financial/accounting data that doesn't follow it can indicate
fabrication, systematic rounding, or a quality problem.

The score is the KL surprise ``D_KL(observed ‖ benford)`` over the leading-digit
distribution (see :mod:`dataraum.entropy.surprise`), NOT a chi-square / Cramér's-V
boost curve. KL is intensive (a per-observation average), so it is sample-size
invariant: clean Benford-following data scores ~0 whether n=100 or n=8000. The old
chi-square path could not — at large n the compliance test rejects any deviation,
dumping every clean column at the non-compliant floor (clean columns baselined at
0.7–0.8). Severity per intent now lives in the loss table (entropy/loss.yaml), so
this detector emits a pure surprise score and feeds the loss path, not a network
node.

Source: statistics/quality.benford_analysis (digit_distribution, via quality_data)
"""

import math

from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
from dataraum.entropy.models import EntropyObject
from dataraum.entropy.surprise import kl_divergence, surprise_score

# Benford's Law expected leading-digit (1..9) probabilities.
_BENFORD_REFERENCE = [math.log10(1 + 1 / d) for d in range(1, 10)]


class BenfordDetector(EntropyDetector):
    """Detector for Benford's Law surprise.

    Only applies to numeric columns with semantic_role = "measure". Scores the KL
    divergence of the observed leading-digit distribution from Benford's Law, so
    the score reflects practical departure, not sample-size-inflated statistical
    significance.

    Source: statistics/quality.benford_analysis
    """

    detector_id = "benford"
    layer = Layer.VALUE
    dimension = Dimension.DISTRIBUTION
    sub_dimension = SubDimension.BENFORD_COMPLIANCE
    required_analyses = [AnalysisKey.STATISTICS, AnalysisKey.SEMANTIC]
    description = "Measures KL surprise from Benford's Law for numeric measure columns"

    # Only measure columns benefit from Benford analysis
    _APPLICABLE_ROLES = frozenset({"measure"})

    # Dimensionless columns (rates, ratios, indices) don't follow Benford's
    # Law — their leading digits are determined by scale, not transaction counts.
    _SKIP_UNIT_SOURCES = frozenset({"dimensionless"})

    def load_data(self, context: DetectorContext) -> None:
        """Load statistics and semantic annotation for this column."""
        if context.session is None or context.column_id is None:
            return
        from dataraum.entropy.detectors.loaders import load_semantic, load_statistics

        stats = load_statistics(
            context.session, context.column_id, context.run_id, base_runs=context.base_runs
        )
        if stats is not None:
            context.analysis_results["statistics"] = stats
        sem = load_semantic(
            context.session, context.column_id, context.run_id, base_runs=context.base_runs
        )
        if sem is not None:
            context.analysis_results["semantic"] = sem

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Detect Benford's Law surprise.

        Skips columns that are not numeric measures. Scores the KL divergence of
        the observed leading-digit distribution (from the statistics quality
        phase) against Benford's Law.

        Args:
            context: Detector context with statistics and semantic analysis

        Returns:
            List with a single EntropyObject for Benford surprise, or empty list
            if not applicable.
        """
        # Only apply to measure columns
        semantic = context.get_analysis("semantic", {})
        if hasattr(semantic, "semantic_role"):
            role = semantic.semantic_role
        else:
            role = semantic.get("semantic_role")
        if role not in self._APPLICABLE_ROLES:
            return []

        # Skip dimensionless columns (rates, ratios, indices) — their
        # leading digit distributions are scale-determined, not transaction-count-driven
        if hasattr(semantic, "unit_source_column"):
            unit_src = semantic.unit_source_column
        else:
            unit_src = semantic.get("unit_source_column")
        if unit_src in self._SKIP_UNIT_SOURCES:
            return []

        config = get_entropy_config()
        detector_config = config.detector("benford")
        min_sample_size = detector_config.get("min_sample_size", 100)

        stats = context.get_analysis("statistics", {})
        n_values = stats.get("total_count", 0) or 0
        quality = stats.get("quality", stats)

        benford_analysis = quality.get("benford_analysis") if isinstance(quality, dict) else None
        # The observed leading-digit distribution is required for a surprise score.
        if not isinstance(benford_analysis, dict):
            return []
        digit_distribution = benford_analysis.get("digit_distribution")
        if not isinstance(digit_distribution, dict):
            return []

        # Unreliable small samples: leading-digit frequencies are too noisy.
        if n_values < min_sample_size:
            return []

        # Align observed frequencies to digits 1..9 (missing digit → 0 observed).
        observed = [float(digit_distribution.get(str(d), 0.0)) for d in range(1, 10)]
        if math.fsum(observed) <= 0.0:
            return []

        score = surprise_score(observed, _BENFORD_REFERENCE)
        kl_bits = kl_divergence(observed, _BENFORD_REFERENCE)

        evidence = [
            {
                "kl_bits": round(kl_bits, 4),
                "digit_distribution": digit_distribution,
                "n_values": n_values,
                "is_compliant": benford_analysis.get("is_compliant"),
                "chi_square": benford_analysis.get("chi_square"),
                "p_value": benford_analysis.get("p_value"),
                "interpretation": benford_analysis.get("interpretation", ""),
            }
        ]

        return [
            self.create_entropy_object(
                context=context,
                score=score,
                evidence=evidence,
            )
        ]
