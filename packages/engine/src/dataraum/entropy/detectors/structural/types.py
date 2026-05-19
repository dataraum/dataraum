"""Type fidelity entropy detector.

Measures uncertainty in type inference based on parse success rate
and quarantine rate. Uses a non-linear boost to amplify small but
significant failure rates — 5% of rows failing to cast is a real problem,
not noise.
"""

import math

from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
from dataraum.entropy.models import EntropyObject


def _boost_rate(rate: float) -> float:
    """Amplify small but significant failure/quarantine rates.

    Linear scoring under-weights real problems: 8% quarantine means 8% of
    your data is broken, but scores only 0.08. This log-based boost maps
    rates to scores that match actual severity:

        0.01 → 0.01  (noise — rounding errors, encoding quirks)
        0.03 → 0.20  (notable — worth investigating)
        0.05 → 0.35  (fires at 0.3 threshold)
        0.08 → 0.56  (clearly broken)
        0.15 → 1.00  (severe)
    """
    if rate <= 0:
        return 0.0
    boosted = ((1 + rate) ** 2 / -math.log10(rate)) - 0.5
    return max(0.0, min(1.0, boosted))


class TypeFidelityDetector(EntropyDetector):
    """Detector for type inference fidelity.

    Uses parse_success_rate from type inference to measure how well
    the detected type fits the actual data.

    Source: typing/TypeCandidate.parse_success_rate
    Formula: entropy = 1.0 - parse_success_rate

    Special case: when the typing phase falls back to VARCHAR because no
    candidate passed min_confidence (decision_source="fallback"), the
    parse_success_rate is 1.0 (VARCHAR parses everything). This is
    misleading — the column couldn't be typed. In this case we use a
    configurable fallback score (default 0.5) instead.

    Thresholds configurable in config/entropy/thresholds.yaml.
    """

    detector_id = "type_fidelity"
    layer = Layer.STRUCTURAL
    dimension = Dimension.TYPES
    sub_dimension = SubDimension.TYPE_FIDELITY
    required_analyses = [AnalysisKey.TYPING]
    description = "Measures uncertainty in type inference based on parse success rate"

    def load_data(self, context: DetectorContext) -> None:
        """Load type decision and candidate info for this column."""
        if context.session is None or context.column_id is None:
            return
        from dataraum.entropy.detectors.loaders import load_typing

        result = load_typing(context.session, context.column_id)
        if result is not None:
            context.analysis_results["typing"] = result

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Detect type fidelity entropy.

        Args:
            context: Detector context with typing analysis results

        Returns:
            List with single EntropyObject for type fidelity
        """
        # Load configuration
        config = get_entropy_config()
        detector_config = config.detector("type_fidelity")

        # Get configurable thresholds
        score_fallback = detector_config.get("score_fallback", 0.5)
        typing_result = context.get_analysis("typing", {})

        # Extract parse success rate and decision metadata
        if hasattr(typing_result, "parse_success_rate"):
            parse_success_rate = typing_result.parse_success_rate
            detected_type = getattr(typing_result, "data_type", None)
            failed_examples = getattr(typing_result, "failed_examples", [])
            decision_source = getattr(typing_result, "decision_source", None)
            quarantine_rate = getattr(typing_result, "quarantine_rate", None)
        else:
            parse_success_rate = typing_result.get("parse_success_rate", 1.0)
            detected_type = typing_result.get("detected_type")
            failed_examples = typing_result.get("failed_examples", [])
            decision_source = typing_result.get("decision_source")
            quarantine_rate = typing_result.get("quarantine_rate")

        # Calculate entropy
        is_fallback = decision_source == "fallback"
        if is_fallback:
            # VARCHAR fallback: typing couldn't determine the type.
            # parse_success_rate=1.0 is meaningless — use configurable score.
            score = score_fallback
        else:
            # Combine parse failure rate with boosted quarantine rate.
            # Quarantine rate gets a non-linear boost because even small rates
            # (5-8%) mean real data is broken — rows that failed TRY_CAST.
            parse_score = 1.0 - parse_success_rate
            boosted_quarantine = _boost_rate(quarantine_rate or 0.0)
            score = max(parse_score, boosted_quarantine)

        # Build evidence
        evidence = [
            {
                "parse_success_rate": parse_success_rate,
                "quarantine_rate": quarantine_rate,
                "detected_type": str(detected_type) if detected_type else None,
                "failure_count": len(failed_examples) if failed_examples else 0,
                "decision_source": decision_source,
                "is_fallback": is_fallback,
            }
        ]

        if failed_examples:
            evidence[0]["failed_examples"] = failed_examples

        return [
            self.create_entropy_object(
                context=context,
                score=score,
                evidence=evidence,
            )
        ]
