"""Type fidelity entropy detector.

Measures uncertainty in type inference as the honest parse-failure /
quarantine rate — the fraction of rows that failed TRY_CAST. No boost curve
(DAT-442 reset): an 8% quarantine scores 0.08, not an amplified 0.56. Severity
per intent lives in the loss table; recall is the ordering "injected separates
from clean", not a point threshold (eval test_detector_recall ORDERING_DETECTORS).
"""

from dataraum.entropy import stats
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
from dataraum.entropy.models import EntropyObject


class TypeFidelityDetector(EntropyDetector):
    """Detector for type inference fidelity.

    Uses parse_success_rate from type inference to measure how well
    the detected type fits the actual data.

    Source: typing/TypeCandidate.parse_success_rate
    Formula: entropy = max(1 - parse_success_rate, quarantine_rate)  (stats.type_fidelity)

    A VARCHAR fallback (decision_source="fallback") means typing couldn't determine a
    type (parse_success_rate=1.0 is meaningless — VARCHAR parses everything). That is
    IGNORANCE about the column's type, flagged as an evidence signal, NOT a fabricated
    0.5 mid-score (DAT-442 two-table).
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

        result = load_typing(context.session, context.column_id, context.run_id)
        if result is not None:
            context.analysis_results["typing"] = result

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Detect type fidelity entropy.

        Args:
            context: Detector context with typing analysis results

        Returns:
            List with single EntropyObject for type fidelity
        """
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

        # The measurement is the honest type-cast failure rate: the worse of the
        # parse-failure fraction and the quarantine fraction (rows that failed
        # TRY_CAST). No boost (DAT-442) — 8% broken is 0.08, eval asserts the ordering.
        # A VARCHAR fallback yields parse_success_rate=1.0 → score ~ quarantine_rate;
        # the undetermined type is IGNORANCE (evidence signal below), not a 0.5 score.
        is_fallback = decision_source == "fallback"
        score = stats.type_fidelity(parse_success_rate, quarantine_rate or 0.0)

        # Build evidence
        evidence = [
            {
                "parse_success_rate": parse_success_rate,
                "quarantine_rate": quarantine_rate,
                "detected_type": str(detected_type) if detected_type else None,
                "failure_count": len(failed_examples) if failed_examples else 0,
                "decision_source": decision_source,
                "is_fallback": is_fallback,
                # Type undetermined → ignorance about the column's type (future loss U).
                "ignorance": 1.0 if is_fallback else 0.0,
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
