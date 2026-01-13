"""Type fidelity entropy detector.

Measures uncertainty in type inference based on parse success rate.
High parse failure rate indicates the detected type may not be correct.
"""

from dataraum_context.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum_context.entropy.models import EntropyObject, ResolutionOption


class TypeFidelityDetector(EntropyDetector):
    """Detector for type inference fidelity.

    Uses parse_success_rate from type inference to measure how well
    the detected type fits the actual data.

    Source: typing/TypeCandidate.parse_success_rate
    Formula: entropy = 1.0 - parse_success_rate
    """

    detector_id = "type_fidelity"
    layer = "structural"
    dimension = "types"
    sub_dimension = "type_fidelity"
    required_analyses = ["typing"]
    description = "Measures uncertainty in type inference based on parse success rate"

    async def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Detect type fidelity entropy.

        Args:
            context: Detector context with typing analysis results

        Returns:
            List with single EntropyObject for type fidelity
        """
        typing_result = context.get_analysis("typing", {})

        # Extract parse success rate
        # Can come as TypeCandidate or dict
        if hasattr(typing_result, "parse_success_rate"):
            parse_success_rate = typing_result.parse_success_rate
            detected_type = getattr(typing_result, "data_type", None)
            failed_examples = getattr(typing_result, "failed_examples", [])
        else:
            parse_success_rate = typing_result.get("parse_success_rate", 1.0)
            detected_type = typing_result.get("detected_type")
            failed_examples = typing_result.get("failed_examples", [])

        # Calculate entropy: lower parse success = higher entropy
        score = 1.0 - parse_success_rate

        # Build evidence
        evidence = [
            {
                "parse_success_rate": parse_success_rate,
                "detected_type": str(detected_type) if detected_type else None,
                "failure_count": len(failed_examples) if failed_examples else 0,
            }
        ]

        if failed_examples:
            evidence[0]["failed_examples"] = failed_examples[:5]  # Limit to 5 examples

        # Build resolution options based on entropy level
        resolution_options: list[ResolutionOption] = []

        if score > 0.3:
            # Significant parse failures - suggest manual type override
            resolution_options.append(
                ResolutionOption(
                    action="override_type",
                    parameters={
                        "column": context.column_name,
                        "suggested_type": "VARCHAR",  # Fallback to string
                    },
                    expected_entropy_reduction=score * 0.8,
                    effort="low",
                    description="Override detected type with VARCHAR to preserve all values",
                )
            )

        if score > 0.1 and failed_examples:
            # Some failures - suggest data cleanup
            resolution_options.append(
                ResolutionOption(
                    action="quarantine_values",
                    parameters={
                        "column": context.column_name,
                        "pattern": "non_parseable",
                    },
                    expected_entropy_reduction=score * 0.9,
                    effort="medium",
                    description="Move non-parseable values to quarantine table",
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
