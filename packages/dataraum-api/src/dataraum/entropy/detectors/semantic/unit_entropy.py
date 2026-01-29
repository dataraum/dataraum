"""Unit declaration entropy detector.

Measures uncertainty in unit declarations for numeric columns.
Columns with undeclared or low-confidence units in measure roles
have higher entropy when used in calculations.

Source: typing.detected_unit, typing.unit_confidence, semantic.semantic_role
"""

from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.models import EntropyObject, ResolutionOption


class UnitEntropyDetector(EntropyDetector):
    """Detector for unit declaration uncertainty.

    Measures whether numeric columns (measures) have declared units.
    Undeclared units on measure columns create high entropy when
    those columns are used in aggregations or calculations.

    Source: typing.detected_unit, typing.unit_confidence, semantic.semantic_role
    Scores configurable in config/entropy/thresholds.yaml.
    """

    detector_id = "unit_entropy"
    layer = "semantic"
    dimension = "units"
    sub_dimension = "unit_declaration"
    required_analyses = ["typing", "semantic"]
    description = "Measures whether numeric columns have declared units"

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Detect unit declaration entropy.

        Only applies to columns with semantic_role='measure'.
        Non-measure columns (dimensions, identifiers, etc.) don't need units.

        Args:
            context: Detector context with typing and semantic analysis

        Returns:
            List with single EntropyObject for unit declaration entropy,
            or empty list if not applicable (non-measure column)
        """
        config = get_entropy_config()
        detector_config = config.detector("unit_entropy")

        # Configurable scores
        score_no_unit = detector_config.get("score_no_unit", 0.8)
        score_low_confidence = detector_config.get("score_low_confidence", 0.5)
        score_declared = detector_config.get("score_declared", 0.1)
        confidence_threshold = detector_config.get("confidence_threshold", 0.5)
        reduction_declare_unit = detector_config.get("reduction_declare_unit", 0.8)

        typing = context.get_analysis("typing", {})
        semantic = context.get_analysis("semantic", {})

        # Get semantic role - only applies to measures
        if hasattr(semantic, "semantic_role"):
            semantic_role = semantic.semantic_role
        else:
            semantic_role = semantic.get("semantic_role")

        # Skip non-measure columns (dimensions, identifiers, etc. don't need units)
        if semantic_role != "measure":
            return []

        # Get unit information from typing analysis
        if hasattr(typing, "detected_unit"):
            detected_unit = typing.detected_unit
            unit_confidence = getattr(typing, "unit_confidence", 0.0) or 0.0
        else:
            detected_unit = typing.get("detected_unit")
            unit_confidence = typing.get("unit_confidence", 0.0) or 0.0

        # Determine score based on unit status
        if not detected_unit:
            score = score_no_unit
            unit_status = "missing"
        elif unit_confidence < confidence_threshold:
            score = score_low_confidence
            unit_status = "low_confidence"
        else:
            score = score_declared
            unit_status = "declared"

        # Build evidence
        evidence = [
            {
                "detected_unit": detected_unit,
                "unit_confidence": unit_confidence,
                "semantic_role": semantic_role,
                "unit_status": unit_status,
            }
        ]

        # Resolution options
        resolution_options: list[ResolutionOption] = []

        if score > 0.3:  # Only suggest resolution for high-entropy columns
            resolution_options.append(
                ResolutionOption(
                    action="declare_unit",
                    parameters={
                        "column": context.column_name,
                        "table": context.table_name,
                        "detected_unit": detected_unit,
                    },
                    expected_entropy_reduction=reduction_declare_unit,
                    effort="low",
                    description=f"Declare the unit for measure column '{context.column_name}'",
                    cascade_dimensions=["computational.derived_values"],
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
