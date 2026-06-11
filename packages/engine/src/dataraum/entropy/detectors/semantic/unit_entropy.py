"""Unit declaration entropy detector.

Measures uncertainty in unit declarations for numeric columns.
Columns with undeclared or low-confidence units in measure roles
have higher entropy when used in calculations.

The score is 1 - unit_confidence (stats.confidence_entropy): a confidently-detected
unit → ~0, an undeclared unit → 1.0. Cross-column inference: when a dimension column
(e.g. 'currency') defines the unit, or the measure is inherently dimensionless, the
unit is resolved → entropy 0.

Source: typing.detected_unit, typing.unit_confidence, semantic.semantic_role,
        semantic.unit_source_column
"""

from dataraum.entropy import stats
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
from dataraum.entropy.models import EntropyObject


class UnitEntropyDetector(EntropyDetector):
    """Detector for unit declaration uncertainty.

    Measures whether numeric columns (measures) have declared units.
    Undeclared units on measure columns create high entropy when
    those columns are used in aggregations or calculations.

    Score = 1 - unit_confidence (no 0.8/0.5/0.1 buckets); a unit resolved by
    cross-column inference (a 'currency' dimension, or a dimensionless measure) → 0.

    Source: typing.detected_unit, typing.unit_confidence, semantic.semantic_role
    """

    detector_id = "unit_entropy"
    layer = Layer.SEMANTIC
    dimension = Dimension.UNITS
    sub_dimension = SubDimension.UNIT_DECLARATION
    required_analyses = [AnalysisKey.TYPING, AnalysisKey.SEMANTIC]
    description = "Measures whether numeric columns have declared units"

    def load_data(self, context: DetectorContext) -> None:
        """Load typing and semantic data for this column."""
        if context.session is None or context.column_id is None:
            return
        from dataraum.entropy.detectors.loaders import load_semantic, load_typing

        typing_result = load_typing(context.session, context.column_id, context.run_id)
        if typing_result is not None:
            context.analysis_results["typing"] = typing_result
        sem = load_semantic(
            context.session, context.column_id, context.run_id, base_runs=context.base_runs
        )
        if sem is not None:
            context.analysis_results["semantic"] = sem

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

        # Check for cross-column unit inference (unit_source_column from semantic analysis)
        if hasattr(semantic, "unit_source_column"):
            unit_source_column = semantic.unit_source_column
        else:
            unit_source_column = semantic.get("unit_source_column")

        # A measure's unit-declaration entropy. The unit is KNOWN (→ 0) when it is
        # inherently dimensionless or inferred from a dimension column (cross-column
        # resolution). Otherwise the entropy is the model's uncertainty about the unit
        # = 1 - unit_confidence (stats.confidence_entropy): a confidently-detected unit
        # → ~0, an undeclared unit (unit_confidence 0) → 1.0. No 0.8/0.5/0.1 buckets
        # (DAT-442 two-table). Teach: declare the unit.
        if detected_unit:
            # A directly-declared unit takes precedence over cross-column inference.
            score = stats.confidence_entropy(unit_confidence)
            unit_status = "declared" if unit_confidence >= 0.5 else "low_confidence"
        elif unit_source_column == "dimensionless":
            score, unit_status = 0.0, "dimensionless"
        elif unit_source_column:
            score, unit_status = 0.0, "inferred_from_dimension"
        else:
            score, unit_status = 1.0, "missing"

        # Build evidence
        evidence_dict = {
            "detected_unit": detected_unit,
            "unit_confidence": unit_confidence,
            "semantic_role": semantic_role,
            "unit_status": unit_status,
        }
        if unit_source_column:
            evidence_dict["unit_source_column"] = unit_source_column

        evidence = [evidence_dict]

        return [
            self.create_entropy_object(
                context=context,
                score=score,
                evidence=evidence,
            )
        ]
