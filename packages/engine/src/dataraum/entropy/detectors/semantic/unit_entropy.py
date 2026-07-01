"""Value-carried unit entropy detector (typing grain).

The TYPING half of unit detection (DAT-647 split): does a measure column carry a
unit IN ITS VALUES (a token like '€'/'kg'/'%' that the typing phase parses out
to cast the column, → ``detected_unit``)? Score = ``1 - unit_confidence``
(stats.confidence_entropy): a confidently-detected value-unit → ~0, an ambiguous
one → high. When there is NO value-carried unit token, this detector abstains
(→ 0) — absence of a value-unit is the norm, not entropy, and whether the unit is
determinable from the CATALOGUE (a dimension column, dimensionless) is the
separate ``unit_source`` detector's question (semantic grain, ColumnConcept).

Source: typing.detected_unit, typing.unit_confidence, semantic.semantic_role.
"""

from dataraum.entropy import stats
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
from dataraum.entropy.models import EntropyObject


class UnitEntropyDetector(EntropyDetector):
    """Detector for value-carried unit uncertainty (typing grain).

    Measures whether a measure column's VALUE-CARRIED unit is confidently
    declared. Score = 1 - unit_confidence when a value-unit token is present;
    0 (abstain) when there is none — the catalogue-grain "is the unit
    determinable" question belongs to :class:`UnitSourceEntropyDetector`.

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

        # Value-carried unit entropy (DAT-647 split): only the unit token in the
        # VALUES is this detector's concern. A confidently-detected value-unit → ~0
        # (score = 1 - unit_confidence, stats.confidence_entropy); an ambiguous one →
        # high. NO value-unit token → abstain (0): absence is the norm, and whether
        # the unit is determinable from the catalogue is unit_source's question.
        # Teach: declare the value-carried unit (the `unit` teach → detected_unit).
        if detected_unit:
            score = stats.confidence_entropy(unit_confidence)
            unit_status = "declared" if unit_confidence >= 0.5 else "low_confidence"
        else:
            score, unit_status = 0.0, "no_value_unit"

        evidence = [
            {
                "detected_unit": detected_unit,
                "unit_confidence": unit_confidence,
                "semantic_role": semantic_role,
                "unit_status": unit_status,
            }
        ]

        return [
            self.create_entropy_object(
                context=context,
                score=score,
                evidence=evidence,
            )
        ]
