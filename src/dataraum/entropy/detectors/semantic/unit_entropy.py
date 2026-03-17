"""Unit declaration entropy detector.

Measures uncertainty in unit declarations for numeric columns.
Columns with undeclared or low-confidence units in measure roles
have higher entropy when used in calculations.

Supports cross-column unit inference: if a dimension column (e.g. 'currency')
defines the unit for measure columns, entropy is reduced to 0.2 (inferred)
instead of 0.8 (missing).

Source: typing.detected_unit, typing.unit_confidence, semantic.semantic_role,
        semantic.unit_source_column
"""

from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
from dataraum.entropy.models import EntropyObject, ResolutionOption
from dataraum.pipeline.fixes.models import FixSchema, FixSchemaField


class UnitEntropyDetector(EntropyDetector):
    """Detector for unit declaration uncertainty.

    Measures whether numeric columns (measures) have declared units.
    Undeclared units on measure columns create high entropy when
    those columns are used in aggregations or calculations.

    When a unit_source_column is identified (e.g., a 'currency' dimension
    defines the unit for monetary measures), the score is reduced to
    score_inferred (default 0.2) instead of score_no_unit (0.8).

    Source: typing.detected_unit, typing.unit_confidence, semantic.semantic_role
    Scores configurable in config/entropy/thresholds.yaml.
    """

    detector_id = "unit_entropy"
    layer = Layer.SEMANTIC
    dimension = Dimension.UNITS
    sub_dimension = SubDimension.UNIT_DECLARATION
    required_analyses = [AnalysisKey.TYPING, AnalysisKey.SEMANTIC]
    description = "Measures whether numeric columns have declared units"

    @property
    def triage_guidance(self) -> str:
        return (
            "Choose based on the data profile and table structure:\n"
            "- declare_unit: DEFAULT. The column always uses the same unit "
            "(e.g., all values are USD, or kg, or percentages). Use the column "
            "name and sample values to propose a unit.\n"
            "- set_unit_source: The unit varies per row and another column in the "
            "same or a related table provides it (e.g., a 'currency' column). "
            "Only use this when such a column exists in the data profile."
        )

    @property
    def fix_schemas(self) -> list[FixSchema]:
        """Schemas for declaring units.

        Two actions covering the two real-world scenarios:
        - declare_unit: fixed unit for the whole column (e.g. always USD)
        - set_unit_source: unit varies per row, comes from another column
        """
        return [
            FixSchema(
                action="declare_unit",
                target="config",
                description="Declare a fixed unit for this column",
                config_path="phases/typing.yaml",
                key_path=["overrides", "units"],
                operation="merge",
                requires_rerun="typing",
                guidance=(
                    "The column always uses the same unit. Ask the user what "
                    "unit the values represent (e.g. USD, EUR, kg, %, "
                    "dimensionless). Use the data profile and column name to "
                    "suggest a likely unit."
                ),
                fields={
                    "unit": FixSchemaField(
                        type="string",
                        required=True,
                        description="Unit of measure (e.g. USD, EUR, kg, dimensionless)",
                    ),
                },
            ),
            FixSchema(
                action="set_unit_source",
                target="config",
                description="Specify which column provides the unit per row",
                config_path="phases/semantic.yaml",
                key_path=["overrides", "units"],
                operation="merge",
                requires_rerun="semantic",
                guidance=(
                    "The unit varies per row and comes from another column "
                    "(e.g. a 'currency' column in the same or a related "
                    "table). Ask the user which column provides the unit. "
                    "Format: column_name for same table, or "
                    "table_name.column_name for a related table."
                ),
                fields={
                    "unit_source_column": FixSchemaField(
                        type="string",
                        required=True,
                        description=(
                            "Column providing the unit per row "
                            "(e.g. 'currency' or 'chart_of_accounts.currency')"
                        ),
                    ),
                },
            ),
        ]

    def load_data(self, context: DetectorContext) -> None:
        """Load typing and semantic data for this column."""
        if context.session is None or context.column_id is None:
            return
        from dataraum.entropy.detectors.loaders import load_semantic, load_typing

        typing_result = load_typing(context.session, context.column_id)
        if typing_result is not None:
            context.analysis_results["typing"] = typing_result
        sem = load_semantic(context.session, context.column_id)
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
        config = get_entropy_config()
        detector_config = config.detector("unit_entropy")

        # Configurable scores
        score_no_unit = detector_config.get("score_no_unit", 0.8)
        score_low_confidence = detector_config.get("score_low_confidence", 0.5)
        score_declared = detector_config.get("score_declared", 0.1)
        score_inferred = detector_config.get("score_inferred", 0.1)
        confidence_threshold = detector_config.get("confidence_threshold", 0.5)

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

        # Determine score based on unit status
        if detected_unit and unit_confidence >= confidence_threshold:
            score = score_declared
            unit_status = "declared"
        elif detected_unit and unit_confidence < confidence_threshold:
            score = score_low_confidence
            unit_status = "low_confidence"
        elif unit_source_column == "dimensionless":
            # Measure is inherently dimensionless (ratio, rate, index, etc.)
            # Having no unit is correct — not a quality issue
            score = score_declared
            unit_status = "dimensionless"
        elif unit_source_column:
            # Unit is inferred from a dimension column — lower entropy than missing
            score = score_inferred
            unit_status = "inferred_from_dimension"
        else:
            score = score_no_unit
            unit_status = "missing"

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
                    effort="low",
                    description=f"Declare a fixed unit for '{context.column_name}'",
                )
            )
            resolution_options.append(
                ResolutionOption(
                    action="set_unit_source",
                    parameters={
                        "column": context.column_name,
                        "table": context.table_name,
                    },
                    effort="low",
                    description=f"Specify which column provides the unit for '{context.column_name}'",
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
