"""Derived value entropy detector.

Measures uncertainty in derived/computed columns.
Low match rate indicates the detected formula may not be correct.
"""

from typing import Any

from dataraum_context.entropy.config import get_entropy_config
from dataraum_context.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum_context.entropy.models import EntropyObject, ResolutionOption


class DerivedValueDetector(EntropyDetector):
    """Detector for derived column correctness.

    Uses DerivedColumn detection from correlation analysis to measure
    how reliably a column matches its detected formula.

    Source: correlation/DerivedColumn.formula, match_rate
    Formula: entropy = 1.0 - match_rate (or 1.0 if no formula detected)

    Thresholds configurable in config/entropy/thresholds.yaml.
    """

    detector_id = "derived_value"
    layer = "computational"
    dimension = "derived_values"
    sub_dimension = "formula_match"
    required_analyses = ["correlation"]
    description = "Measures reliability of detected derived column formulas"

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Detect derived value entropy.

        Args:
            context: Detector context with correlation analysis results

        Returns:
            List with single EntropyObject for derived value reliability
        """
        # Load configuration
        config = get_entropy_config()
        detector_config = config.detector("derived_value")

        # Get configurable thresholds
        match_exact = detector_config.get("match_exact", 0.99)
        match_near_exact = detector_config.get("match_near_exact", 0.95)
        match_approximate = detector_config.get("match_approximate", 0.80)
        reduction_declare = detector_config.get("reduction_declare_formula", 0.8)
        reduction_verify = detector_config.get("reduction_verify_formula", 0.7)
        reduction_investigate = detector_config.get("reduction_investigate", 0.5)

        correlation = context.get_analysis("correlation", {})

        # Extract derived column information
        derived_columns: list[Any] = []
        if hasattr(correlation, "derived_columns"):
            derived_columns = correlation.derived_columns or []
        elif isinstance(correlation, dict):
            derived_columns = correlation.get("derived_columns", [])

        # Find derived column info for current column
        current_derived = None
        for dc in derived_columns:
            if hasattr(dc, "derived_column_name"):
                if dc.derived_column_name == context.column_name:
                    current_derived = dc
                    break
            elif isinstance(dc, dict):
                if dc.get("derived_column_name") == context.column_name:
                    current_derived = dc
                    break

        # Calculate entropy based on derived column status
        if current_derived is None:
            # No formula detected - highest uncertainty for computational layer
            score = 1.0
            status = "no_formula"
            formula: str | None = None
            match_rate: float | None = None
            source_columns: list[str] = []
        else:
            # Extract match rate
            if hasattr(current_derived, "match_rate"):
                match_rate = current_derived.match_rate
                formula = getattr(current_derived, "formula", None)
                source_columns = getattr(current_derived, "source_column_names", [])
            else:
                match_rate = current_derived.get("match_rate", 0.0)
                formula = current_derived.get("formula")
                source_columns = current_derived.get("source_column_names", [])

            # Calculate entropy from match rate
            score = 1.0 - match_rate

            # Classify match quality using configurable thresholds
            if match_rate >= match_exact:
                status = "exact"
            elif match_rate >= match_near_exact:
                status = "near_exact"
            elif match_rate >= match_approximate:
                status = "approximate"
            else:
                status = "poor"

        # Build evidence
        evidence = [
            {
                "status": status,
                "formula": formula,
                "match_rate": match_rate,
                "source_columns": source_columns,
            }
        ]

        if current_derived:
            if hasattr(current_derived, "derivation_type"):
                evidence[0]["derivation_type"] = current_derived.derivation_type
            elif isinstance(current_derived, dict) and "derivation_type" in current_derived:
                evidence[0]["derivation_type"] = current_derived["derivation_type"]

        # Build resolution options using configurable reductions
        resolution_options: list[ResolutionOption] = []

        if status == "no_formula":
            resolution_options.append(
                ResolutionOption(
                    action="declare_formula",
                    parameters={
                        "column": context.column_name,
                        "table": context.table_name,
                    },
                    expected_entropy_reduction=reduction_declare,
                    effort="medium",
                    description="Declare the computation formula for this column",
                    cascade_dimensions=["semantic.business_meaning"],
                )
            )
        elif status in ["approximate", "poor"]:
            resolution_options.append(
                ResolutionOption(
                    action="verify_formula",
                    parameters={
                        "column": context.column_name,
                        "detected_formula": formula,
                    },
                    expected_entropy_reduction=score * reduction_verify,
                    effort="medium",
                    description="Verify or correct the detected formula",
                )
            )
            resolution_options.append(
                ResolutionOption(
                    action="investigate_mismatches",
                    parameters={
                        "column": context.column_name,
                        "formula": formula,
                    },
                    expected_entropy_reduction=score * reduction_investigate,
                    effort="high",
                    description="Investigate rows where the formula doesn't match",
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
