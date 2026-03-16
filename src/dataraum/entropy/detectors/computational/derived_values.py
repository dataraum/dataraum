"""Derived value entropy detector.

Measures uncertainty in derived/computed columns.
Low match rate indicates the detected formula may not be correct.

Uses non-linear scoring: a 5% formula mismatch rate is a real problem,
not 0.05 severity. The boost function maps small mismatch rates to scores
that reflect actual severity (same approach as type_fidelity).
"""

import math
from typing import Any

from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
from dataraum.entropy.models import EntropyObject, ResolutionOption
from dataraum.pipeline.fixes.models import FixSchema, FixSchemaField


def _boost_mismatch_rate(rate: float) -> float:
    """Amplify small but significant formula mismatch rates.

    5% of rows failing a formula check is a real problem, not noise.
    Same log-based boost as type_fidelity._boost_rate():

        0.01 → 0.01  (noise — rounding errors)
        0.03 → 0.20  (notable — worth investigating)
        0.05 → 0.35  (fires at 0.3 threshold)
        0.08 → 0.56  (clearly broken)
        0.15 → 1.00  (severe)
    """
    if rate <= 0:
        return 0.0
    boosted = ((1 + rate) ** 2 / -math.log10(rate)) - 0.5
    return max(0.0, min(1.0, boosted))


class DerivedValueDetector(EntropyDetector):
    """Detector for derived column correctness.

    Uses DerivedColumn detection from correlation analysis to measure
    how reliably a column matches its detected formula.

    Source: correlation/DerivedColumn.formula, match_rate
    Formula: entropy = boost(1.0 - match_rate) with log-based amplification

    Thresholds configurable in config/entropy/thresholds.yaml.
    """

    detector_id = "derived_value"
    layer = Layer.COMPUTATIONAL
    dimension = Dimension.DERIVED_VALUES
    sub_dimension = SubDimension.FORMULA_MATCH
    required_analyses = [AnalysisKey.CORRELATION]
    description = "Measures reliability of detected derived column formulas"

    @property
    def fix_schemas(self) -> list[FixSchema]:
        return [
            FixSchema(
                action="accept_finding",
                target="config",
                description="Accept formula mismatch as expected (e.g., manual adjustments, rounding)",
                config_path="entropy/thresholds.yaml",
                key_path=["detectors", "derived_value", "accepted_columns"],
                operation="append",
                requires_rerun="analysis_review",
                guidance=(
                    "The column was detected as derived (computed from other columns) "
                    "but some rows don't match the formula. Show the user the detected "
                    "formula, the match rate, and the source columns. Ask whether the "
                    "mismatches are expected (manual adjustments, rounding, historical "
                    "corrections) or indicate a real data quality problem."
                ),
                fields={
                    "reason": FixSchemaField(
                        type="string",
                        required=True,
                        description="Why the formula mismatch is expected",
                    ),
                },
            ),
            FixSchema(
                action="recalculate_derived_column",
                target="data",
                description="Recalculate the derived column from its source formula",
                templates={
                    "recalculate": "UPDATE typed_{table} SET {column} = {formula}",
                },
                requires_rerun="correlations",
                guidance=(
                    "The derived column has formula mismatches and the user wants "
                    "to recalculate. Show the detected formula and match rate. "
                    "Ask the user to confirm the correct formula (the detected one "
                    "may be wrong). PROPOSE the detected formula as default."
                ),
                fields={
                    "formula": FixSchemaField(
                        type="duckdb_sql",
                        required=True,
                        description="SQL expression to recalculate (e.g., 'debit - credit')",
                    ),
                },
            ),
        ]

    def load_data(self, context: DetectorContext) -> None:
        """Load correlation (derived column) data for this column."""
        if context.session is None or context.column_id is None:
            return
        from dataraum.entropy.detectors.loaders import load_correlation

        result = load_correlation(context.session, context.column_id, context.column_name)
        if result is not None:
            context.analysis_results["correlation"] = result

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
        score_accepted = self.config.get("score_accepted") or detector_config.get(
            "score_accepted", 0.2
        )
        accepted_columns: list[str] = self.config.get("accepted_columns") or detector_config.get(
            "accepted_columns", []
        )
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

            # Calculate entropy from mismatch rate with non-linear boost
            mismatch_rate = 1.0 - match_rate
            score = _boost_mismatch_rate(mismatch_rate)

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
                    action="document_formula",
                    parameters={
                        "column": context.column_name,
                        "table": context.table_name,
                    },
                    effort="medium",
                    description="Declare the computation formula for this column",
                )
            )
        elif status in ["approximate", "poor"]:
            resolution_options.append(
                ResolutionOption(
                    action="investigate_formula_mismatches",
                    parameters={
                        "column": context.column_name,
                        "detected_formula": formula,
                    },
                    effort="medium",
                    description="Verify formula and investigate rows where it doesn't match",
                )
            )

        # Apply acceptance floor if this column was previously accepted
        target_key = f"{context.table_name}.{context.column_name}"
        if target_key in accepted_columns:
            score = score_accepted
            evidence[0]["accepted"] = True

        return [
            self.create_entropy_object(
                context=context,
                score=score,
                evidence=evidence,
                resolution_options=resolution_options,
            )
        ]
