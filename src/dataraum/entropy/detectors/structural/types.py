"""Type fidelity entropy detector.

Measures uncertainty in type inference based on parse success rate.
High parse failure rate indicates the detected type may not be correct.
"""

from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
from dataraum.entropy.models import EntropyObject, ResolutionOption
from dataraum.pipeline.fixes.models import FixSchema, FixSchemaField


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

    @property
    def fix_schemas(self) -> list[FixSchema]:
        """Schema for adding type patterns."""
        return [
            FixSchema(
                action="add_type_pattern",
                target="config",
                description="Add a custom date/time parsing pattern",
                config_path="phases/typing.yaml",
                key_path=["overrides", "patterns"],
                operation="merge",
                requires_rerun="typing",
                key_template="{pattern_name}",
                guidance=(
                    "Adds a date/time pattern so the typing phase can parse "
                    "this column correctly. From the sample values and column "
                    "name, PROPOSE a concrete pattern:\n"
                    "  1. The regex that matches the raw values (e.g. ^\\d{4}-\\d{2}-\\d{2}$)\n"
                    "  2. The DuckDB STRPTIME expression (e.g. STRPTIME(\"{col}\", '%Y-%m-%d'))\n"
                    "  3. A short pattern_name (e.g. iso_date, fiscal_period)\n"
                    "Present your proposal and ask the user to confirm or correct it. "
                    "Do NOT ask open-ended questions like 'what format is this?' — "
                    "infer the format from the data and propose it."
                ),
                fields={
                    "pattern_name": FixSchemaField(
                        type="string",
                        required=True,
                        description="Short name for this pattern (e.g. fiscal_period, european_date)",
                    ),
                    "pattern": FixSchemaField(
                        type="regex",
                        required=True,
                        description="Regex matching the raw values (e.g. ^\\d{4}-\\d{2}$)",
                    ),
                    "standardization_expr": FixSchemaField(
                        type="duckdb_sql",
                        required=True,
                        description=(
                            'DuckDB expression to parse the value into a date/timestamp. '
                            'Use STRPTIME with {col} placeholder. '
                            'Examples: STRPTIME("{col}", \'%Y-%m\'), '
                            'STRPTIME("{col}", \'%d/%m/%Y\')'
                        ),
                    ),
                },
            )
        ]

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
        suggest_override = detector_config.get("suggest_override_threshold", 0.3)
        suggest_quarantine = detector_config.get("suggest_quarantine_threshold", 0.1)
        score_fallback = detector_config.get("score_fallback", 0.5)

        typing_result = context.get_analysis("typing", {})

        # Extract parse success rate and decision metadata
        if hasattr(typing_result, "parse_success_rate"):
            parse_success_rate = typing_result.parse_success_rate
            detected_type = getattr(typing_result, "data_type", None)
            failed_examples = getattr(typing_result, "failed_examples", [])
            decision_source = getattr(typing_result, "decision_source", None)
        else:
            parse_success_rate = typing_result.get("parse_success_rate", 1.0)
            detected_type = typing_result.get("detected_type")
            failed_examples = typing_result.get("failed_examples", [])
            decision_source = typing_result.get("decision_source")

        # Calculate entropy
        is_fallback = decision_source == "fallback"
        if is_fallback:
            # VARCHAR fallback: typing couldn't determine the type.
            # parse_success_rate=1.0 is meaningless — use configurable score.
            score = score_fallback
        else:
            # Normal case: lower parse success = higher entropy
            score = 1.0 - parse_success_rate

        # Build evidence
        evidence = [
            {
                "parse_success_rate": parse_success_rate,
                "detected_type": str(detected_type) if detected_type else None,
                "failure_count": len(failed_examples) if failed_examples else 0,
                "decision_source": decision_source,
                "is_fallback": is_fallback,
            }
        ]

        if failed_examples:
            evidence[0]["failed_examples"] = failed_examples

        # Build resolution options based on configurable thresholds
        resolution_options: list[ResolutionOption] = []

        if score > suggest_override:
            resolution_options.append(
                ResolutionOption(
                    action="add_type_pattern",
                    parameters={
                        "column": context.column_name,
                        "suggested_type": "VARCHAR",
                    },
                    effort="low",
                    description="Override detected type with VARCHAR to preserve all values",
                )
            )

        if is_fallback:
            # Fallback columns always need type review
            resolution_options.append(
                ResolutionOption(
                    action="add_type_pattern",
                    parameters={
                        "column": context.column_name,
                        "suggested_type": str(detected_type) if detected_type else "VARCHAR",
                    },
                    effort="low",
                    description="Review and confirm type for column where inference was inconclusive",
                )
            )

        if score > suggest_quarantine and failed_examples:
            resolution_options.append(
                ResolutionOption(
                    action="transform_quarantine_values",
                    parameters={
                        "column": context.column_name,
                        "pattern": "non_parseable",
                    },
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
