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
from dataraum.entropy.models import EntropyObject, ResolutionOption
from dataraum.pipeline.fixes.models import FixSchema, FixSchemaField


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

    @property
    def triage_guidance(self) -> str:
        return (
            "Choose based on the evidence (quarantine rate, sample values, column type):\n"
            "- add_type_pattern: The column contains date/time values that the typing "
            "phase couldn't parse. Sample quarantined values look like dates in an "
            "unusual format. Add a parsing pattern so typing handles them.\n"
            "- set_column_type: The inferred type is wrong and quarantining valid data. "
            "Force VARCHAR if the column has mixed types, or force a specific type if "
            "the inference picked the wrong one.\n"
            "- accept_finding: Only if the quarantined values are genuinely bad data "
            "(not a format the system should learn). The user must confirm."
        )

    @property
    def fix_schemas(self) -> list[FixSchema]:
        """Schemas for type fidelity fixes."""
        return [
            FixSchema(
                action="accept_finding",
                target="config",
                description="Mark type fidelity findings as reviewed and accepted",
                config_path="entropy/thresholds.yaml",
                key_path=["detectors", "type_fidelity", "accepted_columns"],
                operation="append",
                requires_rerun="quality_review",
                guidance=(
                    "Present ALL affected columns in a numbered list with their key metric "
                    "(e.g., quarantine rate). For each column show: table.column — "
                    "quarantine rate — detected type — sample quarantined values.\n"
                    "Ask the user to select columns by number (comma-separated), or 'all'.\n"
                    "Then ask WHY the finding is acceptable (e.g., 'mixed-type column', "
                    "'known format variation', 'acceptable data loss')."
                ),
                fields={
                    "reason": FixSchemaField(
                        type="string",
                        required=False,
                        description="Why the finding was accepted",
                    ),
                },
            ),
            FixSchema(
                action="set_column_type",
                target="config",
                description="Force a specific type for this column, overriding type inference",
                config_path="phases/typing.yaml",
                key_path=["overrides", "forced_types"],
                operation="merge",
                requires_rerun="typing",
                guidance=(
                    "The typing phase inferred a type that doesn't fit all values. "
                    "Some values were quarantined (couldn't be cast). "
                    "Show the user: current inferred type, quarantine rate, "
                    "sample quarantined values. Ask whether to:\n"
                    "  1. Force VARCHAR (keeps all values, no quarantine)\n"
                    "  2. Force a different type\n"
                    "PROPOSE VARCHAR as the default when quarantined values look valid."
                ),
                fields={
                    "target_type": FixSchemaField(
                        type="enum",
                        required=True,
                        description="Type to force for this column",
                        enum_values=["VARCHAR", "BIGINT", "DOUBLE", "DATE", "TIMESTAMP", "BOOLEAN"],
                        default="VARCHAR",
                    ),
                },
            ),
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
                            "DuckDB expression to parse the value into a date/timestamp. "
                            "Use STRPTIME with {col} placeholder. "
                            "Examples: STRPTIME(\"{col}\", '%Y-%m'), "
                            "STRPTIME(\"{col}\", '%d/%m/%Y')"
                        ),
                    ),
                },
            ),
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
        accepted_columns: list[str] = self.config.get("accepted_columns") or detector_config.get(
            "accepted_columns", []
        )

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

        if score > suggest_quarantine:
            # Force a different type (e.g. VARCHAR) to avoid quarantine
            resolution_options.append(
                ResolutionOption(
                    action="set_column_type",
                    parameters={
                        "column": context.column_name,
                        "detected_type": str(detected_type) if detected_type else "VARCHAR",
                    },
                    effort="low",
                    description="Force a specific type for this column, overriding type inference",
                )
            )

        if score > 0:
            # Accept finding: user reviewed, type fidelity issue is expected
            resolution_options.append(
                ResolutionOption(
                    action="accept_finding",
                    parameters={
                        "column": context.column_name,
                        "detector_id": self.detector_id,
                    },
                    effort="low",
                    description="Accept type fidelity findings as expected for this column",
                )
            )

        # Mark as accepted (score stays honest, contract overrule handles gate)
        target_key = f"{context.table_name}.{context.column_name}"
        if target_key in accepted_columns:
            evidence[0]["accepted"] = True

        return [
            self.create_entropy_object(
                context=context,
                score=score,
                evidence=evidence,
                resolution_options=resolution_options,
            )
        ]
