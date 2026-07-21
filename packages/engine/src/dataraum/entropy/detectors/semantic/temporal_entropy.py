"""Temporal role entropy detector.

Measures uncertainty in temporal column identification.
Date/time columns that are not marked as timestamps, or columns
marked as timestamps without date/time types, create uncertainty
in time-based analysis.

Source: semantic.semantic_role, typing.data_type
"""

from dataraum.core.duckdb_types import is_datetime_like
from dataraum.entropy import stats
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
from dataraum.entropy.models import EntropyObject


class TemporalEntropyDetector(EntropyDetector):
    """Detector for temporal column identification uncertainty.

    Measures whether timestamp columns are properly identified:
    - Date/time columns should be marked with semantic_role='timestamp'
    - Columns marked as timestamp should have date/time types

    Mismatches between type and role create uncertainty in time-based queries.

    Source: semantic.semantic_role, typing.data_type
    Scores configurable in config/entropy/thresholds.yaml.
    """

    detector_id = "temporal_entropy"
    layer = Layer.SEMANTIC
    dimension = Dimension.TEMPORAL
    sub_dimension = SubDimension.TIME_ROLE
    required_analyses = [AnalysisKey.TYPING, AnalysisKey.SEMANTIC]
    description = "Measures whether temporal columns are properly identified"

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
        """Detect temporal identification entropy.

        Checks for alignment between data type and semantic role:
        - Date/time type + timestamp role = low entropy (aligned)
        - Date/time type + no timestamp role = medium entropy (unmarked)
        - Non-date type + timestamp role = high entropy (mismatch)
        - Non-date type + no timestamp role = N/A (not temporal)

        Args:
            context: Detector context with typing and semantic analysis

        Returns:
            List with single EntropyObject for temporal entropy,
            or empty list if not applicable (non-temporal column)
        """
        typing = context.get_analysis("typing", {})
        semantic = context.get_analysis("semantic", {})

        # Get data type
        if hasattr(typing, "data_type"):
            data_type = str(typing.data_type or "").upper()
        else:
            data_type = str(typing.get("data_type", "") or "").upper()

        # Get semantic role
        if hasattr(semantic, "semantic_role"):
            semantic_role = semantic.semantic_role
        else:
            semantic_role = semantic.get("semantic_role")

        # temporal_behavior (evidence only) — aggregation semantics ("additive",
        # "point_in_time") backfilled from the ontology, NOT a temporal role indicator.
        if hasattr(semantic, "temporal_behavior"):
            temporal_behavior = semantic.temporal_behavior
        else:
            temporal_behavior = (
                semantic.get("temporal_behavior") if isinstance(semantic, dict) else None
            )

        # Temporal in ANY sense — durations and times-of-day included. The
        # question here is whether the TYPE agrees with a timestamp ROLE, not
        # whether the column bounds a window, so this is deliberately the wider
        # family than the one temporal profiling uses (DAT-835).
        is_datetime_type = is_datetime_like(data_type)
        is_marked_timestamp = semantic_role == "timestamp"

        # Not a temporal column at all → nothing to measure.
        if not is_datetime_type and not is_marked_timestamp:
            return []

        # Structural time-role entropy (binary): a timestamp role on a NON-temporal type
        # (unparseable dates fell back to VARCHAR) is the broken case → 1.0; aligned, or a
        # date merely not marked as the time axis → 0.0. No 0.6/0.8/0.1 constants, no
        # (1 - confidence·0.5) modulation (DAT-442 two-table). Teach: re-type / mark role.
        score = stats.time_role_mismatch(
            is_temporal_type=is_datetime_type, is_timestamp_role=is_marked_timestamp
        )
        if score >= 1.0:
            temporal_status = "mismatch"
        elif is_datetime_type and is_marked_timestamp:
            temporal_status = "aligned"
        else:
            temporal_status = "unmarked"

        # Build evidence
        evidence_entry: dict[str, object] = {
            "data_type": data_type,
            "semantic_role": semantic_role,
            "is_datetime_type": is_datetime_type,
            "is_marked_timestamp": is_marked_timestamp,
            "temporal_status": temporal_status,
        }
        if temporal_behavior:
            evidence_entry["temporal_behavior"] = temporal_behavior

        # For mismatch: sample raw values so the agent can see actual formats
        if temporal_status == "mismatch" and context.duckdb_conn is not None and context.view_name:
            try:
                samples = context.duckdb_conn.execute(
                    f'SELECT DISTINCT "{context.column_name}" '
                    f'FROM "{context.view_name}" '
                    f'WHERE "{context.column_name}" IS NOT NULL '
                    f"LIMIT 10"
                ).fetchall()
                evidence_entry["sample_values"] = [str(row[0]) for row in samples]
            except Exception:
                pass

        evidence = [evidence_entry]

        return [
            self.create_entropy_object(
                context=context,
                score=score,
                evidence=evidence,
            )
        ]
