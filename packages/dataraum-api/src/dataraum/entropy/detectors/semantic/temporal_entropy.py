"""Temporal role entropy detector.

Measures uncertainty in temporal column identification.
Date/time columns that are not marked as timestamps, or columns
marked as timestamps without date/time types, create uncertainty
in time-based analysis.

Source: semantic.semantic_role, typing.data_type
"""

from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.models import EntropyObject, ResolutionOption


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
    layer = "semantic"
    dimension = "temporal"
    sub_dimension = "time_role"
    required_analyses = ["typing", "semantic"]
    description = "Measures whether temporal columns are properly identified"

    # Date/time type indicators (uppercase for case-insensitive matching)
    DATETIME_TYPES = frozenset({"DATE", "TIME", "TIMESTAMP", "DATETIME", "INTERVAL"})

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
        config = get_entropy_config()
        detector_config = config.detector("temporal_entropy")

        # Configurable scores
        score_unmarked = detector_config.get("score_unmarked", 0.6)
        score_mismatch = detector_config.get("score_mismatch", 0.8)
        score_aligned = detector_config.get("score_aligned", 0.1)
        reduction_mark_timestamp = detector_config.get("reduction_mark_timestamp", 0.6)

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

        # Check if column is date/time type
        is_datetime_type = any(dt in data_type for dt in self.DATETIME_TYPES)

        # Check if column is marked as timestamp
        is_marked_timestamp = semantic_role == "timestamp"

        # Determine status and score
        if is_datetime_type and not is_marked_timestamp:
            # Date column not marked as timestamp - may confuse time-based queries
            score = score_unmarked
            temporal_status = "unmarked"
        elif not is_datetime_type and is_marked_timestamp:
            # Marked as timestamp but not date type - data type mismatch
            score = score_mismatch
            temporal_status = "mismatch"
        elif is_datetime_type and is_marked_timestamp:
            # Properly identified temporal column
            score = score_aligned
            temporal_status = "aligned"
        else:
            # Not a temporal column - N/A
            return []

        # Build evidence
        evidence = [
            {
                "data_type": data_type,
                "semantic_role": semantic_role,
                "is_datetime_type": is_datetime_type,
                "is_marked_timestamp": is_marked_timestamp,
                "temporal_status": temporal_status,
            }
        ]

        # Resolution options
        resolution_options: list[ResolutionOption] = []

        if temporal_status == "unmarked":
            resolution_options.append(
                ResolutionOption(
                    action="mark_timestamp",
                    parameters={
                        "column": context.column_name,
                        "table": context.table_name,
                        "data_type": data_type,
                    },
                    expected_entropy_reduction=reduction_mark_timestamp,
                    effort="low",
                    description=f"Mark date column '{context.column_name}' as timestamp",
                )
            )
        elif temporal_status == "mismatch":
            resolution_options.append(
                ResolutionOption(
                    action="resolve_temporal_mismatch",
                    parameters={
                        "column": context.column_name,
                        "table": context.table_name,
                        "data_type": data_type,
                        "semantic_role": semantic_role,
                    },
                    expected_entropy_reduction=reduction_mark_timestamp,
                    effort="medium",
                    description=f"Resolve type/role mismatch for '{context.column_name}'",
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
