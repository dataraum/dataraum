"""Join path determinism entropy detector.

Measures uncertainty in join paths between tables.
Ambiguity (multiple paths to SAME table) indicates higher uncertainty,
not connectivity (paths to different tables, which is normal star schema).
"""

from collections import defaultdict
from typing import Any

from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.models import EntropyObject, ResolutionOption


class JoinPathDeterminismDetector(EntropyDetector):
    """Detector for join path determinism.

    Measures whether there's a clear, unambiguous join path between
    the current column's table and other related tables.

    Key insight: Multiple paths to DIFFERENT tables (star schema) = LOW entropy.
    Multiple paths to the SAME table = HIGH entropy (ambiguous which to use).

    Source: relationships from semantic analysis (LLM-confirmed)
    Scores configurable in config/entropy/thresholds.yaml.
    """

    detector_id = "join_path_determinism"
    layer = "structural"
    dimension = "relations"
    sub_dimension = "join_path_determinism"
    required_analyses = ["relationships"]
    description = "Measures ambiguity in join paths (not just connectivity)"

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Detect join path determinism entropy.

        Entropy is based on AMBIGUITY, not connectivity:
        - Orphan (no relationships): high entropy (can't join)
        - Star schema (one path per target table): low entropy (deterministic)
        - Ambiguous (multiple paths to same table): high entropy

        Args:
            context: Detector context with relationship analysis results

        Returns:
            List with single EntropyObject for join path determinism
        """
        config = get_entropy_config()
        detector_config = config.detector("join_path")

        # Configurable scores
        score_orphan = detector_config.get("score_orphan", 0.9)
        score_deterministic = detector_config.get("score_deterministic", 0.1)
        score_ambiguous = detector_config.get("score_ambiguous", 0.7)
        reduction_declare_rel = detector_config.get("reduction_declare_relationship", 0.8)
        reduction_preferred = detector_config.get("reduction_declare_preferred_path", 0.5)

        relationships = context.get_analysis("relationships", [])

        # Handle different input formats
        if isinstance(relationships, dict):
            rels = relationships.get("relationships", [])
        elif isinstance(relationships, list):
            rels = relationships
        else:
            rels = []

        # Group relationships by target table to detect ambiguity
        # For this column, check paths TO other tables (outgoing)
        # and paths FROM other tables (incoming)
        paths_to_table: dict[str, list[dict[str, Any]]] = defaultdict(list)

        for rel in rels:
            from_table = self._get_table(rel, "from_table")
            to_table = self._get_table(rel, "to_table")

            if from_table == context.table_name and to_table:
                # Outgoing relationship
                paths_to_table[to_table].append(rel)
            elif to_table == context.table_name and from_table:
                # Incoming relationship
                paths_to_table[from_table].append(rel)

        # Analyze ambiguity
        total_connections = len(paths_to_table)  # Number of distinct tables connected
        ambiguous_tables = [t for t, paths in paths_to_table.items() if len(paths) > 1]

        # Determine score based on ambiguity, not connectivity
        if total_connections == 0:
            # No relationships - orphan
            score = score_orphan
            path_status = "orphan"
        elif ambiguous_tables:
            # Multiple paths to at least one table - ambiguous
            score = score_ambiguous
            path_status = "ambiguous"
        else:
            # Each connected table has exactly one path - deterministic (star schema OK)
            score = score_deterministic
            path_status = "deterministic"

        # Build evidence
        evidence = [
            {
                "path_status": path_status,
                "connected_tables": total_connections,
                "ambiguous_tables": ambiguous_tables,
                "relationships_per_table": {t: len(p) for t, p in paths_to_table.items()},
            }
        ]

        # Build resolution options
        resolution_options: list[ResolutionOption] = []

        if path_status == "orphan":
            resolution_options.append(
                ResolutionOption(
                    action="declare_relationship",
                    parameters={"table": context.table_name, "type": "foreign_key"},
                    expected_entropy_reduction=reduction_declare_rel,
                    effort="medium",
                    description="Declare a relationship to connect this table to the schema",
                )
            )
        elif path_status == "ambiguous":
            resolution_options.append(
                ResolutionOption(
                    action="declare_preferred_path",
                    parameters={
                        "table": context.table_name,
                        "ambiguous_targets": ambiguous_tables,
                    },
                    expected_entropy_reduction=reduction_preferred,
                    effort="low",
                    description=f"Specify preferred join path for: {', '.join(ambiguous_tables)}",
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

    def _get_table(self, rel: Any, field: str) -> str | None:
        """Get table name from relationship."""
        if hasattr(rel, field):
            value = getattr(rel, field)
            return str(value) if value is not None else None
        if isinstance(rel, dict):
            value = rel.get(field)
            return str(value) if value is not None else None
        return None
