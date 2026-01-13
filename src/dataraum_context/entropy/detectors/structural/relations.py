"""Join path determinism entropy detector.

Measures uncertainty in join paths between tables.
Multiple paths or no paths indicate higher uncertainty for aggregations.
"""

from typing import Any

from dataraum_context.entropy.config import get_entropy_config
from dataraum_context.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum_context.entropy.models import EntropyObject, ResolutionOption


class JoinPathDeterminismDetector(EntropyDetector):
    """Detector for join path determinism.

    Measures whether there's a clear, unambiguous join path between
    the current column's table and other related tables.

    Source: relationships + graph_topology analysis
    Scores configurable in config/entropy/thresholds.yaml.
    """

    detector_id = "join_path_determinism"
    layer = "structural"
    dimension = "relations"
    sub_dimension = "join_path_determinism"
    required_analyses = ["relationships"]
    description = "Measures uncertainty in join paths between tables"

    async def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Detect join path determinism entropy.

        Args:
            context: Detector context with relationship analysis results

        Returns:
            List with single EntropyObject for join path determinism
        """
        # Load configuration
        config = get_entropy_config()
        detector_config = config.detector("join_path")

        # Get configurable scores and thresholds
        score_orphan = detector_config.get("score_orphan", 0.9)
        score_single = detector_config.get("score_single", 0.1)
        score_few = detector_config.get("score_few", 0.4)
        score_multiple = detector_config.get("score_multiple", 0.7)
        few_path_max = detector_config.get("few_path_max", 3)
        reduction_declare_rel = detector_config.get("reduction_declare_relationship", 0.8)
        reduction_preferred = detector_config.get("reduction_declare_preferred_path", 0.5)

        relationships = context.get_analysis("relationships", {})

        # Extract relationship information
        # Can be a list of Relationship objects or dicts
        if isinstance(relationships, dict):
            rels = relationships.get("relationships", [])
            outgoing = relationships.get("outgoing_count", 0)
            incoming = relationships.get("incoming_count", 0)
        elif isinstance(relationships, list):
            rels = relationships
            outgoing = len([r for r in rels if self._is_outgoing(r, context.table_name)])
            incoming = len([r for r in rels if self._is_incoming(r, context.table_name)])
        else:
            rels = []
            outgoing = 0
            incoming = 0

        total_paths = outgoing + incoming

        # Calculate entropy based on path count using configurable scores
        if total_paths == 0:
            # No relationships - orphan table
            score = score_orphan
            path_status = "orphan"
        elif total_paths == 1:
            # Single clear path - deterministic
            score = score_single
            path_status = "single"
        elif total_paths <= few_path_max:
            # Few paths - some ambiguity
            score = score_few
            path_status = "few"
        else:
            # Multiple paths - high ambiguity
            score = score_multiple
            path_status = "multiple"

        # Build evidence
        evidence = [
            {
                "path_status": path_status,
                "outgoing_relationships": outgoing,
                "incoming_relationships": incoming,
                "total_paths": total_paths,
            }
        ]

        # Build resolution options
        resolution_options: list[ResolutionOption] = []

        if path_status == "orphan":
            resolution_options.append(
                ResolutionOption(
                    action="declare_relationship",
                    parameters={
                        "table": context.table_name,
                        "type": "foreign_key",
                    },
                    expected_entropy_reduction=reduction_declare_rel,
                    effort="medium",
                    description="Declare a relationship to connect this table to the schema",
                )
            )
        elif path_status == "multiple":
            resolution_options.append(
                ResolutionOption(
                    action="declare_preferred_path",
                    parameters={
                        "table": context.table_name,
                    },
                    expected_entropy_reduction=reduction_preferred,
                    effort="low",
                    description="Specify the preferred join path for aggregations",
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

    def _is_outgoing(self, rel: Any, table_name: str) -> bool:
        """Check if relationship is outgoing from table."""
        if hasattr(rel, "from_table"):
            return bool(rel.from_table == table_name)
        if isinstance(rel, dict):
            return rel.get("from_table") == table_name
        return False

    def _is_incoming(self, rel: Any, table_name: str) -> bool:
        """Check if relationship is incoming to table."""
        if hasattr(rel, "to_table"):
            return bool(rel.to_table == table_name)
        if isinstance(rel, dict):
            return rel.get("to_table") == table_name
        return False
