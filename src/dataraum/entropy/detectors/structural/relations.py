"""Join path determinism entropy detector.

Measures uncertainty in join paths between tables.
Ambiguity (multiple paths to SAME table) indicates higher uncertainty,
not connectivity (paths to different tables, which is normal star schema).
"""

from collections import defaultdict
from typing import Any

from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import AnalysisKey, Dimension, FixAction, Layer, SubDimension
from dataraum.entropy.models import EntropyObject, ResolutionOption
from dataraum.pipeline.fixes.models import FixSchema, FixSchemaField


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
    layer = Layer.STRUCTURAL
    dimension = Dimension.RELATIONS
    sub_dimension = SubDimension.JOIN_PATH_DETERMINISM
    required_analyses = [AnalysisKey.RELATIONSHIPS]
    description = "Measures ambiguity in join paths (not just connectivity)"

    @property
    def fixable_actions(self) -> set[FixAction]:
        """Resolving join ambiguity specifies the preferred path."""
        return {FixAction.RESOLVE_JOIN_AMBIGUITY}

    @property
    def fix_schemas(self) -> list[FixSchema]:
        """Schema for resolving join ambiguity."""
        return [
            FixSchema(
                action="resolve_join_ambiguity",
                target="config",
                description="Set preferred join path for ambiguous table connections",
                config_path="phases/relationships.yaml",
                key_path=["overrides", "preferred_joins"],
                operation="merge",
                requires_rerun="relationships",
                key_template="{table}->{target_table}",
                guidance=(
                    "Resolves ambiguity when multiple join paths exist between tables. "
                    "Ask which join path is the correct one for analytics queries."
                ),
                fields={
                    "table": FixSchemaField(
                        type="string",
                        required=True,
                        description="Source table with ambiguous paths",
                    ),
                    "target_table": FixSchemaField(
                        type="string",
                        required=True,
                        description="Target table",
                    ),
                    "preferred_column": FixSchemaField(
                        type="string",
                        required=True,
                        description="Column to use for the join",
                    ),
                },
            )
        ]

    def load_data(self, context: DetectorContext) -> None:
        """Load relationships for this column."""
        if context.session is None or context.column_id is None or context.table_id is None:
            return
        from dataraum.entropy.detectors.loaders import load_relationships

        result = load_relationships(context.session, context.column_id, context.table_id)
        if result is not None:
            context.analysis_results["relationships"] = result

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
            # Proportional: ratio of ambiguous tables to total connections
            ambiguity_ratio = len(ambiguous_tables) / total_connections
            # Scale between score_deterministic and score_ambiguous
            score = score_deterministic + (score_ambiguous - score_deterministic) * ambiguity_ratio
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
                    action="confirm_relationship",
                    parameters={"table": context.table_name, "type": "foreign_key"},
                    effort="medium",
                    description="Declare a relationship to connect this table to the schema",
                )
            )
        elif path_status == "ambiguous":
            resolution_options.append(
                ResolutionOption(
                    action="resolve_join_ambiguity",
                    parameters={
                        "table": context.table_name,
                        "ambiguous_targets": ambiguous_tables,
                    },
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
