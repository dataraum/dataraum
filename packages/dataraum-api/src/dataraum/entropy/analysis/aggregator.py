"""Entropy aggregator for dynamic summary computation.

Layer 2 of the entropy framework - provides dynamic aggregation of
EntropyObjects into caller-appropriate summaries.

These summaries are computed on demand (NOT stored as database models)
to avoid duplication and ensure freshness.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from dataraum.core.logging import get_logger
from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.models import CompoundRisk, EntropyObject, ResolutionOption

if TYPE_CHECKING:
    from dataraum.entropy.interpretation import EntropyInterpretation

logger = get_logger(__name__)


@dataclass
class ColumnSummary:
    """Computed entropy summary for a single column.

    NOT a stored model - computed dynamically from EntropyObjects.
    """

    column_id: str
    column_name: str
    table_id: str
    table_name: str

    # Composite score (weighted average of layer scores)
    composite_score: float = 0.0

    # Readiness classification
    readiness: str = "investigate"  # ready, investigate, blocked

    # Per-layer scores (0.0-1.0)
    layer_scores: dict[str, float] = field(default_factory=dict)
    # Keys: "structural", "semantic", "value", "computational"

    # Dimension-level scores
    dimension_scores: dict[str, float] = field(default_factory=dict)
    # Keys: "structural.types.type_fidelity", etc.

    # High entropy dimensions (score > threshold)
    high_entropy_dimensions: list[str] = field(default_factory=list)

    # Top resolution hints
    top_resolution_hints: list[ResolutionOption] = field(default_factory=list)

    # Compound risks affecting this column
    compound_risks: list[CompoundRisk] = field(default_factory=list)

    # Raw entropy objects (for detail views)
    entropy_objects: list[EntropyObject] = field(default_factory=list)

    # LLM interpretation (optional)
    interpretation: EntropyInterpretation | None = None


@dataclass
class TableSummary:
    """Computed entropy summary for a table.

    NOT a stored model - computed dynamically from ColumnSummaries.
    """

    table_id: str
    table_name: str

    # Column summaries
    columns: list[ColumnSummary] = field(default_factory=list)

    # Aggregate scores
    avg_composite_score: float = 0.0
    max_composite_score: float = 0.0

    # Per-layer averages
    avg_layer_scores: dict[str, float] = field(default_factory=dict)
    max_layer_scores: dict[str, float] = field(default_factory=dict)

    # Readiness classification
    readiness: str = "investigate"  # ready, investigate, blocked

    # Problem columns
    high_entropy_columns: list[str] = field(default_factory=list)
    blocked_columns: list[str] = field(default_factory=list)

    # Table-level compound risks
    compound_risks: list[CompoundRisk] = field(default_factory=list)


@dataclass
class RelationshipSummary:
    """Computed entropy summary for a relationship.

    NOT a stored model - computed dynamically.
    """

    from_table: str
    from_column: str
    to_table: str
    to_column: str

    # Relationship-specific entropy scores
    cardinality_entropy: float = 0.0
    join_path_entropy: float = 0.0
    referential_integrity_entropy: float = 0.0
    semantic_clarity_entropy: float = 0.0

    # Composite score (max of above)
    composite_score: float = 0.0

    # Is this join deterministic?
    is_deterministic: bool = True

    # Warning for graph agent
    join_warning: str | None = None

    @property
    def relationship_key(self) -> str:
        """Return a unique key for this relationship."""
        return f"{self.from_table}.{self.from_column}->{self.to_table}.{self.to_column}"


class EntropyAggregator:
    """Aggregates EntropyObjects into summaries on demand.

    This class computes summaries dynamically rather than storing them.
    Summaries are cheap to compute and always reflect current data.
    """

    def __init__(self) -> None:
        """Initialize aggregator with config."""
        self._config = get_entropy_config()

    def summarize_column(
        self,
        column_id: str,
        column_name: str,
        table_id: str,
        table_name: str,
        entropy_objects: list[EntropyObject],
        *,
        compound_risks: list[CompoundRisk] | None = None,
        interpretation: EntropyInterpretation | None = None,
    ) -> ColumnSummary:
        """Compute entropy summary for a column.

        Args:
            column_id: Column ID
            column_name: Column name
            table_id: Table ID
            table_name: Table name
            entropy_objects: List of EntropyObjects for this column
            compound_risks: Optional pre-computed compound risks
            interpretation: Optional LLM interpretation

        Returns:
            ColumnSummary with computed scores
        """
        summary = ColumnSummary(
            column_id=column_id,
            column_name=column_name,
            table_id=table_id,
            table_name=table_name,
            entropy_objects=entropy_objects,
            compound_risks=compound_risks or [],
            interpretation=interpretation,
        )

        if not entropy_objects:
            summary.readiness = "ready"
            return summary

        # Group by layer and compute layer scores
        layer_scores_raw: dict[str, list[float]] = defaultdict(list)
        dimension_scores: dict[str, float] = {}

        for obj in entropy_objects:
            layer_scores_raw[obj.layer].append(obj.score)
            dimension_scores[obj.dimension_path] = obj.score

        # Calculate layer averages
        layer_scores: dict[str, float] = {}
        for layer in ["structural", "semantic", "value", "computational"]:
            if layer_scores_raw[layer]:
                layer_scores[layer] = sum(layer_scores_raw[layer]) / len(layer_scores_raw[layer])
            else:
                layer_scores[layer] = 0.0

        summary.layer_scores = layer_scores
        summary.dimension_scores = dimension_scores

        # Calculate composite score using config weights
        weights = self._config.composite_weights
        summary.composite_score = (
            layer_scores.get("structural", 0.0) * weights["structural"]
            + layer_scores.get("semantic", 0.0) * weights["semantic"]
            + layer_scores.get("value", 0.0) * weights["value"]
            + layer_scores.get("computational", 0.0) * weights["computational"]
        )

        # Identify high-entropy dimensions
        high_threshold = self._config.high_entropy_threshold
        summary.high_entropy_dimensions = [
            dim for dim, score in dimension_scores.items() if score >= high_threshold
        ]

        # Determine readiness
        summary.readiness = self._config.get_readiness(summary.composite_score)

        # Collect top resolution hints
        all_hints: list[ResolutionOption] = []
        for obj in entropy_objects:
            all_hints.extend(obj.resolution_options)

        # Sort by priority and take top 3
        all_hints.sort(key=lambda h: h.priority_score(), reverse=True)
        summary.top_resolution_hints = all_hints[:3]

        return summary

    def summarize_table(
        self,
        table_id: str,
        table_name: str,
        column_summaries: list[ColumnSummary],
    ) -> TableSummary:
        """Compute entropy summary for a table from column summaries.

        Args:
            table_id: Table ID
            table_name: Table name
            column_summaries: List of ColumnSummary for this table

        Returns:
            TableSummary with aggregated scores
        """
        summary = TableSummary(
            table_id=table_id,
            table_name=table_name,
            columns=column_summaries,
        )

        if not column_summaries:
            summary.readiness = "ready"
            return summary

        # Calculate aggregate scores
        composite_scores = [c.composite_score for c in column_summaries]
        summary.avg_composite_score = sum(composite_scores) / len(composite_scores)
        summary.max_composite_score = max(composite_scores)

        # Per-layer aggregates
        layers = ["structural", "semantic", "value", "computational"]
        avg_layer: dict[str, float] = {}
        max_layer: dict[str, float] = {}

        for layer in layers:
            layer_scores = [c.layer_scores.get(layer, 0.0) for c in column_summaries]
            avg_layer[layer] = sum(layer_scores) / len(layer_scores)
            max_layer[layer] = max(layer_scores)

        summary.avg_layer_scores = avg_layer
        summary.max_layer_scores = max_layer

        # Identify problem columns
        high_threshold = self._config.high_entropy_threshold
        critical_threshold = self._config.critical_entropy_threshold

        summary.high_entropy_columns = [
            c.column_name for c in column_summaries if c.composite_score >= high_threshold
        ]
        summary.blocked_columns = [
            c.column_name for c in column_summaries if c.composite_score >= critical_threshold
        ]

        # Collect all compound risks
        all_risks: list[CompoundRisk] = []
        for col in column_summaries:
            all_risks.extend(col.compound_risks)
        summary.compound_risks = all_risks

        # Determine table readiness
        if summary.blocked_columns:
            summary.readiness = "blocked"
        elif summary.high_entropy_columns:
            summary.readiness = "investigate"
        else:
            summary.readiness = "ready"

        return summary

    def summarize_columns_by_table(
        self,
        entropy_objects: list[EntropyObject],
        table_map: dict[str, Any],  # table_id -> Table
        column_map: dict[str, Any],  # column_id -> Column
    ) -> tuple[dict[str, ColumnSummary], dict[str, TableSummary]]:
        """Summarize all columns grouped by table.

        Args:
            entropy_objects: All EntropyObjects to summarize
            table_map: Mapping of table_id to Table model
            column_map: Mapping of column_id to Column model

        Returns:
            Tuple of (column_summaries, table_summaries)
            - column_summaries keyed by "table_name.column_name"
            - table_summaries keyed by table_name
        """
        # Group entropy objects by column
        objects_by_column: dict[str, list[EntropyObject]] = defaultdict(list)
        for obj in entropy_objects:
            if obj.target.startswith("column:"):
                # Extract column_id from target or use column_id field
                # Target format: "column:{table}.{column}"
                pass
            # Use the column_id from the object's source if available
            # For now, group by target
            objects_by_column[obj.target].append(obj)

        # Also group by column_id if we can infer it
        for obj in entropy_objects:
            for col_id, col in column_map.items():
                table = table_map.get(col.table_id)
                if table:
                    target = f"column:{table.table_name}.{col.column_name}"
                    if obj.target == target:
                        objects_by_column[col_id].append(obj)

        # Build column summaries
        column_summaries: dict[str, ColumnSummary] = {}
        columns_by_table: dict[str, list[ColumnSummary]] = defaultdict(list)

        for col_id, col in column_map.items():
            table = table_map.get(col.table_id)
            if not table:
                continue

            # Get entropy objects for this column
            target = f"column:{table.table_name}.{col.column_name}"
            col_objects = objects_by_column.get(target, [])
            # Also check by column_id
            col_objects.extend(objects_by_column.get(col_id, []))
            # Deduplicate by object_id
            seen_ids = set()
            unique_objects = []
            for obj in col_objects:
                if obj.object_id not in seen_ids:
                    seen_ids.add(obj.object_id)
                    unique_objects.append(obj)

            col_summary = self.summarize_column(
                column_id=col_id,
                column_name=col.column_name,
                table_id=table.table_id,
                table_name=table.table_name,
                entropy_objects=unique_objects,
            )

            key = f"{table.table_name}.{col.column_name}"
            column_summaries[key] = col_summary
            columns_by_table[table.table_name].append(col_summary)

        # Build table summaries
        table_summaries: dict[str, TableSummary] = {}
        for table_name, col_sums in columns_by_table.items():
            # Find table_id
            table_id = ""
            for tid, t in table_map.items():
                if t.table_name == table_name:
                    table_id = tid
                    break

            table_summaries[table_name] = self.summarize_table(
                table_id=table_id,
                table_name=table_name,
                column_summaries=col_sums,
            )

        return column_summaries, table_summaries

    def summarize_relationship(
        self,
        from_table: str,
        from_column: str,
        to_table: str,
        to_column: str,
        *,
        confidence: float = 0.5,
        detection_method: str = "unknown",
        relationship_type: str | None = None,
        cardinality: str | None = None,
    ) -> RelationshipSummary:
        """Compute entropy summary for a relationship.

        Args:
            from_table: Source table name
            from_column: Source column name
            to_table: Target table name
            to_column: Target column name
            confidence: Relationship detection confidence
            detection_method: How relationship was detected
            relationship_type: Type of relationship
            cardinality: Cardinality string (e.g., "1:N")

        Returns:
            RelationshipSummary with computed entropy
        """
        summary = RelationshipSummary(
            from_table=from_table,
            from_column=from_column,
            to_table=to_table,
            to_column=to_column,
        )

        # Cardinality entropy based on confidence
        summary.cardinality_entropy = 1.0 - confidence

        # Join path entropy based on detection method
        if detection_method == "llm":
            summary.join_path_entropy = 0.2  # LLM-detected is reasonably reliable
        elif detection_method == "exact_match":
            summary.join_path_entropy = 0.1
        else:
            summary.join_path_entropy = 0.5  # Statistical methods less certain

        # Referential integrity - use confidence as proxy
        summary.referential_integrity_entropy = max(0.0, 0.5 - confidence)

        # Semantic clarity based on relationship type
        if relationship_type and relationship_type != "unknown":
            summary.semantic_clarity_entropy = 0.2
        else:
            summary.semantic_clarity_entropy = 0.7

        # Composite is max of all components
        summary.composite_score = max(
            summary.cardinality_entropy,
            summary.join_path_entropy,
            summary.referential_integrity_entropy,
            summary.semantic_clarity_entropy,
        )

        summary.is_deterministic = summary.composite_score < 0.5

        # Add warning if not deterministic
        if not summary.is_deterministic:
            summary.join_warning = (
                f"Join between {from_table} and {to_table} has "
                f"entropy {summary.composite_score:.2f} - verify join conditions"
            )

        return summary


def aggregate_column(
    column_id: str,
    column_name: str,
    table_id: str,
    table_name: str,
    entropy_objects: list[EntropyObject],
) -> ColumnSummary:
    """Convenience function to aggregate a single column.

    Args:
        column_id: Column ID
        column_name: Column name
        table_id: Table ID
        table_name: Table name
        entropy_objects: Entropy objects for this column

    Returns:
        ColumnSummary
    """
    aggregator = EntropyAggregator()
    return aggregator.summarize_column(
        column_id=column_id,
        column_name=column_name,
        table_id=table_id,
        table_name=table_name,
        entropy_objects=entropy_objects,
    )


def aggregate_table(
    table_id: str,
    table_name: str,
    column_summaries: list[ColumnSummary],
) -> TableSummary:
    """Convenience function to aggregate a table.

    Args:
        table_id: Table ID
        table_name: Table name
        column_summaries: Column summaries for this table

    Returns:
        TableSummary
    """
    aggregator = EntropyAggregator()
    return aggregator.summarize_table(
        table_id=table_id,
        table_name=table_name,
        column_summaries=column_summaries,
    )
