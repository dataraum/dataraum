"""Entropy context for graph execution.

Provides EntropyForGraph view for graphs/context.py consumption.
This is a read-only view optimized for graph agent decision-making.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.orm import Session

from dataraum.core.logging import get_logger
from dataraum.entropy.analysis.aggregator import (
    ColumnSummary,
    EntropyAggregator,
    RelationshipSummary,
    TableSummary,
)
from dataraum.entropy.core.storage import EntropyRepository

logger = get_logger(__name__)


@dataclass
class EntropyForGraph:
    """Entropy context optimized for graph execution.

    Provides the graph agent with:
    - Per-column entropy scores and readiness
    - Per-table entropy aggregates
    - Relationship entropy for join decisions
    - Overall readiness assessment
    - Resolution hints for improvement
    """

    # Column summaries keyed by "table.column"
    columns: dict[str, ColumnSummary] = field(default_factory=dict)

    # Table summaries keyed by table_name
    tables: dict[str, TableSummary] = field(default_factory=dict)

    # Relationship summaries keyed by "from.col->to.col"
    relationships: dict[str, RelationshipSummary] = field(default_factory=dict)

    # Global summary statistics
    overall_readiness: str = "investigate"  # ready, investigate, blocked
    high_entropy_count: int = 0
    critical_entropy_count: int = 0
    compound_risk_count: int = 0
    readiness_blockers: list[str] = field(default_factory=list)

    # Metadata
    computed_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def get_column_entropy(self, table: str, column: str) -> ColumnSummary | None:
        """Get entropy summary for a specific column.

        Args:
            table: Table name
            column: Column name

        Returns:
            ColumnSummary or None if not found
        """
        key = f"{table}.{column}"
        return self.columns.get(key)

    def get_table_entropy(self, table: str) -> TableSummary | None:
        """Get entropy summary for a specific table.

        Args:
            table: Table name

        Returns:
            TableSummary or None if not found
        """
        return self.tables.get(table)

    def get_relationship_entropy(
        self, from_table: str, from_col: str, to_table: str, to_col: str
    ) -> RelationshipSummary | None:
        """Get entropy summary for a relationship.

        Args:
            from_table: Source table
            from_col: Source column
            to_table: Target table
            to_col: Target column

        Returns:
            RelationshipSummary or None if not found
        """
        key = f"{from_table}.{from_col}->{to_table}.{to_col}"
        return self.relationships.get(key)

    def has_blocking_issues(self) -> bool:
        """Check if there are any blocking entropy issues."""
        return self.overall_readiness == "blocked"

    def get_high_entropy_columns(self, threshold: float = 0.5) -> list[str]:
        """Get columns with entropy above threshold.

        Args:
            threshold: Entropy threshold (default 0.5)

        Returns:
            List of "table.column" keys above threshold
        """
        return [
            key for key, summary in self.columns.items() if summary.composite_score >= threshold
        ]

    def to_summary_dict(self) -> dict[str, Any]:
        """Convert to summary dictionary for inclusion in larger context.

        Returns:
            Dictionary with summary statistics suitable for LLM prompts
        """
        return {
            "overall_readiness": self.overall_readiness,
            "high_entropy_count": self.high_entropy_count,
            "critical_entropy_count": self.critical_entropy_count,
            "compound_risk_count": self.compound_risk_count,
            "readiness_blockers": self.readiness_blockers,
        }


def build_for_graph(
    session: Session,
    table_ids: list[str],
) -> EntropyForGraph:
    """Build entropy context for graph execution.

    Loads entropy data for typed tables only and aggregates into
    a view optimized for graph agent consumption.

    Args:
        session: SQLAlchemy session
        table_ids: List of table IDs to include

    Returns:
        EntropyForGraph with computed summaries
    """
    if not table_ids:
        return EntropyForGraph()

    repo = EntropyRepository(session)
    aggregator = EntropyAggregator()

    # Get table/column metadata (for typed tables only)
    typed_table_ids = repo.get_typed_table_ids(table_ids)
    if not typed_table_ids:
        logger.warning("No typed tables found for entropy context")
        return EntropyForGraph(overall_readiness="ready")

    table_map, column_map = repo.get_table_column_mapping(typed_table_ids)

    # Load entropy objects
    entropy_objects = repo.load_for_tables(typed_table_ids, enforce_typed=True)

    if not entropy_objects:
        logger.debug("No entropy objects found")
        return EntropyForGraph(overall_readiness="ready")

    # Aggregate into summaries
    column_summaries, table_summaries = aggregator.summarize_columns_by_table(
        entropy_objects=entropy_objects,
        table_map=table_map,
        column_map=column_map,
    )

    # Build relationship summaries
    relationship_summaries = _load_relationship_entropy(
        session=session,
        table_ids=typed_table_ids,
        table_map=table_map,
        aggregator=aggregator,
    )

    # Calculate global statistics
    from dataraum.entropy.config import get_entropy_config

    config = get_entropy_config()
    high_threshold = config.high_entropy_threshold
    critical_threshold = config.critical_entropy_threshold

    high_entropy_count = sum(
        1 for c in column_summaries.values() if c.composite_score >= high_threshold
    )
    critical_entropy_count = sum(
        1 for c in column_summaries.values() if c.composite_score >= critical_threshold
    )

    compound_risk_count = sum(len(c.compound_risks) for c in column_summaries.values())

    # Determine overall readiness
    readiness_blockers: list[str] = []
    if critical_entropy_count > 0:
        overall_readiness = "blocked"
        readiness_blockers = [
            key for key, c in column_summaries.items() if c.composite_score >= critical_threshold
        ]
    elif high_entropy_count > 0:
        overall_readiness = "investigate"
    else:
        overall_readiness = "ready"

    return EntropyForGraph(
        columns=column_summaries,
        tables=table_summaries,
        relationships=relationship_summaries,
        overall_readiness=overall_readiness,
        high_entropy_count=high_entropy_count,
        critical_entropy_count=critical_entropy_count,
        compound_risk_count=compound_risk_count,
        readiness_blockers=readiness_blockers,
    )


def _load_relationship_entropy(
    session: Session,
    table_ids: list[str],
    table_map: dict[str, Any],
    aggregator: EntropyAggregator,
) -> dict[str, RelationshipSummary]:
    """Load and compute relationship entropy summaries.

    Args:
        session: SQLAlchemy session
        table_ids: Table IDs to load relationships for
        table_map: Mapping of table_id to Table
        aggregator: EntropyAggregator instance

    Returns:
        Dictionary of relationship summaries keyed by relationship key
    """
    from sqlalchemy import select

    from dataraum.analysis.relationships.db_models import Relationship
    from dataraum.storage import Column

    # Load relationships between typed tables
    rel_stmt = select(Relationship).where(
        (Relationship.from_table_id.in_(table_ids)) & (Relationship.to_table_id.in_(table_ids))
    )
    relationships = session.execute(rel_stmt).scalars().all()

    if not relationships:
        return {}

    # Get table names
    table_names = {t.table_id: t.table_name for t in table_map.values()}

    # Load columns for name lookup
    col_stmt = select(Column).where(Column.table_id.in_(table_ids))
    columns = session.execute(col_stmt).scalars().all()
    column_names = {c.column_id: c.column_name for c in columns}

    # Build summaries
    summaries: dict[str, RelationshipSummary] = {}

    for rel in relationships:
        from_table = table_names.get(rel.from_table_id, "unknown")
        to_table = table_names.get(rel.to_table_id, "unknown")
        from_col = column_names.get(rel.from_column_id, "unknown")
        to_col = column_names.get(rel.to_column_id, "unknown")

        summary = aggregator.summarize_relationship(
            from_table=from_table,
            from_column=from_col,
            to_table=to_table,
            to_column=to_col,
            confidence=rel.confidence or 0.5,
            detection_method=rel.detection_method or "unknown",
            relationship_type=rel.relationship_type,
            cardinality=rel.cardinality,
        )

        summaries[summary.relationship_key] = summary

    return summaries


def format_entropy_for_prompt(context: EntropyForGraph) -> str:
    """Format entropy information for LLM prompts.

    Creates a concise entropy summary that helps the LLM understand:
    - Overall data readiness
    - High-entropy columns that need assumptions
    - Dangerous combinations (compound risks)
    - Columns that may block reliable answers

    Args:
        context: EntropyForGraph with entropy data

    Returns:
        Formatted string for entropy section, or empty string if no issues
    """
    lines: list[str] = []

    # Overall readiness header
    readiness = context.overall_readiness
    if readiness == "ready":
        lines.append("## DATA READINESS: ✓ READY")
        lines.append("Data quality is sufficient for reliable answers.")
    elif readiness == "investigate":
        lines.append("## DATA READINESS: ⚠ INVESTIGATE")
        lines.append("Some columns have elevated uncertainty - state assumptions when using them.")
    else:  # blocked
        lines.append("## DATA READINESS: ✗ BLOCKED")
        lines.append(
            "Critical data quality issues exist - consider refusing or asking clarification."
        )

    lines.append("")

    # Stats summary
    if context.high_entropy_count > 0 or context.critical_entropy_count > 0:
        lines.append(f"- High entropy columns: {context.high_entropy_count}")
        if context.critical_entropy_count > 0:
            lines.append(f"- Critical entropy columns: {context.critical_entropy_count}")
        if context.compound_risk_count > 0:
            lines.append(f"- Compound risks: {context.compound_risk_count}")
        lines.append("")

    # Blocked columns (critical)
    if context.readiness_blockers:
        lines.append("### BLOCKING ISSUES")
        lines.append("These columns have critical uncertainty and should be clarified before use:")
        for blocker in context.readiness_blockers[:5]:
            lines.append(f"  - {blocker}")
        lines.append("")

    # High entropy columns with details
    high_cols = _collect_high_entropy_columns(context)
    if high_cols:
        lines.append("### HIGH UNCERTAINTY COLUMNS")
        lines.append("State assumptions when using these columns:")
        for col_info in high_cols[:10]:
            dims = ", ".join(col_info["dimensions"][:2])
            lines.append(f"  - {col_info['name']} (entropy: {col_info['score']:.2f}) - {dims}")
        lines.append("")

    # Compound risks
    compound_warnings = _format_compound_risks(context)
    if compound_warnings:
        lines.append("### DANGEROUS COMBINATIONS")
        lines.append("These dimension combinations create multiplicative risk:")
        lines.extend(compound_warnings)
        lines.append("")

    return "\n".join(lines)


def _collect_high_entropy_columns(context: EntropyForGraph) -> list[dict[str, Any]]:
    """Collect columns with high entropy from the context."""
    high_cols: list[dict[str, Any]] = []

    for key, summary in context.columns.items():
        if summary.composite_score >= 0.5:
            high_cols.append(
                {
                    "name": key,
                    "score": summary.composite_score,
                    "dimensions": summary.high_entropy_dimensions,
                    "readiness": summary.readiness,
                }
            )

    # Sort by score descending
    high_cols.sort(key=lambda x: x["score"], reverse=True)
    return high_cols


def _format_compound_risks(context: EntropyForGraph) -> list[str]:
    """Format compound risk warnings."""
    warnings: list[str] = []

    for table_name, table_summary in context.tables.items():
        if table_summary.compound_risks:
            risk_count = len(table_summary.compound_risks)
            blocked = table_summary.blocked_columns
            if blocked:
                warnings.append(
                    f"  - Table '{table_name}': {risk_count} compound risks "
                    f"affecting columns: {', '.join(blocked[:3])}"
                )

    return warnings


def get_column_entropy_summary(summary: ColumnSummary) -> dict[str, Any]:
    """Get a summary dict for a column suitable for graph context.

    Args:
        summary: ColumnSummary to format

    Returns:
        Dict with key entropy fields
    """
    result: dict[str, Any] = {
        "composite_score": summary.composite_score,
        "readiness": summary.readiness,
        "structural_entropy": summary.layer_scores.get("structural", 0.0),
        "semantic_entropy": summary.layer_scores.get("semantic", 0.0),
        "value_entropy": summary.layer_scores.get("value", 0.0),
        "computational_entropy": summary.layer_scores.get("computational", 0.0),
        "high_entropy_dimensions": summary.high_entropy_dimensions,
        "resolution_hints": [
            {
                "action": hint.action,
                "description": hint.description,
                "expected_reduction": hint.expected_entropy_reduction,
                "effort": hint.effort,
            }
            for hint in summary.top_resolution_hints[:3]
        ],
    }

    # Include interpretation if available
    if summary.interpretation is not None:
        result["interpretation"] = {
            "explanation": summary.interpretation.explanation,
            "assumptions": [
                {
                    "dimension": a.dimension,
                    "assumption_text": a.assumption_text,
                    "confidence": a.confidence,
                    "impact": a.impact,
                    "basis": a.basis,
                }
                for a in summary.interpretation.assumptions
            ],
            "resolution_actions": [
                {
                    "action": r.action,
                    "description": r.description,
                    "priority": r.priority,
                    "effort": r.effort,
                    "expected_impact": r.expected_impact,
                }
                for r in summary.interpretation.resolution_actions
            ],
        }

    return result


def get_table_entropy_summary(summary: TableSummary) -> dict[str, Any]:
    """Get a summary dict for a table suitable for graph context.

    Args:
        summary: TableSummary to format

    Returns:
        Dict with key entropy fields
    """
    return {
        "avg_composite_score": summary.avg_composite_score,
        "max_composite_score": summary.max_composite_score,
        "readiness": summary.readiness,
        "high_entropy_columns": summary.high_entropy_columns,
        "blocked_columns": summary.blocked_columns,
        "compound_risk_count": len(summary.compound_risks),
    }
