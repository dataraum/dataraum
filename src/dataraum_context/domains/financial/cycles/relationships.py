"""Financial relationship gathering and structure analysis.

Provides rich relationship context for LLM consumption instead of
compressed metrics that lose information.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import networkx as nx
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.analysis.relationships.db_models import Relationship as RelationshipModel
from dataraum_context.core.models.base import Cardinality, RelationshipType
from dataraum_context.storage import Column, Table

# =============================================================================
# Models for LLM Context
# =============================================================================


class RelationshipInfo(BaseModel):
    """Relationship information for LLM context."""

    from_table: str
    from_column: str
    to_table: str
    to_column: str
    relationship_type: str
    cardinality: str | None
    confidence: float


class TableRole(BaseModel):
    """Table's role in the relationship graph."""

    table_name: str
    table_id: str
    connection_count: int
    connects_to: list[str]
    role: str  # "hub", "dimension", "bridge", "isolated"


class CyclePath(BaseModel):
    """A cycle in the relationship graph as a sequence of tables."""

    tables: list[str]
    """Table names forming the cycle (in order)."""

    table_ids: list[str]
    """Table IDs forming the cycle (in order)."""

    length: int
    """Number of tables in the cycle."""


class RelationshipStructure(BaseModel):
    """Rich description of relationship graph for LLM context.

    Provides full structural information instead of compressed metrics.
    The LLM can interpret this to understand business processes.
    """

    # Tables with their roles
    tables: list[TableRole]

    # All relationships with details
    relationships: list[RelationshipInfo]

    # Graph pattern classification
    pattern: str
    """Overall pattern: 'star_schema', 'hub_and_spoke', 'chain', 'mesh', 'disconnected', 'single_table'"""

    pattern_description: str
    """Human-readable description of the pattern."""

    # Structural highlights
    hub_tables: list[str]
    """Tables with 3+ connections (central to the structure)."""

    leaf_tables: list[str]
    """Tables with only 1 connection (dimensions/lookups)."""

    bridge_tables: list[str]
    """Tables connecting otherwise separate groups."""

    isolated_tables: list[str]
    """Tables with no relationships."""

    # Graph cycles (potential business process loops)
    cycles: list[CyclePath]
    """Cycles in the graph - may represent business processes."""

    # Basic counts
    connected_components: int
    """Number of disconnected table groups."""

    total_tables: int
    total_relationships: int


class EnrichedRelationship(BaseModel):
    """Relationship with resolved table/column names.

    Used by gather_relationships() to return relationships with full metadata.
    """

    relationship_id: str
    from_table: str
    from_column: str
    from_column_id: str
    from_table_id: str
    to_table: str
    to_column: str
    to_column_id: str
    to_table_id: str
    relationship_type: RelationshipType
    cardinality: Cardinality | None
    confidence: float
    detection_method: str
    evidence: dict[str, Any]


# =============================================================================
# Main Functions
# =============================================================================


async def gather_relationships(
    table_ids: list[str],
    session: AsyncSession,
    *,
    detection_method: str = "llm",
) -> list[EnrichedRelationship]:
    """Gather relationships confirmed by LLM semantic enrichment.

    Returns ALL relationships matching the detection method without
    arbitrary confidence filtering. The LLM should interpret relationship
    quality, not magic thresholds.

    Args:
        table_ids: List of table IDs to analyze
        session: Async database session
        detection_method: Filter by detection method (default: "llm")

    Returns:
        List of enriched relationships with full metadata
    """
    stmt = (
        select(RelationshipModel)
        .where(
            (RelationshipModel.from_table_id.in_(table_ids))
            & (RelationshipModel.to_table_id.in_(table_ids))
            & (RelationshipModel.detection_method == detection_method)
        )
        .order_by(RelationshipModel.confidence.desc())
    )

    result = await session.execute(stmt)
    db_relationships = result.scalars().all()

    # Convert to enriched format, dedupe bidirectional relationships
    enriched = []
    seen_pairs: set[tuple[str, str]] = set()

    for db_rel in db_relationships:
        # Check BOTH directions for duplicates
        pair_forward = (db_rel.from_column_id, db_rel.to_column_id)
        pair_reverse = (db_rel.to_column_id, db_rel.from_column_id)

        if pair_forward in seen_pairs or pair_reverse in seen_pairs:
            continue

        # Load column/table metadata
        from_col = await session.get(Column, db_rel.from_column_id)
        to_col = await session.get(Column, db_rel.to_column_id)
        from_table = await session.get(Table, db_rel.from_table_id)
        to_table = await session.get(Table, db_rel.to_table_id)

        if from_col is None or to_col is None or from_table is None or to_table is None:
            continue

        enriched.append(
            EnrichedRelationship(
                relationship_id=db_rel.relationship_id,
                from_table=from_table.table_name,
                from_column=from_col.column_name,
                from_column_id=db_rel.from_column_id,
                from_table_id=db_rel.from_table_id,
                to_table=to_table.table_name,
                to_column=to_col.column_name,
                to_column_id=db_rel.to_column_id,
                to_table_id=db_rel.to_table_id,
                relationship_type=RelationshipType(db_rel.relationship_type),
                cardinality=Cardinality(db_rel.cardinality) if db_rel.cardinality else None,
                confidence=db_rel.confidence,
                detection_method=db_rel.detection_method,
                evidence=db_rel.evidence or {},
            )
        )

        seen_pairs.add(pair_forward)
        seen_pairs.add(pair_reverse)

    return enriched


def describe_relationship_structure(
    table_ids: list[str],
    relationships: Sequence[EnrichedRelationship],
    table_names: dict[str, str] | None = None,
) -> RelationshipStructure:
    """Describe the relationship graph structure for LLM context.

    Instead of compressing to metrics like betti_0, provides rich
    structural information that preserves context for LLM interpretation.

    Args:
        table_ids: List of table IDs in the dataset
        relationships: Relationships between tables
        table_names: Optional mapping of table_id -> table_name

    Returns:
        Rich structure description for LLM context
    """
    # Build name lookup
    id_to_name: dict[str, str] = table_names or {}
    for rel in relationships:
        id_to_name[rel.from_table_id] = rel.from_table
        id_to_name[rel.to_table_id] = rel.to_table

    # Build NetworkX graph for analysis
    G: nx.Graph = nx.Graph()  # Undirected for structure analysis
    G_directed: nx.DiGraph = nx.DiGraph()  # Directed for cycle detection

    for table_id in table_ids:
        name = id_to_name.get(table_id, table_id)
        G.add_node(table_id, name=name)
        G_directed.add_node(table_id, name=name)

    for rel in relationships:
        G.add_edge(rel.from_table_id, rel.to_table_id)
        G_directed.add_edge(rel.from_table_id, rel.to_table_id)

    # Analyze table roles
    table_roles: list[TableRole] = []
    hub_tables: list[str] = []
    leaf_tables: list[str] = []
    bridge_tables: list[str] = []
    isolated_tables: list[str] = []

    for table_id in table_ids:
        name = id_to_name.get(table_id, table_id)
        degree = G.degree(table_id) if table_id in G else 0
        neighbors = list(G.neighbors(table_id)) if table_id in G else []
        neighbor_names = [id_to_name.get(n, n) for n in neighbors]

        # Classify role
        if degree == 0:
            role = "isolated"
            isolated_tables.append(name)
        elif degree >= 3:
            role = "hub"
            hub_tables.append(name)
        elif degree == 1:
            role = "dimension"
            leaf_tables.append(name)
        else:
            role = "bridge"
            bridge_tables.append(name)

        table_roles.append(
            TableRole(
                table_name=name,
                table_id=table_id,
                connection_count=degree,
                connects_to=neighbor_names,
                role=role,
            )
        )

    # Convert relationships to info format
    rel_infos = [
        RelationshipInfo(
            from_table=rel.from_table,
            from_column=rel.from_column,
            to_table=rel.to_table,
            to_column=rel.to_column,
            relationship_type=rel.relationship_type.value
            if hasattr(rel.relationship_type, "value")
            else str(rel.relationship_type),
            cardinality=rel.cardinality.value
            if rel.cardinality and hasattr(rel.cardinality, "value")
            else str(rel.cardinality)
            if rel.cardinality
            else None,
            confidence=rel.confidence,
        )
        for rel in relationships
    ]

    # Detect cycles in directed graph
    cycles: list[CyclePath] = []
    try:
        for cycle_ids in nx.simple_cycles(G_directed):
            if len(cycle_ids) >= 2:  # Ignore self-loops
                cycle_names = [id_to_name.get(tid, tid) for tid in cycle_ids]
                cycles.append(
                    CyclePath(
                        tables=cycle_names,
                        table_ids=cycle_ids,
                        length=len(cycle_ids),
                    )
                )
    except Exception:
        pass  # No cycles or error

    # Count connected components
    connected_components = nx.number_connected_components(G) if len(G) > 0 else 0

    # Classify overall pattern
    pattern, pattern_desc = _classify_graph_pattern(
        total_tables=len(table_ids),
        total_relationships=len(relationships),
        hub_count=len(hub_tables),
        leaf_count=len(leaf_tables),
        isolated_count=len(isolated_tables),
        cycle_count=len(cycles),
        component_count=connected_components,
    )

    return RelationshipStructure(
        tables=table_roles,
        relationships=rel_infos,
        pattern=pattern,
        pattern_description=pattern_desc,
        hub_tables=hub_tables,
        leaf_tables=leaf_tables,
        bridge_tables=bridge_tables,
        isolated_tables=isolated_tables,
        cycles=cycles,
        connected_components=connected_components,
        total_tables=len(table_ids),
        total_relationships=len(relationships),
    )


def _classify_graph_pattern(
    total_tables: int,
    total_relationships: int,
    hub_count: int,
    leaf_count: int,
    isolated_count: int,
    cycle_count: int,
    component_count: int,
) -> tuple[str, str]:
    """Classify the overall graph pattern."""
    if total_tables == 0:
        return "empty", "No tables in dataset"

    if total_tables == 1:
        return "single_table", "Single table dataset - no relationships possible"

    if total_relationships == 0:
        return "disconnected", "Tables exist but no relationships detected"

    if component_count > 1:
        return (
            "disconnected",
            f"Tables form {component_count} separate groups with no connections between them",
        )

    if isolated_count > 0:
        isolated_pct = isolated_count / total_tables
        if isolated_pct > 0.5:
            return "sparse", f"{isolated_count} of {total_tables} tables have no relationships"

    # Single component patterns
    if hub_count == 1 and leaf_count >= 2 and cycle_count == 0:
        return (
            "star_schema",
            f"Classic star schema: 1 central hub table connected to {leaf_count} dimension tables",
        )

    if hub_count >= 1 and leaf_count >= 1 and cycle_count == 0:
        return (
            "hub_and_spoke",
            f"{hub_count} hub table(s) connecting to {leaf_count} dimension table(s)",
        )

    if cycle_count > 0 and hub_count >= 1:
        return (
            "mesh_with_cycles",
            f"Interconnected tables with {cycle_count} cycle(s) - may indicate business process flows",
        )

    if cycle_count > 0:
        return (
            "cyclic",
            f"Tables form {cycle_count} cycle(s) - relationships loop back",
        )

    if hub_count == 0 and total_relationships == total_tables - 1:
        return "chain", "Tables connected in a linear chain"

    return "mesh", "Tables interconnected in a mesh pattern"
