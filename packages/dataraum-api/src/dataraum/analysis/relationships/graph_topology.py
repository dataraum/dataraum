"""Graph topology analysis for relationship structures.

Analyzes the graph structure of table relationships to:
- Classify tables by role (hub, dimension, bridge, isolated)
- Detect overall graph patterns (star_schema, mesh, etc.)
- Find schema-level cycles (circular references between tables)

This analysis runs after relationship detection and provides context
for semantic and cycles agents.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

import networkx as nx
from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from dataraum.analysis.relationships.db_models import Relationship
    from dataraum.analysis.relationships.models import RelationshipCandidate


class TableRole(BaseModel):
    """Table's role in the relationship graph."""

    table_name: str
    table_id: str
    connection_count: int
    connects_to: list[str] = Field(default_factory=list)
    role: str  # "hub", "dimension", "bridge", "isolated"


class SchemaCycle(BaseModel):
    """A cycle in the relationship graph (schema-level, not business cycle).

    These are circular references between tables in the schema,
    distinct from business process cycles detected by the cycles agent.
    """

    tables: list[str] = Field(default_factory=list)
    """Table names forming the cycle (in order)."""

    table_ids: list[str] = Field(default_factory=list)
    """Table IDs forming the cycle (in order)."""

    length: int = 0
    """Number of tables in the cycle."""


class GraphStructure(BaseModel):
    """Graph topology analysis result.

    Provides structural information about the relationship graph
    for use as context by semantic and cycles agents.
    """

    # Table classifications
    tables: list[TableRole] = Field(default_factory=list)
    hub_tables: list[str] = Field(default_factory=list)
    """Tables with 3+ connections (central to the structure)."""

    leaf_tables: list[str] = Field(default_factory=list)
    """Tables with only 1 connection (dimensions/lookups)."""

    bridge_tables: list[str] = Field(default_factory=list)
    """Tables with 2 connections (linking other tables)."""

    isolated_tables: list[str] = Field(default_factory=list)
    """Tables with no relationships."""

    # Pattern classification
    pattern: str = "unknown"
    """Overall pattern: 'star_schema', 'hub_and_spoke', 'chain', 'mesh', 'disconnected', 'single_table'"""

    pattern_description: str = ""
    """Human-readable description of the pattern."""

    # Graph metrics
    connected_components: int = 0
    """Number of disconnected table groups."""

    schema_cycles: list[SchemaCycle] = Field(default_factory=list)
    """Cycles in the graph (circular table references)."""

    total_tables: int = 0
    total_relationships: int = 0


def analyze_graph_topology(
    table_ids: list[str],
    relationships: Sequence[RelationshipCandidate | Relationship | dict[str, Any]],
    table_names: dict[str, str] | None = None,
) -> GraphStructure:
    """Analyze relationship graph topology.

    Builds a graph from relationships and analyzes its structure
    to classify tables and detect patterns.

    Args:
        table_ids: List of table IDs to analyze
        relationships: Detected relationships - can be:
            - RelationshipCandidate objects (from detector)
            - Relationship DB objects (from database)
            - Dict with table1/table2 or from_table_id/to_table_id keys
        table_names: Optional mapping of table_id -> table_name.
            If not provided, will be inferred from relationships.

    Returns:
        GraphStructure with pattern classification and table roles
    """
    if not table_ids:
        return GraphStructure(pattern="empty", pattern_description="No tables provided")

    # Build name lookup
    id_to_name: dict[str, str] = dict(table_names) if table_names else {}

    # Normalize relationships to edges
    edges: list[tuple[str, str]] = []

    for rel in relationships:
        from_id, to_id = _extract_table_ids(rel)
        if from_id and to_id:
            edges.append((from_id, to_id))

            # Try to extract names if not provided
            if from_id not in id_to_name:
                name = _extract_table_name(rel, "from")
                if name:
                    id_to_name[from_id] = name

            if to_id not in id_to_name:
                name = _extract_table_name(rel, "to")
                if name:
                    id_to_name[to_id] = name

    # Ensure all table_ids have names (use ID as fallback)
    for tid in table_ids:
        if tid not in id_to_name:
            id_to_name[tid] = tid

    # Build NetworkX graphs
    G: nx.Graph = nx.Graph()  # Undirected for structure analysis
    G_directed: nx.DiGraph = nx.DiGraph()  # Directed for cycle detection

    for table_id in table_ids:
        name = id_to_name.get(table_id, table_id)
        G.add_node(table_id, name=name)
        G_directed.add_node(table_id, name=name)

    for from_id, to_id in edges:
        # Only add edges between tables in our analysis scope
        if from_id in table_ids and to_id in table_ids:
            G.add_edge(from_id, to_id)
            G_directed.add_edge(from_id, to_id)

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

        # Classify role based on connection count
        if degree == 0:
            role = "isolated"
            isolated_tables.append(name)
        elif degree >= 3:
            role = "hub"
            hub_tables.append(name)
        elif degree == 1:
            role = "dimension"
            leaf_tables.append(name)
        else:  # degree == 2
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

    # Detect schema cycles in directed graph
    schema_cycles: list[SchemaCycle] = []
    try:
        for cycle_ids in nx.simple_cycles(G_directed):
            if len(cycle_ids) >= 2:  # Ignore self-loops
                cycle_names = [id_to_name.get(tid, tid) for tid in cycle_ids]
                schema_cycles.append(
                    SchemaCycle(
                        tables=cycle_names,
                        table_ids=list(cycle_ids),
                        length=len(cycle_ids),
                    )
                )
    except Exception:
        pass  # No cycles or error in cycle detection

    # Count connected components
    connected_components = nx.number_connected_components(G) if len(G) > 0 else 0

    # Classify overall pattern
    pattern, pattern_desc = _classify_graph_pattern(
        total_tables=len(table_ids),
        total_relationships=len(edges),
        hub_count=len(hub_tables),
        leaf_count=len(leaf_tables),
        isolated_count=len(isolated_tables),
        cycle_count=len(schema_cycles),
        component_count=connected_components,
    )

    return GraphStructure(
        tables=table_roles,
        hub_tables=hub_tables,
        leaf_tables=leaf_tables,
        bridge_tables=bridge_tables,
        isolated_tables=isolated_tables,
        pattern=pattern,
        pattern_description=pattern_desc,
        connected_components=connected_components,
        schema_cycles=schema_cycles,
        total_tables=len(table_ids),
        total_relationships=len(edges),
    )


def _extract_table_ids(
    rel: RelationshipCandidate | Relationship | dict[str, Any],
) -> tuple[str | None, str | None]:
    """Extract from/to table IDs from a relationship object.

    Handles multiple formats:
    - RelationshipCandidate: table1, table2 (names, used as IDs)
    - Relationship DB model: from_table_id, to_table_id
    - Dict: various key combinations

    Returns:
        Tuple of (from_table_id, to_table_id)
    """
    if isinstance(rel, dict):
        # Dict format - try various key combinations
        from_id = rel.get("from_table_id") or rel.get("table1") or rel.get("from_table")
        to_id = rel.get("to_table_id") or rel.get("table2") or rel.get("to_table")
        return from_id, to_id

    # Object format
    if hasattr(rel, "from_table_id"):
        # Relationship DB model
        return rel.from_table_id, rel.to_table_id  # type: ignore[union-attr]
    elif hasattr(rel, "table1"):
        # RelationshipCandidate (uses table names as identifiers)
        return rel.table1, rel.table2  # type: ignore[union-attr]

    return None, None


def _extract_table_name(
    rel: RelationshipCandidate | Relationship | dict[str, Any],
    side: str,  # "from" or "to"
) -> str | None:
    """Extract table name from a relationship object.

    Args:
        rel: Relationship object
        side: "from" or "to"

    Returns:
        Table name or None
    """
    if isinstance(rel, dict):
        if side == "from":
            return rel.get("from_table") or rel.get("table1")
        else:
            return rel.get("to_table") or rel.get("table2")

    if hasattr(rel, "table1"):
        # RelationshipCandidate uses table names directly
        return rel.table1 if side == "from" else rel.table2  # type: ignore[union-attr]

    return None


def _classify_graph_pattern(
    total_tables: int,
    total_relationships: int,
    hub_count: int,
    leaf_count: int,
    isolated_count: int,
    cycle_count: int,
    component_count: int,
) -> tuple[str, str]:
    """Classify the overall graph pattern.

    Args:
        total_tables: Total number of tables
        total_relationships: Total number of relationships
        hub_count: Number of hub tables (3+ connections)
        leaf_count: Number of leaf tables (1 connection)
        isolated_count: Number of isolated tables (0 connections)
        cycle_count: Number of schema cycles
        component_count: Number of connected components

    Returns:
        Tuple of (pattern_name, pattern_description)
    """
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
            f"Interconnected tables with {cycle_count} cycle(s) - may indicate circular references",
        )

    if cycle_count > 0:
        return (
            "cyclic",
            f"Tables form {cycle_count} cycle(s) - relationships loop back",
        )

    if hub_count == 0 and total_relationships == total_tables - 1:
        return "chain", "Tables connected in a linear chain"

    return "mesh", "Tables interconnected in a mesh pattern"


def format_graph_structure_for_context(structure: GraphStructure) -> str:
    """Format GraphStructure as readable text for LLM context.

    Args:
        structure: GraphStructure from analyze_graph_topology()

    Returns:
        Formatted string suitable for LLM prompt context
    """
    lines = []

    lines.append("## SCHEMA TOPOLOGY")
    lines.append("")
    lines.append(f"Pattern: {structure.pattern}")
    lines.append(f"Description: {structure.pattern_description}")
    lines.append("")

    lines.append(f"- Total tables: {structure.total_tables}")
    lines.append(f"- Total relationships: {structure.total_relationships}")
    lines.append(f"- Connected components: {structure.connected_components}")
    lines.append("")

    if structure.hub_tables:
        lines.append(f"Hub tables (central, 3+ connections): {', '.join(structure.hub_tables)}")

    if structure.leaf_tables:
        lines.append(f"Leaf tables (dimensions, 1 connection): {', '.join(structure.leaf_tables)}")

    if structure.bridge_tables:
        lines.append(f"Bridge tables (2 connections): {', '.join(structure.bridge_tables)}")

    if structure.isolated_tables:
        lines.append(f"Isolated tables (no relationships): {', '.join(structure.isolated_tables)}")

    if structure.schema_cycles:
        lines.append("")
        lines.append(f"Schema cycles detected: {len(structure.schema_cycles)}")
        for i, cycle in enumerate(structure.schema_cycles[:5], 1):  # Limit to 5
            lines.append(f"  {i}. {' → '.join(cycle.tables)} → {cycle.tables[0]}")

    lines.append("")
    lines.append("Table roles:")
    for table in structure.tables:
        lines.append(f"  - {table.table_name}: {table.role} ({table.connection_count} connections)")
        if table.connects_to:
            lines.append(f"    connects to: {', '.join(table.connects_to)}")

    return "\n".join(lines)


__all__ = [
    "TableRole",
    "SchemaCycle",
    "GraphStructure",
    "analyze_graph_topology",
    "format_graph_structure_for_context",
]
