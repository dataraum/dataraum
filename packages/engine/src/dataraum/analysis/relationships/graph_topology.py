"""Graph topology analysis for relationship structures.

Analyzes the graph structure of table relationships to:
- Classify tables by role (hub, dimension, bridge, isolated)
- Detect overall graph patterns (star_schema, mesh, etc.)
- Find circular reference groups (strongly connected components)

This analysis runs after relationship detection and provides context
for semantic and cycles agents.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

import networkx as nx
from pydantic import BaseModel, Field

from dataraum.core.logging import get_logger

logger = get_logger(__name__)

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


class CyclicGroup(BaseModel):
    """A set of tables that reference each other circularly (schema-level).

    A **strongly connected component** of size >= 2: every table in the group is
    reachable from every other by following references. This is the complete,
    non-redundant answer to "where are the circular references" — distinct from
    business process cycles, which the cycles agent detects.

    Replaces per-cycle enumeration (DAT-834). Listing individual cycles is
    listing the *elements* of a set that grows super-exponentially with density:
    nine mutually-referencing tables produced 53,902 of them, all naming the same
    nine tables. The component names those tables once. There are at most
    ``V // 2`` non-trivial components, by construction rather than by a cap, and
    Tarjan finds them in O(V + E).
    """

    tables: list[str] = Field(default_factory=list)
    """Table names in the component (sorted — the group is a set, not a path)."""

    table_ids: list[str] = Field(default_factory=list)
    """Table IDs in the component (sorted, aligned with ``tables``)."""

    size: int = 0
    """Number of tables that reference each other circularly."""


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

    cyclic_groups: list[CyclicGroup] = Field(default_factory=list)
    """Table sets that reference each other circularly (non-trivial SCCs)."""

    circuit_rank: int = 0
    """Independent cycles in the graph: ``mu = E - V + C`` (the cycle space's
    dimension / first Betti number).

    The magnitude of join-path ambiguity, exactly: a spanning forest needs
    ``V - C`` edges, so every additional edge closes one independent cycle. Every
    cycle in the graph is a combination of ``mu`` basis cycles, which is why
    enumerating them all is enumerating a vector space instead of describing it.
    ``mu = 0`` means a forest — exactly one join path between any two tables."""

    density: float = 0.0
    """Undirected edge count over ``V*(V-1)/2``. ``1.0`` = complete graph.

    Reported, never thresholded. It is how a reader can tell a schema from noise:
    at density 1.0 every table is connected to every other, so roles, patterns and
    cycles are all vacuous — the topology carries no information regardless of how
    much of it there is."""

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
            # A self-referential FK (a within-table hierarchy, e.g.
            # chart_of_accounts.parent_id -> account_id, now a routine Layer-A
            # candidate — DAT-763) is NOT an inter-table structural edge. A
            # NetworkX self-loop double-counts degree (+2) and lists a table as
            # its own neighbor, so it would misclassify hub/bridge/dimension roles
            # and corrupt the ContextDocument. Topology measures inter-table
            # connectivity; the self-FK still lives in the relationship catalog.
            # (SCC detection below is unaffected: a self-loop makes a size-1 component,
            # which the size >= 2 filter drops.)
            if from_id == to_id:
                continue
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

    # Circular references = the non-trivial strongly connected components.
    #
    # This used to enumerate `nx.simple_cycles(G_directed)`. The number of simple
    # cycles is super-exponential in density, and every one of them was rendered
    # into the semantic + cycles prompts: nine mutually-referencing tables emitted
    # 53,902 cycles and a 1.9M-token prompt, twice the context window (DAT-834).
    # Those 53,902 cycles named the same nine tables over and over — one strongly
    # connected component. Tarjan finds it in O(V + E) and says the same thing
    # once. Not a truncation of the old answer; the whole of it, deduplicated.
    cyclic_groups: list[CyclicGroup] = []
    for component in nx.strongly_connected_components(G_directed):
        if len(component) < 2:  # a single node is only "cyclic" via a self-loop
            continue
        ids = sorted(component)
        cyclic_groups.append(
            CyclicGroup(
                tables=sorted(id_to_name.get(tid, tid) for tid in ids),
                table_ids=ids,
                size=len(ids),
            )
        )
    cyclic_groups.sort(key=lambda g: (-g.size, g.tables))

    # Count connected components
    connected_components = nx.number_connected_components(G) if len(G) > 0 else 0

    # The cycle space, described rather than enumerated: mu = E - V + C is its
    # dimension, so it counts the join paths beyond a spanning forest. Uses the
    # UNDIRECTED edge count — join ambiguity does not care which way an FK points.
    n_nodes = G.number_of_nodes()
    n_undirected = G.number_of_edges()
    circuit_rank = max(0, n_undirected - n_nodes + connected_components)
    max_edges = n_nodes * (n_nodes - 1) // 2
    density = (n_undirected / max_edges) if max_edges else 0.0

    # Classify overall pattern
    pattern, pattern_desc = _classify_graph_pattern(
        total_tables=len(table_ids),
        total_relationships=len(edges),
        hub_count=len(hub_tables),
        leaf_count=len(leaf_tables),
        isolated_count=len(isolated_tables),
        cyclic_group_count=len(cyclic_groups),
        component_count=connected_components,
        density=density,
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
        cyclic_groups=cyclic_groups,
        circuit_rank=circuit_rank,
        density=density,
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
    cyclic_group_count: int,
    component_count: int,
    density: float,
) -> tuple[str, str]:
    """Classify the overall graph pattern.

    Args:
        total_tables: Total number of tables
        total_relationships: Total number of relationships
        hub_count: Number of hub tables (3+ connections)
        leaf_count: Number of leaf tables (1 connection)
        isolated_count: Number of isolated tables (0 connections)
        cyclic_group_count: Number of circularly-referencing table groups
        component_count: Number of connected components
        density: Undirected edges over the maximum possible (1.0 = complete)

    Returns:
        Tuple of (pattern_name, pattern_description)
    """
    if total_tables == 0:
        return "empty", "No tables in dataset"

    if total_tables == 1:
        return "single_table", "Single table dataset - no relationships possible"

    if total_relationships == 0:
        return "disconnected", "Tables exist but no relationships detected"

    # Checked before every structural pattern below, because it invalidates them
    # all: if every table is connected to every other, then every table is a hub,
    # every table pair is a cycle, and "star" / "mesh" / "chain" describe nothing.
    # The exact mathematical condition (every pair present), not a tuned cutoff —
    # a schema is either complete or it is not.
    if density >= 1.0:
        return (
            "complete",
            f"Every one of the {total_tables * (total_tables - 1) // 2} table pairs is "
            "connected. A complete graph carries no topological information — treat "
            "these relationships as unfiltered candidates, not as structure",
        )

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
    if hub_count == 1 and leaf_count >= 2 and cyclic_group_count == 0:
        return (
            "star_schema",
            f"Classic star schema: 1 central hub table connected to {leaf_count} dimension tables",
        )

    if hub_count >= 1 and leaf_count >= 1 and cyclic_group_count == 0:
        return (
            "hub_and_spoke",
            f"{hub_count} hub table(s) connecting to {leaf_count} dimension table(s)",
        )

    if cyclic_group_count > 0 and hub_count >= 1:
        return (
            "mesh_with_cycles",
            f"Interconnected tables, with {cyclic_group_count} group(s) of tables that "
            "reference each other circularly",
        )

    if cyclic_group_count > 0:
        return (
            "cyclic",
            f"{cyclic_group_count} group(s) of tables reference each other circularly",
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
    lines.append(f"- Graph density: {structure.density:.2f} (1.00 = every table pair connected)")
    lines.append(f"- Independent join paths beyond a tree (circuit rank): {structure.circuit_rank}")
    lines.append("")

    # On a complete graph the role/cycle sections below describe an artifact of
    # candidate generation, not a schema. Say so instead of listing it (DAT-834).
    if structure.density >= 1.0:
        lines.append(
            "NOTE: the graph is complete — every table pair is connected, so the roles "
            "and cyclic groups below are consequences of that, not evidence about the "
            "schema. Judge each relationship on its own merits."
        )
        lines.append("")

    if structure.hub_tables:
        lines.append(f"Hub tables (central, 3+ connections): {', '.join(structure.hub_tables)}")

    if structure.leaf_tables:
        lines.append(f"Leaf tables (dimensions, 1 connection): {', '.join(structure.leaf_tables)}")

    if structure.bridge_tables:
        lines.append(f"Bridge tables (2 connections): {', '.join(structure.bridge_tables)}")

    if structure.isolated_tables:
        lines.append(f"Isolated tables (no relationships): {', '.join(structure.isolated_tables)}")

    if structure.cyclic_groups:
        lines.append("")
        lines.append(f"Circular reference groups: {len(structure.cyclic_groups)}")
        for i, group in enumerate(structure.cyclic_groups, 1):
            lines.append(
                f"  {i}. {group.size} tables reference each other circularly: "
                f"{', '.join(group.tables)}"
            )

    lines.append("")
    lines.append("Table roles:")
    for table in structure.tables:
        lines.append(f"  - {table.table_name}: {table.role} ({table.connection_count} connections)")
        if table.connects_to:
            lines.append(f"    connects to: {', '.join(table.connects_to)}")

    return "\n".join(lines)


__all__ = [
    "TableRole",
    "CyclicGroup",
    "GraphStructure",
    "analyze_graph_topology",
    "format_graph_structure_for_context",
]
