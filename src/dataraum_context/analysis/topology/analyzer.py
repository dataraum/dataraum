"""Main topological quality analyzer.

This module provides the main entry points for topological analysis:
- analyze_topological_quality: Single table analysis
- analyze_topological_quality_multi_table: Multi-table analysis
"""

import logging
from datetime import UTC, datetime
from typing import Any

import duckdb
import numpy as np
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.analysis.topology.extraction import (
    compute_persistent_entropy,
    detect_persistent_cycles,
    extract_betti_numbers,
    process_persistence_diagrams,
)
from dataraum_context.analysis.topology.stability import (
    assess_homological_stability,
    compute_historical_complexity,
    get_previous_topology,
)
from dataraum_context.analysis.topology.tda.extractor import TableTopologyExtractor
from dataraum_context.core.models.base import Result
from dataraum_context.enrichment.db_models import Relationship as RelationshipModel
from dataraum_context.quality.models import (
    BettiNumbers,
    TopologicalAnomaly,
    TopologicalQualityResult,
)
from dataraum_context.storage import Table

logger = logging.getLogger(__name__)

# Module-level TDA extractor instance
_extractor = TableTopologyExtractor()


async def analyze_topological_quality(
    table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    min_persistence: float = 0.1,
    stability_threshold: float = 0.1,
) -> Result[TopologicalQualityResult]:
    """Analyze topological quality for a single table.

    Computes:
    - Betti numbers (components, cycles, voids)
    - Persistence diagrams
    - Persistent entropy
    - Cycle detection
    - Stability assessment (vs. previous analysis)

    Args:
        table_id: Table to analyze
        duckdb_conn: DuckDB connection
        session: Database session
        min_persistence: Minimum persistence for cycle detection
        stability_threshold: Threshold for stability assessment

    Returns:
        Result containing TopologicalQualityResult
    """
    try:
        # Get table info
        stmt = select(Table.table_name, Table.layer, Table.duckdb_path).where(
            Table.table_id == table_id
        )
        result = await session.execute(stmt)
        row = result.fetchone()

        if row is None:
            return Result.fail(f"Table not found: {table_id}")

        table_name, layer, duckdb_path = row
        # Use duckdb_path if available, otherwise construct from layer prefix
        actual_table = duckdb_path or (
            f"typed_{table_name}" if layer == "typed" else f"raw_{table_name}"
        )

        # Load data
        try:
            df = duckdb_conn.execute(f"SELECT * FROM {actual_table} LIMIT 10000").df()
        except Exception as e:
            return Result.fail(f"Failed to load table data: {e}")

        if df.empty:
            return Result.fail("Table is empty")

        # Extract topology using TDA
        topology = _extractor.extract_topology(df)
        persistence = topology.get("global_persistence", {})
        diagrams = persistence.get("diagrams", [])

        if not diagrams:
            return Result.fail("TDA extraction returned no persistence diagrams")

        # Convert diagrams to numpy arrays if needed
        np_diagrams = [
            np.array(dgm) if not isinstance(dgm, np.ndarray) else dgm for dgm in diagrams
        ]

        # Extract Betti numbers
        betti_result = await extract_betti_numbers(np_diagrams)
        if not betti_result.success or betti_result.value is None:
            return Result.fail(f"Betti extraction failed: {betti_result.error}")

        betti_numbers = betti_result.value

        # Process persistence diagrams
        diagrams_result = await process_persistence_diagrams(np_diagrams)
        persistence_diagrams = diagrams_result.value if diagrams_result.success else []

        # Compute persistent entropy
        persistent_entropy = compute_persistent_entropy(np_diagrams)

        # Detect cycles
        cycles_result = await detect_persistent_cycles(np_diagrams, min_persistence)
        persistent_cycles = cycles_result.value if cycles_result.success else []

        # Get previous topology for stability assessment
        previous_diagrams = await get_previous_topology(session, table_id)

        # Assess stability
        stability_result = await assess_homological_stability(
            np_diagrams,
            previous_diagrams=previous_diagrams,
            threshold=stability_threshold,
        )
        stability = stability_result.value if stability_result.success else None

        # Compute historical complexity statistics
        current_complexity = betti_numbers.total_complexity
        history_result = await compute_historical_complexity(session, table_id, current_complexity)
        history: dict[str, Any] = (
            history_result.value if (history_result.success and history_result.value) else {}
        )

        # Detect anomalies
        anomalies = []

        # Check for fragmentation (too many disconnected components)
        if betti_numbers.betti_0 > 3:
            anomalies.append(
                TopologicalAnomaly(
                    anomaly_type="fragmentation",
                    severity="high" if betti_numbers.betti_0 > 5 else "medium",
                    description=f"Data is fragmented into {betti_numbers.betti_0} disconnected components",
                    evidence={"betti_0": betti_numbers.betti_0},
                )
            )

        # Check for complexity spike
        z_score = history.get("z_score")
        if z_score is not None and abs(z_score) > 2:
            anomalies.append(
                TopologicalAnomaly(
                    anomaly_type="complexity_spike",
                    severity="high" if abs(z_score) > 3 else "medium",
                    description=f"Complexity is {abs(z_score):.1f} standard deviations from historical mean",
                    evidence={
                        "z_score": z_score,
                        "mean": history.get("mean"),
                        "std": history.get("std"),
                    },
                )
            )

        # Generate topology description
        topology_description = _generate_topology_description(
            betti_numbers, persistent_cycles or [], anomalies
        )

        # Build result
        topo_result = TopologicalQualityResult(
            table_id=table_id,
            table_name=table_name,
            betti_numbers=betti_numbers,
            persistence_diagrams=persistence_diagrams or [],
            persistent_cycles=persistent_cycles,
            stability=stability,
            structural_complexity=current_complexity,
            persistent_entropy=persistent_entropy,
            orphaned_components=betti_numbers.betti_0 - 1 if betti_numbers.betti_0 > 1 else 0,
            complexity_trend=history.get("trend"),
            complexity_within_bounds=history.get("within_bounds", True),
            complexity_mean=history.get("mean"),
            complexity_std=history.get("std"),
            complexity_z_score=z_score,
            has_anomalies=len(anomalies) > 0,
            anomalies=anomalies,
            topology_description=topology_description,
        )

        return Result.ok(topo_result)

    except Exception as e:
        logger.exception("Topological analysis failed")
        return Result.fail(f"Topological analysis failed: {e}")


async def analyze_topological_quality_multi_table(
    table_ids: list[str],
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    min_persistence: float = 0.1,
    stability_threshold: float = 0.1,
) -> Result[dict[str, Any]]:
    """Analyze topological quality across multiple tables.

    In addition to single-table analysis, this computes:
    - Cross-table relationships
    - Graph-level Betti numbers
    - Cross-table cycles (business processes)

    Args:
        table_ids: Tables to analyze
        duckdb_conn: DuckDB connection
        session: Database session
        min_persistence: Minimum persistence for cycle detection
        stability_threshold: Threshold for stability assessment

    Returns:
        Result containing multi-table topology analysis
    """
    try:
        if not table_ids:
            return Result.fail("No table IDs provided")

        # Analyze each table individually
        per_table_results: dict[str, TopologicalQualityResult] = {}

        for table_id in table_ids:
            result = await analyze_topological_quality(
                table_id,
                duckdb_conn,
                session,
                min_persistence=min_persistence,
                stability_threshold=stability_threshold,
            )
            if result.success and result.value:
                per_table_results[table_id] = result.value

        if not per_table_results:
            return Result.fail("No tables could be analyzed")

        # Load relationships between tables
        relationships = await _load_table_relationships(session, table_ids)

        # Compute graph-level metrics
        graph_betti_0 = _compute_graph_connectivity(table_ids, relationships)
        cross_table_cycles = _detect_cross_table_cycles(relationships)

        # Aggregate anomalies
        all_anomalies = []
        for table_result in per_table_results.values():
            all_anomalies.extend(table_result.anomalies)

        # Check for disconnected graph
        if graph_betti_0 > 1:
            all_anomalies.append(
                TopologicalAnomaly(
                    anomaly_type="disconnected_graph",
                    severity="high" if graph_betti_0 > 3 else "medium",
                    description=f"Table graph has {graph_betti_0} disconnected components",
                    evidence={"graph_betti_0": graph_betti_0},
                    affected_tables=table_ids,
                )
            )

        # Build analysis_data structure matching test expectations
        analysis_data = {
            "per_table": {tid: _serialize_topo_result(tr) for tid, tr in per_table_results.items()},
            "cross_table": {
                "betti_0": graph_betti_0,
                "cycles": cross_table_cycles,
                "is_connected": graph_betti_0 == 1,
            },
            "relationship_count": len(relationships),
            "anomalies": [_serialize_anomaly(a) for a in all_anomalies],
            "has_anomalies": len(all_anomalies) > 0,
        }

        # Persist to database
        from dataraum_context.enrichment.db_models import MultiTableTopologyMetrics

        metrics = MultiTableTopologyMetrics(
            table_ids=table_ids,
            cross_table_cycles=len(cross_table_cycles),
            graph_betti_0=graph_betti_0,
            relationship_count=len(relationships),
            has_cross_table_cycles=len(cross_table_cycles) > 0,
            is_connected_graph=graph_betti_0 == 1,
            analysis_data=analysis_data,
        )
        session.add(metrics)
        await session.commit()

        # Build full result
        multi_table_result = {
            "computed_at": datetime.now(UTC).isoformat(),
            "table_ids": table_ids,
            **analysis_data,
        }

        return Result.ok(multi_table_result)

    except Exception as e:
        logger.exception("Multi-table topological analysis failed")
        return Result.fail(f"Multi-table analysis failed: {e}")


async def _load_table_relationships(
    session: AsyncSession,
    table_ids: list[str],
) -> list[dict[str, Any]]:
    """Load relationships involving the specified tables."""
    try:
        stmt = select(RelationshipModel).where(
            (RelationshipModel.from_table_id.in_(table_ids))
            | (RelationshipModel.to_table_id.in_(table_ids))
        )

        result = await session.execute(stmt)
        relationships = result.scalars().all()

        return [
            {
                "from_table_id": r.from_table_id,
                "to_table_id": r.to_table_id,
                "relationship_type": r.relationship_type,
                "confidence": r.confidence,
            }
            for r in relationships
        ]

    except Exception as e:
        logger.warning(f"Failed to load relationships: {e}")
        return []


def _compute_graph_connectivity(
    table_ids: list[str],
    relationships: list[dict[str, Any]],
) -> int:
    """Compute number of connected components in the table graph.

    Uses simple union-find algorithm.

    Args:
        table_ids: List of table IDs (nodes)
        relationships: List of relationships (edges)

    Returns:
        Number of connected components (graph Betti 0)
    """
    if not table_ids:
        return 0

    # Union-find data structure
    parent = {tid: tid for tid in table_ids}

    def find(x: str) -> str:
        if parent[x] != x:
            parent[x] = find(parent[x])
        return parent[x]

    def union(x: str, y: str) -> None:
        px, py = find(x), find(y)
        if px != py:
            parent[px] = py

    # Process relationships
    for rel in relationships:
        from_id = rel["from_table_id"]
        to_id = rel["to_table_id"]
        if from_id in parent and to_id in parent:
            union(from_id, to_id)

    # Count unique roots
    roots = {find(tid) for tid in table_ids}
    return len(roots)


def _detect_cross_table_cycles(
    relationships: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Detect cycles in the relationship graph.

    A cycle in the relationship graph may indicate:
    - Circular foreign keys
    - Business process flows (A → B → C → A)

    Args:
        relationships: List of table relationships

    Returns:
        List of detected cycles
    """
    # Build adjacency list
    graph: dict[str, list[str]] = {}
    for rel in relationships:
        from_id = rel["from_table_id"]
        to_id = rel["to_table_id"]

        if from_id not in graph:
            graph[from_id] = []
        if to_id not in graph:
            graph[to_id] = []

        graph[from_id].append(to_id)

    # DFS to find cycles
    cycles = []
    visited: set[str] = set()
    rec_stack: set[str] = set()

    def dfs(node: str, path: list[str]) -> None:
        visited.add(node)
        rec_stack.add(node)
        path.append(node)

        for neighbor in graph.get(node, []):
            if neighbor not in visited:
                dfs(neighbor, path)
            elif neighbor in rec_stack:
                # Found a cycle
                cycle_start = path.index(neighbor)
                cycle_nodes = path[cycle_start:]
                cycles.append(
                    {
                        "tables": cycle_nodes,
                        "length": len(cycle_nodes),
                    }
                )

        path.pop()
        rec_stack.remove(node)

    for node in graph:
        if node not in visited:
            dfs(node, [])

    return cycles


def _serialize_topo_result(result: TopologicalQualityResult) -> dict[str, Any]:
    """Serialize TopologicalQualityResult for storage/API."""
    return {
        "table_name": result.table_name,
        "betti_numbers": {
            "betti_0": result.betti_numbers.betti_0,
            "betti_1": result.betti_numbers.betti_1,
            "betti_2": result.betti_numbers.betti_2,
            "total_complexity": result.betti_numbers.total_complexity,
            "is_connected": result.betti_numbers.is_connected,
            "has_cycles": result.betti_numbers.has_cycles,
        },
        "structural_complexity": result.structural_complexity,
        "persistent_entropy": result.persistent_entropy,
        "orphaned_components": result.orphaned_components,
        "has_anomalies": result.has_anomalies,
        "cycle_count": len(result.persistent_cycles),
    }


def _serialize_anomaly(anomaly: TopologicalAnomaly) -> dict[str, Any]:
    """Serialize TopologicalAnomaly for storage/API."""
    return {
        "type": anomaly.anomaly_type,
        "severity": anomaly.severity,
        "description": anomaly.description,
        "evidence": anomaly.evidence,
        "affected_tables": anomaly.affected_tables,
        "affected_columns": anomaly.affected_columns,
    }


def _generate_topology_description(
    betti_numbers: BettiNumbers,
    cycles: list[Any],
    anomalies: list[TopologicalAnomaly],
) -> str:
    """Generate a human-readable topology description.

    Args:
        betti_numbers: Computed Betti numbers
        cycles: Detected persistent cycles
        anomalies: Detected anomalies

    Returns:
        Natural language description of the topology
    """
    parts = []

    # Connectivity
    if betti_numbers.is_connected:
        parts.append("Data is fully connected (single component)")
    else:
        parts.append(f"Data has {betti_numbers.betti_0} disconnected components")

    # Cycles
    if betti_numbers.has_cycles:
        parts.append(f"with {betti_numbers.betti_1} topological cycle(s)")
    else:
        parts.append("with no topological cycles (tree-like structure)")

    # Complexity assessment
    complexity = betti_numbers.total_complexity
    if complexity <= 2:
        parts.append("Low structural complexity.")
    elif complexity <= 5:
        parts.append("Moderate structural complexity.")
    else:
        parts.append("High structural complexity.")

    # Anomalies
    if anomalies:
        anomaly_types = [a.anomaly_type for a in anomalies]
        parts.append(f"Anomalies detected: {', '.join(anomaly_types)}.")

    return " ".join(parts)
