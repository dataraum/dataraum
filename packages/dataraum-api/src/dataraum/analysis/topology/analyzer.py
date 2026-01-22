"""Main topological quality analyzer.

This module provides single-table topological analysis via TDA.
For cross-table schema analysis, see relationships/graph_topology.py.
"""

from typing import Any

import duckdb
import numpy as np
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.topology.extraction import (
    compute_persistent_entropy,
    detect_persistent_cycles,
    extract_betti_numbers,
    process_persistence_diagrams,
)
from dataraum.analysis.topology.models import (
    BettiNumbers,
    TopologicalAnomaly,
    TopologicalQualityResult,
)
from dataraum.analysis.topology.stability import (
    assess_homological_stability,
    compute_historical_complexity,
    get_previous_topology,
)
from dataraum.analysis.topology.tda.extractor import TableTopologyExtractor
from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.storage import Table

logger = get_logger(__name__)

# Module-level TDA extractor instance
_extractor = TableTopologyExtractor()


def analyze_topological_quality(
    table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: Session,
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
        result = session.execute(stmt)
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
        betti_result = extract_betti_numbers(np_diagrams)
        if not betti_result.success or betti_result.value is None:
            return Result.fail(f"Betti extraction failed: {betti_result.error}")

        betti_numbers = betti_result.value

        # Process persistence diagrams
        diagrams_result = process_persistence_diagrams(np_diagrams)
        persistence_diagrams = diagrams_result.value if diagrams_result.success else []

        # Compute persistent entropy
        persistent_entropy = compute_persistent_entropy(np_diagrams)

        # Detect cycles
        cycles_result = detect_persistent_cycles(np_diagrams, min_persistence)
        persistent_cycles = cycles_result.value if cycles_result.success else []

        # Get previous topology for stability assessment
        previous_diagrams = get_previous_topology(session, table_id)

        # Assess stability
        stability_result = assess_homological_stability(
            np_diagrams,
            previous_diagrams=previous_diagrams,
            threshold=stability_threshold,
        )
        stability = stability_result.value if stability_result.success else None

        # Compute historical complexity statistics
        current_complexity = betti_numbers.total_complexity
        history_result = compute_historical_complexity(session, table_id, current_complexity)
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
