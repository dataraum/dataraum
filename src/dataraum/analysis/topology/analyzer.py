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
from dataraum.analysis.topology.tda.extractor import TableTopologyExtractor
from dataraum.core.config import load_yaml_config
from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.storage import Table

logger = get_logger(__name__)

# Module-level TDA extractor instance
_extractor = TableTopologyExtractor()

_CONFIG_CACHE: dict[str, object] | None = None


def _load_config() -> dict[str, object]:
    """Load topology config from YAML (cached)."""
    global _CONFIG_CACHE
    if _CONFIG_CACHE is None:
        _CONFIG_CACHE = load_yaml_config("system/topology.yaml")
    return _CONFIG_CACHE


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
        # Load thresholds from config
        cfg = _load_config()
        ad = cfg.get("anomaly_detection", {})
        fragmentation_threshold = ad.get("fragmentation_threshold", 3)
        fragmentation_high_threshold = ad.get("fragmentation_high_threshold", 5)
        table_sample_limit = ad.get("table_sample_limit", 10000)

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
            df = duckdb_conn.execute(
                f"SELECT * FROM {actual_table} LIMIT {table_sample_limit}"
            ).df()
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

        current_complexity = betti_numbers.total_complexity

        # Detect anomalies
        anomalies = []

        # Check for fragmentation (too many disconnected components)
        if betti_numbers.betti_0 > fragmentation_threshold:
            anomalies.append(
                TopologicalAnomaly(
                    anomaly_type="fragmentation",
                    severity="high"
                    if betti_numbers.betti_0 > fragmentation_high_threshold
                    else "medium",
                    description=f"Data is fragmented into {betti_numbers.betti_0} disconnected components",
                    evidence={"betti_0": betti_numbers.betti_0},
                )
            )

        # Generate topology description
        complexity_low = ad.get("complexity_low", 2)
        complexity_moderate = ad.get("complexity_moderate", 5)
        topology_description = _generate_topology_description(
            betti_numbers,
            persistent_cycles or [],
            anomalies,
            complexity_low,
            complexity_moderate,
        )

        # Build result
        topo_result = TopologicalQualityResult(
            table_id=table_id,
            table_name=table_name,
            betti_numbers=betti_numbers,
            persistence_diagrams=persistence_diagrams or [],
            persistent_cycles=persistent_cycles,
            structural_complexity=current_complexity,
            persistent_entropy=persistent_entropy,
            orphaned_components=betti_numbers.betti_0 - 1 if betti_numbers.betti_0 > 1 else 0,
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
    complexity_low: int = 2,
    complexity_moderate: int = 5,
) -> str:
    """Generate a human-readable topology description.

    Args:
        betti_numbers: Computed Betti numbers
        cycles: Detected persistent cycles
        anomalies: Detected anomalies
        complexity_low: Threshold for low complexity
        complexity_moderate: Threshold for moderate complexity

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
    if complexity <= complexity_low:
        parts.append("Low structural complexity.")
    elif complexity <= complexity_moderate:
        parts.append("Moderate structural complexity.")
    else:
        parts.append("High structural complexity.")

    # Anomalies
    if anomalies:
        anomaly_types = [a.anomaly_type for a in anomalies]
        parts.append(f"Anomalies detected: {', '.join(anomaly_types)}.")

    return " ".join(parts)
