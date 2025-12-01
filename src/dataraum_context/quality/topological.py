"""Topological Quality Analysis (Pillar 2).

This module extracts quality metrics from topological data analysis (TDA).
It computes Betti numbers, persistence diagrams, stability metrics, and detects
structural anomalies.

Key concepts:
- Betti numbers: Topological invariants (components, cycles, voids)
- Persistence diagrams: Birth/death of topological features
- Homological stability: How topology changes over time
- Structural complexity: Sum of Betti numbers and entropy
"""

from datetime import UTC, datetime
from uuid import uuid4

import duckdb
import numpy as np

# TDA libraries (required dependencies)
from persim import bottleneck
from scipy.stats import entropy as scipy_entropy
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models.base import Result
from dataraum_context.core.models.topological import (
    BettiNumbers,
    HomologicalStability,
    PersistenceDiagram,
    PersistencePoint,
    PersistentCycleResult,
    StructuralComplexity,
    TopologicalAnomaly,
    TopologicalQualityResult,
)
from dataraum_context.storage.models_v2.core import Table
from dataraum_context.storage.models_v2.topological_context import (
    PersistentCycle as DBPersistentCycle,
)
from dataraum_context.storage.models_v2.topological_context import (
    StructuralComplexityHistory as DBComplexityHistory,
)
from dataraum_context.storage.models_v2.topological_context import (
    TopologicalQualityMetrics as DBTopologicalMetrics,
)

# ============================================================================
# Betti Number Extraction
# ============================================================================


async def extract_betti_numbers(
    persistence_diagrams: list[np.ndarray],
) -> Result[BettiNumbers]:
    """Extract Betti numbers from persistence diagrams.

    Betti numbers are topological invariants:
    - β₀: Number of connected components
    - β₁: Number of cycles (holes)
    - β₂: Number of voids (cavities)

    Args:
        persistence_diagrams: List of persistence diagrams from ripser

    Returns:
        Result containing BettiNumbers
    """
    try:
        if not persistence_diagrams:
            return Result.fail("No persistence diagrams provided")

        # Extract Betti numbers by counting features in each dimension
        # Note: We count features with finite persistence (death < inf)
        betti_0 = 0
        betti_1 = 0
        betti_2 = None

        if len(persistence_diagrams) > 0:
            # Dimension 0: connected components
            dgm_0 = persistence_diagrams[0]
            # Count finite persistence features
            finite_mask = dgm_0[:, 1] < np.inf
            betti_0 = int(np.sum(finite_mask))
            # Add 1 for the infinite component (the whole dataset)
            betti_0 += 1

        if len(persistence_diagrams) > 1:
            # Dimension 1: cycles
            dgm_1 = persistence_diagrams[1]
            finite_mask = dgm_1[:, 1] < np.inf
            betti_1 = int(np.sum(finite_mask))

        if len(persistence_diagrams) > 2:
            # Dimension 2: voids
            dgm_2 = persistence_diagrams[2]
            finite_mask = dgm_2[:, 1] < np.inf
            betti_2 = int(np.sum(finite_mask))

        total_complexity = betti_0 + betti_1 + (betti_2 or 0)

        betti_numbers = BettiNumbers(
            betti_0=betti_0,
            betti_1=betti_1,
            betti_2=betti_2,
            total_complexity=total_complexity,
            is_connected=betti_0 == 1,
            has_cycles=betti_1 > 0,
            has_voids=betti_2 is not None and betti_2 > 0,
        )

        return Result.ok(betti_numbers)

    except Exception as e:
        return Result.fail(f"Betti number extraction failed: {e}")


# ============================================================================
# Persistence Diagram Processing
# ============================================================================


async def process_persistence_diagrams(
    persistence_diagrams: list[np.ndarray],
) -> Result[list[PersistenceDiagram]]:
    """Convert raw persistence diagrams to structured format.

    Args:
        persistence_diagrams: Raw diagrams from ripser

    Returns:
        Result containing list of PersistenceDiagram objects
    """
    try:
        if not persistence_diagrams:
            return Result.ok([])

        diagrams = []

        for dimension, dgm in enumerate(persistence_diagrams):
            if len(dgm) == 0:
                continue

            # Filter out infinite persistence for statistics
            finite_mask = dgm[:, 1] < np.inf
            finite_dgm = dgm[finite_mask]

            # Create persistence points
            points = []
            for birth, death in dgm:
                if death < np.inf:  # Only include finite features
                    persistence = float(death - birth)
                    points.append(
                        PersistencePoint(
                            dimension=dimension,
                            birth=float(birth),
                            death=float(death),
                            persistence=persistence,
                        )
                    )

            if not points:
                continue

            # Calculate statistics
            max_persistence = max(p.persistence for p in points)

            diagram = PersistenceDiagram(
                dimension=dimension,
                points=points,
                max_persistence=max_persistence,
                num_features=len(points),
            )

            diagrams.append(diagram)

        return Result.ok(diagrams)

    except Exception as e:
        return Result.fail(f"Persistence diagram processing failed: {e}")


# ============================================================================
# Persistent Entropy (Complexity Measure)
# ============================================================================


def compute_persistent_entropy(persistence_diagrams: list[np.ndarray]) -> float:
    """Compute persistent entropy as a measure of topological complexity.

    Persistent entropy quantifies the distribution of lifetimes in the
    persistence diagram. Higher entropy = more complex topology.

    Args:
        persistence_diagrams: Raw diagrams from ripser

    Returns:
        Persistent entropy value
    """
    try:
        all_lifetimes = []

        for dgm in persistence_diagrams:
            if len(dgm) == 0:
                continue

            # Get finite lifetimes
            finite_mask = dgm[:, 1] < np.inf
            births = dgm[finite_mask, 0]
            deaths = dgm[finite_mask, 1]
            lifetimes = deaths - births

            all_lifetimes.extend(lifetimes)

        if not all_lifetimes:
            return 0.0

        # Normalize lifetimes to probabilities
        lifetimes = np.array(all_lifetimes)
        total = np.sum(lifetimes)

        if total == 0:
            return 0.0

        probabilities = lifetimes / total

        # Compute Shannon entropy
        return float(scipy_entropy(probabilities))

    except Exception:
        return 0.0


# ============================================================================
# Detect Persistent Cycles
# ============================================================================


async def detect_persistent_cycles(
    persistence_diagrams: list[np.ndarray],
    metric_id: str,
    min_persistence: float = 0.1,
) -> Result[list[PersistentCycleResult]]:
    """Detect significant persistent cycles (dimension 1 features).

    Cycles represent circular relationships or flows in the data.

    Args:
        persistence_diagrams: Raw diagrams from ripser
        metric_id: ID of parent metric
        min_persistence: Minimum persistence to consider significant

    Returns:
        Result containing list of detected cycles
    """
    try:
        cycles = []

        if len(persistence_diagrams) < 2:
            return Result.ok([])

        # Get dimension 1 persistence diagram (cycles)
        dgm_1 = persistence_diagrams[1]

        if len(dgm_1) == 0:
            return Result.ok([])

        # Filter for finite, significant cycles
        finite_mask = dgm_1[:, 1] < np.inf
        finite_dgm = dgm_1[finite_mask]

        for birth, death in finite_dgm:
            persistence = death - birth

            if persistence < min_persistence:
                continue

            now = datetime.now(UTC)

            cycle = PersistentCycleResult(
                cycle_id=str(uuid4()),
                dimension=1,
                birth=float(birth),
                death=float(death),
                persistence=float(persistence),
                involved_columns=[],  # Would need additional analysis
                cycle_type=None,  # Would need domain knowledge
                is_anomalous=False,
                anomaly_reason=None,
                first_detected=now,
                last_seen=now,
            )

            cycles.append(cycle)

        return Result.ok(cycles)

    except Exception as e:
        return Result.fail(f"Cycle detection failed: {e}")


# ============================================================================
# Homological Stability Assessment
# ============================================================================


async def assess_homological_stability(
    current_diagrams: list[np.ndarray],
    table_id: str,
    session: AsyncSession,
    stability_threshold: float = 0.2,
) -> Result[HomologicalStability | None]:
    """Assess homological stability by comparing with previous period.

    Uses bottleneck distance to measure how much the topology has changed.
    Lower distance = more stable topology.

    Args:
        current_diagrams: Current persistence diagrams
        table_id: Table being analyzed
        session: Database session
        stability_threshold: Threshold for stability (default: 0.2)

    Returns:
        Result containing HomologicalStability or None if no previous data
    """
    try:
        # Get most recent previous metric
        stmt = (
            select(DBTopologicalMetrics)
            .where(DBTopologicalMetrics.table_id == table_id)
            .order_by(DBTopologicalMetrics.computed_at.desc())
            .limit(1)
        )
        result = await session.execute(stmt)
        previous_metric = result.scalar_one_or_none()

        if not previous_metric or not previous_metric.persistence_diagrams:
            return Result.ok(None)  # No previous data to compare

        # Extract previous diagrams from JSON
        prev_diagrams_data = previous_metric.persistence_diagrams.get("diagrams", [])
        if not prev_diagrams_data:
            return Result.ok(None)

        # Reconstruct numpy arrays from stored data
        prev_diagrams = []
        for dgm_data in prev_diagrams_data:
            points = dgm_data.get("points", [])
            if points:
                dgm_array = np.array([[p["birth"], p["death"]] for p in points])
                prev_diagrams.append(dgm_array)

        if not prev_diagrams or not current_diagrams:
            return Result.ok(None)

        # Compute bottleneck distance for each dimension
        bottleneck_distances = {}
        max_distance = 0.0

        for dim in range(min(len(current_diagrams), len(prev_diagrams))):
            curr_dgm = current_diagrams[dim]
            prev_dgm = prev_diagrams[dim]

            # Filter out infinite persistence for distance computation
            curr_finite = curr_dgm[curr_dgm[:, 1] < np.inf]
            prev_finite = prev_dgm[prev_dgm[:, 1] < np.inf]

            if len(curr_finite) > 0 and len(prev_finite) > 0:
                distance = bottleneck(curr_finite, prev_finite)
                bottleneck_distances[f"H{dim}"] = float(distance)
                max_distance = max(max_distance, distance)

        if not bottleneck_distances:
            return Result.ok(None)

        # Determine stability
        is_stable = max_distance <= stability_threshold

        # Determine stability level
        if max_distance <= stability_threshold:
            stability_level = "stable"
        elif max_distance <= stability_threshold * 2:
            stability_level = "minor_changes"
        elif max_distance <= stability_threshold * 3:
            stability_level = "significant_changes"
        else:
            stability_level = "unstable"

        # Compare Betti numbers to count changes
        curr_betti = await extract_betti_numbers(current_diagrams)
        if not curr_betti.success:
            return Result.ok(None)

        prev_betti_result = await extract_betti_numbers(prev_diagrams)
        if not prev_betti_result.success:
            return Result.ok(None)

        curr_b = curr_betti.value
        prev_b = prev_betti_result.value

        components_added = max(0, curr_b.betti_0 - prev_b.betti_0)
        components_removed = max(0, prev_b.betti_0 - curr_b.betti_0)
        cycles_added = max(0, curr_b.betti_1 - prev_b.betti_1)
        cycles_removed = max(0, prev_b.betti_1 - curr_b.betti_1)

        stability = HomologicalStability(
            bottleneck_distance=max_distance,
            is_stable=is_stable,
            threshold=stability_threshold,
            components_added=components_added,
            components_removed=components_removed,
            cycles_added=cycles_added,
            cycles_removed=cycles_removed,
            stability_level=stability_level,
        )

        return Result.ok(stability)

    except Exception:
        # Don't fail the whole analysis if stability computation fails
        return Result.ok(None)


# ============================================================================
# Structural Complexity Assessment
# ============================================================================


async def assess_structural_complexity(
    betti_numbers: BettiNumbers,
    persistent_entropy: float,
    table_id: str,
    session: AsyncSession,
) -> Result[StructuralComplexity]:
    """Assess structural complexity with historical context.

    Computes complexity metrics and compares against historical baselines.

    Args:
        betti_numbers: Current Betti numbers
        persistent_entropy: Current persistent entropy
        table_id: Table being analyzed
        session: Database session

    Returns:
        Result containing StructuralComplexity assessment
    """
    try:
        # Get historical complexity data
        stmt = (
            select(DBComplexityHistory)
            .where(DBComplexityHistory.table_id == table_id)
            .order_by(DBComplexityHistory.measured_at.desc())
            .limit(30)  # Last 30 measurements
        )
        result = await session.execute(stmt)
        history = result.scalars().all()

        # Calculate historical statistics
        complexity_mean = None
        complexity_std = None
        complexity_z_score = None
        within_bounds = True
        complexity_trend = None

        if len(history) >= 3:
            historical_values = [h.total_complexity for h in history]
            complexity_mean = float(np.mean(historical_values))
            complexity_std = float(np.std(historical_values))

            if complexity_std > 0:
                complexity_z_score = (
                    betti_numbers.total_complexity - complexity_mean
                ) / complexity_std
                within_bounds = abs(complexity_z_score) <= 2.0  # Within 2σ

            # Detect trend (simple approach: compare first half vs second half)
            if len(historical_values) >= 6:
                mid = len(historical_values) // 2
                first_half_mean = np.mean(historical_values[:mid])
                second_half_mean = np.mean(historical_values[mid:])

                if second_half_mean > first_half_mean * 1.1:
                    complexity_trend = "increasing"
                elif second_half_mean < first_half_mean * 0.9:
                    complexity_trend = "decreasing"
                else:
                    complexity_trend = "stable"

        complexity = StructuralComplexity(
            total_complexity=betti_numbers.total_complexity,
            betti_numbers=betti_numbers,
            persistent_entropy=persistent_entropy,
            complexity_mean=complexity_mean,
            complexity_std=complexity_std,
            complexity_z_score=complexity_z_score,
            complexity_trend=complexity_trend,
            within_bounds=within_bounds,
        )

        return Result.ok(complexity)

    except Exception as e:
        return Result.fail(f"Complexity assessment failed: {e}")


# ============================================================================
# Main Analysis Function
# ============================================================================


async def analyze_topological_quality(
    table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    max_dimension: int = 2,
    min_persistence: float = 0.1,
) -> Result[TopologicalQualityResult]:
    """Analyze topological quality for a table.

    This is the main entry point for topological quality analysis.

    Args:
        table_id: Table to analyze
        duckdb_conn: DuckDB connection
        session: Database session
        max_dimension: Maximum homology dimension to compute
        min_persistence: Minimum persistence for significant features

    Returns:
        Result containing complete topological quality assessment
    """

    try:
        # Get table info
        stmt = select(Table).where(Table.table_id == table_id)
        result = await session.execute(stmt)
        table = result.scalar_one_or_none()

        if not table:
            return Result.fail(f"Table {table_id} not found")

        # Load table data from DuckDB

        table_name = table.duckdb_path
        query = f"SELECT * FROM {table_name} LIMIT 10000"  # Limit for performance
        df = duckdb_conn.execute(query).fetchdf()

        if df.empty:
            return Result.fail("Table is empty")

        # Use existing topology extractor
        from dataraum_context.enrichment.tda import TableTopologyExtractor

        extractor = TableTopologyExtractor(max_dimension=max_dimension)
        topology_result = extractor.extract_topology(df)

        # Extract persistence diagrams
        persistence = topology_result.get("global_persistence", {})
        diagrams = persistence.get("diagrams", [])

        if not diagrams:
            return Result.fail("No persistence diagrams computed")

        # Extract Betti numbers
        betti_result = await extract_betti_numbers(diagrams)
        if not betti_result.success:
            return Result.fail(betti_result.error)
        betti_numbers = betti_result.value

        # Process persistence diagrams
        diagram_result = await process_persistence_diagrams(diagrams)
        if not diagram_result.success:
            return Result.fail(diagram_result.error)
        persistence_diagrams = diagram_result.value

        # Compute persistent entropy
        persistent_entropy = compute_persistent_entropy(diagrams)

        # Assess homological stability (compare with previous period)
        stability_result = await assess_homological_stability(
            diagrams, table_id, session, stability_threshold=0.2
        )
        if not stability_result.success:
            return Result.fail(stability_result.error)
        stability = stability_result.value

        # Detect cycles
        cycle_result = await detect_persistent_cycles(
            diagrams, metric_id="temp", min_persistence=min_persistence
        )
        if not cycle_result.success:
            return Result.fail(cycle_result.error)
        cycles = cycle_result.value

        # Assess complexity
        complexity_result = await assess_structural_complexity(
            betti_numbers, persistent_entropy, table_id, session
        )
        if not complexity_result.success:
            return Result.fail(complexity_result.error)
        complexity = complexity_result.value

        # Detect anomalies
        anomalies = []
        orphaned_components = max(0, betti_numbers.betti_0 - 1)  # Expect 1 component

        if orphaned_components > 0:
            anomalies.append(
                TopologicalAnomaly(
                    anomaly_type="orphaned_components",
                    severity="medium",
                    description=f"Found {orphaned_components} disconnected components",
                    evidence={"component_count": betti_numbers.betti_0},
                    affected_tables=[table_id],
                )
            )

        if not complexity.within_bounds:
            anomalies.append(
                TopologicalAnomaly(
                    anomaly_type="complexity_spike",
                    severity="high" if abs(complexity.complexity_z_score or 0) > 3 else "medium",
                    description=f"Structural complexity outside historical norms (z={complexity.complexity_z_score:.2f})",
                    evidence={"z_score": complexity.complexity_z_score},
                    affected_tables=[table_id],
                )
            )

        # Build topology description
        desc_parts = []
        if betti_numbers.betti_0 == 1:
            desc_parts.append("fully connected")
        else:
            desc_parts.append(f"{betti_numbers.betti_0} components")

        if betti_numbers.betti_1 > 0:
            desc_parts.append(f"{betti_numbers.betti_1} cycles")

        if betti_numbers.betti_2 and betti_numbers.betti_2 > 0:
            desc_parts.append(f"{betti_numbers.betti_2} voids")

        topology_description = ", ".join(desc_parts) if desc_parts else "trivial topology"

        # Quality warnings
        quality_warnings = [a.description for a in anomalies]

        # Calculate quality score (0-1)
        quality_score = 1.0
        if anomalies:
            quality_score -= len(anomalies) * 0.2
        if not complexity.within_bounds:
            quality_score -= 0.3
        quality_score = max(0.0, quality_score)

        # Create metric ID and store in database
        metric_id = str(uuid4())
        computed_at = datetime.now(UTC)

        # Store in database
        db_metric = DBTopologicalMetrics(
            metric_id=metric_id,
            table_id=table_id,
            computed_at=computed_at,
            betti_0=betti_numbers.betti_0,
            betti_1=betti_numbers.betti_1,
            betti_2=betti_numbers.betti_2,
            persistent_entropy=persistent_entropy,
            max_persistence_h0=(
                persistence_diagrams[0].max_persistence if persistence_diagrams else None
            ),
            max_persistence_h1=(
                persistence_diagrams[1].max_persistence if len(persistence_diagrams) > 1 else None
            ),
            persistence_diagrams={
                "diagrams": [
                    {
                        "dimension": d.dimension,
                        "points": [
                            {"birth": p.birth, "death": p.death, "persistence": p.persistence}
                            for p in d.points
                        ],
                    }
                    for d in persistence_diagrams
                ]
            },
            homologically_stable=stability.is_stable if stability else None,
            bottleneck_distance=stability.bottleneck_distance if stability else None,
            structural_complexity=complexity.total_complexity,
            complexity_trend=complexity.complexity_trend,
            complexity_within_bounds=complexity.within_bounds,
            anomalous_cycles={"cycles": [c.cycle_id for c in cycles if c.is_anomalous]},
            orphaned_components=orphaned_components,
            topology_description=topology_description,
            quality_warnings={"warnings": quality_warnings},
        )
        session.add(db_metric)

        # Store complexity history
        db_history = DBComplexityHistory(
            history_id=str(uuid4()),
            table_id=table_id,
            measured_at=computed_at,
            betti_0=betti_numbers.betti_0,
            betti_1=betti_numbers.betti_1,
            betti_2=betti_numbers.betti_2,
            total_complexity=complexity.total_complexity,
            complexity_mean=complexity.complexity_mean,
            complexity_std=complexity.complexity_std,
            complexity_z_score=complexity.complexity_z_score,
        )
        session.add(db_history)

        # Store persistent cycles
        for cycle in cycles:
            db_cycle = DBPersistentCycle(
                cycle_id=cycle.cycle_id,
                metric_id=metric_id,
                dimension=cycle.dimension,
                birth=cycle.birth,
                death=cycle.death,
                persistence=cycle.persistence,
                involved_columns={"column_ids": cycle.involved_columns},
                cycle_type=cycle.cycle_type,
                is_anomalous=cycle.is_anomalous,
                anomaly_reason=cycle.anomaly_reason,
                first_detected=cycle.first_detected,
                last_seen=cycle.last_seen,
            )
            session.add(db_cycle)

        await session.commit()

        # Build result
        result_obj = TopologicalQualityResult(
            metric_id=metric_id,
            table_id=table_id,
            table_name=table.table_name,
            computed_at=computed_at,
            betti_numbers=betti_numbers,
            persistence_diagrams=persistence_diagrams,
            persistent_entropy=persistent_entropy,
            stability=stability,
            complexity=complexity,
            persistent_cycles=cycles,
            anomalies=anomalies,
            orphaned_components=orphaned_components,
            topology_description=topology_description,
            quality_warnings=quality_warnings,
            quality_score=quality_score,
            has_issues=len(anomalies) > 0,
        )

        return Result.ok(result_obj)

    except Exception as e:
        return Result.fail(f"Topological quality analysis failed: {e}")
