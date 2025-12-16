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
from dataraum_context.enrichment.db_models import Relationship as RelationshipModel
from dataraum_context.enrichment.db_models import (
    TopologicalQualityMetrics as DBTopologicalMetrics,
)

# Re-export from relationships package for backward compatibility
# TODO: Remove this re-export and update all imports to use
#       dataraum_context.enrichment.relationships directly
from dataraum_context.enrichment.relationships.graph_analysis import (
    analyze_relationship_graph,
)
from dataraum_context.quality.models import (
    BettiNumbers,
    CycleDetection,
    PersistenceDiagram,
    PersistencePoint,
    StabilityAnalysis,
    TopologicalQualityResult,
)
from dataraum_context.storage.models_v2.core import Table

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
            betti_2=betti_2 or 0,
            total_complexity=total_complexity,
            is_connected=betti_0 == 1,  # Single connected component
            has_cycles=betti_1 > 0,
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
            _finite_dgm = dgm[finite_mask]

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

            # Compute persistent entropy for this dimension
            lifetimes = [p.persistence for p in points]
            total_lifetime = sum(lifetimes)
            dim_entropy = 0.0
            if total_lifetime > 0:
                probabilities = [lt / total_lifetime for lt in lifetimes]
                dim_entropy = float(scipy_entropy(probabilities))

            diagram = PersistenceDiagram(
                dimension=dimension,
                points=points,
                max_persistence=max_persistence,
                num_features=len(points),  # Count of features (avoids len(points) everywhere)
                persistent_entropy=dim_entropy,
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
    min_persistence: float = 0.1,
) -> Result[list[CycleDetection]]:
    """Detect significant persistent cycles (dimension 1 features).

    Cycles represent circular relationships or flows in the data.

    Args:
        persistence_diagrams: Raw diagrams from ripser
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
            cycle = CycleDetection(
                cycle_id=str(uuid4()),  # Unique identifier for tracking
                dimension=1,
                birth=float(birth),
                death=float(death),
                persistence=float(persistence),
                involved_columns=[],  # Would need additional analysis
                cycle_type=None,  # CRITICAL: Would be inferred from domain analysis
                is_anomalous=False,
                anomaly_reason=None,
                first_detected=now,  # When cycle first appeared
                last_seen=now,  # Temporal tracking
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
) -> Result[StabilityAnalysis | None]:
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

        if not previous_metric or not previous_metric.topology_data:
            return Result.ok(None)  # No previous data to compare

        # Deserialize previous topology data from JSONB
        try:
            # TopologicalQualityResult already imported at module level
            prev_topology = TopologicalQualityResult.model_validate(previous_metric.topology_data)
        except Exception:
            # Fallback to dict access
            prev_topology = None

        if not prev_topology or not prev_topology.persistence_diagrams:
            return Result.ok(None)

        # Reconstruct numpy arrays from previous persistence diagrams
        prev_diagrams = []
        for dgm in prev_topology.persistence_diagrams:
            if dgm.points:
                dgm_array = np.array([[p.birth, p.death] for p in dgm.points])
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
                distance, _ = bottleneck(curr_finite, prev_finite)
                bottleneck_distances[f"H{dim}"] = float(distance)
                max_distance = max(max_distance, distance)

        if not bottleneck_distances:
            return Result.ok(None)

        # Determine stability
        is_stable = max_distance <= stability_threshold

        # Determine stability level based on distance
        if max_distance < stability_threshold * 0.5:
            stability_level = "stable"
        elif max_distance < stability_threshold:
            stability_level = "minor_changes"
        elif max_distance < stability_threshold * 2:
            stability_level = "significant_changes"
        else:
            stability_level = "unstable"

        # Compute change counts (compare Betti numbers)
        # Extract current Betti numbers from diagrams
        curr_betti_0 = len(current_diagrams[0]) if len(current_diagrams) > 0 else 0
        curr_betti_1 = len(current_diagrams[1]) if len(current_diagrams) > 1 else 0

        # Extract previous Betti numbers
        prev_betti_0 = len(prev_diagrams[0]) if len(prev_diagrams) > 0 else 0
        prev_betti_1 = len(prev_diagrams[1]) if len(prev_diagrams) > 1 else 0

        components_added = max(0, curr_betti_0 - prev_betti_0)
        components_removed = max(0, prev_betti_0 - curr_betti_0)
        cycles_added = max(0, curr_betti_1 - prev_betti_1)
        cycles_removed = max(0, prev_betti_1 - curr_betti_1)

        stability = StabilityAnalysis(
            bottleneck_distance=max_distance,
            is_stable=is_stable,
            stability_threshold=stability_threshold,
            stability_level=stability_level,  # CRITICAL: Graded assessment
            components_added=components_added,
            components_removed=components_removed,
            cycles_added=cycles_added,
            cycles_removed=cycles_removed,
        )

        return Result.ok(stability)

    except Exception:
        # Don't fail the whole analysis if stability computation fails
        return Result.ok(None)


# ============================================================================
# Main Analysis Function
# ============================================================================


async def compute_historical_complexity(
    session: AsyncSession,
    table_id: str,
    current_complexity: int,
    lookback_days: int = 30,
) -> dict[str, float | None]:
    """Compute historical complexity statistics for trend analysis.

    Queries previous TopologicalQualityResult records to compute baseline statistics
    and detect complexity anomalies.

    Args:
        session: Database session
        table_id: Table to analyze
        current_complexity: Current complexity value
        lookback_days: Number of days to look back for historical data

    Returns:
        Dict with keys: mean, std, z_score (all None if insufficient data)
    """
    from datetime import timedelta

    from sqlalchemy import select

    from dataraum_context.enrichment.db_models import TopologicalQualityMetrics

    try:
        # Query historical complexity values
        cutoff_date = datetime.now(UTC) - timedelta(days=lookback_days)

        stmt = (
            select(TopologicalQualityMetrics.structural_complexity)
            .where(TopologicalQualityMetrics.table_id == table_id)
            .where(TopologicalQualityMetrics.computed_at >= cutoff_date)
            .order_by(TopologicalQualityMetrics.computed_at.desc())
        )

        result = await session.execute(stmt)
        complexities = [row[0] for row in result.all() if row[0] is not None]

        # Need at least 5 data points for meaningful statistics
        if len(complexities) < 5:
            return {"mean": None, "std": None, "z_score": None}

        # Compute statistics
        import numpy as np

        mean = float(np.mean(complexities))
        std = float(np.std(complexities))

        # Compute Z-score (how many standard deviations from mean)
        z_score = (current_complexity - mean) / std if std > 0 else 0.0

        return {"mean": mean, "std": std, "z_score": z_score}

    except Exception as e:
        # If anything goes wrong, return None values (don't fail the analysis)
        import logging

        logging.warning(f"Failed to compute historical complexity: {e}")
        return {"mean": None, "std": None, "z_score": None}


async def analyze_topological_quality(
    table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    max_dimension: int = 2,
    min_persistence: float = 0.1,
) -> Result[TopologicalQualityResult]:
    """Analyze topological quality for a table.

    This is the main entry point for raw topological quality analysis.

    For LLM-enhanced cycle classification and domain-specific analysis,
    use the financial_orchestrator instead:
        from dataraum_context.quality.domains.financial_orchestrator import (
            analyze_complete_financial_quality,
        )
        result = await analyze_complete_financial_quality(
            table_id, duckdb_conn, session, llm_service
        )

    The orchestrator will:
    1. Call this function for raw topological analysis
    2. Classify cycles using LLM with domain context
    3. Run domain rules (fiscal stability, anomaly detection, quality scoring)
    4. Generate holistic LLM interpretation

    Args:
        table_id: Table to analyze
        duckdb_conn: DuckDB connection
        session: Database session
        max_dimension: Maximum homology dimension to compute
        min_persistence: Minimum persistence for significant features

    Returns:
        Result containing complete topological quality assessment

    Example:
        # Raw topological analysis (no cycle classification)
        result = await analyze_topological_quality(table_id, conn, session)

        # For LLM-enhanced analysis with domain rules (recommended)
        from dataraum_context.quality.domains.financial_orchestrator import (
            analyze_complete_financial_quality,
        )
        result = await analyze_complete_financial_quality(
            table_id, conn, session, llm_service
        )
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
            return Result.fail(betti_result.error if betti_result.error else "Unknown error")
        betti_numbers = betti_result.unwrap()

        # Process persistence diagrams
        diagram_result = await process_persistence_diagrams(diagrams)
        if not diagram_result.success:
            return Result.fail(diagram_result.error if diagram_result.error else "Unknown error")
        persistence_diagrams = diagram_result.unwrap()

        # Compute persistent entropy
        persistent_entropy = compute_persistent_entropy(diagrams)

        # Assess homological stability (compare with previous period)
        stability_result = await assess_homological_stability(
            diagrams, table_id, session, stability_threshold=0.2
        )
        if not stability_result.success:
            return Result.fail(
                stability_result.error if stability_result.error else "Unknown error"
            )
        stability = stability_result.value  # Can be None if no previous data

        # Detect cycles
        cycle_result = await detect_persistent_cycles(diagrams, min_persistence=min_persistence)
        if not cycle_result.success:
            return Result.fail(cycle_result.error if cycle_result.error else "Unknown error")
        cycles = cycle_result.unwrap()

        # Calculate complexity and anomalies
        orphaned_components = max(0, betti_numbers.betti_0 - 1)  # Expect 1 component
        structural_complexity = betti_numbers.total_complexity

        # Generate quality warnings
        quality_warnings = []
        has_anomalies = False

        if orphaned_components > 0:
            quality_warnings.append(f"Found {orphaned_components} disconnected components")
            has_anomalies = True

        if betti_numbers.has_cycles and len(cycles) > 5:
            quality_warnings.append(f"High number of cycles detected ({len(cycles)})")
            has_anomalies = True

        if not (stability and stability.is_stable):
            quality_warnings.append("Topology has changed significantly from previous period")
            has_anomalies = True

        # Determine complexity trend (simplified - compare with historical mean if available)
        complexity_trend = None
        complexity_within_bounds = True

        # Build topology description
        desc_parts = []
        if betti_numbers.betti_0 == 1:
            desc_parts.append("fully connected")
        else:
            desc_parts.append(f"{betti_numbers.betti_0} components")

        if betti_numbers.betti_1 > 0:
            desc_parts.append(f"{betti_numbers.betti_1} cycles")

        if betti_numbers.betti_2 > 0:
            desc_parts.append(f"{betti_numbers.betti_2} voids")

        topology_description = ", ".join(desc_parts) if desc_parts else "trivial topology"

        # Identify anomalous cycles
        anomalous_cycles = [c for c in cycles if c.is_anomalous]

        # Build comprehensive anomaly records
        from dataraum_context.quality.models import TopologicalAnomaly

        anomalies = []
        if orphaned_components > 0:
            anomalies.append(
                TopologicalAnomaly(
                    anomaly_type="orphaned_component",
                    severity="medium" if orphaned_components < 3 else "high",
                    description=f"Found {orphaned_components} disconnected components",
                    evidence={"component_count": orphaned_components},
                    affected_tables=[table.table_name],
                    affected_columns=[],
                )
            )

        if len(cycles) > 10:
            anomalies.append(
                TopologicalAnomaly(
                    anomaly_type="complexity_spike",
                    severity="high",
                    description=f"Unusually high number of cycles ({len(cycles)})",
                    evidence={"cycle_count": len(cycles)},
                    affected_tables=[table.table_name],
                    affected_columns=[],
                )
            )

        # Compute historical complexity statistics
        complexity_stats = await compute_historical_complexity(
            session=session,
            table_id=table_id,
            current_complexity=structural_complexity,
            lookback_days=30,  # Default to 30 days
        )

        complexity_mean = complexity_stats["mean"]
        complexity_std = complexity_stats["std"]
        complexity_z_score = complexity_stats["z_score"]

        # Build TopologicalQualityResult (Pydantic source of truth)
        computed_at = datetime.now(UTC)

        quality_result = TopologicalQualityResult(
            table_id=table_id,
            table_name=table.table_name,
            betti_numbers=betti_numbers,
            persistence_diagrams=persistence_diagrams,
            persistent_cycles=cycles,
            stability=stability,
            structural_complexity=structural_complexity,
            persistent_entropy=persistent_entropy,
            orphaned_components=orphaned_components,
            complexity_trend=complexity_trend,
            complexity_within_bounds=complexity_within_bounds,
            complexity_mean=complexity_mean,
            complexity_std=complexity_std,
            complexity_z_score=complexity_z_score,
            has_anomalies=has_anomalies,
            anomalies=anomalies,
            anomalous_cycles=anomalous_cycles,
            quality_warnings=quality_warnings,
            topology_description=topology_description,
        )

        # Persist using hybrid storage
        metric_id = str(uuid4())
        db_metric = DBTopologicalMetrics(
            metric_id=metric_id,
            table_id=table_id,
            computed_at=computed_at,
            # STRUCTURED: Queryable core dimensions
            betti_0=betti_numbers.betti_0,
            betti_1=betti_numbers.betti_1,
            betti_2=betti_numbers.betti_2,
            structural_complexity=structural_complexity,
            orphaned_components=orphaned_components,
            homologically_stable=stability.is_stable if stability else None,
            has_cycles=betti_numbers.has_cycles,
            has_anomalies=has_anomalies,
            # JSONB: Full Pydantic model (zero mapping!)
            topology_data=quality_result.model_dump(mode="json"),
        )
        session.add(db_metric)
        await session.commit()

        return Result.ok(quality_result)

    except Exception as e:
        return Result.fail(f"Topological quality analysis failed: {type(e).__name__}: {str(e)}")


# ============================================================================
# Multi-Table Topological Analysis (NEW)
# ============================================================================


async def load_table_relationships(
    session: AsyncSession,
    table_ids: list[str],
) -> list[RelationshipModel]:
    """Load relationships between the specified tables from the database.

    Args:
        session: Database session
        table_ids: List of table IDs to load relationships for

    Returns:
        List of relationship dicts with from_table_id, to_table_id, confidence, etc.
    """

    stmt = (
        select(RelationshipModel)
        .where(RelationshipModel.from_table_id.in_(table_ids))
        .where(RelationshipModel.to_table_id.in_(table_ids))
    )
    result = await session.execute(stmt)
    relationships = result.scalars().all()

    return list(relationships)


# NOTE: analyze_relationship_graph is now imported from
# dataraum_context.enrichment.relationships.graph_analysis
# and re-exported above for backward compatibility


async def analyze_topological_quality_multi_table(
    table_ids: list[str],
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    max_dimension: int = 2,
    min_persistence: float = 0.1,
) -> Result[dict[str, TopologicalQualityResult | dict[str, object]]]:
    """Analyze topological quality across multiple tables.

    This function performs BOTH:
    1. Single-table TDA for each table (existing functionality)
    2. Cross-table relationship graph analysis

    For LLM-enhanced cycle classification and domain-specific analysis,
    use the financial_orchestrator instead.

    Args:
        table_ids: List of tables to analyze
        duckdb_conn: DuckDB connection
        session: Database session
        max_dimension: Maximum homology dimension to compute
        min_persistence: Minimum persistence for significant features

    Returns:
        Result containing dict with:
        - per_table: Dict[table_id, TopologicalQualityResult] for each table
        - cross_table: Dict with graph analysis results (cycles, betti_0, etc.)
        - relationship_count: Number of relationships between tables

    Example:
        result = await analyze_topological_quality_multi_table(
            ["transactions", "customers", "vendors"],
            conn,
            session,
        )

        if result.success:
            data = result.value
            print(f"Per-table results: {data['per_table']}")
            print(f"Cross-table cycles: {data['cross_table']['cycles']}")
    """
    try:
        # 1. Run single-table analysis for each table
        per_table_results: dict[str, TopologicalQualityResult] = {}

        for table_id in table_ids:
            single_result = await analyze_topological_quality(
                table_id=table_id,
                duckdb_conn=duckdb_conn,
                session=session,
                max_dimension=max_dimension,
                min_persistence=min_persistence,
            )

            if single_result.success:
                per_table_results[table_id] = single_result.unwrap()

        # 2. Load relationships between these tables
        relationships = await load_table_relationships(session, table_ids)

        # 3. Analyze relationship graph for cross-table cycles
        graph_analysis = analyze_relationship_graph(table_ids, relationships)

        result_data = {
            "per_table": per_table_results,
            "cross_table": graph_analysis,
            "relationship_count": len(relationships),
        }

        # Save multi-table analysis to database
        from dataraum_context.enrichment.db_models import MultiTableTopologyMetrics

        # Extract cycles safely with proper type handling
        cycles_value = graph_analysis.get("cycles", [])
        cycles_list: list[list[str]] = cycles_value if isinstance(cycles_value, list) else []

        betti_0_value = graph_analysis.get("betti_0", 1)
        betti_0_int: int = betti_0_value if isinstance(betti_0_value, int) else 1

        db_multi_metric = MultiTableTopologyMetrics(
            table_ids=table_ids,
            cross_table_cycles=len(cycles_list),
            graph_betti_0=betti_0_int,
            relationship_count=len(relationships),
            has_cross_table_cycles=len(cycles_list) > 0,
            is_connected_graph=betti_0_int == 1,
            analysis_data=result_data,
        )
        session.add(db_multi_metric)
        await session.commit()

        return Result.ok(result_data)  # type: ignore[arg-type]

    except Exception as e:
        return Result.fail(
            f"Multi-table topological quality analysis failed: {type(e).__name__}: {str(e)}"
        )
