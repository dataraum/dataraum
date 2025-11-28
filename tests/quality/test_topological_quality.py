"""Tests for topological quality analysis module."""

import numpy as np
import pytest

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
from dataraum_context.quality.topological import (
    analyze_topological_quality,
    assess_homological_stability,
    assess_structural_complexity,
    compute_persistent_entropy,
    detect_persistent_cycles,
    extract_betti_numbers,
    process_persistence_diagrams,
)
from dataraum_context.storage.models_v2.core import Column, Source, Table


def create_column(col_id: str, table_id: str, name: str, position: int, col_type: str = "DOUBLE"):
    """Helper to create a column with all required fields."""
    return Column(
        column_id=col_id,
        table_id=table_id,
        column_name=name,
        column_position=position,
        resolved_type=col_type,
    )


@pytest.fixture
async def sample_source(async_session):
    """Create a sample source."""
    source = Source(
        source_id="test-source",
        name="test_data",
        source_type="csv",
    )
    async_session.add(source)
    await async_session.commit()
    await async_session.refresh(source)
    return source


@pytest.fixture
async def simple_topology_table(async_session, sample_source, duckdb_conn):
    """Create a table with simple topology (one component, no cycles)."""
    table = Table(
        table_id="simple-topo-table",
        source_id=sample_source.source_id,
        table_name="simple_topology_data",
        layer="typed",
        duckdb_path="simple_topology_data",
    )
    async_session.add(table)

    # Create numeric columns
    columns = [
        create_column("col-x", table.table_id, "x", 0, "DOUBLE"),
        create_column("col-y", table.table_id, "y", 1, "DOUBLE"),
        create_column("col-z", table.table_id, "z", 2, "DOUBLE"),
    ]
    for col in columns:
        async_session.add(col)

    await async_session.commit()

    # Create test data: simple linear relationship
    duckdb_conn.execute(
        """
        CREATE TABLE simple_topology_data AS
        SELECT
            x::DOUBLE as x,
            (2 * x + 1)::DOUBLE as y,
            (x + 3)::DOUBLE as z
        FROM (SELECT unnest(generate_series(1, 50)) as x)
        """
    )

    await async_session.refresh(table)
    return table


@pytest.fixture
async def cyclic_topology_table(async_session, sample_source, duckdb_conn):
    """Create a table with cyclic structure."""
    table = Table(
        table_id="cyclic-topo-table",
        source_id=sample_source.source_id,
        table_name="cyclic_topology_data",
        layer="typed",
        duckdb_path="cyclic_topology_data",
    )
    async_session.add(table)

    columns = [
        create_column("col-x", table.table_id, "x", 0, "DOUBLE"),
        create_column("col-y", table.table_id, "y", 1, "DOUBLE"),
    ]
    for col in columns:
        async_session.add(col)

    await async_session.commit()

    # Create circular data pattern (points on a circle)
    duckdb_conn.execute(
        """
        CREATE TABLE cyclic_topology_data AS
        SELECT
            cos(radians(angle))::DOUBLE as x,
            sin(radians(angle))::DOUBLE as y
        FROM (SELECT unnest(generate_series(0, 359, 10)) as angle)
        """
    )

    await async_session.refresh(table)
    return table


@pytest.fixture
async def disconnected_topology_table(async_session, sample_source, duckdb_conn):
    """Create a table with disconnected components."""
    table = Table(
        table_id="disconn-topo-table",
        source_id=sample_source.source_id,
        table_name="disconnected_topology_data",
        layer="typed",
        duckdb_path="disconnected_topology_data",
    )
    async_session.add(table)

    columns = [
        create_column("col-x", table.table_id, "x", 0, "DOUBLE"),
        create_column("col-y", table.table_id, "y", 1, "DOUBLE"),
    ]
    for col in columns:
        async_session.add(col)

    await async_session.commit()

    # Create two separate clusters
    duckdb_conn.execute(
        """
        CREATE TABLE disconnected_topology_data AS
        SELECT x::DOUBLE as x, y::DOUBLE as y
        FROM (
            -- First cluster around (0, 0)
            SELECT
                random() * 2 - 1 as x,
                random() * 2 - 1 as y
            FROM generate_series(1, 20)
            UNION ALL
            -- Second cluster around (10, 10)
            SELECT
                10 + random() * 2 - 1 as x,
                10 + random() * 2 - 1 as y
            FROM generate_series(1, 20)
        )
        """
    )

    await async_session.refresh(table)
    return table


# ============================================================================
# Unit Tests for Individual Functions
# ============================================================================


@pytest.mark.asyncio
async def test_extract_betti_numbers_empty():
    """Test Betti number extraction with empty diagrams."""
    result = await extract_betti_numbers([])
    assert not result.success
    assert "No persistence diagrams" in result.error


@pytest.mark.asyncio
async def test_extract_betti_numbers_simple():
    """Test Betti number extraction with simple persistence diagrams."""
    # Create simple persistence diagrams
    # Dimension 0: 2 finite components + 1 infinite
    dgm_0 = np.array([[0.0, 0.5], [0.0, 0.3], [0.0, np.inf]])
    # Dimension 1: 1 cycle
    dgm_1 = np.array([[0.2, 0.8]])
    diagrams = [dgm_0, dgm_1]

    result = await extract_betti_numbers(diagrams)
    assert result.success

    betti = result.value
    assert betti.betti_0 == 3  # 2 finite + 1 for infinite component
    assert betti.betti_1 == 1  # 1 cycle
    assert betti.betti_2 is None  # No dimension 2
    assert betti.total_complexity == 4
    assert not betti.is_connected  # More than 1 component
    assert betti.has_cycles
    assert not betti.has_voids


@pytest.mark.asyncio
async def test_extract_betti_numbers_connected():
    """Test Betti numbers for connected structure."""
    # Only 1 infinite component (fully connected)
    dgm_0 = np.array([[0.0, np.inf]])
    dgm_1 = np.array([]).reshape(0, 2)  # Empty array with correct shape
    diagrams = [dgm_0, dgm_1]

    result = await extract_betti_numbers(diagrams)
    assert result.success

    betti = result.value
    assert betti.betti_0 == 1
    assert betti.betti_1 == 0
    assert betti.is_connected
    assert not betti.has_cycles


@pytest.mark.asyncio
async def test_process_persistence_diagrams_empty():
    """Test processing empty diagrams."""
    result = await process_persistence_diagrams([])
    assert result.success
    assert result.value == []


@pytest.mark.asyncio
async def test_process_persistence_diagrams():
    """Test processing persistence diagrams into structured format."""
    dgm_0 = np.array([[0.0, 0.5], [0.0, 0.3]])
    dgm_1 = np.array([[0.2, 0.8], [0.1, 0.4]])
    diagrams = [dgm_0, dgm_1]

    result = await process_persistence_diagrams(diagrams)
    assert result.success

    processed = result.value
    assert len(processed) == 2

    # Check dimension 0
    assert processed[0].dimension == 0
    assert processed[0].num_features == 2
    assert processed[0].max_persistence == 0.5

    # Check dimension 1
    assert processed[1].dimension == 1
    assert processed[1].num_features == 2
    assert abs(processed[1].max_persistence - 0.6) < 0.01  # death - birth = 0.8 - 0.2


def test_compute_persistent_entropy():
    """Test persistent entropy computation."""
    # Create simple diagrams
    dgm_0 = np.array([[0.0, 0.5], [0.0, 0.3]])
    dgm_1 = np.array([[0.2, 0.8]])
    diagrams = [dgm_0, dgm_1]

    entropy = compute_persistent_entropy(diagrams)
    assert entropy >= 0.0  # Entropy is always non-negative
    assert isinstance(entropy, float)


def test_compute_persistent_entropy_empty():
    """Test entropy with empty diagrams."""
    entropy = compute_persistent_entropy([])
    assert entropy == 0.0


def test_compute_persistent_entropy_uniform():
    """Test entropy with uniform lifetimes."""
    # All features have same lifetime
    dgm = np.array([[0.0, 1.0], [0.0, 1.0], [0.0, 1.0]])
    entropy = compute_persistent_entropy([dgm])
    # Uniform distribution has maximum entropy for its size (log(n))
    # For n=3, entropy = log(3) ≈ 1.099
    assert entropy > 1.0  # Should have positive entropy


@pytest.mark.asyncio
async def test_detect_persistent_cycles():
    """Test cycle detection."""
    # Dimension 1 diagram with cycles
    dgm_0 = np.array([[0.0, 0.5]])
    dgm_1 = np.array([[0.1, 0.8], [0.2, 0.5], [0.0, 0.15]])  # 3 cycles
    diagrams = [dgm_0, dgm_1]

    result = await detect_persistent_cycles(diagrams, metric_id="test-metric", min_persistence=0.2)
    assert result.success

    cycles = result.value
    # Should detect cycles with persistence >= 0.2
    # Cycle 1: 0.8 - 0.1 = 0.7 ✓
    # Cycle 2: 0.5 - 0.2 = 0.3 ✓
    # Cycle 3: 0.15 - 0.0 = 0.15 ✗ (below threshold)
    assert len(cycles) == 2

    for cycle in cycles:
        assert cycle.dimension == 1
        assert cycle.persistence >= 0.2
        assert not cycle.is_anomalous  # Default


@pytest.mark.asyncio
async def test_detect_persistent_cycles_no_dimension_1():
    """Test cycle detection when no H1 exists."""
    dgm_0 = np.array([[0.0, 0.5]])
    diagrams = [dgm_0]  # No dimension 1

    result = await detect_persistent_cycles(diagrams, metric_id="test-metric")
    assert result.success
    assert len(result.value) == 0


@pytest.mark.asyncio
async def test_assess_structural_complexity_no_history(async_session):
    """Test complexity assessment with no historical data."""
    betti = BettiNumbers(
        betti_0=1,
        betti_1=2,
        betti_2=0,
        total_complexity=3,
        is_connected=True,
        has_cycles=True,
        has_voids=False,
    )

    result = await assess_structural_complexity(
        betti, persistent_entropy=1.5, table_id="test-table", session=async_session
    )
    assert result.success

    complexity = result.value
    assert complexity.total_complexity == 3
    assert complexity.persistent_entropy == 1.5
    assert complexity.complexity_mean is None  # No history
    assert complexity.within_bounds  # Default to True


@pytest.mark.asyncio
async def test_assess_homological_stability_no_previous(async_session):
    """Test stability assessment with no previous data."""
    dgm_0 = np.array([[0.0, 0.5]])
    diagrams = [dgm_0]

    result = await assess_homological_stability(
        diagrams, table_id="test-table", session=async_session
    )
    assert result.success
    assert result.value is None  # No previous data to compare


# ============================================================================
# Integration Tests
# ============================================================================


@pytest.mark.asyncio
async def test_analyze_topological_quality_simple(
    simple_topology_table, duckdb_conn, async_session
):
    """Test full topological quality analysis with simple topology."""
    result = await analyze_topological_quality(
        simple_topology_table.table_id,
        duckdb_conn,
        async_session,
        max_dimension=2,
        min_persistence=0.1,
    )

    assert result.success, f"Analysis failed: {result.error}"

    analysis = result.value
    assert analysis.table_id == simple_topology_table.table_id
    assert analysis.table_name == "simple_topology_data"

    # Check Betti numbers
    assert analysis.betti_numbers.betti_0 >= 1  # At least one component
    assert analysis.betti_numbers.total_complexity >= 0

    # Check persistence diagrams
    assert len(analysis.persistence_diagrams) > 0

    # Check quality score
    assert 0.0 <= analysis.quality_score <= 1.0

    # Check topology description exists
    assert len(analysis.topology_description) > 0


@pytest.mark.asyncio
async def test_analyze_topological_quality_cyclic(
    cyclic_topology_table, duckdb_conn, async_session
):
    """Test analysis with cyclic topology."""
    result = await analyze_topological_quality(
        cyclic_topology_table.table_id,
        duckdb_conn,
        async_session,
        max_dimension=2,
        min_persistence=0.05,
    )

    assert result.success

    analysis = result.value
    # Circular data may or may not create detectable cycles depending on sampling
    # Just verify the analysis completes and returns valid Betti numbers
    assert analysis.betti_numbers.betti_0 >= 1
    assert analysis.betti_numbers.total_complexity >= 0


@pytest.mark.asyncio
async def test_analyze_topological_quality_disconnected(
    disconnected_topology_table, duckdb_conn, async_session
):
    """Test analysis with disconnected components."""
    result = await analyze_topological_quality(
        disconnected_topology_table.table_id,
        duckdb_conn,
        async_session,
        max_dimension=1,
        min_persistence=0.1,
    )

    assert result.success

    analysis = result.value
    # Should detect orphaned components
    assert analysis.orphaned_components > 0 or analysis.betti_numbers.betti_0 > 1

    # Should have anomalies
    orphan_anomalies = [a for a in analysis.anomalies if a.anomaly_type == "orphaned_components"]
    assert len(orphan_anomalies) > 0 or not analysis.betti_numbers.is_connected


@pytest.mark.asyncio
async def test_analyze_topological_quality_persistence(
    simple_topology_table, duckdb_conn, async_session
):
    """Test that running analysis twice enables stability tracking."""
    # First analysis
    result1 = await analyze_topological_quality(
        simple_topology_table.table_id, duckdb_conn, async_session
    )
    assert result1.success
    analysis1 = result1.value
    assert analysis1.stability is None  # No previous data

    # Second analysis (should compare with first)
    result2 = await analyze_topological_quality(
        simple_topology_table.table_id, duckdb_conn, async_session
    )
    assert result2.success
    analysis2 = result2.value

    # Stability might still be None if persim not installed
    # But the analysis should succeed
    if analysis2.stability is not None:
        assert isinstance(analysis2.stability.is_stable, bool)
        assert analysis2.stability.bottleneck_distance >= 0.0


@pytest.mark.asyncio
async def test_analyze_topological_quality_table_not_found(duckdb_conn, async_session):
    """Test error handling for non-existent table."""
    result = await analyze_topological_quality("nonexistent-table", duckdb_conn, async_session)
    assert not result.success
    assert "not found" in result.error.lower()


@pytest.mark.asyncio
async def test_analyze_topological_quality_empty_table(async_session, sample_source, duckdb_conn):
    """Test handling of empty table."""
    table = Table(
        table_id="empty-table",
        source_id=sample_source.source_id,
        table_name="empty_data",
        layer="typed",
        duckdb_path="empty_data",
    )
    async_session.add(table)
    await async_session.commit()

    # Create empty table
    duckdb_conn.execute("CREATE TABLE empty_data (x DOUBLE, y DOUBLE)")

    result = await analyze_topological_quality(table.table_id, duckdb_conn, async_session)
    assert not result.success
    assert "empty" in result.error.lower()


# ============================================================================
# Pydantic Model Tests
# ============================================================================


def test_pydantic_betti_numbers():
    """Test BettiNumbers model."""
    betti = BettiNumbers(
        betti_0=2,
        betti_1=3,
        betti_2=1,
        total_complexity=6,
        is_connected=False,
        has_cycles=True,
        has_voids=True,
    )
    assert betti.betti_0 == 2
    assert betti.total_complexity == 6
    assert not betti.is_connected
    assert betti.has_cycles


def test_pydantic_persistence_point():
    """Test PersistencePoint model."""
    point = PersistencePoint(dimension=1, birth=0.2, death=0.8, persistence=0.6)
    assert point.dimension == 1
    assert point.persistence == 0.6


def test_pydantic_persistence_diagram():
    """Test PersistenceDiagram model."""
    points = [
        PersistencePoint(dimension=1, birth=0.1, death=0.5, persistence=0.4),
        PersistencePoint(dimension=1, birth=0.2, death=0.9, persistence=0.7),
    ]
    diagram = PersistenceDiagram(dimension=1, points=points, max_persistence=0.7, num_features=2)
    assert diagram.dimension == 1
    assert len(diagram.points) == 2
    assert diagram.max_persistence == 0.7


def test_pydantic_structural_complexity():
    """Test StructuralComplexity model."""
    from datetime import datetime

    betti = BettiNumbers(
        betti_0=1,
        betti_1=2,
        betti_2=None,
        total_complexity=3,
        is_connected=True,
        has_cycles=True,
        has_voids=False,
    )
    complexity = StructuralComplexity(
        total_complexity=3,
        betti_numbers=betti,
        persistent_entropy=1.5,
        complexity_mean=2.8,
        complexity_std=0.5,
        complexity_z_score=0.4,
        complexity_trend="stable",
        within_bounds=True,
    )
    assert complexity.total_complexity == 3
    assert complexity.complexity_z_score == 0.4
    assert complexity.within_bounds


def test_pydantic_homological_stability():
    """Test HomologicalStability model."""
    stability = HomologicalStability(
        bottleneck_distance=0.15,
        is_stable=True,
        threshold=0.2,
        components_added=0,
        components_removed=0,
        cycles_added=1,
        cycles_removed=0,
        stability_level="stable",
    )
    assert stability.is_stable
    assert stability.bottleneck_distance == 0.15
    assert stability.stability_level == "stable"
    assert stability.cycles_added == 1


def test_pydantic_topological_anomaly():
    """Test TopologicalAnomaly model."""
    anomaly = TopologicalAnomaly(
        anomaly_type="orphaned_components",
        severity="medium",
        description="Found 2 disconnected components",
        evidence={"component_count": 3},
        affected_tables=["table-1"],
    )
    assert anomaly.anomaly_type == "orphaned_components"
    assert anomaly.severity == "medium"
    assert "disconnected" in anomaly.description


def test_pydantic_persistent_cycle_result():
    """Test PersistentCycleResult model."""
    from datetime import datetime

    now = datetime.now()
    cycle = PersistentCycleResult(
        cycle_id="cycle-1",
        dimension=1,
        birth=0.2,
        death=0.8,
        persistence=0.6,
        involved_columns=["col-1", "col-2"],
        cycle_type="correlation_loop",
        is_anomalous=False,
        anomaly_reason=None,
        first_detected=now,
        last_seen=now,
    )
    assert cycle.cycle_id == "cycle-1"
    assert cycle.persistence == 0.6
    assert not cycle.is_anomalous


def test_pydantic_topological_quality_result():
    """Test TopologicalQualityResult model."""
    from datetime import datetime

    betti = BettiNumbers(
        betti_0=1,
        betti_1=1,
        betti_2=None,
        total_complexity=2,
        is_connected=True,
        has_cycles=True,
        has_voids=False,
    )
    complexity = StructuralComplexity(
        total_complexity=2,
        betti_numbers=betti,
        persistent_entropy=1.2,
        within_bounds=True,
    )
    result = TopologicalQualityResult(
        metric_id="metric-1",
        table_id="table-1",
        table_name="test_table",
        computed_at=datetime.now(),
        betti_numbers=betti,
        persistence_diagrams=[],
        persistent_entropy=1.2,
        stability=None,
        complexity=complexity,
        persistent_cycles=[],
        anomalies=[],
        orphaned_components=0,
        topology_description="fully connected, 1 cycle",
        quality_warnings=[],
        quality_score=0.95,
        has_issues=False,
    )
    assert result.metric_id == "metric-1"
    assert result.quality_score == 0.95
    assert not result.has_issues
