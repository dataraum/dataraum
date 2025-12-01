"""Pydantic models for Topological Context (Pillar 2).

These models are used for:
- API responses for topological queries
- Internal data transfer for topology analysis
- Validation of topology results

Note: These are separate from SQLAlchemy models (storage/models_v2/topological_context.py)
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

# ============================================================================
# Persistence Models
# ============================================================================


class PersistencePoint(BaseModel):
    """A single point in a persistence diagram."""

    dimension: int  # 0 for components, 1 for cycles, 2 for voids
    birth: float  # When feature appears
    death: float  # When feature disappears
    persistence: float  # death - birth (how long it lasts)


class PersistenceDiagram(BaseModel):
    """Persistence diagram for a given dimension."""

    dimension: int
    points: list[PersistencePoint]
    max_persistence: float  # Longest-lived feature
    num_features: int  # Total number of features


# ============================================================================
# Betti Number Models
# ============================================================================


class BettiNumbers(BaseModel):
    """Betti numbers (topological invariants)."""

    betti_0: int  # Number of connected components
    betti_1: int  # Number of cycles / holes
    betti_2: int | None = None  # Number of voids / cavities

    total_complexity: int  # Sum of all Betti numbers

    # Interpretation helpers
    is_connected: bool  # betti_0 == 1
    has_cycles: bool  # betti_1 > 0
    has_voids: bool  # betti_2 > 0 if computed


# ============================================================================
# Cycle Models
# ============================================================================


class PersistentCycleResult(BaseModel):
    """A detected persistent cycle in the data."""

    cycle_id: str
    dimension: int
    birth: float
    death: float
    persistence: float

    # Involved entities
    involved_columns: list[str] = Field(default_factory=lambda: [])  # Column IDs

    # Domain interpretation
    cycle_type: str | None = None  # 'money_flow', 'order_fulfillment', etc.
    is_anomalous: bool = False
    anomaly_reason: str | None = None

    # Lifecycle
    first_detected: datetime
    last_seen: datetime


# ============================================================================
# Stability Models
# ============================================================================


class HomologicalStability(BaseModel):
    """Stability comparison between two periods."""

    bottleneck_distance: float  # Wassertein distance between diagrams
    is_stable: bool  # Within acceptable threshold
    threshold: float  # Configured threshold

    # Change summary
    components_added: int = 0
    components_removed: int = 0
    cycles_added: int = 0
    cycles_removed: int = 0

    # Interpretation
    stability_level: str  # 'stable', 'minor_changes', 'significant_changes', 'unstable'


# ============================================================================
# Complexity Models
# ============================================================================


class StructuralComplexity(BaseModel):
    """Structural complexity metrics."""

    total_complexity: int  # Sum of Betti numbers
    betti_numbers: BettiNumbers
    persistent_entropy: float | None = None  # Information-theoretic complexity

    # Historical context
    complexity_mean: float | None = None  # Historical average
    complexity_std: float | None = None  # Historical standard deviation
    complexity_z_score: float | None = None  # Current vs historical

    # Trend
    complexity_trend: str | None = None  # 'increasing', 'stable', 'decreasing'
    within_bounds: bool = True  # Within historical norms (±2σ)


# ============================================================================
# Anomaly Models
# ============================================================================


class TopologicalAnomaly(BaseModel):
    """A detected topological anomaly."""

    anomaly_type: str  # 'unexpected_cycle', 'orphaned_component', 'complexity_spike', etc.
    severity: str  # 'low', 'medium', 'high'
    description: str  # Human-readable description
    evidence: dict[str, Any] = Field(default_factory=lambda: {})  # Supporting data

    # Affected entities
    affected_tables: list[str] = Field(default_factory=lambda: [])
    affected_columns: list[str] = Field(default_factory=lambda: [])


# ============================================================================
# Aggregate Results
# ============================================================================


class TopologicalQualityResult(BaseModel):
    """Complete topological quality assessment for a table."""

    metric_id: str
    table_id: str
    table_name: str
    computed_at: datetime

    # Core metrics
    betti_numbers: BettiNumbers
    persistence_diagrams: list[PersistenceDiagram]
    persistent_entropy: float | None = None

    # Stability (if comparing with previous period)
    stability: HomologicalStability | None = None

    # Complexity
    complexity: StructuralComplexity

    # Detected cycles
    persistent_cycles: list[PersistentCycleResult] = Field(default_factory=lambda: [])

    # Anomalies
    anomalies: list[TopologicalAnomaly] = Field(default_factory=lambda: [])
    orphaned_components: int = 0

    # Summary for LLM
    topology_description: str  # "3 connected components, 12 significant cycles"
    quality_warnings: list[str] = Field(default_factory=lambda: [])

    # Overall assessment
    quality_score: float  # 0-1, based on stability + complexity bounds
    has_issues: bool = False


class TopologicalSummary(BaseModel):
    """High-level summary of topological features."""

    num_tables: int
    total_components: int
    total_cycles: int
    total_voids: int

    avg_complexity: float
    complexity_range: tuple[int, int]  # (min, max)

    stability_status: str  # 'all_stable', 'some_unstable', 'many_unstable'
    num_anomalies: int

    # Key findings
    key_findings: list[str] = Field(default_factory=lambda: [])
    recommendations: list[str] = Field(default_factory=lambda: [])
