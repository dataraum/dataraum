"""Topological quality analysis models.

Pydantic models for topological analysis results:
- Betti numbers (connected components, cycles, voids)
- Persistence diagrams and points
- Cycle detection
- Stability analysis
- Topological anomalies
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class BettiNumbers(BaseModel):
    """Betti numbers from homology analysis."""

    betti_0: int  # Connected components
    betti_1: int  # Cycles / holes
    betti_2: int  # Voids / cavities
    total_complexity: int  # Sum of Betti numbers
    is_connected: bool  # betti_0 == 1
    has_cycles: bool  # betti_1 > 0


class PersistencePoint(BaseModel):
    """A point in a persistence diagram."""

    dimension: int  # 0, 1, or 2
    birth: float
    death: float
    persistence: float  # death - birth


class PersistenceDiagram(BaseModel):
    """Persistence diagram for a specific dimension."""

    dimension: int
    points: list[PersistencePoint]
    max_persistence: float
    num_features: int
    persistent_entropy: float | None = None


class CycleDetection(BaseModel):
    """Detected persistent cycle."""

    cycle_id: str
    dimension: int
    birth: float
    death: float
    persistence: float
    involved_columns: list[str] = Field(default_factory=list)
    cycle_type: str | None = None  # 'money_flow', 'order_fulfillment', etc.
    is_anomalous: bool = False
    anomaly_reason: str | None = None
    first_detected: datetime
    last_seen: datetime


class StabilityAnalysis(BaseModel):
    """Homological stability assessment."""

    bottleneck_distance: float
    is_stable: bool
    stability_threshold: float = 0.1
    stability_level: str  # 'stable', 'minor_changes', 'significant_changes', 'unstable'

    # Change counts
    components_added: int = 0
    components_removed: int = 0
    cycles_added: int = 0
    cycles_removed: int = 0


class TopologicalAnomaly(BaseModel):
    """Detected topological anomaly."""

    anomaly_type: str  # 'unexpected_cycle', 'orphaned_component', 'complexity_spike'
    severity: str  # 'low', 'medium', 'high'
    description: str
    evidence: dict[str, Any] = Field(default_factory=dict)
    affected_tables: list[str] = Field(default_factory=list)
    affected_columns: list[str] = Field(default_factory=list)


class TopologicalQualityResult(BaseModel):
    """Comprehensive topological quality assessment.

    This is the Pydantic source of truth for topological quality metrics.
    Gets serialized to TopologicalQualityMetrics.topology_data JSONB field.
    """

    table_id: str
    table_name: str

    # Betti numbers
    betti_numbers: BettiNumbers

    # Persistence diagrams
    persistence_diagrams: list[PersistenceDiagram] = Field(default_factory=list)

    # Detected cycles
    persistent_cycles: list[CycleDetection] = Field(default_factory=list)

    # Stability
    stability: StabilityAnalysis | None = None

    # Complexity metrics
    structural_complexity: int
    persistent_entropy: float | None = None
    orphaned_components: int
    complexity_trend: str | None = None
    complexity_within_bounds: bool = True

    # Historical complexity context
    complexity_mean: float | None = None
    complexity_std: float | None = None
    complexity_z_score: float | None = None

    # Quality assessment
    has_anomalies: bool = False
    anomalies: list[TopologicalAnomaly] = Field(default_factory=list)
    anomalous_cycles: list[CycleDetection] = Field(default_factory=list)
    quality_warnings: list[str] = Field(default_factory=list)
    topology_description: str | None = None
