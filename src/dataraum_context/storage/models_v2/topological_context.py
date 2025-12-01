"""Topological Context Models (Pillar 2).

These models store topological quality metrics derived from TDA (Topological Data Analysis).
Topology provides insights into structural patterns, cycles, and relationships in data.
"""

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import JSON, Boolean, DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from dataraum_context.storage.models_v2.base import Base


class TopologicalQualityMetrics(Base):
    """Topological quality metrics for a table.

    Tracks structural features like Betti numbers, persistence, and stability.
    These metrics help detect structural anomalies and track topology changes over time.
    """

    __tablename__ = "topological_quality_metrics"

    metric_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    table_id: Mapped[str] = mapped_column(
        ForeignKey("tables.table_id", ondelete="CASCADE"), nullable=False
    )
    computed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Betti numbers (homology dimensions)
    betti_0: Mapped[int | None] = mapped_column(Integer)  # Connected components
    betti_1: Mapped[int | None] = mapped_column(Integer)  # Cycles / holes
    betti_2: Mapped[int | None] = mapped_column(Integer)  # Voids / cavities

    # Persistence metrics
    persistent_entropy: Mapped[float | None] = mapped_column(Float)  # Complexity measure
    max_persistence_h0: Mapped[float | None] = mapped_column(Float)  # Longest-lived component
    max_persistence_h1: Mapped[float | None] = mapped_column(Float)  # Longest-lived cycle

    # Persistence diagrams stored as JSON: [{"dimension": 0, "birth": 0.1, "death": 0.5}, ...]
    persistence_diagrams: Mapped[dict[str, Any] | None] = mapped_column(JSON)

    # Stability metrics (comparison with previous period)
    bottleneck_distance: Mapped[float | None] = mapped_column(Float)  # Distance from previous
    homologically_stable: Mapped[bool | None] = mapped_column(Boolean)  # Within threshold?

    # Complexity metrics
    structural_complexity: Mapped[int | None] = mapped_column(Integer)  # Sum of Betti numbers
    complexity_trend: Mapped[str | None] = mapped_column(
        String
    )  # 'increasing', 'stable', 'decreasing'
    complexity_within_bounds: Mapped[bool | None] = mapped_column(Boolean)  # Historical norms

    # Anomaly detection
    anomalous_cycles: Mapped[dict[str, Any] | None] = mapped_column(JSON)  # Unexpected flow patterns
    orphaned_components: Mapped[int | None] = mapped_column(Integer)  # Disconnected subgraphs

    # Summary for LLM context
    topology_description: Mapped[str | None] = mapped_column(String)
    quality_warnings: Mapped[dict[str, Any] | None] = mapped_column(JSON)  # List of warning strings


class PersistentCycle(Base):
    """Individual persistent cycle detected in the data.

    Cycles represent circular relationships or flows (e.g., money flow cycles in financial data).
    Tracking individual cycles helps identify domain-specific patterns.
    """

    __tablename__ = "persistent_cycles"

    cycle_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    metric_id: Mapped[str] = mapped_column(
        ForeignKey("topological_quality_metrics.metric_id", ondelete="CASCADE"),
        nullable=False,
    )

    # Cycle properties
    dimension: Mapped[int] = mapped_column(Integer, nullable=False)  # Usually 1 for cycles
    birth: Mapped[float] = mapped_column(Float, nullable=False)  # When cycle appears
    death: Mapped[float] = mapped_column(Float, nullable=False)  # When cycle disappears
    persistence: Mapped[float] = mapped_column(Float, nullable=False)  # death - birth

    # Involved entities (columns/tables)
    involved_columns: Mapped[dict[str, Any] | None] = mapped_column(JSON)  # List of column IDs

    # Domain interpretation
    cycle_type: Mapped[str | None] = mapped_column(
        String
    )  # 'money_flow', 'order_fulfillment', etc.
    is_anomalous: Mapped[bool] = mapped_column(Boolean, default=False)
    anomaly_reason: Mapped[str | None] = mapped_column(String)

    # Lifecycle tracking
    first_detected: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    last_seen: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )


class StructuralComplexityHistory(Base):
    """Historical tracking of structural complexity.

    Tracks Betti numbers and complexity over time to detect trends and anomalies.
    This enables baseline comparison and drift detection.
    """

    __tablename__ = "structural_complexity_history"

    history_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    table_id: Mapped[str] = mapped_column(
        ForeignKey("tables.table_id", ondelete="CASCADE"), nullable=False
    )
    measured_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Snapshot of Betti numbers
    betti_0: Mapped[int] = mapped_column(Integer, nullable=False)
    betti_1: Mapped[int] = mapped_column(Integer, nullable=False)
    betti_2: Mapped[int | None] = mapped_column(Integer)

    # Derived complexity
    total_complexity: Mapped[int] = mapped_column(Integer, nullable=False)  # Sum of Betti

    # Statistical bounds (from historical data)
    complexity_mean: Mapped[float | None] = mapped_column(Float)
    complexity_std: Mapped[float | None] = mapped_column(Float)
    complexity_z_score: Mapped[float | None] = mapped_column(Float)  # Current vs historical
