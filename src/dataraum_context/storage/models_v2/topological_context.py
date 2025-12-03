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

    HYBRID STORAGE APPROACH:
    - Structured fields: Queryable core dimensions (Betti numbers, flags)
    - JSONB field: Full topological analysis results

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

    # STRUCTURED: Queryable core dimensions
    # Betti numbers (homology dimensions)
    betti_0: Mapped[int | None] = mapped_column(Integer)  # Connected components
    betti_1: Mapped[int | None] = mapped_column(Integer)  # Cycles / holes
    betti_2: Mapped[int | None] = mapped_column(Integer)  # Voids / cavities

    # Overall complexity
    structural_complexity: Mapped[int | None] = mapped_column(Integer)  # Sum of Betti numbers
    orphaned_components: Mapped[int | None] = mapped_column(Integer)  # Disconnected subgraphs

    # Flags for filtering
    homologically_stable: Mapped[bool | None] = mapped_column(Boolean)  # Within threshold?
    has_cycles: Mapped[bool | None] = mapped_column(Boolean)  # betti_1 > 0
    has_anomalies: Mapped[bool | None] = mapped_column(Boolean)

    # JSONB: Full topological analysis
    # Stores: persistence diagrams, stability metrics, complexity history, anomalous cycles, etc.
    topology_data: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)


class MultiTableTopologyMetrics(Base):
    """Multi-table topology analysis results.

    Tracks cross-table relationships, graph topology, and business process detection
    across multiple related tables. Enables historical tracking of schema evolution,
    relationship changes, and business process patterns over time.
    """

    __tablename__ = "multi_table_topology_metrics"

    analysis_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    computed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Table IDs included in analysis (JSON array of strings)
    table_ids: Mapped[list[str]] = mapped_column(JSON, nullable=False)

    # STRUCTURED: Queryable cross-table metrics
    cross_table_cycles: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    graph_betti_0: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1
    )  # Graph connectivity
    relationship_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Flags for filtering
    has_cross_table_cycles: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_connected_graph: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True
    )  # graph_betti_0 == 1

    # JSONB: Full multi-table analysis result
    # Stores: per_table results (references), cross_table details, domain_analysis, business_processes
    analysis_data: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)


# DEPRECATED: These models are replaced by JSONB storage in TopologicalQualityMetrics.topology_data
#
# PersistentCycle and StructuralComplexityHistory data is now stored as JSON within the
# topology_data field. This provides schema flexibility and reduces the number of tables/joins.
#
# Migration path: These tables can be dropped once all code is updated to use the hybrid
# storage approach.


class PersistentCycle(Base):
    """Individual persistent cycle detected in the data.

    DEPRECATED: Cycles are now stored in TopologicalQualityMetrics.topology_data JSONB field.

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

    DEPRECATED: Complexity history is now stored in TopologicalQualityMetrics.topology_data JSONB field.

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
