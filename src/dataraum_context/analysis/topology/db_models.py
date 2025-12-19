"""SQLAlchemy models for topological analysis.

This module contains database models for storing topological analysis results:
- TopologicalQualityMetrics: Single-table topology metrics
- MultiTableTopologyMetrics: Cross-table topology analysis
- BusinessCycleClassification: LLM-classified business cycles
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
)
from sqlalchemy.orm import Mapped, mapped_column

from dataraum_context.storage import Base


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


class BusinessCycleClassification(Base):
    """LLM-classified business cycle from cross-table topology.

    Stores business process cycles detected in multi-table analysis,
    classified by LLM (e.g., accounts_receivable_cycle, revenue_cycle).

    These classifications provide context for:
    - Filter generation (which business process is this data part of?)
    - Quality assessment (is the cycle complete?)
    - Downstream calculations (which tables feed into which calculations?)
    """

    __tablename__ = "business_cycle_classifications"

    cycle_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    analysis_id: Mapped[str] = mapped_column(
        ForeignKey("multi_table_topology_metrics.analysis_id", ondelete="CASCADE"),
        nullable=False,
    )
    classified_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Classification result
    cycle_type: Mapped[str] = mapped_column(
        String, nullable=False
    )  # e.g., "accounts_receivable_cycle"
    confidence: Mapped[float] = mapped_column(Float, nullable=False)  # 0.0 to 1.0
    business_value: Mapped[str] = mapped_column(String, nullable=False)  # "high", "medium", "low"
    completeness: Mapped[str] = mapped_column(
        String, nullable=False
    )  # "complete", "partial", "incomplete"

    # Tables involved in this cycle (JSON array of table_ids)
    table_ids: Mapped[list[str]] = mapped_column(JSON, nullable=False)

    # LLM explanation
    explanation: Mapped[str | None] = mapped_column(String, nullable=True)

    # Missing elements if incomplete
    missing_elements: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)

    # Source tracking
    llm_model: Mapped[str | None] = mapped_column(
        String, nullable=True
    )  # Model used for classification


__all__ = [
    "TopologicalQualityMetrics",
    "MultiTableTopologyMetrics",
    "BusinessCycleClassification",
]
