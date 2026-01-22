"""SQLAlchemy models for topological analysis.

This module contains database models for storing topological analysis results:
- TopologicalQualityMetrics: Single-table topology metrics

For cross-table schema analysis, see relationships/graph_topology.py.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
)
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage import Base


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


# Index for efficient table lookups
Index("idx_topology_metrics_table", TopologicalQualityMetrics.table_id)


__all__ = [
    "TopologicalQualityMetrics",
]
