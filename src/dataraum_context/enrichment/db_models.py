"""SQLAlchemy models for enrichment domain.

This module contains all database models for the enrichment subsystem:
- Semantic Context (SemanticAnnotation, TableEntity) - imported from analysis/semantic
- Relationship Models (Relationship, JoinPath) - Relationship imported from analysis/relationships
- Topological Context (TopologicalQualityMetrics, MultiTableTopologyMetrics, BusinessCycleClassification)

NOTE: Temporal Context models have been moved to analysis/temporal/db_models.py
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import (
    JSON,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

# Import models from their new canonical locations
from dataraum_context.analysis.relationships.db_models import Relationship
from dataraum_context.analysis.semantic.db_models import SemanticAnnotation, TableEntity
from dataraum_context.analysis.topology.db_models import (
    BusinessCycleClassification,
    MultiTableTopologyMetrics,
    TopologicalQualityMetrics,
)
from dataraum_context.storage import Base

# =============================================================================
# Relationship Models (Pillar 2)
# =============================================================================
# Relationship is imported from analysis/relationships/db_models.py


class JoinPath(Base):
    """Computed join paths between tables.

    Pre-computed paths for efficient multi-table queries.
    Updated when relationships change.
    """

    __tablename__ = "join_paths"
    __table_args__ = (
        UniqueConstraint("from_table_id", "to_table_id", "path_steps", name="uq_join_path"),
    )

    path_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))

    from_table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), nullable=False)
    to_table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), nullable=False)

    # Path definition
    path_steps: Mapped[dict[str, Any]] = mapped_column(
        JSON, nullable=False
    )  # List of relationship IDs forming the path
    path_length: Mapped[int] = mapped_column(Integer, nullable=False)  # Number of hops
    total_confidence: Mapped[float | None] = mapped_column(
        Float
    )  # Product of relationship confidences

    computed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )


# =============================================================================
# Topological Context Models (Pillar 2)
# Re-exported from analysis/topology/db_models.py for backward compatibility
# =============================================================================
# TopologicalQualityMetrics, MultiTableTopologyMetrics, BusinessCycleClassification
# are imported at the top of the file


# =============================================================================
# NOTE: Temporal Context Models moved to analysis/temporal/db_models.py
# Import from dataraum_context.analysis.temporal instead
# =============================================================================


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Semantic Context
    "SemanticAnnotation",
    "TableEntity",
    # Relationship Models
    "Relationship",
    "JoinPath",
    # Topological Context
    "TopologicalQualityMetrics",
    "MultiTableTopologyMetrics",
    "BusinessCycleClassification",
]
