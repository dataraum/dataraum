"""Statistical Profile Database Models.

SQLAlchemy model for statistical profiling persistence:
- StatisticalProfile: Column-level statistical metrics

StatisticalQualityMetrics lives in quality_db_models.py (owned by statistical_quality phase).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from sqlalchemy import (
    JSON,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dataraum.storage import Base

if TYPE_CHECKING:
    from dataraum.storage import Column


class StatisticalProfile(Base):
    """Statistical profile of a column.

    HYBRID STORAGE APPROACH:
    - Structured fields: Queryable core dimensions (counts, ratios, flags)
    - JSONB field: Full Pydantic ColumnProfile model for flexibility

    This allows:
    - Fast queries on core metrics (null_ratio, cardinality_ratio)
    - Schema flexibility for experimentation
    - Zero mapping code (Pydantic handles serialization)
    """

    __tablename__ = "statistical_profiles"

    profile_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    column_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE"), nullable=False
    )
    profiled_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Layer indicator: "raw" or "typed"
    # Determines which stage produced this profile
    layer: Mapped[str] = mapped_column(String, nullable=False, default="raw")

    # STRUCTURED: Queryable core dimensions
    total_count: Mapped[int] = mapped_column(Integer, nullable=False)
    null_count: Mapped[int] = mapped_column(Integer, nullable=False)
    distinct_count: Mapped[int | None] = mapped_column(Integer)
    null_ratio: Mapped[float | None] = mapped_column(Float)
    cardinality_ratio: Mapped[float | None] = mapped_column(Float)

    # Flags for filtering (fast queries)
    is_unique: Mapped[bool | None] = mapped_column(Integer)  # All values unique (potential PK)
    is_numeric: Mapped[bool | None] = mapped_column(Integer)  # Has numeric stats

    # JSONB: Full Pydantic ColumnProfile model
    # Stores: numeric_stats, string_stats, histogram, top_values
    profile_data: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)

    # Relationships
    column: Mapped[Column] = relationship(back_populates="statistical_profiles")


# =============================================================================
# Indexes for efficient queries
# =============================================================================

Index(
    "idx_statistical_profiles_column",
    StatisticalProfile.column_id,
    StatisticalProfile.profiled_at.desc(),
)
