"""Temporal analysis database models.

SQLAlchemy models for persisting temporal analysis results.
Uses hybrid storage: structured fields for queries + JSONB for full data.

- TemporalColumnProfile: Per-column temporal analysis (like StatisticalProfile)
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import JSON, Boolean, DateTime, Float, ForeignKey, Index, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dataraum.storage import Base

if TYPE_CHECKING:
    from dataraum.storage import Column


class TemporalColumnProfile(Base):
    """Per-column temporal analysis profile.

    Similar to StatisticalProfile but for temporal characteristics.

    HYBRID STORAGE APPROACH:
    - Structured fields: Queryable dimensions (IDs, timestamps, key metrics)
    - JSONB field: Full Pydantic model for flexibility

    This allows:
    - Fast queries on core dimensions
    - Schema flexibility for experimentation
    - Zero mapping code (Pydantic handles serialization)
    """

    __tablename__ = "temporal_column_profiles"

    profile_id: Mapped[str] = mapped_column(String, primary_key=True)
    column_id: Mapped[str] = mapped_column(ForeignKey("columns.column_id"), nullable=False)
    profiled_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    # Relationships
    column: Mapped[Column] = relationship(back_populates="temporal_profiles")

    # STRUCTURED: Queryable core dimensions
    min_timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    max_timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    detected_granularity: Mapped[str] = mapped_column(String, nullable=False)
    completeness_ratio: Mapped[float | None] = mapped_column(Float)

    # Flags for filtering (fast queries)
    has_seasonality: Mapped[bool | None] = mapped_column(Boolean)
    has_trend: Mapped[bool | None] = mapped_column(Boolean)
    is_stale: Mapped[bool | None] = mapped_column(Boolean)

    # JSONB: Full TemporalAnalysisResult model
    profile_data: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)


# Index for efficient column lookups
Index("idx_temporal_profiles_column", TemporalColumnProfile.column_id)


__all__ = [
    "TemporalColumnProfile",
]
