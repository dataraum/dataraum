"""Temporal analysis database models.

SQLAlchemy models for persisting temporal analysis results.
Uses hybrid storage: structured fields for queries + JSONB for full data.

- TemporalColumnProfile: Per-column temporal analysis (like StatisticalProfile)
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dataraum.storage import Base

if TYPE_CHECKING:
    from dataraum.storage import Column


class TemporalColumnProfile(Base):
    """Per-column temporal analysis profile.

    Similar to StatisticalProfile but for temporal characteristics. Every computed
    fact is a typed served column — there is NO write-only ``profile_data`` blob
    (DAT-783 promoted the load-bearing coverage facts to flat columns + the ``gaps``
    JSON interior, and deleted the WRONG fiscal/update-frequency components rather
    than persisting them prettier).
    """

    __tablename__ = "temporal_column_profiles"
    # One profile per column PER RUN (DAT-413): widened to ``(column_id, run_id)``
    # so the writer can upsert idempotently under Temporal at-least-once retries
    # and two coexisting runs' rows don't collide.
    __table_args__ = (
        UniqueConstraint("column_id", "run_id", name="uq_temporal_column_profiles_column_run"),
    )

    profile_id: Mapped[str] = mapped_column(String, primary_key=True)
    column_id: Mapped[str] = mapped_column(ForeignKey("columns.column_id"), nullable=False)
    # Snapshot version axis (DAT-413): the run that wrote this row.
    run_id: Mapped[str] = mapped_column(String, nullable=False)
    profiled_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    # Relationships
    column: Mapped[Column] = relationship(back_populates="temporal_profiles")

    # Data window
    min_timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    max_timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    span_days: Mapped[float] = mapped_column(Float, nullable=False)

    # Detected cadence (the vocabulary is the config granularity set + irregular/unknown)
    detected_granularity: Mapped[str] = mapped_column(String, nullable=False)
    granularity_confidence: Mapped[float] = mapped_column(Float, nullable=False)

    # Coverage / completeness (from the DISTINCT-timestamp pass)
    completeness_ratio: Mapped[float | None] = mapped_column(Float)
    expected_periods: Mapped[int | None] = mapped_column(Integer)
    actual_periods: Mapped[int | None] = mapped_column(Integer)
    gap_count: Mapped[int | None] = mapped_column(Integer)
    largest_gap_days: Mapped[float | None] = mapped_column(Float)

    # Staleness: freshest observation old relative to the detected cadence.
    is_stale: Mapped[bool | None] = mapped_column(Boolean)

    # JSON interior: the bounded list of significant gaps (largest first). Each entry
    # is a strict ``TemporalGapInfo`` submodel validated at the writer (DAT-783).
    gaps: Mapped[list[dict[str, Any]]] = mapped_column(JSON, nullable=False, default=list)


# Index for efficient column lookups
Index("idx_temporal_profiles_column", TemporalColumnProfile.column_id)


__all__ = [
    "TemporalColumnProfile",
]
