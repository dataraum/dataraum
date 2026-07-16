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
    CheckConstraint,
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
    # One profile per column PER RUN (DAT-413): widened to ``(column_id, run_id)``
    # so the writer can upsert idempotently under Temporal at-least-once retries
    # and two coexisting runs' rows don't collide.
    __table_args__ = (
        UniqueConstraint("column_id", "run_id", name="uq_statistical_profiles_column_run"),
        # Closed-vocabulary enforcement (DAT-802 audit): every writer — production
        # (the primary profiling pass, ``analysis/statistics/profiler.py``, which
        # REQUIRES a typed table, and the enriched-views dimension profiling,
        # ``enriched_views_phase.py``) and every test fixture, engine-wide —
        # produces only 'typed' / 'enriched'. The former ``default="raw"`` was
        # vestigial (verified: zero constructors, production or test, ever
        # omitted ``layer=``) and actively misleading (implied 'raw' was a
        # legitimate resting state); removed rather than kept dead, so a future
        # caller that forgets to set ``layer`` fails loud immediately (a clear
        # NOT NULL error) instead of silently defaulting to a value the CHECK
        # would reject anyway.
        CheckConstraint("layer IN ('typed', 'enriched')", name="layer"),
    )

    profile_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    column_id: Mapped[str] = mapped_column(ForeignKey("columns.column_id"), nullable=False)
    # Snapshot version axis (DAT-413): the run that wrote this row.
    run_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    profiled_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Which stage produced this profile. Closed vocab: see ck_statistical_profiles_layer.
    # No default: every writer (production and test) must state it explicitly.
    layer: Mapped[str] = mapped_column(String, nullable=False)

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
