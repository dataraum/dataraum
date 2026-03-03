"""Statistical Quality Database Models.

SQLAlchemy model for statistical quality assessment:
- StatisticalQualityMetrics: Benford's Law compliance, outlier detection

Split from db_models.py to give the statistical_quality phase its own db_models module.
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


class StatisticalQualityMetrics(Base):
    """Statistical quality assessment for a column.

    HYBRID STORAGE APPROACH:
    - Structured fields: Queryable quality indicators (flags, scores, key ratios)
    - JSONB field: Full quality analysis results for flexibility

    Advanced quality metrics that may be expensive to compute:
    - Benford's Law compliance (fraud detection for financial amounts)
    - Outlier detection (IQR + Modified Z-Score)

    Note: Distribution stability (KS test) is handled by temporal quality module.
    """

    __tablename__ = "statistical_quality_metrics"

    metric_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    column_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE"), nullable=False
    )
    computed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # STRUCTURED: Queryable quality indicators
    # Flags for filtering (fast queries)
    benford_compliant: Mapped[bool | None] = mapped_column(Integer)
    has_outliers: Mapped[bool | None] = mapped_column(Integer)

    # Key metrics for sorting/filtering
    iqr_outlier_ratio: Mapped[float | None] = mapped_column(Float)
    zscore_outlier_ratio: Mapped[float | None] = mapped_column(Float)

    # JSONB: Full quality analysis results
    # Stores: Benford analysis, outlier details, quality issues
    quality_data: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)

    # Relationships
    column: Mapped[Column] = relationship(back_populates="statistical_quality_metrics")


Index(
    "idx_statistical_quality_column",
    StatisticalQualityMetrics.column_id,
    StatisticalQualityMetrics.computed_at.desc(),
)
