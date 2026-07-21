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

from dataraum.analysis.statistics.models import BENFORD_STATUSES
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
    # One metric row per column PER RUN (DAT-413): widened to ``(column_id, run_id)``
    # so the writer can upsert idempotently under Temporal at-least-once retries
    # and two coexisting runs' rows don't collide.
    __table_args__ = (
        UniqueConstraint("column_id", "run_id", name="uq_statistical_quality_metrics_column_run"),
        # Benford applicability vocabulary (DAT-843): derived from the single-home
        # constants in ``analysis.statistics.models`` (strict Pydantic at the
        # writer; this is the DB backstop). Sorted for a deterministic dump.
        CheckConstraint(
            "benford_status IS NULL OR benford_status IN ("
            + ", ".join(f"'{v}'" for v in sorted(BENFORD_STATUSES))
            + ")",
            name="benford_status",
        ),
    )

    metric_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    column_id: Mapped[str] = mapped_column(ForeignKey("columns.column_id"), nullable=False)
    # Snapshot version axis (DAT-413): the run that wrote this row.
    run_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    computed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # STRUCTURED: Queryable quality indicators
    # Flags for filtering (fast queries)
    # Typed Benford applicability state (DAT-843): 'compliant' / 'violating' /
    # 'not_applicable'; NULL = not computed (n < 100 or the test errored). The
    # engine's truth — the boolean below is its cockpit-facing projection
    # (True/False only for a measured verdict, else NULL).
    benford_status: Mapped[str | None] = mapped_column(String)
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
