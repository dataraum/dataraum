"""Quality context synthesis models (Pillar 5).

Aggregates quality metrics from all other pillars into a unified quality assessment
with standard data quality dimensions.
"""

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import JSON, DateTime, Float, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base


class QualityContext(Base):
    """Unified quality assessment for a source.

    Synthesizes quality metrics from all pillars:
    - Statistical quality (Pillar 1)
    - Topological quality (Pillar 2)
    - Temporal quality (Pillar 4)
    - Domain quality (Pillar 5)

    Provides standard data quality dimension scores.
    """

    __tablename__ = "quality_contexts"

    context_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    source_id: Mapped[UUID] = mapped_column(ForeignKey("sources.source_id"))
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    # Standard DQ dimensions (0-1 scale)
    completeness_score: Mapped[float] = mapped_column(Float)  # From statistical + temporal
    consistency_score: Mapped[float] = mapped_column(Float)  # From semantic + topological
    accuracy_score: Mapped[float] = mapped_column(Float)  # From domain-specific rules
    timeliness_score: Mapped[float] = mapped_column(Float)  # From temporal
    uniqueness_score: Mapped[float] = mapped_column(Float)  # From statistical

    # Overall quality score (weighted average)
    overall_score: Mapped[float] = mapped_column(Float)

    # Aggregated issues and recommendations
    critical_issues: Mapped[list[dict[str, Any]]] = mapped_column(JSON)
    warnings: Mapped[list[str]] = mapped_column(JSON)
    recommendations: Mapped[list[str]] = mapped_column(JSON)

    # Relationships
    source: Mapped["Source"] = relationship(back_populates="quality_contexts")
    dimension_details: Mapped[list["QualityDimensionDetail"]] = relationship(
        back_populates="quality_context", cascade="all, delete-orphan"
    )


class QualityDimensionDetail(Base):
    """Detailed breakdown of a quality dimension score.

    Explains how each dimension score was calculated from the various quality metrics.
    """

    __tablename__ = "quality_dimension_details"

    detail_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    context_id: Mapped[UUID] = mapped_column(ForeignKey("quality_contexts.context_id"))
    dimension: Mapped[str] = mapped_column(String)  # 'completeness', 'consistency', etc.

    # Score breakdown
    dimension_score: Mapped[float] = mapped_column(Float)
    component_scores: Mapped[dict[str, float]] = mapped_column(JSON)  # Which metrics contributed

    # Explanation
    calculation_method: Mapped[str] = mapped_column(String)
    contributing_metrics: Mapped[list[str]] = mapped_column(JSON)

    # Relationships
    quality_context: Mapped["QualityContext"] = relationship(back_populates="dimension_details")


class QualityIssueAggregate(Base):
    """Aggregated quality issues from all pillars.

    Consolidates issues from statistical, topological, temporal, and domain quality checks.
    """

    __tablename__ = "quality_issue_aggregates"

    issue_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    context_id: Mapped[UUID] = mapped_column(ForeignKey("quality_contexts.context_id"))

    # Issue classification
    issue_type: Mapped[str] = mapped_column(String)
    severity: Mapped[str] = mapped_column(String)  # 'minor', 'moderate', 'severe', 'critical'
    category: Mapped[str] = mapped_column(
        String
    )  # 'statistical', 'topological', 'temporal', 'domain'

    # Details
    description: Mapped[str] = mapped_column(String)
    affected_entities: Mapped[list[str]] = mapped_column(JSON)  # Tables, columns affected

    # Source
    source_pillar: Mapped[str] = mapped_column(String)  # Which pillar detected this
    source_metric_id: Mapped[UUID | None] = mapped_column(
        String, nullable=True
    )  # Reference to original metric

    # Recommendations
    recommendation: Mapped[str | None] = mapped_column(String, nullable=True)
    auto_fixable: Mapped[bool] = mapped_column(default=False)

    # Relationships
    quality_context: Mapped["QualityContext"] = relationship()


class QualityTrend(Base):
    """Quality trends over time.

    Tracks how quality scores evolve across multiple computations.
    """

    __tablename__ = "quality_trends"

    trend_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    source_id: Mapped[UUID] = mapped_column(ForeignKey("sources.source_id"))
    dimension: Mapped[str] = mapped_column(String)  # 'overall', 'completeness', etc.

    # Time series data
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    score: Mapped[float] = mapped_column(Float)

    # Change detection
    is_regression: Mapped[bool] = mapped_column(default=False)  # Score decreased significantly
    change_magnitude: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Relationships
    source: Mapped["Source"] = relationship()
