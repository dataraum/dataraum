"""SQLAlchemy models for business cycle detection.

Contains database models for persisting detected business cycles
and their analysis metadata.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dataraum.storage import Base

if TYPE_CHECKING:
    pass


class BusinessCycleAnalysisRun(Base):
    """A business cycle analysis run across one or more tables.

    Stores metadata about the analysis run including timing,
    configuration, and summary statistics.
    """

    __tablename__ = "business_cycle_analysis_runs"

    analysis_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))

    # Scope
    table_ids: Mapped[list[str]] = mapped_column(JSON, nullable=False)

    # Timing
    started_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    duration_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Summary
    total_cycles_detected: Mapped[int] = mapped_column(Integer, default=0)
    high_value_cycles: Mapped[int] = mapped_column(Integer, default=0)
    overall_cycle_health: Mapped[float | None] = mapped_column(Float, nullable=True)

    # LLM metadata
    llm_model: Mapped[str | None] = mapped_column(String, nullable=True)
    tool_calls_count: Mapped[int] = mapped_column(Integer, default=0)

    # Business interpretation
    business_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    detected_processes: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    data_quality_observations: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    recommendations: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)

    # Context provided to agent
    context_summary: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    # Relationships
    detected_cycles: Mapped[list[DetectedBusinessCycle]] = relationship(
        back_populates="analysis_run", cascade="all, delete-orphan"
    )


class DetectedBusinessCycle(Base):
    """A detected business cycle within an analysis run.

    Stores the details of each detected cycle including its type,
    stages, entity flows, and completion metrics.
    """

    __tablename__ = "detected_business_cycles"

    cycle_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    analysis_id: Mapped[str] = mapped_column(
        ForeignKey("business_cycle_analysis_runs.analysis_id", ondelete="CASCADE"),
        nullable=False,
    )

    # Classification
    cycle_name: Mapped[str] = mapped_column(String, nullable=False)
    cycle_type: Mapped[str] = mapped_column(String, nullable=False)  # Raw LLM output
    canonical_type: Mapped[str | None] = mapped_column(
        String, nullable=True
    )  # Mapped to vocabulary
    is_known_type: Mapped[bool] = mapped_column(Boolean, default=False)  # True if in vocabulary
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    business_value: Mapped[str] = mapped_column(String, default="medium")
    confidence: Mapped[float] = mapped_column(Float, default=0.0)

    # Structure
    tables_involved: Mapped[list[str]] = mapped_column(JSON, default=list)
    stages: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list)
    entity_flows: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list)

    # Status tracking
    status_table: Mapped[str | None] = mapped_column(String, nullable=True)
    status_column: Mapped[str | None] = mapped_column(String, nullable=True)
    completion_value: Mapped[str | None] = mapped_column(String, nullable=True)

    # Metrics
    total_records: Mapped[int | None] = mapped_column(Integer, nullable=True)
    completed_cycles: Mapped[int | None] = mapped_column(Integer, nullable=True)
    completion_rate: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Evidence
    evidence: Mapped[list[str]] = mapped_column(JSON, default=list)

    # Timestamps
    detected_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Relationships
    analysis_run: Mapped[BusinessCycleAnalysisRun] = relationship(back_populates="detected_cycles")


__all__ = [
    "BusinessCycleAnalysisRun",
    "DetectedBusinessCycle",
]
