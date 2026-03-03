"""SQLAlchemy models for business cycle detection.

Contains database models for persisting detected business cycles.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage import Base


class DetectedBusinessCycle(Base):
    """A detected business cycle.

    Stores the details of each detected cycle including its type,
    stages, entity flows, and completion metrics.
    """

    __tablename__ = "detected_business_cycles"
    __table_args__ = (Index("idx_detected_cycles_source", "source_id"),)

    cycle_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    source_id: Mapped[str] = mapped_column(
        ForeignKey("sources.source_id", ondelete="CASCADE"), nullable=False
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


__all__ = [
    "DetectedBusinessCycle",
]
