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
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage import Base


class DetectedBusinessCycle(Base):
    """A detected business cycle for one operating_model run.

    Stores the details of each detected cycle including its type,
    stages, entity flows, and completion metrics.

    Run-versioned (DAT-455): one row per ``(session, canonical_type, run)`` —
    the schema axis of the versioned-model consumer contract. Source-free past
    the add_source boundary: cycles are detected in operating_model over the
    session's typed tables (``tables_involved``), never scoped to a
    ``source_id``. A re-run supersedes by writing rows under its fresh
    ``run_id``; readers scope to the promoted ``operating_model`` head (or,
    in-run, to this run's id), never across runs.
    """

    __tablename__ = "detected_business_cycles"
    __table_args__ = (
        UniqueConstraint("session_id", "canonical_type", "run_id", name="uq_detected_cycle_run"),
    )

    cycle_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    session_id: Mapped[str] = mapped_column(
        ForeignKey("investigation_sessions.session_id"), nullable=False, index=True
    )
    run_id: Mapped[str] = mapped_column(String, nullable=False)

    # Classification
    cycle_name: Mapped[str] = mapped_column(String, nullable=False)
    cycle_type: Mapped[str] = mapped_column(String, nullable=False)  # Raw LLM output
    canonical_type: Mapped[str] = mapped_column(
        String, nullable=False
    )  # The declared cycle vocabulary key — the artifact identity
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
