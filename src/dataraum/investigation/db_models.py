"""Investigation session database models.

SQLAlchemy models for tracking MCP investigation sessions and individual
tool invocations within them. Provides the audit trail for reproducibility,
outcome justification, and investigation pattern analysis.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import DateTime, Float, ForeignKey, Index, Integer, String
from sqlalchemy.dialects.sqlite import JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dataraum.storage.base import Base


class InvestigationSession(Base):
    """A bounded investigation session initiated by an MCP agent.

    Created by ``begin_session``, closed by ``deliver``, ``refuse``,
    or ``escalate``. Contains an ordered list of investigation steps.
    """

    __tablename__ = "investigation_sessions"

    session_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    source_id: Mapped[str] = mapped_column(ForeignKey("sources.source_id"), nullable=False)

    # Lifecycle
    status: Mapped[str] = mapped_column(String, nullable=False, default="active")
    started_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    ended_at: Mapped[datetime | None] = mapped_column(DateTime)
    duration_seconds: Mapped[float | None] = mapped_column(Float)

    # Intent (from begin_session)
    intent: Mapped[str] = mapped_column(String, nullable=False)
    contract: Mapped[str | None] = mapped_column(String)

    # Outcome (from deliver/refuse/escalate)
    outcome_summary: Mapped[str | None] = mapped_column(String)
    outcome_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON)

    # Denormalized metrics
    step_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Relationship
    steps: Mapped[list[InvestigationStep]] = relationship(
        "InvestigationStep",
        back_populates="session",
        cascade="all, delete-orphan",
        order_by="InvestigationStep.ordinal",
    )


class InvestigationStep(Base):
    """A single tool invocation within an investigation session."""

    __tablename__ = "investigation_steps"

    step_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    session_id: Mapped[str] = mapped_column(
        ForeignKey("investigation_sessions.session_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Identification
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    tool_name: Mapped[str] = mapped_column(String, nullable=False)

    # Input
    arguments: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)

    # Output
    status: Mapped[str] = mapped_column(String, nullable=False)  # success | error
    result_summary: Mapped[str | None] = mapped_column(String)
    error: Mapped[str | None] = mapped_column(String)

    # Timing
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    duration_seconds: Mapped[float] = mapped_column(Float, nullable=False)

    # Context (extracted from arguments for querying)
    target: Mapped[str | None] = mapped_column(String)
    dimension: Mapped[str | None] = mapped_column(String)

    # Relationship
    session: Mapped[InvestigationSession] = relationship(
        "InvestigationSession", back_populates="steps"
    )


Index("idx_inv_session_source", InvestigationSession.source_id)
Index("idx_inv_step_tool", InvestigationStep.tool_name)
Index("idx_inv_step_target", InvestigationStep.target)
