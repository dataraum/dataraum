"""Investigation session database models.

SQLAlchemy models for tracking analytical investigation sessions and
individual tool invocations within them. Provides the audit trail for reproducibility,
outcome justification, and investigation pattern analysis.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import JSON, DateTime, Float, ForeignKey, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dataraum.storage.base import Base


class InvestigationSession(Base):
    """A bounded run over a set of tables.

    A session owns one run's measurements: an ``add_source`` run is a session
    whose table set is one source's typed tables; a ``begin_session`` run is a
    session whose table set spans sources. The composed tables are reached
    through the :class:`SessionTable` M:N association — a session never
    references a source directly. A session's source(s) are *derived* from its
    tables (see :func:`sources_for_session`), the same way
    :class:`~dataraum.analysis.relationships.db_models.Relationship` derives
    source from its from/to tables rather than storing it.
    """

    __tablename__ = "investigation_sessions"

    session_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))

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
    vertical: Mapped[str | None] = mapped_column(String)

    # Outcome (from deliver/refuse/escalate)
    outcome_summary: Mapped[str | None] = mapped_column(String)
    outcome_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON)

    # Denormalized metrics
    step_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Relationships
    steps: Mapped[list[InvestigationStep]] = relationship(
        "InvestigationStep",
        back_populates="session",
        cascade="all, delete-orphan",
        order_by="InvestigationStep.ordinal",
    )
    table_links: Mapped[list[SessionTable]] = relationship(
        "SessionTable",
        back_populates="session",
        cascade="all, delete-orphan",
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


class SessionTable(Base):
    """Association linking a session to the typed tables it composes (M:N).

    The source(s) of a session are derived by joining through this table to
    ``Table.source_id`` — the session carries no ``source_id`` of its own. An
    ``add_source`` run links one source's typed tables; a ``begin_session`` run
    links a selection that may span sources.
    """

    __tablename__ = "session_tables"

    session_id: Mapped[str] = mapped_column(
        ForeignKey("investigation_sessions.session_id", ondelete="CASCADE"),
        primary_key=True,
    )
    table_id: Mapped[str] = mapped_column(
        ForeignKey("tables.table_id", ondelete="CASCADE"),
        primary_key=True,
    )

    session: Mapped[InvestigationSession] = relationship(
        "InvestigationSession", back_populates="table_links"
    )


Index("idx_inv_step_tool", InvestigationStep.tool_name)
Index("idx_inv_step_target", InvestigationStep.target)
Index("idx_session_tables_table", SessionTable.table_id)
