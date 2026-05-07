"""MCP server-state database models.

Holds workspace-level pointers used to bootstrap session state on each
`call_tool` invocation. Lives in the workspace database (not a per-session
database), because the active-session lookup happens before any session
manager is opened.
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import JSON, DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage.base import Base


class ActiveSession(Base):
    """Pointer to the currently-active investigation session.

    Resolves the chicken-and-egg lookup: every `call_tool` needs to know
    which session is active before it can open the corresponding session
    database. This pointer lives in the workspace DB (always available)
    and identifies which `sessions/{fingerprint}/` directory to open.

    At most one row exists at any time. `begin_session` writes the row
    after the session DB is fully initialized; `end_session` deletes it
    after archiving the session directory.
    """

    __tablename__ = "active_session"

    # Single-row enforcement: primary key is constant.
    id: Mapped[int] = mapped_column(primary_key=True, default=1)

    session_id: Mapped[str] = mapped_column(String, nullable=False)
    fingerprint: Mapped[str] = mapped_column(String, nullable=False)
    started_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )


class ArchivedSession(Base):
    """Index of finalized sessions available for resume.

    Written by `end_session` once the session directory has been moved to
    ``archive/{session_id}/``. ``resume_session`` reads from this table to
    list available archives and to look up the metadata (fingerprint,
    contract, vertical) needed to restore a session without scanning every
    archived metadata.db.

    The row is consumed (deleted) when the session is resumed — the data
    moves back into the active session DB.
    """

    __tablename__ = "archived_sessions"

    session_id: Mapped[str] = mapped_column(String, primary_key=True)
    fingerprint: Mapped[str] = mapped_column(String, nullable=False)
    intent: Mapped[str] = mapped_column(String, nullable=False)
    contract: Mapped[str] = mapped_column(String, nullable=False)
    vertical: Mapped[str | None] = mapped_column(String, nullable=True)
    outcome: Mapped[str] = mapped_column(String, nullable=False)
    summary: Mapped[str | None] = mapped_column(String, nullable=True)
    source_names: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    ended_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    step_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
