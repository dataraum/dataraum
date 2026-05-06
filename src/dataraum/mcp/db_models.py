"""MCP server-state database models.

Holds workspace-level pointers used to bootstrap session state on each
`call_tool` invocation. Lives in the workspace database (not a per-session
database), because the active-session lookup happens before any session
manager is opened.
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import DateTime, String
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
