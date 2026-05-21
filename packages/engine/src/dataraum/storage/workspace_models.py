"""Workspace model — single per-process bootstrap row.

The Workspace owns the writable config overlay directory. Slice 1 has
exactly one Workspace per server; the bootstrap step in
``dataraum.server.workspace`` picks the first existing row or creates a
``name="default"`` one. Multi-workspace UX lands in DAT-357.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import DateTime, String
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage.base import Base


class Workspace(Base):
    """A workspace owns a writable config overlay directory.

    ``config_dir`` is an absolute filesystem path under
    ``${DATARAUM_HOME}/workspaces/<workspace_id>/config/`` populated at
    bootstrap by copying from the read-only baked-in config root. Teach
    edits land in this directory.
    """

    __tablename__ = "workspaces"

    workspace_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )
    name: Mapped[str] = mapped_column(String, nullable=False)
    config_dir: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
