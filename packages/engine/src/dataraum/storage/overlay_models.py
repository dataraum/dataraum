"""Config overlay SQLAlchemy model (DAT-343).

Stores per-workspace teach edits as JSON rows that the engine's config
loaders merge over the baked-in YAML from ``packages/dataraum-config``.
One row = one teach mutation; rows are soft-superseded for undo
(``superseded_at IS NOT NULL`` excludes them from layered reads).

Lives in the workspace's ``ws_<id>`` schema by virtue of the schema-per-
workspace setup — ``workspace_id`` is kept as a column for audit and for
the cockpit's Drizzle-side filter, but it's redundant with the connection's
search_path. The cockpit writes rows via its (otherwise read-only) Drizzle
metadata client; the engine reads them via ``dataraum.core.overlay``.

Single source of truth for the slice-1 teach surface — the legacy
``DataFix``/``ConfigInterpreter`` machinery it replaces is deleted in the
same PR (no backwards-compat shim).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import JSON, DateTime, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage.base import Base


class ConfigOverlay(Base):
    """One teach mutation pending or active for a workspace.

    Columns:
        overlay_id: uuid4 primary key.
        workspace_id: the owning workspace (redundant with schema; kept for audit).
        session_id: NULL for workspace-scoped teaches (``type_pattern``,
            ``null_value``, ``concept_property``); non-NULL for session-scoped
            teaches (slice-2+: ``metric``, ``validation``, ``cycle``).
        type: the teach type — one of the registered teach types
            (validated by the cockpit's write path, not by the DB).
        payload: per-type JSON payload; shape is owned by the matching
            applier in :mod:`dataraum.core.overlay`.
        created_at: insertion time; layered reads order by ASC and let the
            last write win for same-key payloads.
        superseded_at: undo marker. ``NULL`` = active; non-NULL excludes the
            row from layered reads. Layered reads never look at the value
            beyond null/non-null.
    """

    __tablename__ = "config_overlay"

    overlay_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    workspace_id: Mapped[str] = mapped_column(String, nullable=False)
    session_id: Mapped[str | None] = mapped_column(String, nullable=True)
    type: Mapped[str] = mapped_column(String, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    superseded_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


# The loader hits this index for every config read on a workspace with any
# active overlay rows: filter to unsuperseded, optionally by type.
Index(
    "idx_config_overlay_active",
    ConfigOverlay.workspace_id,
    ConfigOverlay.superseded_at,
    ConfigOverlay.type,
)
