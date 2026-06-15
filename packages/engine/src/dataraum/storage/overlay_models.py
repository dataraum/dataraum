"""Config overlay SQLAlchemy model (DAT-343).

Stores per-workspace teach edits as JSON rows that the engine's config
loaders merge over the baked-in YAML from ``packages/dataraum-config``.
One row = one teach mutation; rows are soft-superseded for undo
(``superseded_at IS NOT NULL`` excludes them from layered reads).

Lives in the workspace's ``ws_<id>`` schema by virtue of the schema-per-
workspace setup — workspace identity is implicit in the schema name; no
``workspace_id`` column. The cockpit writes rows via its (otherwise
read-only) Drizzle metadata client; the engine reads them via
:mod:`dataraum.core.overlay`. If multi-workspace shared-schema ever lands
(DAT-357), the column comes back with a simple migration.

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
    type: Mapped[str] = mapped_column(String, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    superseded_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


# The loader hits this index for every config read with any active overlay
# rows: filter to unsuperseded, optionally by type. Workspace scope is
# implicit in the schema this table lives in (ws_<id>).
Index(
    "idx_config_overlay_active",
    ConfigOverlay.superseded_at,
    ConfigOverlay.type,
)
