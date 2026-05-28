"""Postgres-backed overlay resolver for the worker substrate (DAT-343).

Hooks :mod:`dataraum.core.overlay`'s resolver pointer up to the worker's
single :class:`~dataraum.core.connections.ConnectionManager`, so every
``load_yaml_config`` / ``load_phase_config`` call from inside an activity
gets the workspace's active overlay rows merged onto the base YAML.

Lives in ``server/`` (not ``core/``) on purpose: ``core/overlay.py`` is
substrate that knows nothing about connections, and the worker's
substrate-side wiring already lives under ``server/`` (storage, workspace).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import asc, select

from dataraum.core.logging import get_logger
from dataraum.core.overlay import OverlayRow, set_overlay_resolver
from dataraum.storage import ConfigOverlay

if TYPE_CHECKING:
    from dataraum.core.connections import ConnectionManager

logger = get_logger(__name__)


def install_overlay_resolver(manager: ConnectionManager) -> None:
    """Wire the Postgres-backed resolver into ``dataraum.core.overlay``.

    Called once from :func:`bootstrap_worker_substrate` after the
    ConnectionManager is initialized and the active workspace is
    bootstrapped. After this point every ``load_yaml_config`` call goes
    through the layered read: base YAML + active overlay rows for the
    workspace.

    The resolver leases a short-lived session each call and returns rows
    ordered by ``created_at ASC`` (matches the appliers' last-write-wins
    semantics for keyed payloads). Workspace scope is implicit in the
    ``ws_<id>`` schema the ConfigOverlay table lives in — schema-per-
    workspace means the manager's connection already targets the right
    schema and no per-row workspace filter is needed.
    """

    def resolver() -> list[OverlayRow]:
        with manager.session_scope() as session:
            rows = session.execute(
                select(ConfigOverlay.type, ConfigOverlay.payload)
                .where(ConfigOverlay.superseded_at.is_(None))
                .order_by(asc(ConfigOverlay.created_at))
            ).all()
        return [OverlayRow(type=r.type, payload=r.payload or {}) for r in rows]

    set_overlay_resolver(resolver)
    logger.info("overlay_resolver_installed")


def uninstall_overlay_resolver() -> None:
    """Drop the registered resolver (worker shutdown / tests)."""
    set_overlay_resolver(None)
    logger.info("overlay_resolver_uninstalled")
