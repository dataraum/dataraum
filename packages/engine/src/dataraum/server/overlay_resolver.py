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

from sqlalchemy import asc, distinct, select

from dataraum.analysis.semantic.db_models import Concept
from dataraum.core.logging import get_logger
from dataraum.core.overlay import OverlayRow, set_overlay_resolver
from dataraum.core.vertical import set_framed_concept_resolver
from dataraum.storage import ConfigOverlay

if TYPE_CHECKING:
    from dataraum.core.connections import ConnectionManager

logger = get_logger(__name__)

# Only user-declared concept rows mark a *framed* vertical; a shipped vertical's
# seeded rows (source='seed') are classified from its on-disk dir (shipped-first
# in ``resolve_vertical``), so excluding them here keeps ``_framed_verticals``
# semantically precise — a vertical here has no on-disk ontology.
_FRAMED_CONCEPT_SOURCES: tuple[str, ...] = ("frame", "teach")


def install_overlay_resolver(manager: ConnectionManager) -> None:
    """Wire the Postgres-backed resolvers into ``core.overlay`` / ``core.vertical``.

    Called once from :func:`bootstrap_worker_substrate` after the
    ConnectionManager is initialized and the active workspace is
    bootstrapped. After this point every ``load_yaml_config`` call goes
    through the layered read: base YAML + active overlay rows for the
    workspace; and ``core.vertical`` can classify a concept-only framed
    vertical from its typed ``concepts`` rows (DAT-728).

    Each resolver leases a short-lived session per call. The overlay resolver
    returns rows ordered by ``created_at ASC`` (matches the appliers'
    last-write-wins semantics for keyed payloads). Workspace scope is implicit
    in the ``ws_<id>`` schema both tables live in — schema-per-workspace means
    the manager's connection already targets the right schema and no per-row
    workspace filter is needed.
    """

    def resolver() -> list[OverlayRow]:
        with manager.session_scope() as session:
            rows = session.execute(
                select(ConfigOverlay.type, ConfigOverlay.payload)
                .where(ConfigOverlay.superseded_at.is_(None))
                .order_by(asc(ConfigOverlay.created_at))
            ).all()
        return [OverlayRow(type=r.type, payload=r.payload or {}) for r in rows]

    def framed_concept_resolver() -> set[str]:
        with manager.session_scope() as session:
            rows = session.execute(
                select(distinct(Concept.vertical)).where(
                    Concept.superseded_at.is_(None),
                    Concept.source.in_(_FRAMED_CONCEPT_SOURCES),
                )
            ).all()
        return {r[0] for r in rows if r[0]}

    set_overlay_resolver(resolver)
    set_framed_concept_resolver(framed_concept_resolver)
    logger.info("overlay_resolver_installed")


def uninstall_overlay_resolver() -> None:
    """Drop the registered resolvers (worker shutdown / tests)."""
    set_overlay_resolver(None)
    set_framed_concept_resolver(None)
    logger.info("overlay_resolver_uninstalled")
