"""The vertical's own identity — typed rows, config→DB (DAT-883).

The runtime home for a vertical's envelope (name/version/description), the third
config→DB seam alongside :mod:`concept_store` (DAT-728) and :mod:`convention_store`
(DAT-789). ``load_workspace_concepts`` used to re-parse ``ontology.yaml`` on every
call just to read this envelope, and fabricated ``version="1.0.0"`` for a framed
vertical (no on-disk YAML) instead of admitting it has none. This module seeds the
shipped vertical's envelope as a single typed row and serves it back — a framed
vertical seeds nothing (there is no on-disk source to seed from) and so reads as
absent, never synthesized.
"""

from __future__ import annotations

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from dataraum.analysis.semantic.db_models import VerticalEnvelope, WorkspaceSettings
from dataraum.analysis.semantic.ontology import OntologyLoader
from dataraum.core.logging import get_logger
from dataraum.core.vertical import VerticalKind, resolve_vertical
from dataraum.storage.upsert import insert_if_absent

logger = get_logger(__name__)


def _active_vertical(session: Session) -> str | None:
    """The workspace's bound active vertical, or ``None`` if none is bound yet.

    Mirrors ``concept_store._active_vertical`` — the same DAT-848 binding every
    typed vocabulary home (concepts, conventions, now the envelope) scopes on.
    """
    return session.execute(select(WorkspaceSettings.active_vertical)).scalar_one_or_none()


def ensure_envelope_seeded(session: Session, vertical: str) -> int:
    """Idempotently seed the shipped vertical's envelope as a typed row (DAT-883).

    Only a vertical with an on-disk ``ontology.yaml`` (SHIPPED, or the PLACEHOLDER
    ``_adhoc``) has a genuine envelope to seed — gated explicitly via
    :func:`~dataraum.core.vertical.resolve_vertical`, unlike ``ensure_concepts_seeded``
    (which needs no explicit gate: a framed vertical's YAML-shaped ``empty_base``
    naturally carries zero concepts). The envelope has no such natural tell — the
    empty_base sets a ``name`` too (the vertical's own key) — so without this gate a
    framed vertical would seed a synthesized envelope row, exactly the fabrication
    this ticket removes. A FRAMED or UNKNOWN vertical seeds nothing here; its
    envelope stays absent until a future frame-time writer declares one (no such
    writer exists yet — see :class:`~dataraum.analysis.semantic.db_models.VerticalEnvelope`).

    Race-safe via ``INSERT … ON CONFLICT DO NOTHING`` on the active-row partial-unique
    index, mirroring :func:`~dataraum.analysis.semantic.concept_store.ensure_concepts_seeded`.
    Returns 1 if a row was inserted, 0 if skipped (already seeded, or nothing to seed).
    """
    if resolve_vertical(vertical) not in (VerticalKind.SHIPPED, VerticalKind.PLACEHOLDER):
        return 0
    definition = OntologyLoader().load(vertical)
    if definition is None:
        return 0
    seeded = insert_if_absent(
        session,
        VerticalEnvelope,
        [
            {
                "vertical": vertical,
                "name": definition.name,
                "version": definition.version,
                "description": definition.description,
                "source": "seed",
            }
        ],
        index_elements=["vertical"],
        index_where=text("superseded_at IS NULL"),
    )
    if seeded:
        logger.info("envelope_seeded", vertical=vertical, name=definition.name)
    return seeded


def load_workspace_envelope(session: Session, vertical: str) -> VerticalEnvelope | None:
    """The workspace's active envelope row for a vertical, or ``None`` if absent.

    ``None`` means exactly that: no envelope has ever been seeded or declared for
    this vertical (a framed vertical with no on-disk source, or a shipped vertical
    whose seed hasn't run yet). The caller (``concept_store.load_workspace_concepts``)
    decides how to serve that absence — typed-absent, never a fabricated default.

    **Scoped to the workspace's bound active vertical (DAT-848),** exactly like
    :func:`~dataraum.analysis.semantic.concept_store.load_workspace_concepts` and
    :func:`~dataraum.analysis.semantic.convention_store.load_workspace_conventions`:
    the read filters on ``workspace_settings.active_vertical`` (never blindly on the
    caller's ``vertical``), so an un-gated reader threaded a mismatched vertical
    still serves the workspace's real envelope; ``vertical`` is the fallback for an
    UNBOUND workspace (no binding yet).
    """
    effective = _active_vertical(session) or vertical
    return session.execute(
        select(VerticalEnvelope).where(
            VerticalEnvelope.vertical == effective, VerticalEnvelope.superseded_at.is_(None)
        )
    ).scalar_one_or_none()


__all__ = ["ensure_envelope_seeded", "load_workspace_envelope"]
