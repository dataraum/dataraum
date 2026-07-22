"""The workspace convention vocabulary — typed rows, config→DB (DAT-789).

The runtime home for a workspace's domain conventions. The shipped vertical YAML is
the *seed*, normalized into ``conventions`` rows once per workspace; the three
SQL-authoring consumers — extraction (``graphs/context.py``), validation
(``validation_phase``), and the cockpit Q&A agent (``prompts/conventions.ts``) — read
the typed rows (never the YAML), so a *framed* vertical whose conventions exist only as
rows is served identically to a builtin.

Conventions stay PROSE: ``statement`` is declared human judgment served verbatim — this
cut moves only the HOME and the authoring, never the content (the engine never
interprets a convention). The seam replacing the YAML-only runtime read (DAT-645 →
DAT-789): the renderers no longer resolve ``OntologyLoader().load(vertical).conventions``
at read time — they read this table, which the seed (builtin YAML) and ``frame``
(declared/edited, via the cockpit's write surface) both write. The convention envelope
stays typed through :class:`~dataraum.analysis.semantic.ontology.OntologyConvention` at
both the writer (seed) and the reader.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from dataraum.analysis.semantic.db_models import Convention, WorkspaceSettings
from dataraum.analysis.semantic.ontology import OntologyConvention, OntologyLoader
from dataraum.core.logging import get_logger
from dataraum.storage.upsert import insert_if_absent

logger = get_logger(__name__)


def _active_vertical(session: Session) -> str | None:
    """The workspace's bound active vertical, or ``None`` if none is bound yet.

    Reads the single ``workspace_settings`` row (the ``pin`` CHECK keeps it at most
    one). ``None`` = unbound: no non-placeholder vertical has run yet. Mirrors
    ``concept_store._active_vertical`` — the same DAT-848 binding both scope on.
    """
    return session.execute(select(WorkspaceSettings.active_vertical)).scalar_one_or_none()


def ensure_conventions_seeded(session: Session, vertical: str) -> int:
    """Idempotently seed the shipped vertical's conventions as typed rows (DAT-789).

    Reads the vertical's YAML definition (the seed source) and inserts a typed
    :class:`Convention` row for every convention with no active row yet, via
    ``INSERT … ON CONFLICT DO NOTHING`` on the active-row partial-unique index — so a
    re-run is a no-op, a ``frame`` edit (which supersedes) is never clobbered, and it is
    race-safe against a concurrent seed / ``frame`` write (no read-then-insert TOCTOU).
    Mirrors :func:`~dataraum.analysis.semantic.concept_store.ensure_concepts_seeded`.

    The convention envelope is typed at the writer: the YAML already parsed into
    :class:`OntologyConvention` objects (``id`` / ``targets`` / ``statement`` /
    ``concept_groups``), so the engine stores that validated shape, never a free dict.
    Content is NOT linted — the ``statement`` prose is served verbatim (DAT-789) — and
    the concept↔group *resolve* lint already ran at YAML-authoring time on the
    ``OntologyDefinition``; re-checking a possibly-narrowed live concept set at seed
    would be wrong (a superseded concept a convention still names is stale-but-served).

    A framed vertical (no on-disk YAML) seeds nothing here — its conventions arrive
    through ``frame``'s typed writes, not the shipped seed. Returns the number of rows
    actually inserted (conflicts skipped).
    """
    definition = OntologyLoader().load(vertical)
    if definition is None:
        return 0
    rows: list[dict[str, Any]] = [
        {
            "vertical": vertical,
            "name": conv.id,
            "statement": conv.statement,
            "targets": conv.targets or None,
            "concept_groups": conv.concept_groups or None,
            "source": "seed",
        }
        for conv in definition.conventions
    ]
    if not rows:
        return 0
    seeded = insert_if_absent(
        session,
        Convention,
        rows,
        index_elements=["vertical", "name"],
        index_where=text("superseded_at IS NULL"),
    )
    if seeded:
        logger.info("conventions_seeded", vertical=vertical, count=seeded)
    return seeded


def load_workspace_conventions(session: Session, vertical: str) -> list[OntologyConvention]:
    """The workspace's conventions as typed :class:`OntologyConvention` objects (DAT-789).

    Reads the active ``conventions`` rows (the config→DB home) and returns the typed
    envelope the renderer (``OntologyLoader.format_conventions_for_prompt``) already
    accepts — only the SOURCE moved off the YAML onto the typed table.

    **Scoped to the workspace's bound active vertical (DAT-848),** exactly like
    :func:`~dataraum.analysis.semantic.concept_store.load_workspace_concepts`: the read
    filters on ``workspace_settings.active_vertical`` (never blindly on the caller's
    ``vertical``), so an un-gated reader threaded a mismatched vertical still serves the
    workspace's real conventions; ``vertical`` is the fallback for an UNBOUND workspace.

    Each row's JSON is re-typed through :class:`OntologyConvention` — the ENVELOPE only
    (``id`` / ``targets`` / ``statement`` / ``concept_groups`` field types), NEVER the
    concept↔group resolve lint (which lives on ``OntologyDefinition``, not here). A
    convention naming a since-superseded concept is stale-but-served, never a crash —
    the ``model_construct`` bypass contract ``load_workspace_concepts`` preserves.
    """
    effective = _active_vertical(session) or vertical
    rows = list(
        session.execute(
            select(Convention)
            .where(Convention.vertical == effective, Convention.superseded_at.is_(None))
            .order_by(Convention.name)
        ).scalars()
    )
    return [
        OntologyConvention(
            id=r.name,
            targets=list(r.targets or []),
            statement=r.statement,
            concept_groups=dict(r.concept_groups or {}),
        )
        for r in rows
    ]


__all__ = ["ensure_conventions_seeded", "load_workspace_conventions"]
