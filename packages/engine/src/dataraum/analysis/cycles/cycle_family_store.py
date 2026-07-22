"""The workspace cycle-family vocabulary — typed rows, config→DB (DAT-856).

The runtime home for a workspace's cycle *families* — the direction axis over the
cycle vocabulary. The shipped vertical ``cycles.yaml`` ``cycle_families`` block is
the *seed*, normalized into :class:`~dataraum.analysis.cycles.db_models.CycleFamily`
rows once per workspace; the two consumers — the cycle judge's DOMAIN KNOWLEDGE
serving (``cycles/context.py``) and the save-time direction resolution
(``cycles/config.resolve_cycle_identity``) — read the typed rows, never the YAML,
so a *framed* vertical whose families exist only as rows is served identically to a
builtin.

Mirrors :mod:`~dataraum.analysis.semantic.convention_store` (config→DB, DAT-789):
LOAD + LAYER live here; the declaration content (which directions a family has, the
member each resolves to) stays in config, this table is just the typed home. The
directions map is validated against the ``cycle_types`` vocabulary at seed (a
direction resolving to a non-existent member is a config error, born-loud), and a
family name colliding with a cycle-type name is refused (the two share the
``canonical_type`` identity space in ``detected_business_cycles``).
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from dataraum.analysis.cycles.config import get_cycles_config
from dataraum.analysis.cycles.db_models import CycleFamily
from dataraum.analysis.semantic.db_models import WorkspaceSettings
from dataraum.core.logging import get_logger
from dataraum.storage.upsert import insert_if_absent

logger = get_logger(__name__)


def _active_vertical(session: Session) -> str | None:
    """The workspace's bound active vertical, or ``None`` if none is bound yet.

    Reads the single ``workspace_settings`` row (the ``pin`` CHECK keeps it at most
    one). ``None`` = unbound: no non-placeholder vertical has run yet. Mirrors
    ``convention_store._active_vertical`` — the same DAT-848 binding both scope on.
    """
    return session.execute(select(WorkspaceSettings.active_vertical)).scalar_one_or_none()


def ensure_cycle_families_seeded(session: Session, vertical: str) -> int:
    """Idempotently seed the shipped vertical's cycle families as typed rows (DAT-856).

    Reads the vertical's ``cycles.yaml`` ``cycle_families`` block (the seed source)
    and inserts a typed :class:`CycleFamily` row for every family with no active row
    yet, via ``INSERT … ON CONFLICT DO NOTHING`` on the active-row partial-unique
    index — so a re-run is a no-op and it is race-safe against a concurrent seed (no
    read-then-insert TOCTOU). Mirrors
    :func:`~dataraum.analysis.semantic.convention_store.ensure_conventions_seeded`.

    Born-loud on a bad declaration (the config is wrong, not the data): a family with
    no directions, a direction that resolves to a cycle type not in the vocabulary, or
    a family name that collides with a cycle-type name (they share the
    ``canonical_type`` identity space). A framed vertical with no on-disk block seeds
    nothing. Returns the number of rows actually inserted (conflicts skipped).
    """
    config = get_cycles_config(vertical)
    families: dict[str, Any] = config.get("cycle_families") or {}
    if not families:
        return 0
    cycle_types = config.get("cycle_types") or {}
    rows: list[dict[str, Any]] = []
    for family_name, defn in families.items():
        directions = (defn or {}).get("directions") or {}
        if not directions:
            raise ValueError(
                f"cycle family '{family_name}' in vertical '{vertical}' declares no "
                f"directions; a family exists to declare a direction axis."
            )
        if family_name in cycle_types:
            raise ValueError(
                f"cycle family '{family_name}' in vertical '{vertical}' collides with a "
                f"cycle type of the same name; families and cycle types share the "
                f"canonical_type identity space and must not overlap."
            )
        for label, member in directions.items():
            if member not in cycle_types:
                raise ValueError(
                    f"cycle family '{family_name}' direction '{label}' in vertical "
                    f"'{vertical}' resolves to '{member}', which is not a declared cycle "
                    f"type; a direction must map to a member of cycle_types."
                )
        rows.append(
            {
                "vertical": vertical,
                "family": family_name,
                "directions": dict(directions),
                "source": "seed",
            }
        )
    seeded = insert_if_absent(
        session,
        CycleFamily,
        rows,
        index_elements=["vertical", "family"],
        index_where=text("superseded_at IS NULL"),
    )
    if seeded:
        logger.info("cycle_families_seeded", vertical=vertical, count=seeded)
    return seeded


def load_workspace_cycle_families(session: Session, vertical: str) -> dict[str, dict[str, str]]:
    """The workspace's cycle families as ``{family: {direction_label: member}}`` (DAT-856).

    Reads the active ``cycle_families`` rows (the config→DB home) into the flat
    mapping the serving and the save-time resolution both consume.

    **Scoped to the workspace's bound active vertical (DAT-848),** exactly like
    :func:`~dataraum.analysis.semantic.convention_store.load_workspace_conventions`:
    the read filters on ``workspace_settings.active_vertical`` (never blindly on the
    caller's ``vertical``), so an un-gated reader threaded a mismatched vertical still
    serves the workspace's real families; ``vertical`` is the fallback for an UNBOUND
    workspace. An unknown / familyless vertical resolves to an EMPTY dict — "no
    declared families" is a normal outcome (non-direction-typed verticals), never a
    crash.
    """
    effective = _active_vertical(session) or vertical
    rows = list(
        session.execute(
            select(CycleFamily)
            .where(CycleFamily.vertical == effective, CycleFamily.superseded_at.is_(None))
            .order_by(CycleFamily.family)
        ).scalars()
    )
    return {r.family: dict(r.directions or {}) for r in rows}


def format_cycle_families_for_context(cycle_families: dict[str, dict[str, str]]) -> str:
    """Render the cycle-family declaration as a DOMAIN KNOWLEDGE block for the judge.

    Served as CONTEXT data (DAT-856), NOT as generic-prompt text — the mechanism prose
    is domain-free (family / direction / undetermined), and the family + member names are
    the vertical's DECLARED data (each member's own prose already sits in KNOWN BUSINESS
    CYCLE TYPES above). Empty declaration → empty block (the caller omits it). This keeps
    the de-leak tripwire green: no who-owes-whom vocabulary is hardcoded anywhere generic.
    """
    if not cycle_families:
        return ""
    lines = [
        "## CYCLE FAMILIES (direction axis)",
        "",
        "Some cycle types differ ONLY in direction — the same shape of flow read from "
        "opposite sides. They are grouped into families below. When you detect a cycle "
        "in one of these families, set `family` to the family name and `direction` to "
        "the direction the served evidence decides. When the served evidence does NOT "
        'decide the direction, set `direction` to "undetermined": detecting the family '
        "without directing it is the honest answer, never a failure — do NOT guess a "
        "direction to fill the field.",
        "",
    ]
    for family_name in sorted(cycle_families):
        directions = cycle_families[family_name]
        lines.append(f"### {family_name}")
        lines.append("Directions (label → the cycle type it resolves to):")
        for label in sorted(directions):
            lines.append(f"  - {label} → {directions[label]}")
        lines.append("")
    return "\n".join(lines).rstrip()


__all__ = [
    "ensure_cycle_families_seeded",
    "format_cycle_families_for_context",
    "load_workspace_cycle_families",
]
