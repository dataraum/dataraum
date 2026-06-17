"""Durable hierarchy/alias teaches as config overlays (DAT-537).

Mirrors the relationship-overlay pattern (``analysis/relationships/utils.py``) with
direct reads (NO YAML applier): a ``ConfigOverlay(type='hierarchy')`` row carries

    {action, table_id, kind, members}

where ``action`` is ``add`` (assert a drill-down chain g3 missed → a durable
``manual`` drilldown), ``alias`` (assert A ≡ B → a ``manual`` alias group), or
``reject`` (suppress a g3-discovered structure this run). ``members`` is the ordered
list of enriched-view column NAMES (finest → coarsest for a drilldown; the group for
an alias) — the member identity the g3 pass and the ``signature`` key use.

The heavy half of the relationship apparatus is ELIDED on purpose: g3 is
DETERMINISTIC, so there is no silent-accept ``keeper`` lift-up and no witness/detect
pool (those exist to adjudicate non-deterministic LLM discovery). A teach here is a
plain assert/suppress folded into the run that recomputes the same g3 set every time.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import select

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@dataclass(frozen=True)
class HierarchyTeach:
    """One parsed, well-formed hierarchy/alias overlay (DAT-537)."""

    action: str  # 'add' | 'reject' | 'alias'
    table_id: str
    members: list[str]  # ordered enriched-view column names (finest→coarsest for add)


def hierarchy_overlay_specs(session: Session, action: str) -> list[HierarchyTeach]:
    """Active ``ConfigOverlay(type='hierarchy')`` teaches for one ``action``.

    Every engine reader of hierarchy teaches goes through this one parser so they
    agree on the shape; ``superseded_at IS NULL`` filters undone teaches out. A
    payload missing ``table_id`` or a non-empty string ``members`` list is skipped
    (malformed) — the parser never emits a half-formed spec.
    """
    from dataraum.storage import ConfigOverlay

    rows = list(
        session.execute(
            select(ConfigOverlay).where(
                ConfigOverlay.type == "hierarchy",
                ConfigOverlay.superseded_at.is_(None),
            )
        ).scalars()
    )
    out: list[HierarchyTeach] = []
    for row in rows:
        payload = row.payload or {}
        if payload.get("action") != action:
            continue
        table_id = payload.get("table_id")
        members = payload.get("members")
        if not isinstance(table_id, str) or not table_id:
            continue
        if (
            not isinstance(members, list)
            or not members
            or not all(isinstance(m, str) for m in members)
        ):
            continue
        out.append(HierarchyTeach(action=action, table_id=table_id, members=list(members)))
    return out
