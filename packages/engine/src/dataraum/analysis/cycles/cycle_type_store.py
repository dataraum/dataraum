"""Seed the typed cycle-type vocabulary home (DAT-881, config→DB).

The shipped vertical ``cycles.yaml`` ``cycle_types`` block is the *seed*,
normalized into typed :class:`~dataraum.analysis.cycles.db_models.CycleType` rows
once per workspace. Mirrors :func:`~dataraum.analysis.cycles.cycle_family_store.ensure_cycle_families_seeded`
and :func:`~dataraum.analysis.validation.validation_store.ensure_validations_seeded`:
``INSERT … ON CONFLICT DO NOTHING`` on the active-row partial-unique index, so a
re-run is a no-op and it is race-safe against a concurrent seed.

**Seed source is deliberately the SHIPPED base, not the overlay-layered collection.**
A ``cycle`` type has a live overlay-teach path (``core.overlay._apply_cycle`` upserts
a taught cycle into the SAME ``cycle_types`` mapping key that
:func:`~dataraum.analysis.cycles.config.get_cycles_config` resolves, shipped ⊕
overlay). Seeding from that resolved view would risk writing a user-taught cycle
into this table mislabeled ``source='seed'`` — the exact trap
``validation_store.ensure_validations_seeded`` already avoids for its own
overlay-fronted family, via
:meth:`~dataraum.core.vertical_loader.VerticalLoader.shipped_base`. This module
does the same.

**Nothing on the engine side reads this table back.** The cycle judge's DOMAIN
KNOWLEDGE serving still reads the overlay-inclusive
:func:`~dataraum.analysis.cycles.config.get_cycle_types` (shipped ⊕ taught) — it
legitimately needs taught cycles visible to detection, which this shipped-only
table must not carry. This table's only consumer is the cockpit's shipped-baseline
readers (``teach_cycle``'s override-shadow detection, the frame induction few-shot
seed), which query the mirrored read view directly — no engine-side Python reader
is wired here, so none is built (a `load_workspace_cycle_types` with zero callers
would be dead code).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import text

from dataraum.analysis.cycles.db_models import CycleType
from dataraum.core.logging import get_logger
from dataraum.core.vertical_loader import Family, VerticalLoader
from dataraum.storage.upsert import insert_if_absent

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = get_logger(__name__)


def _row_values(vertical: str, name: str, defn: dict[str, Any]) -> dict[str, Any]:
    """One :class:`CycleType` row dict from a raw ``cycle_types`` entry."""
    return {
        "vertical": vertical,
        "name": name,
        "description": defn.get("description"),
        "business_value": defn.get("business_value"),
        "aliases": defn.get("aliases") or None,
        "typical_stages": defn.get("typical_stages") or None,
        "completion_indicators": defn.get("completion_indicators") or None,
        "feeds_into": defn.get("feeds_into") or None,
        "source": "seed",
    }


def ensure_cycle_types_seeded(session: Session, vertical: str) -> int:
    """Idempotently seed the shipped vertical's cycle types as typed rows (DAT-881).

    Reads the vertical's SHIPPED ``cycles.yaml`` ``cycle_types`` block (the seed
    source, WITHOUT the ``cycle`` teach overlay — that stays a read-time ``⊕`` layer
    the engine's judge-facing :func:`~dataraum.analysis.cycles.config.get_cycle_types`
    applies, never this table) and inserts a typed :class:`CycleType` row for every
    cycle type with no active row yet. A framed vertical (no on-disk ``cycles.yaml``)
    seeds nothing. Returns the number of rows actually inserted (conflicts skipped).
    """
    base = VerticalLoader(vertical).shipped_base(Family.CYCLES)
    cycle_types: dict[str, Any] = base.get("cycle_types") or {}
    if not cycle_types:
        return 0
    rows = [_row_values(vertical, name, defn or {}) for name, defn in cycle_types.items()]
    seeded = insert_if_absent(
        session,
        CycleType,
        rows,
        index_elements=["vertical", "name"],
        index_where=text("superseded_at IS NULL"),
    )
    if seeded:
        logger.info("cycle_types_seeded", vertical=vertical, count=seeded)
    return seeded


__all__ = ["ensure_cycle_types_seeded"]
