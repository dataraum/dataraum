"""Loader for validation specs — the typed DB home, overlay-aware (DAT-735).

Reads the workspace's validation vocabulary from the typed ``validations`` home
(:mod:`~dataraum.analysis.validation.validation_store`: seed ``⊕`` generated rows)
``⊕`` the ``validation`` teach overlay applied at read time. This replaces the raw
YAML directory walk: the shipped YAML is now the SEED source, normalized into typed
rows at connect (``ensure_validations_seeded``), and agentic induction adds
``source='generated'`` rows — so a *framed* vertical whose validations exist only as
rows is served identically to a builtin, and the teach overlay ``⊕`` layer keeps
working.

An explicit ``verticals_dir`` (tests / fixtures) reads raw YAML and bypasses BOTH the
DB home and the overlay — the same escape hatch the pre-DAT-735 loader offered.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from dataraum.analysis.validation.models import ValidationSpec
from dataraum.analysis.validation.validation_store import load_workspace_validations
from dataraum.core.logging import get_logger
from dataraum.core.overlay import apply_overlay
from dataraum.core.vertical_loader import Family, VerticalLoader

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = get_logger(__name__)


def load_all_validation_specs(
    vertical: str,
    session: Session | None = None,
    *,
    verticals_dir: Path | None = None,
) -> dict[str, ValidationSpec]:
    """Load a vertical's validation specs — DB home ``⊕`` teach overlay (DAT-735).

    Production (``session`` given): reads the active ``validations`` rows (seed ``⊕``
    generated) as the config→DB base, then layers the ``validation`` teach overlay
    over it (upsert-replace by ``validation_id``, the same
    :func:`~dataraum.core.overlay.apply_overlay` machinery). An unknown / framed
    vertical with no rows and no overlay resolves to an EMPTY dict, never raises —
    "no declared validations" is the phase tier's loud outcome.

    Tests (``verticals_dir`` given): reads raw YAML under that root and bypasses BOTH
    the DB home and the overlay.

    Neither given: returns EMPTY (a caller with no DB session has no typed home to
    read) — fail-quiet, mirroring the framed-vertical contract.

    Returns:
        Dict mapping validation_id to ValidationSpec.
    """
    if verticals_dir is not None:
        collection = VerticalLoader(vertical, verticals_dir).collection(Family.VALIDATIONS)
    elif session is not None:
        base = {
            "validations": [
                spec.model_dump(mode="json")
                for spec in load_workspace_validations(session, vertical)
            ]
        }
        collection = apply_overlay(f"verticals/{vertical}/validations", base)
    else:
        return {}

    specs: dict[str, ValidationSpec] = {}
    for data in collection.get("validations") or []:
        spec = ValidationSpec.model_validate(data)
        specs[spec.validation_id] = spec
        logger.debug("validation_spec_loaded", validation_id=spec.validation_id)

    logger.debug("validation_specs_loaded", vertical=vertical, count=len(specs))
    return specs


def get_validation_specs_for_cycles(
    cycle_types: list[str], vertical: str, session: Session | None = None
) -> list[ValidationSpec]:
    """Get validation specs relevant to detected cycle types.

    Returns specs that either:
    - Have relevant_cycles overlapping with cycle_types, or
    - Have empty relevant_cycles (universal applicability)

    Args:
        cycle_types: Detected cycle canonical types (e.g. ['journal_entry_cycle'])
        vertical: Vertical name (e.g. 'finance')
        session: DB session for the typed home read (production).

    Returns:
        List of matching ValidationSpecs
    """
    all_specs = load_all_validation_specs(vertical, session)
    cycle_set = set(cycle_types)
    return [
        spec
        for spec in all_specs.values()
        if not spec.relevant_cycles or set(spec.relevant_cycles) & cycle_set
    ]


__all__ = [
    "load_all_validation_specs",
    "get_validation_specs_for_cycles",
]
