"""The typed cycle-family vocabulary — seed + read (DAT-856, config→DB).

Pins the config→DB seam for the direction axis: the shipped vertical's
``cycles.yaml`` ``cycle_families`` block seeds typed ``CycleFamily`` rows once
(idempotently, ON CONFLICT DO NOTHING on the active-row index), and the two
consumers (the judge's DOMAIN KNOWLEDGE serving and the save-time direction
resolution) read the active rows back as a flat ``{family: {label: member}}``
mapping. Born-loud on a bad declaration — the config is wrong, not the data.
"""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.cycles import cycle_family_store
from dataraum.analysis.cycles.cycle_family_store import (
    ensure_cycle_families_seeded,
    format_cycle_families_for_context,
    load_workspace_cycle_families,
)
from dataraum.analysis.cycles.db_models import CycleFamily
from dataraum.analysis.semantic.db_models import WorkspaceSettings


def _bind_vertical(session: Session, vertical: str) -> None:
    session.add(WorkspaceSettings(pin=True, active_vertical=vertical))
    session.flush()


def _active(session: Session, vertical: str) -> dict[str, CycleFamily]:
    return {
        r.family: r
        for r in session.execute(
            select(CycleFamily).where(
                CycleFamily.vertical == vertical, CycleFamily.superseded_at.is_(None)
            )
        ).scalars()
    }


def test_seed_finance_creates_the_settlement_family(session: Session) -> None:
    n = ensure_cycle_families_seeded(session, "finance")
    assert n == 1
    rows = _active(session, "finance")
    assert set(rows) == {"settlement"}
    settlement = rows["settlement"]
    assert settlement.source == "seed"
    # The directions map round-trips typed (JSON object) — each label resolving to a
    # declared cycle_types member.
    assert settlement.directions == {
        "incoming": "accounts_receivable",
        "outgoing": "accounts_payable",
    }


def test_seed_is_idempotent(session: Session) -> None:
    assert ensure_cycle_families_seeded(session, "finance") == 1
    # A re-run is a no-op — ON CONFLICT DO NOTHING on the active-row index.
    assert ensure_cycle_families_seeded(session, "finance") == 0
    assert len(_active(session, "finance")) == 1


def test_load_returns_flat_mapping(session: Session) -> None:
    _bind_vertical(session, "finance")
    ensure_cycle_families_seeded(session, "finance")
    families = load_workspace_cycle_families(session, "finance")
    assert families == {
        "settlement": {"incoming": "accounts_receivable", "outgoing": "accounts_payable"}
    }


def test_load_scopes_to_bound_vertical_not_the_arg(session: Session) -> None:
    # A reader threaded a mismatched vertical still serves the workspace's real
    # families (DAT-848): the bound active_vertical wins over the arg.
    _bind_vertical(session, "finance")
    ensure_cycle_families_seeded(session, "finance")
    families = load_workspace_cycle_families(session, "marketing")
    assert set(families) == {"settlement"}


def test_load_unbound_falls_back_to_the_arg(session: Session) -> None:
    # No binding: the vertical arg is the fallback (a cold-start / placeholder read).
    ensure_cycle_families_seeded(session, "finance")
    assert set(load_workspace_cycle_families(session, "finance")) == {"settlement"}


def test_unknown_vertical_seeds_nothing_not_an_error(session: Session) -> None:
    assert ensure_cycle_families_seeded(session, "nonexistent") == 0
    assert load_workspace_cycle_families(session, "nonexistent") == {}


def _patch_config(monkeypatch: pytest.MonkeyPatch, config: dict[str, Any]) -> None:
    monkeypatch.setattr(cycle_family_store, "get_cycles_config", lambda vertical: config)


def test_born_loud_family_with_no_directions(
    session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_config(
        monkeypatch,
        {"cycle_types": {"accounts_payable": {}}, "cycle_families": {"settlement": {}}},
    )
    with pytest.raises(ValueError, match="declares no directions"):
        ensure_cycle_families_seeded(session, "finance")


def test_born_loud_direction_resolves_to_unknown_member(
    session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_config(
        monkeypatch,
        {
            "cycle_types": {"accounts_receivable": {}},
            "cycle_families": {"settlement": {"directions": {"outgoing": "not_a_cycle_type"}}},
        },
    )
    with pytest.raises(ValueError, match="not a declared cycle"):
        ensure_cycle_families_seeded(session, "finance")


def test_born_loud_family_name_collides_with_cycle_type(
    session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A family sharing a name with a cycle type would collide in the canonical_type
    # identity space of detected_business_cycles.
    _patch_config(
        monkeypatch,
        {
            "cycle_types": {"settlement": {}, "accounts_payable": {}},
            "cycle_families": {"settlement": {"directions": {"outgoing": "accounts_payable"}}},
        },
    )
    with pytest.raises(ValueError, match="collides with a cycle type"):
        ensure_cycle_families_seeded(session, "finance")


def test_format_families_for_context_is_generic_and_carries_the_declaration() -> None:
    block = format_cycle_families_for_context(
        {"settlement": {"incoming": "accounts_receivable", "outgoing": "accounts_payable"}}
    )
    # Generic mechanism header + the vertical's DECLARED data (family + member names).
    assert "CYCLE FAMILIES (direction axis)" in block
    assert "### settlement" in block
    assert "incoming → accounts_receivable" in block
    assert "outgoing → accounts_payable" in block
    # The honest-answer instruction rides along, domain-free.
    assert "undetermined" in block


def test_format_families_empty_is_empty() -> None:
    assert format_cycle_families_for_context({}) == ""
