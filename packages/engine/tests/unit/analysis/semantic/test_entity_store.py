"""The typed table-entity taxonomy — seed + read (DAT-724, config→DB).

Mirrors ``test_concept_store``: the shipped vertical YAML seeds typed
:class:`VerticalEntity` rows once (idempotently, born-loud on role and cycle), and
the runtime reads the active rows back onto the ``OntologyDefinition`` the two
table-grain agents already consume.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from dataraum.analysis.cycles.cycle_type_store import ensure_cycle_types_seeded
from dataraum.analysis.semantic.concept_store import (
    ensure_concepts_seeded,
    load_workspace_concepts,
)
from dataraum.analysis.semantic.db_models import TableRole, VerticalEntity
from dataraum.analysis.semantic.entity_store import (
    ensure_entities_seeded,
    load_workspace_entities,
)
from dataraum.analysis.semantic.ontology import OntologyDefinition, OntologyEntity

_FINANCE_ENTITIES = 15


def _mock_loader(monkeypatch: pytest.MonkeyPatch, definition: OntologyDefinition) -> None:
    """Serve ``definition`` in place of the shipped YAML.

    ``model_construct`` where a test needs an INVALID document: the parse-time
    validators are themselves under test elsewhere, and a born-loud-at-seed test must
    be able to build the document the seed is supposed to reject.
    """
    loader = MagicMock()
    loader.load.return_value = definition
    monkeypatch.setattr("dataraum.analysis.semantic.entity_store.OntologyLoader", lambda: loader)


def _roles(session: Session, vertical: str) -> dict[str, str | None]:
    return {
        r.name: r.role
        for r in session.execute(
            select(VerticalEntity).where(
                VerticalEntity.vertical == vertical, VerticalEntity.superseded_at.is_(None)
            )
        ).scalars()
    }


def test_seed_finance_creates_typed_rows_with_roles(session: Session) -> None:
    ensure_cycle_types_seeded(session, "finance")
    assert ensure_entities_seeded(session, "finance") == _FINANCE_ENTITIES

    roles = _roles(session, "finance")
    assert len(roles) == _FINANCE_ENTITIES
    # The two ticket-named ambiguity cases, settled by the shipped content.
    # journal_entries: an event header with no measure column is still a FACT — a
    # factless event fact whose measure is COUNT. Calling it a dimension would drop
    # the ledger's event spine out of the fact side entirely.
    assert roles["gl_entry"] == TableRole.FACT
    # fx_rates: a date-keyed rate LOOKUP. Structure alone would read the date in its
    # grain as a periodic snapshot; the declaration is what says the date is a
    # validity key, not a measurement period.
    assert roles["fx_rate"] == TableRole.DIMENSION
    assert roles["trial_balance"] == TableRole.PERIODIC_SNAPSHOT
    assert roles["balance_sheet"] == TableRole.PERIODIC_SNAPSHOT
    assert roles["account"] == TableRole.DIMENSION
    assert roles["gl_line"] == TableRole.FACT


def test_finance_seed_declares_a_role_for_every_entity(session: Session) -> None:
    """NULL role is reserved for a framed vertical mid-authoring — a shipped vertical
    that left one unset would be seeding evidence that says nothing."""
    ensure_cycle_types_seeded(session, "finance")
    ensure_entities_seeded(session, "finance")
    assert [name for name, role in _roles(session, "finance").items() if role is None] == []


def test_seed_is_idempotent(session: Session) -> None:
    ensure_cycle_types_seeded(session, "finance")
    assert ensure_entities_seeded(session, "finance") == _FINANCE_ENTITIES
    assert ensure_entities_seeded(session, "finance") == 0
    assert len(_roles(session, "finance")) == _FINANCE_ENTITIES


def test_seed_does_not_clobber_a_superseding_edit(session: Session) -> None:
    """The ``ON CONFLICT DO NOTHING`` contract: a re-seed skips an entity whose active
    row is an edit, and never RAISES on the collision (the read-then-insert this
    replaces would ``IntegrityError`` under a concurrent seed)."""
    ensure_cycle_types_seeded(session, "finance")
    assert ensure_entities_seeded(session, "finance") == _FINANCE_ENTITIES
    session.execute(
        update(VerticalEntity)
        .where(
            VerticalEntity.vertical == "finance",
            VerticalEntity.name == "fx_rate",
            VerticalEntity.superseded_at.is_(None),
        )
        .values(superseded_at=datetime.now(UTC))
    )
    session.add(
        VerticalEntity(
            vertical="finance",
            name="fx_rate",
            role=TableRole.FACT,
            description="edited",
            source="seed",
        )
    )
    session.flush()

    assert ensure_entities_seeded(session, "finance") == 0
    assert _roles(session, "finance")["fx_rate"] == TableRole.FACT


def test_seed_round_trips_concepts_cycles_and_aliases(session: Session) -> None:
    ensure_cycle_types_seeded(session, "finance")
    ensure_entities_seeded(session, "finance")

    by_name = {e.name: e for e in load_workspace_entities(session, "finance")}
    gl_line = by_name["gl_line"]
    assert gl_line.role == TableRole.FACT
    assert "debit" in gl_line.concepts and "credit" in gl_line.concepts
    assert gl_line.cycles == ["journal_entry_cycle", "period_close"]
    assert "journal_lines" in gl_line.aliases
    # Empty lists round-trip as empty, never as None — the store writes NULL and the
    # lift restores the list, so a consumer never has to handle two absent shapes.
    assert by_name["customer"].concepts == []
    assert by_name["fx_rate"].cycles == []


def test_load_orders_by_name_for_a_stable_prompt(session: Session) -> None:
    """An unordered read reshuffles the served evidence between processes — the same
    determinism trap the graph-topology roles hit (DAT-725)."""
    ensure_cycle_types_seeded(session, "finance")
    ensure_entities_seeded(session, "finance")
    names = [e.name for e in load_workspace_entities(session, "finance")]
    assert names == sorted(names)


def test_load_workspace_concepts_lifts_the_taxonomy(session: Session) -> None:
    """The single seam both agents read: entities ride the same definition the
    concepts do, so neither agent needs a second read or a second scoping decision."""
    ensure_concepts_seeded(session, "finance")
    ensure_cycle_types_seeded(session, "finance")
    ensure_entities_seeded(session, "finance")

    definition = load_workspace_concepts(session, "finance")
    assert len(definition.entities) == _FINANCE_ENTITIES
    assert {e.name for e in definition.entities} >= {"gl_line", "fx_rate", "trial_balance"}


def test_load_is_empty_for_a_vertical_with_no_taxonomy(session: Session) -> None:
    """A framed vertical seeds nothing here — its taxonomy is genuinely absent, and
    the read must say so rather than fail."""
    assert load_workspace_entities(session, "_adhoc") == []
    assert load_workspace_concepts(session, "_adhoc").entities == []


def test_seed_born_loud_on_unknown_role(session: Session, monkeypatch: pytest.MonkeyPatch) -> None:
    _mock_loader(
        monkeypatch,
        OntologyDefinition(name="x", entities=[OntologyEntity(name="ledger", role="bridge")]),
    )
    with pytest.raises(ValueError, match="declares no valid role"):
        ensure_entities_seeded(session, "x")


def test_seed_born_loud_on_missing_role(session: Session, monkeypatch: pytest.MonkeyPatch) -> None:
    """An entity with no role declares nothing usable — the evidence would render as a
    bare name, which is worse than no declaration at all."""
    _mock_loader(
        monkeypatch, OntologyDefinition(name="x", entities=[OntologyEntity(name="ledger")])
    )
    with pytest.raises(ValueError, match="declares no valid role"):
        ensure_entities_seeded(session, "x")


def test_seed_born_loud_on_unknown_cycle(session: Session, monkeypatch: pytest.MonkeyPatch) -> None:
    """``cycles`` resolves against the typed ``cycle_types`` home (DAT-881), which the
    phase seeds first in the same transaction."""
    ensure_cycle_types_seeded(session, "finance")
    _mock_loader(
        monkeypatch,
        OntologyDefinition(
            name="finance",
            entities=[
                OntologyEntity(name="ledger", role="fact", cycles=["quote_to_cash"]),
            ],
        ),
    )
    with pytest.raises(ValueError, match="not a declared cycle type"):
        ensure_entities_seeded(session, "finance")


def test_seed_accepts_a_declared_cycle(session: Session, monkeypatch: pytest.MonkeyPatch) -> None:
    ensure_cycle_types_seeded(session, "finance")
    _mock_loader(
        monkeypatch,
        OntologyDefinition(
            name="finance",
            entities=[OntologyEntity(name="ledger", role="fact", cycles=["period_close"])],
        ),
    )
    assert ensure_entities_seeded(session, "finance") == 1


def test_seed_skips_the_cycle_read_when_nothing_names_a_cycle(
    session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A taxonomy naming no cycle must not require the cycle vocabulary to exist —
    otherwise a vertical with no cycles.yaml could not declare entities at all."""
    _mock_loader(
        monkeypatch,
        OntologyDefinition(name="x", entities=[OntologyEntity(name="ledger", role="fact")]),
    )
    assert ensure_entities_seeded(session, "x") == 1


def test_seed_is_a_noop_for_a_vertical_with_no_entities(
    session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    _mock_loader(monkeypatch, OntologyDefinition(name="x"))
    assert ensure_entities_seeded(session, "x") == 0
