"""The typed concept vocabulary — seed + read (DAT-728, config→DB).

Pins the config→DB seam: the shipped vertical YAML seeds typed ``Concept`` rows
once (idempotently, kind-validated born-loud), and the runtime reads the active
rows back — the source the grounding/context consumers moved onto, off the
``OntologyLoader`` YAML/overlay merge.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from dataraum.analysis.semantic.concept_store import (
    ensure_concepts_seeded,
    load_workspace_concepts,
)
from dataraum.analysis.semantic.db_models import Concept
from dataraum.analysis.semantic.ontology import OntologyConcept, OntologyDefinition


def _kinds(session: Session, vertical: str) -> dict[str, str]:
    return {
        r.name: r.kind
        for r in session.execute(
            select(Concept).where(Concept.vertical == vertical, Concept.superseded_at.is_(None))
        ).scalars()
    }


def test_seed_finance_creates_typed_rows_with_kinds(session: Session) -> None:
    n = ensure_concepts_seeded(session, "finance")
    assert n == 22
    kinds = _kinds(session, "finance")
    assert len(kinds) == 22
    # The 18 measures (the DAT-657 temporal_behavior carriers) + the four non-measures.
    assert kinds["revenue"] == "measure"
    assert kinds["account_balance"] == "measure"
    assert kinds["fiscal_period"] == "dimension"
    assert kinds["entity"] == "entity"
    assert kinds["account"] == "entity"
    assert kinds["currency"] == "unit"
    assert sum(1 for k in kinds.values() if k == "measure") == 18


def test_seed_is_idempotent(session: Session) -> None:
    assert ensure_concepts_seeded(session, "finance") == 22
    # A re-run (or a later phase re-entering) inserts nothing — never duplicates,
    # never clobbers an edited row.
    assert ensure_concepts_seeded(session, "finance") == 0
    assert len(_kinds(session, "finance")) == 22


def test_seed_does_not_clobber_a_frame_edit(session: Session) -> None:
    """A frame edit (supersede + insert a new active row) survives a re-seed.

    The re-seed's ``ON CONFLICT DO NOTHING`` skips the concept whose active row is
    the frame edit — the seed never overwrites a user's declared concept, and never
    RAISES on the collision. This is the race-safety contract (the old read-then-
    insert would ``IntegrityError`` here under concurrency)."""
    assert ensure_concepts_seeded(session, "finance") == 22
    # Simulate a frame edit of 'revenue': supersede the seed row, insert a new
    # active row (a different concept_id, source='frame', an edited description).
    session.execute(
        update(Concept)
        .where(
            Concept.vertical == "finance",
            Concept.name == "revenue",
            Concept.superseded_at.is_(None),
        )
        .values(superseded_at=datetime.now(UTC))
    )
    session.add(
        Concept(
            vertical="finance",
            name="revenue",
            kind="measure",
            description="user-edited revenue",
            source="frame",
        )
    )
    session.flush()
    # Re-seed: 'revenue' collides on the active partial-unique index → skipped, no
    # error; every other concept already present is likewise skipped.
    assert ensure_concepts_seeded(session, "finance") == 0
    active = {c.name: c for c in load_workspace_concepts(session, "finance").concepts}
    assert active["revenue"].description == "user-edited revenue"  # frame edit kept
    assert len(active) == 22


def test_seed_born_loud_on_missing_kind(session: Session, monkeypatch: pytest.MonkeyPatch) -> None:
    bad = OntologyDefinition(name="x", concepts=[OntologyConcept(name="foo")])  # no kind
    loader = MagicMock()
    loader.load.return_value = bad
    monkeypatch.setattr("dataraum.analysis.semantic.concept_store.OntologyLoader", lambda: loader)
    with pytest.raises(ValueError, match="no valid kind"):
        ensure_concepts_seeded(session, "x")


def test_load_workspace_concepts_reads_typed_rows(session: Session) -> None:
    ensure_concepts_seeded(session, "finance")
    definition = load_workspace_concepts(session, "finance")
    by_name = {c.name: c for c in definition.concepts}
    assert len(by_name) == 22
    assert by_name["revenue"].kind == "measure"
    # Conventions still come from YAML (not config→DB in this phase).
    assert any(conv.id == "sign_natural_balance" for conv in definition.conventions)


def test_load_excludes_superseded_rows(session: Session) -> None:
    ensure_concepts_seeded(session, "finance")
    session.execute(
        update(Concept)
        .where(Concept.vertical == "finance", Concept.name == "revenue")
        .values(superseded_at=datetime.now(UTC))
    )
    session.flush()
    names = {c.name for c in load_workspace_concepts(session, "finance").concepts}
    assert "revenue" not in names
    assert len(names) == 21
