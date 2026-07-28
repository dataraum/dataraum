"""The typed vertical envelope — seed + read (DAT-883, config→DB).

Pins the config→DB seam for the vertical's own identity (name/version/description):
a shipped vertical's YAML seeds a single typed ``VerticalEnvelope`` row, and the
runtime reads it back — the source ``load_workspace_concepts`` moved onto, off the
``OntologyLoader`` YAML re-parse. A framed vertical (no on-disk YAML) seeds nothing
and reads back as absent, never a fabricated placeholder.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from dataraum.analysis.semantic.db_models import VerticalEnvelope
from dataraum.analysis.semantic.envelope_store import (
    ensure_envelope_seeded,
    load_workspace_envelope,
)


def test_seed_finance_creates_typed_envelope_row(session: Session) -> None:
    assert ensure_envelope_seeded(session, "finance") == 1
    row = load_workspace_envelope(session, "finance")
    assert row is not None
    assert row.name == "financial_reporting"
    assert row.version == "1.0.0"
    assert row.description is not None
    assert row.source == "seed"


def test_seed_is_idempotent(session: Session) -> None:
    assert ensure_envelope_seeded(session, "finance") == 1
    # A re-run (or a later phase re-entering) inserts nothing.
    assert ensure_envelope_seeded(session, "finance") == 0
    rows = list(
        session.execute(select(VerticalEnvelope).where(VerticalEnvelope.vertical == "finance"))
        .scalars()
        .all()
    )
    assert len(rows) == 1


def test_seed_does_not_clobber_a_superseding_edit(session: Session) -> None:
    """A supersede + insert (the same shape a future frame edit would use) survives a re-seed.

    Mirrors ``test_concept_store.test_seed_does_not_clobber_a_frame_edit`` — the
    re-seed's ``ON CONFLICT DO NOTHING`` skips the vertical whose active row was
    already superseded-and-replaced, never overwriting or raising.
    """
    assert ensure_envelope_seeded(session, "finance") == 1
    session.execute(
        update(VerticalEnvelope)
        .where(VerticalEnvelope.vertical == "finance", VerticalEnvelope.superseded_at.is_(None))
        .values(superseded_at=datetime.now(UTC))
    )
    session.add(
        VerticalEnvelope(
            vertical="finance",
            name="financial_reporting",
            version="2.0.0",
            description="edited description",
            source="seed",
        )
    )
    session.flush()
    # Re-seed: 'finance' collides on the active partial-unique index → skipped.
    assert ensure_envelope_seeded(session, "finance") == 0
    row = load_workspace_envelope(session, "finance")
    assert row is not None
    assert row.version == "2.0.0"  # the edit survives, not the reseed


def test_framed_vertical_seeds_nothing(session: Session) -> None:
    """A vertical with no on-disk ontology.yaml is FRAMED — no envelope to seed.

    Without the explicit ``resolve_vertical`` gate this would seed a synthesized
    row (the empty ``{"name": v}`` base OntologyLoader falls back to for any
    resolvable-but-off-disk name) — exactly the fabrication DAT-883 removes.
    """
    assert ensure_envelope_seeded(session, "some_framed_vertical") == 0
    assert load_workspace_envelope(session, "some_framed_vertical") is None


def test_unknown_vertical_seeds_nothing(session: Session) -> None:
    assert ensure_envelope_seeded(session, "totally_unknown_typo") == 0
    assert load_workspace_envelope(session, "totally_unknown_typo") is None


def test_placeholder_adhoc_seeds_its_real_envelope(session: Session) -> None:
    """``_adhoc`` ships real YAML (name/version/description) despite being the
    no-vertical placeholder — it resolves PLACEHOLDER, not SHIPPED, but still has
    an on-disk envelope worth seeding honestly."""
    assert ensure_envelope_seeded(session, "_adhoc") == 1
    row = load_workspace_envelope(session, "_adhoc")
    assert row is not None
    assert row.name == "_adhoc"
    assert row.version == "1.0.0"


def test_load_returns_none_when_never_seeded(session: Session) -> None:
    assert load_workspace_envelope(session, "finance") is None


def test_envelope_check_constraint_rejects_a_source_with_no_live_writer(session: Session) -> None:
    """The CHECK constraint admits only the one live writer — 'seed'.

    No writer in this codebase constructs a 'frame' (or any other) envelope row
    today (see the model docstring) — a CHECK admitting a value nothing produces
    is the DAT-802 defect this guards against.
    """
    session.add(VerticalEnvelope(vertical="finance", name="x", source="frame"))
    with pytest.raises(Exception):  # noqa: B017 - dialect-specific IntegrityError/ProgrammingError
        session.flush()
