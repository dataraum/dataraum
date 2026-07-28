"""The typed vertical envelope — seed + read (DAT-883, config→DB).

Pins the config→DB seam for the vertical's own identity (name/version/description):
a shipped vertical's YAML seeds a single typed ``VerticalEnvelope`` row, and the
runtime reads it back — the source ``load_workspace_concepts`` moved onto, off the
``OntologyLoader`` YAML re-parse. A framed vertical (no on-disk YAML) seeds nothing
and reads back as absent, never a fabricated placeholder.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime

import pytest
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from dataraum.analysis.semantic.db_models import VerticalEnvelope
from dataraum.analysis.semantic.envelope_store import (
    ensure_envelope_seeded,
    load_workspace_envelope,
)
from dataraum.core.vertical import VerticalKind, resolve_vertical, set_framed_concept_resolver


@pytest.fixture
def framed_wildcat() -> Iterator[None]:
    """Make ``wildcat`` a KNOWN (framed) vertical for the duration of a test.

    Registers a real framed footprint via the resolver seam ``core/vertical.py``
    exposes for exactly this purpose (mirrors ``test_active_vertical.framed_sales``)
    — ``resolve_vertical("wildcat")`` genuinely returns FRAMED, not UNKNOWN, so a
    test using this fixture exercises the FRAMED branch, not a same-shaped-but-wrong
    UNKNOWN one.
    """
    set_framed_concept_resolver(lambda: {"wildcat"})
    yield
    set_framed_concept_resolver(None)


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
    already superseded-and-replaced, never overwriting or raising. This is also the
    concrete shape of the "a bumped ontology.yaml never reaches an existing
    workspace" consequence the seed function's docstring now states: a re-seed is a
    no-op regardless of what's on disk once ANY active row exists.
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


def test_framed_vertical_seeds_nothing(session: Session, framed_wildcat: None) -> None:
    """A GENUINELY framed vertical (real resolver footprint, not just an unknown
    name) has no on-disk source and so seeds no envelope.

    A prior version of this test used a bare unregistered name and asserted the
    same outcome as ``test_unknown_vertical_seeds_nothing`` below — but
    ``resolve_vertical`` of an unregistered bare name in a unit session is UNKNOWN,
    not FRAMED, so that version silently tested the UNKNOWN path twice and would
    stay green even if the FRAMED branch were mishandled (a real mutation hole: a
    prior gate that widened to admit FRAMED left the full suite green, since
    nothing exercised FRAMED at all). This builds a REAL framed footprint via
    ``set_framed_concept_resolver`` first, so ``resolve_vertical`` genuinely
    returns FRAMED, before asserting the seed is refused.
    """
    assert resolve_vertical("wildcat") is VerticalKind.FRAMED  # sanity: genuinely framed
    assert ensure_envelope_seeded(session, "wildcat") == 0
    assert load_workspace_envelope(session, "wildcat") is None


def test_unknown_vertical_seeds_nothing(session: Session) -> None:
    assert resolve_vertical("totally_unknown_typo") is VerticalKind.UNKNOWN  # sanity
    assert ensure_envelope_seeded(session, "totally_unknown_typo") == 0
    assert load_workspace_envelope(session, "totally_unknown_typo") is None


def test_wild_underscore_name_with_no_real_directory_seeds_nothing(session: Session) -> None:
    """An unseen leading-underscore name resolves PLACEHOLDER by NAME-SHAPE alone
    (``core.vertical._is_placeholder``) — it must NOT seed, because it has no real
    on-disk ``ontology.yaml`` (unlike ``_adhoc``, which genuinely ships one).

    This is the regression the gate fix closes: gating on
    ``resolve_vertical(...) in (SHIPPED, PLACEHOLDER)`` admitted ANY leading-
    underscore name, seeding a ``source='seed'`` row synthesized from
    ``OntologyLoader``'s empty ``{"name": v}`` base — a fabricated envelope with no
    real seed source, burning the singleton slot against a future frame-time
    writer. The fix gates on ``OntologyLoader.list_verticals()`` (real on-disk
    glob) instead.
    """
    assert resolve_vertical("_wild_no_such_dir") is VerticalKind.PLACEHOLDER  # name-shape only
    assert ensure_envelope_seeded(session, "_wild_no_such_dir") == 0
    assert load_workspace_envelope(session, "_wild_no_such_dir") is None


def test_placeholder_adhoc_seeds_its_real_envelope(session: Session) -> None:
    """``_adhoc`` ships REAL on-disk YAML (name/version/description) despite being
    the no-vertical placeholder — the on-disk-presence gate admits it (it is
    genuinely in ``OntologyLoader.list_verticals()``'s glob), same as any other
    on-disk vertical; contrast ``test_wild_underscore_name_with_no_real_directory_
    seeds_nothing`` above, an underscore name with NO real directory."""
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
    is the DAT-802 defect this guards against. Asserts the SPECIFIC constraint
    fires (by name), not just "some exception" — a bare ``pytest.raises(Exception)``
    would also pass for an unrelated failure (a typo'd column, a missing FK), which
    would silently stop testing the CHECK at all.
    """
    session.add(VerticalEnvelope(vertical="finance", name="x", source="frame"))
    with pytest.raises(IntegrityError, match="ck_vertical_envelopes_source"):
        session.flush()
