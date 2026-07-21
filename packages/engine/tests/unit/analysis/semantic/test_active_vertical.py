"""The workspace active-vertical binding gate (DAT-848).

Pins ``require_active_vertical``: the resolve-time seam that binds a workspace to
ONE vertical and fails loud when a later run's ``--vertical`` disagrees — the fact
that was missing, so a wrong vertical seeded a second vocabulary beside the first.
"""

from __future__ import annotations

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.semantic.concept_store import (
    ensure_concepts_seeded,
    load_workspace_concepts,
    require_active_vertical,
)
from dataraum.analysis.semantic.db_models import Concept, WorkspaceSettings
from dataraum.core.vertical import (
    VerticalKind,
    set_framed_concept_resolver,
)


@pytest.fixture
def framed_sales() -> None:
    """Make ``sales`` a KNOWN (framed) vertical for the duration of a test.

    A mismatch can only be exercised with a SECOND known vertical — otherwise the
    unknown-name guard fires first. Mirrors ``tests/unit/core/test_vertical.py``.
    """
    set_framed_concept_resolver(lambda: {"sales"})
    yield
    set_framed_concept_resolver(None)


def _bound(session: Session) -> str | None:
    return session.execute(select(WorkspaceSettings.active_vertical)).scalar_one_or_none()


def test_binds_first_non_placeholder(session: Session) -> None:
    kind = require_active_vertical(session, "finance")
    assert kind is VerticalKind.SHIPPED
    assert _bound(session) == "finance"


def test_matching_vertical_passes_and_stays_singleton(session: Session) -> None:
    require_active_vertical(session, "finance")
    # A second run with the SAME vertical is a no-op — no error, one row.
    require_active_vertical(session, "finance")
    assert _bound(session) == "finance"
    assert len(session.execute(select(WorkspaceSettings)).scalars().all()) == 1


def test_singleton_bind_conflict_is_a_noop(session: Session) -> None:
    """The race the bind relies on: a second INSERT on the pinned singleton is a
    DO-NOTHING no-op (the winner keeps the row), never a duplicate or an error.

    Single-threaded SQLite can't produce the true "SELECT saw nothing, then INSERT
    conflicts" interleaving, so this pins the mechanic ``require_active_vertical``
    leans on: ``insert_if_absent`` on the ``pin`` index leaves the first writer's
    row untouched and reports zero rows inserted for the loser."""
    from dataraum.storage.upsert import insert_if_absent

    # Mirrors the gate's call exactly: ON CONFLICT (pin) targets the singleton PK
    # (a full unique index — no partial index_where).
    first = insert_if_absent(
        session,
        WorkspaceSettings,
        [{"pin": True, "active_vertical": "finance"}],
        index_elements=["pin"],
    )
    loser = insert_if_absent(
        session,
        WorkspaceSettings,
        [{"pin": True, "active_vertical": "sales"}],
        index_elements=["pin"],
    )
    assert first == 1
    assert loser == 0  # the second writer inserts nothing — the winner's row stands
    assert _bound(session) == "finance"


def test_mismatch_fails_loud(session: Session, framed_sales: None) -> None:
    require_active_vertical(session, "finance")
    with pytest.raises(RuntimeError, match="Workspace vertical mismatch"):
        require_active_vertical(session, "sales")
    # The binding is unchanged — a rejected run never re-binds.
    assert _bound(session) == "finance"


def test_placeholder_never_binds_and_never_checked(session: Session) -> None:
    # An ad-hoc run declares no domain: it binds nothing.
    assert require_active_vertical(session, "_adhoc") is VerticalKind.PLACEHOLDER
    assert require_active_vertical(session, "") is VerticalKind.PLACEHOLDER
    assert _bound(session) is None
    # Even on a bound workspace, a placeholder passes (it is not a "wrong vertical",
    # it is "no vertical") rather than mismatching the binding.
    require_active_vertical(session, "finance")
    assert require_active_vertical(session, "_adhoc") is VerticalKind.PLACEHOLDER
    assert _bound(session) == "finance"


def test_unknown_vertical_fails_loud_without_binding(session: Session) -> None:
    with pytest.raises(RuntimeError, match="Unknown vertical"):
        require_active_vertical(session, "finanace")  # typo
    assert _bound(session) is None


def test_wrong_vertical_never_contaminates_the_bound_vocabulary(
    session: Session, framed_sales: None
) -> None:
    """The DAT-848 regression: two verticals cannot coexist as the served vocabulary.

    Bind + seed finance (vertical A). A row for ANOTHER vertical then lands directly
    in the table — the shape the eval's ``frame_wild_vertical`` stand-in produces,
    and the shape a wrong ``--vertical`` used to seed. The active-vertical reader
    never returns it (finance stays intact), and a run that tries to SWITCH to
    another known vertical fails loud rather than seeding a second vocabulary.
    """
    require_active_vertical(session, "finance")
    ensure_concepts_seeded(session, "finance")

    # A wild-vertical concept row, written directly (bypassing the seed gate).
    session.add(Concept(vertical="healthcare", name="diagnosis", kind="entity", source="frame"))
    session.flush()

    # A-only reads: the bound vertical's vocabulary is served whole; the wild row —
    # present in the table — is never surfaced.
    served = {c.name for c in load_workspace_concepts(session, "finance").concepts}
    assert "revenue" in served
    assert "diagnosis" not in served

    # Switching to B is loud, not silent: the workspace stays bound to finance.
    with pytest.raises(RuntimeError, match="Workspace vertical mismatch"):
        require_active_vertical(session, "sales")
    assert _bound(session) == "finance"
