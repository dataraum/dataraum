"""Concept-edge seed — disjoint_with + part_of from the vertical (DAT-729).

Pins the config→DB seam for the graph's vocabulary edges: the shipped vertical's
convention ``concept_groups`` seed typed ``disjoint_with`` rows (both directions) and
its ``compositions`` seed directed ``part_of`` rows — idempotently, the same lever that
promoted concepts in DAT-728. The live PGQ binding + closure are exercised in
``tests/integration/storage/test_property_graph.py``.
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from dataraum.analysis.semantic.concept_edge_store import ensure_concept_edges_seeded
from dataraum.analysis.semantic.concept_store import ensure_concepts_seeded
from dataraum.analysis.semantic.db_models import Concept, ConceptEdge, ConceptEdgePredicate

_DISJOINT = ConceptEdgePredicate.DISJOINT_WITH.value
_PART_OF = ConceptEdgePredicate.PART_OF.value

# finance has two group-bearing conventions: sign_natural_balance (credit_normal 4 ×
# debit_normal 8 = 32 unordered cross-pairs) and balance_sheet_composition (asset
# family 4 × liability family 2 = 8) — but every balance pair is ALSO a sign pair
# (the asset family is wholly debit-normal, the liability family wholly credit-normal),
# so the union is 32 unordered = 64 directed disjoint edges. Compositions add 4 part_of
# edges: current_assets ← {cash, accounts_receivable, inventory}, current_liabilities ← {AP}.
_FINANCE_DISJOINT = 64
_FINANCE_PART_OF = 4
_FINANCE_TOTAL = _FINANCE_DISJOINT + _FINANCE_PART_OF


def _active_edges(session: Session, vertical: str) -> list[ConceptEdge]:
    return list(
        session.execute(
            select(ConceptEdge).where(
                ConceptEdge.vertical == vertical, ConceptEdge.superseded_at.is_(None)
            )
        ).scalars()
    )


def _pairs(session: Session, vertical: str, predicate: str) -> set[tuple[str, str]]:
    return {
        (e.from_concept, e.to_concept)
        for e in _active_edges(session, vertical)
        if e.predicate == predicate
    }


def test_seed_finance_creates_disjoint_and_part_of_edges(session: Session) -> None:
    n = ensure_concept_edges_seeded(session, "finance")
    assert n == _FINANCE_TOTAL
    edges = _active_edges(session, "finance")
    assert {e.predicate for e in edges} == {_DISJOINT, _PART_OF}
    assert {e.source for e in edges} == {"seed"}
    assert all(e.tolerance is None for e in edges)
    # No self-loops (groups are mutually exclusive; a whole is never its own part).
    assert all(e.from_concept != e.to_concept for e in edges)
    assert sum(1 for e in edges if e.predicate == _DISJOINT) == _FINANCE_DISJOINT
    assert sum(1 for e in edges if e.predicate == _PART_OF) == _FINANCE_PART_OF


def test_seed_disjoint_is_symmetric(session: Session) -> None:
    """A symmetric predicate is stored both ways so a directed MATCH finds it either side."""
    ensure_concept_edges_seeded(session, "finance")
    pairs = _pairs(session, "finance", _DISJOINT)
    assert pairs, "no disjoint edges seeded"
    assert all((b, a) in pairs for (a, b) in pairs), "disjoint_with must be bidirectional"


def test_seed_captures_the_named_disjoint_examples(session: Session) -> None:
    """The DD's illustrative pairs fall out of the sign/family partitions, both ways."""
    ensure_concept_edges_seeded(session, "finance")
    pairs = _pairs(session, "finance", _DISJOINT)
    # AP ⊥ AR (payable is credit-normal, receivable debit-normal).
    assert ("accounts_payable", "accounts_receivable") in pairs
    assert ("accounts_receivable", "accounts_payable") in pairs
    # ASSET ⊥ LIABILITY at the concept level.
    assert ("current_assets", "current_liabilities") in pairs
    assert ("current_liabilities", "current_assets") in pairs


def test_seed_part_of_is_directed_composition(session: Session) -> None:
    """Each composition part rolls up into its whole — directed, one row, no reverse."""
    ensure_concept_edges_seeded(session, "finance")
    part_of = _pairs(session, "finance", _PART_OF)
    assert part_of == {
        ("cash", "current_assets"),
        ("accounts_receivable", "current_assets"),
        ("inventory", "current_assets"),
        ("accounts_payable", "current_liabilities"),
    }
    # Directed: the whole is NOT part_of its part.
    assert ("current_assets", "cash") not in part_of


def test_seed_is_idempotent(session: Session) -> None:
    assert ensure_concept_edges_seeded(session, "finance") == _FINANCE_TOTAL
    # A re-run inserts nothing — never duplicates, never clobbers an edited edge.
    assert ensure_concept_edges_seeded(session, "finance") == 0
    assert len(_active_edges(session, "finance")) == _FINANCE_TOTAL


def test_seed_does_not_clobber_a_superseded_edge(session: Session) -> None:
    """A frame edit (supersede + insert) survives a re-seed — the race-safety contract.

    The re-seed's ``ON CONFLICT DO NOTHING`` skips the edge whose active row is the
    edit (same active partial-unique index as concepts), never overwriting it and
    never RAISING on the collision.
    """
    assert ensure_concept_edges_seeded(session, "finance") == _FINANCE_TOTAL
    # Supersede one active edge and insert a new active row in its place (source=frame).
    session.execute(
        update(ConceptEdge)
        .where(
            ConceptEdge.vertical == "finance",
            ConceptEdge.from_concept == "accounts_payable",
            ConceptEdge.to_concept == "accounts_receivable",
            ConceptEdge.superseded_at.is_(None),
        )
        .values(superseded_at=datetime.now(UTC))
    )
    session.add(
        ConceptEdge(
            vertical="finance",
            predicate=_DISJOINT,
            from_concept="accounts_payable",
            to_concept="accounts_receivable",
            source="frame",
        )
    )
    session.flush()
    # Re-seed: that pair collides on the active index → skipped, no error.
    assert ensure_concept_edges_seeded(session, "finance") == 0
    edited = session.execute(
        select(ConceptEdge).where(
            ConceptEdge.vertical == "finance",
            ConceptEdge.from_concept == "accounts_payable",
            ConceptEdge.to_concept == "accounts_receivable",
            ConceptEdge.superseded_at.is_(None),
        )
    ).scalar_one()
    assert edited.source == "frame"  # the edit kept, not clobbered


def test_seed_edges_reference_seeded_concept_names(session: Session) -> None:
    """Every edge endpoint is a real concept name — the element view's JOIN resolves it.

    The convention/composition linters guarantee members are declared concepts; this
    pins that the seeded edges stay within the seeded concept vocabulary, so the
    ``og_concept_edges`` JOIN to ``concepts`` never silently drops a seed edge.
    """
    ensure_concepts_seeded(session, "finance")
    ensure_concept_edges_seeded(session, "finance")
    endpoints = {e.from_concept for e in _active_edges(session, "finance")} | {
        e.to_concept for e in _active_edges(session, "finance")
    }
    seeded_concepts = {
        c.name
        for c in session.execute(
            select(Concept).where(Concept.vertical == "finance", Concept.superseded_at.is_(None))
        ).scalars()
    }
    assert endpoints <= seeded_concepts
