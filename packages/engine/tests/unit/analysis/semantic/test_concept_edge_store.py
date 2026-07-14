"""Concept-edge seed — disjoint_with from convention partitions (DAT-729).

Pins the config→DB seam for the graph's vocabulary edges: the shipped vertical's
convention ``concept_groups`` seed typed ``disjoint_with`` rows (both directions,
idempotently), the same lever that promoted concepts in DAT-728. The live PGQ binding
is exercised in ``tests/integration/storage/test_property_graph.py``.
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from dataraum.analysis.semantic.concept_edge_store import ensure_concept_edges_seeded
from dataraum.analysis.semantic.concept_store import ensure_concepts_seeded
from dataraum.analysis.semantic.db_models import Concept, ConceptEdge, ConceptEdgePredicate


def _active_edges(session: Session, vertical: str) -> list[ConceptEdge]:
    return list(
        session.execute(
            select(ConceptEdge).where(
                ConceptEdge.vertical == vertical, ConceptEdge.superseded_at.is_(None)
            )
        ).scalars()
    )


def _pairs(session: Session, vertical: str) -> set[tuple[str, str]]:
    return {(e.from_concept, e.to_concept) for e in _active_edges(session, vertical)}


# finance has two group-bearing conventions: sign_natural_balance (credit_normal 4 ×
# debit_normal 8 = 32 unordered cross-pairs) and balance_sheet_composition (asset
# family 4 × liability family 2 = 8) — but every balance pair is ALSO a sign pair
# (the asset family is wholly debit-normal, the liability family wholly credit-normal),
# so the union is 32 unordered = 64 directed edges.
_FINANCE_DIRECTED_DISJOINT = 64


def test_seed_finance_creates_bidirectional_disjoint_edges(session: Session) -> None:
    n = ensure_concept_edges_seeded(session, "finance")
    assert n == _FINANCE_DIRECTED_DISJOINT
    edges = _active_edges(session, "finance")
    # Every edge is a seeded disjoint_with — this phase authors only that predicate.
    assert {e.predicate for e in edges} == {ConceptEdgePredicate.DISJOINT_WITH.value}
    assert {e.source for e in edges} == {"seed"}
    assert all(e.tolerance is None for e in edges)
    # No self-loops (groups are validated mutually exclusive, so a != b always).
    assert all(e.from_concept != e.to_concept for e in edges)


def test_seed_disjoint_is_symmetric(session: Session) -> None:
    """A symmetric predicate is stored both ways so a directed MATCH finds it either side."""
    ensure_concept_edges_seeded(session, "finance")
    pairs = _pairs(session, "finance")
    assert pairs, "no disjoint edges seeded"
    assert all((b, a) in pairs for (a, b) in pairs), "disjoint_with must be bidirectional"


def test_seed_captures_the_named_disjoint_examples(session: Session) -> None:
    """The DD's illustrative pairs fall out of the sign/family partitions, both ways."""
    ensure_concept_edges_seeded(session, "finance")
    pairs = _pairs(session, "finance")
    # AP ⊥ AR (payable is credit-normal, receivable debit-normal).
    assert ("accounts_payable", "accounts_receivable") in pairs
    assert ("accounts_receivable", "accounts_payable") in pairs
    # ASSET ⊥ LIABILITY at the concept level.
    assert ("current_assets", "current_liabilities") in pairs
    assert ("current_liabilities", "current_assets") in pairs


def test_seed_is_idempotent(session: Session) -> None:
    assert ensure_concept_edges_seeded(session, "finance") == _FINANCE_DIRECTED_DISJOINT
    # A re-run inserts nothing — never duplicates, never clobbers an edited edge.
    assert ensure_concept_edges_seeded(session, "finance") == 0
    assert len(_active_edges(session, "finance")) == _FINANCE_DIRECTED_DISJOINT


def test_seed_does_not_clobber_a_superseded_edge(session: Session) -> None:
    """A frame edit (supersede + insert) survives a re-seed — the race-safety contract.

    The re-seed's ``ON CONFLICT DO NOTHING`` skips the edge whose active row is the
    edit (same active partial-unique index as concepts), never overwriting it and
    never RAISING on the collision.
    """
    assert ensure_concept_edges_seeded(session, "finance") == _FINANCE_DIRECTED_DISJOINT
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
            predicate=ConceptEdgePredicate.DISJOINT_WITH.value,
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

    The convention linter already guarantees group members are declared concepts; this
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
