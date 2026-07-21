"""``Relationship.oriented_row`` — the single relationship-row builder (DAT-777)
and the two-layer confirmation-source / orientation enforcement (DAT-776/777).

``oriented_row`` is the ONE chokepoint every write path builds through, so the
FK orientation invariant (persist many→one, child→parent) and the
confirmation-source vocabulary are set in one place instead of a per-path call
the other writers forgot. The DB ``CheckConstraint``s are the structural backstop:
a mis-oriented ``one-to-many`` row or a dead confirmation-source value fails loud
at flush even on a writer that bypasses the helper.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from dataraum.analysis.relationships.db_models import (
    _SYMMETRIC_EVIDENCE_KEYS,
    Relationship,
    swap_directional_evidence,
)
from dataraum.storage import Column, Source, Table
from dataraum.storage.base import init_database

# --- Pure builder: orientation + confirmation-source, no DB. -----------------


def test_one_to_many_flips_to_many_to_one_child_parent() -> None:
    # Judge emitted parent→child (journal_entries → journal_lines): measured
    # one-to-many. Flip so the row is stored many-to-one child→parent.
    row = Relationship.oriented_row(
        run_id="r1",
        from_table_id="entries_tbl",
        from_column_id="entries_col",
        to_table_id="lines_tbl",
        to_column_id="lines_col",
        relationship_type="foreign_key",
        cardinality="one-to-many",
        confidence=0.9,
        detection_method="llm",
        confirmation_source="judge",
        evidence={"left_referential_integrity": 0.6, "right_referential_integrity": 1.0},
    )
    assert (row["from_table_id"], row["from_column_id"]) == ("lines_tbl", "lines_col")
    assert (row["to_table_id"], row["to_column_id"]) == ("entries_tbl", "entries_col")
    assert row["cardinality"] == "many-to-one"
    # Directional evidence follows the swap; RI(from→to) exchanges.
    assert row["evidence"]["left_referential_integrity"] == 1.0
    assert row["evidence"]["right_referential_integrity"] == 0.6
    # A many-to-one child→parent join never fans out.
    assert row["evidence"]["introduces_duplicates"] is False


def test_flip_swaps_every_directional_metric_generically() -> None:
    # Uniqueness (and any other left_/right_ pair) follows the endpoint, not just RI.
    row = Relationship.oriented_row(
        run_id="r1",
        from_table_id="a",
        from_column_id="ac",
        to_table_id="b",
        to_column_id="bc",
        relationship_type="candidate",
        cardinality="one-to-many",
        confidence=0.7,
        detection_method="candidate",
        confirmation_source="unconfirmed",
        evidence={
            "left_uniqueness": 0.1,
            "right_uniqueness": 0.99,
            "left_orphan_count": 3,
            "cardinality": "one-to-many",
            "join_confidence": 0.7,
        },
    )
    assert row["evidence"]["left_uniqueness"] == 0.99
    assert row["evidence"]["right_uniqueness"] == 0.1
    # The orphan count is a FROM-SIDE measurement, so it moves with its endpoint
    # like every other prefixed metric. It was unprefixed before DAT-725 and
    # therefore stayed put, leaving a stored row that read "L=100% RI" beside
    # "orphans=3" — a reading that cannot be true, shipped to the judge and to
    # the orphan-rate detector as fact.
    assert "left_orphan_count" not in row["evidence"]
    assert row["evidence"]["right_orphan_count"] == 3
    # The evidence copy of the cardinality tracks the column it mirrors.
    assert row["evidence"]["cardinality"] == "many-to-one"
    # Symmetric measurements are untouched.
    assert row["evidence"]["join_confidence"] == 0.7


@pytest.mark.parametrize("cardinality", ["many-to-one", None])
def test_unorientable_cardinalities_are_left_as_emitted(cardinality: str | None) -> None:
    row = Relationship.oriented_row(
        run_id="r1",
        from_table_id="a",
        from_column_id="ac",
        to_table_id="b",
        to_column_id="bc",
        relationship_type="foreign_key",
        cardinality=cardinality,
        confidence=1.0,
        detection_method="manual",
        confirmation_source="user",
        evidence={"left_referential_integrity": 1.0},
    )
    assert (row["from_column_id"], row["to_column_id"]) == ("ac", "bc")
    assert row["cardinality"] == cardinality
    # Untouched — no fabricated fan-out flag, no evidence swap.
    assert row["evidence"] == {"left_referential_integrity": 1.0}


# --- Edge-kind resolution (DAT-850): measurement refutes a reference claim. ---


@pytest.mark.parametrize("claimed", ["foreign_key", "hierarchy"])
def test_many_to_many_resolves_a_reference_claim_to_conformed_dimension(claimed: str) -> None:
    """A reference needs a unique parent side; measured m2m is a fact meeting.

    The judge's claim is kept as evidence (``resolved_from_type``), its
    EXISTENCE verdict survives (confirmation_source untouched), and the
    endpoints/cardinality are not reoriented — m2m is symmetric.
    """
    row = Relationship.oriented_row(
        run_id="r1",
        from_table_id="fact_a",
        from_column_id="a_region",
        to_table_id="fact_b",
        to_column_id="b_region",
        relationship_type=claimed,
        cardinality="many-to-many",
        confidence=0.9,
        detection_method="llm",
        confirmation_source="judge",
        evidence={"join_confidence": 0.8},
    )
    assert row["relationship_type"] == "conformed_dimension"
    assert row["evidence"]["resolved_from_type"] == claimed
    assert row["cardinality"] == "many-to-many"
    assert (row["from_column_id"], row["to_column_id"]) == ("a_region", "b_region")
    assert row["confirmation_source"] == "judge"
    assert row["evidence"]["join_confidence"] == 0.8


def test_many_to_many_candidate_passes_through_unresolved() -> None:
    """A structural candidate is a measurement awaiting a claim, not a claim."""
    row = Relationship.oriented_row(
        run_id="r1",
        from_table_id="a",
        from_column_id="ac",
        to_table_id="b",
        to_column_id="bc",
        relationship_type="candidate",
        cardinality="many-to-many",
        confidence=0.7,
        detection_method="candidate",
        confirmation_source="unconfirmed",
        evidence=None,
    )
    assert row["relationship_type"] == "candidate"
    assert "resolved_from_type" not in row["evidence"]


def test_unmeasured_reference_claim_is_not_resolved() -> None:
    """NULL cardinality is unknown, not contradicted — the claim stands."""
    row = Relationship.oriented_row(
        run_id="r1",
        from_table_id="a",
        from_column_id="ac",
        to_table_id="b",
        to_column_id="bc",
        relationship_type="foreign_key",
        cardinality=None,
        confidence=0.9,
        detection_method="llm",
        confirmation_source="judge",
        evidence=None,
    )
    assert row["relationship_type"] == "foreign_key"
    assert "resolved_from_type" not in row["evidence"]


# --- 1:1 orientation from measured containment asymmetry (DAT-725). ----------


def _one_to_one_row(evidence: dict[str, object] | None) -> dict[str, object]:
    return Relationship.oriented_row(
        run_id="r1",
        from_table_id="parent_tbl",
        from_column_id="parent_col",
        to_table_id="child_tbl",
        to_column_id="child_col",
        relationship_type="foreign_key",
        cardinality="one-to-one",
        confidence=0.95,
        detection_method="llm",
        confirmation_source="judge",
        evidence=evidence,
    )


@pytest.mark.parametrize(
    ("shape", "evidence"),
    [
        # A clean subset, emitted parent-first. The old rule swapped this and
        # was right — but it cannot tell this shape from the next one.
        (
            "clean subset, emitted parent-first",
            {"left_referential_integrity": 24.33, "right_referential_integrity": 100.0},
        ),
        # A child carrying ORPHAN values, emitted CORRECTLY child-first. It
        # measures the same way round as the case above, so the old rule swapped
        # it too — inverting a correct emission. joins.py deliberately admits
        # these ("a dirty subset FK is still an FK") and the judge prompt
        # deliberately confirms them.
        (
            "orphan-bearing child, emitted correctly",
            {
                "left_referential_integrity": 60.0,
                "right_referential_integrity": 100.0,
                "left_key_coverage": 60.0,
                "right_key_coverage": 100.0,
                "left_orphan_count": 2,
                "right_orphan_count": 0,
                "cardinality_verified": True,
            },
        ),
        # Identical value sets: containment is silent either way.
        (
            "dense bijection",
            {"left_referential_integrity": 100.0, "right_referential_integrity": 100.0},
        ),
        # Completeness disagrees loudly. Still not this helper's call.
        (
            "symmetric containment, lopsided completeness",
            {
                "left_referential_integrity": 100.0,
                "right_referential_integrity": 100.0,
                "left_uniqueness": 1.0,
                "right_uniqueness": 0.4698,
            },
        ),
    ],
)
def test_one_to_one_is_never_re_oriented(shape: str, evidence: dict[str, object]) -> None:
    """A 1:1 keeps the orientation it was written with, whatever the numbers say.

    The removed rule swapped when forward containment measured below reverse.
    That reduces to "the from side has more distinct values" — right for a clean
    subset, WRONG for a child with orphans, and the two are indistinguishable to
    containment. Since it cannot separate them, it does not decide: direction
    for a 1:1 belongs to the judge, which is told to reason from dependence
    (``semantic_per_table``'s orientation section).
    """
    row = _one_to_one_row(evidence)

    assert (row["from_column_id"], row["to_column_id"]) == ("parent_col", "child_col"), shape
    assert "orientation_swapped" not in row["evidence"], shape


def test_one_to_one_evidence_is_carried_verbatim() -> None:
    """No swap means no relabelling: per-side metrics stay on the side measured."""
    row = _one_to_one_row(
        {
            "left_referential_integrity": 24.33,
            "right_referential_integrity": 100.0,
            "left_uniqueness": 1.0,
            "right_uniqueness": 0.4698,
        }
    )

    assert row["evidence"]["left_uniqueness"] == 1.0
    assert row["evidence"]["right_uniqueness"] == 0.4698
    assert row["evidence"]["left_referential_integrity"] == 24.33


def test_confirmation_source_is_passed_through_verbatim() -> None:
    for source in ("unconfirmed", "judge", "user", "keeper"):
        row = Relationship.oriented_row(
            run_id="r1",
            from_table_id="a",
            from_column_id="ac",
            to_table_id="b",
            to_column_id="bc",
            relationship_type="foreign_key",
            cardinality="many-to-one",
            confidence=1.0,
            detection_method="llm",
            confirmation_source=source,
            evidence=None,
        )
        assert row["confirmation_source"] == source


# --- Structural backstop: the DB CHECK constraints. --------------------------


@pytest.fixture
def session() -> Session:
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    init_database(engine)
    db = Session(engine)
    db.add_all(
        [
            Source(source_id="s1", name="s1", source_type="csv"),
            Table(table_id="t1", source_id="s1", table_name="orders", layer="typed"),
            Table(table_id="t2", source_id="s1", table_name="customers", layer="typed"),
            Column(column_id="ca", table_id="t1", column_name="customer_id", column_position=0),
            Column(column_id="cb", table_id="t2", column_name="id", column_position=0),
        ]
    )
    db.commit()
    yield db
    db.close()


def _add(session: Session, **overrides: object) -> None:
    fields: dict[str, object] = {
        "run_id": "r1",
        "from_table_id": "t1",
        "from_column_id": "ca",
        "to_table_id": "t2",
        "to_column_id": "cb",
        "relationship_type": "foreign_key",
        "cardinality": "many-to-one",
        "confidence": 0.9,
        "detection_method": "llm",
        "confirmation_source": "judge",
    }
    fields.update(overrides)
    session.add(Relationship(**fields))
    session.flush()


def test_check_rejects_dead_confirmation_source(session: Session) -> None:
    """DAT-776 regression: a value outside the closed vocabulary fails at flush."""
    with pytest.raises(IntegrityError):
        _add(session, confirmation_source="confirmed")


def test_check_rejects_a_reversed_one_to_many_row(session: Session) -> None:
    """DAT-777: a mis-oriented row cannot persist even bypassing ``oriented_row``."""
    with pytest.raises(IntegrityError):
        _add(session, cardinality="one-to-many")


def test_check_admits_the_canonical_orientations(session: Session) -> None:
    # A REFERENCE row's legal cardinalities exclude many-to-many (DAT-850 —
    # that shape refutes the reference claim, see the test below).
    for cardinality in ("many-to-one", "one-to-one", None):
        _add(session, cardinality=cardinality, from_column_id="ca", to_column_id="cb")
        session.rollback()  # each is its own attempt on the unique pair
    # many-to-many persists as the conformed kind (or a structural candidate).
    _add(session, relationship_type="conformed_dimension", cardinality="many-to-many")
    session.rollback()
    _add(
        session,
        relationship_type="candidate",
        cardinality="many-to-many",
        detection_method="candidate",
        confirmation_source="unconfirmed",
    )
    session.rollback()


@pytest.mark.parametrize("claimed", ["foreign_key", "hierarchy"])
def test_check_rejects_a_reference_claim_with_many_to_many(
    session: Session, claimed: str
) -> None:
    """DAT-850: the fk+m2m contradiction is unwritable even bypassing the helper."""
    with pytest.raises(IntegrityError):
        _add(session, relationship_type=claimed, cardinality="many-to-many")


def test_check_rejects_a_value_outside_the_type_vocabulary(session: Session) -> None:
    with pytest.raises(IntegrityError):
        _add(session, relationship_type="semantic_reference")


# --- the prefix contract, enforced rather than documented (DAT-725). ---------


def test_an_undeclared_bare_key_raises_instead_of_passing_through() -> None:
    """The guard that makes "prefix a directional metric" a rule, not a habit.

    ``evidence`` is a schema-less JSON column, so nothing typed the difference
    between a per-side measurement and a fact about the pair. Twice a writer
    added a from-side metric without a prefix — ``orphan_count``,
    ``join_success_rate`` — and the flip carried it through unchanged, leaving
    it describing a side it was no longer on. Both shipped, both were invisible
    until the flip was run on real data. A pass-through cannot tell "symmetric"
    from "misspelled", so it refuses to guess.
    """
    with pytest.raises(ValueError, match="neither left_/right_-prefixed nor declared symmetric"):
        swap_directional_evidence({"orphan_count": 3})


def test_declared_symmetric_keys_survive_the_flip_unchanged() -> None:
    evidence = dict.fromkeys(_SYMMETRIC_EVIDENCE_KEYS, "unchanged")
    assert swap_directional_evidence(evidence) == evidence


def test_every_key_the_detector_writes_is_classifiable() -> None:
    """The evidence a real writer produces must pass the guard.

    Mirrors ``detector._store_candidates``'s evidence dict plus the per-side
    metrics ``evaluate_join_candidate`` adds — if a new measurement is added
    there without a prefix or a declaration, this fails before a run does.
    """
    detector_evidence = {
        "join_confidence": 0.97,
        "cardinality": "one-to-many",
        "left_uniqueness": 0.02,
        "right_uniqueness": 1.0,
        "statistical_confidence": 1.0,
        "algorithm": "exact",
        "source": "value_overlap",
        "left_referential_integrity": 100.0,
        "right_referential_integrity": 62.5,
        "left_key_coverage": 100.0,
        "right_key_coverage": 62.5,
        "left_orphan_count": 0,
        "right_orphan_count": 3,
        "cardinality_verified": True,
        "introduces_duplicates": True,
    }
    flipped = swap_directional_evidence(detector_evidence)

    assert flipped["cardinality"] == "many-to-one"
    assert flipped["left_referential_integrity"] == 62.5
    assert flipped["left_orphan_count"] == 3
    assert "introduces_duplicates" not in flipped
