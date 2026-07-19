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

from dataraum.analysis.relationships.db_models import Relationship
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
        evidence={"left_uniqueness": 0.1, "right_uniqueness": 0.99, "orphan_count": 3},
    )
    assert row["evidence"]["left_uniqueness"] == 0.99
    assert row["evidence"]["right_uniqueness"] == 0.1
    assert row["evidence"]["orphan_count"] == 3  # non-directional key stays put


@pytest.mark.parametrize("cardinality", ["many-to-one", "many-to-many", None])
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


def test_one_to_one_flips_when_emitted_from_the_referenced_side() -> None:
    """Run-#2 A2 class: a 1:1 FK confirmed parent→child gets re-oriented.

    The referencing side of a 1:1 FK is wholly contained in the referenced
    side (forward containment ~100%, reverse lower). A smaller forward than
    reverse containment on the emission means it points parent→child — the
    chokepoint swaps the endpoints and the directional evidence; cardinality
    stays one-to-one. (RI-only evidence is the candidate-metrics shape, where
    row-weighted equals distinct-weighted: both columns globally unique.)
    """
    row = _one_to_one_row(
        {"left_referential_integrity": 24.0, "right_referential_integrity": 100.0}
    )
    assert (row["from_table_id"], row["from_column_id"]) == ("child_tbl", "child_col")
    assert (row["to_table_id"], row["to_column_id"]) == ("parent_tbl", "parent_col")
    assert row["cardinality"] == "one-to-one"
    evidence = row["evidence"]
    assert isinstance(evidence, dict)
    assert evidence["left_referential_integrity"] == 100.0
    assert evidence["right_referential_integrity"] == 24.0
    # The swap leaves an audit trace — a re-oriented 1:1 row is otherwise
    # indistinguishable from a kept emission (the cardinality label is
    # unchanged, unlike the one-to-many flip).
    assert evidence["orientation_swapped"] is True
    # Unlike the one-to-many flip, no fan-out flag is fabricated: a 1:1 join
    # never fans out in either direction, and the flag was not measured here.
    assert "introduces_duplicates" not in evidence


def test_one_to_one_distinct_containment_outranks_biased_row_weighted_ri() -> None:
    """Duplicated ORPHAN rows must not invert a correct emission.

    The 1:1 measurement only checks the matched population, so the from side
    can carry duplicate rows of orphan values — row-weighted left RI then
    under-states containment (30% here) and, compared against the
    distinct-weighted right RI (42.9%), would wrongly swap a CORRECT
    child→parent emission. The distinct-weighted ``left_value_containment``
    (75%) is the like-for-like basis and must win.
    """
    row = _one_to_one_row(
        {
            "left_referential_integrity": 30.0,
            "left_value_containment": 75.0,
            "right_referential_integrity": 42.86,
        }
    )
    assert (row["from_column_id"], row["to_column_id"]) == ("parent_col", "child_col")
    evidence = row["evidence"]
    assert isinstance(evidence, dict)
    assert "orientation_swapped" not in evidence


def test_one_to_one_containment_key_still_swaps_a_true_flip() -> None:
    """The containment key decides in BOTH directions: a genuinely flipped
    emission (forward containment below reverse) still swaps."""
    row = _one_to_one_row(
        {
            "left_referential_integrity": 42.86,
            "left_value_containment": 42.86,
            "right_referential_integrity": 75.0,
        }
    )
    assert (row["from_column_id"], row["to_column_id"]) == ("child_col", "parent_col")
    evidence = row["evidence"]
    assert isinstance(evidence, dict)
    # The directional keys followed the swap.
    assert evidence["right_value_containment"] == 42.86
    assert evidence["left_referential_integrity"] == 75.0


def test_one_to_one_contradicted_cardinality_never_swaps() -> None:
    """``cardinality_verified is False`` means the declared one-to-one is
    measurably wrong — the containment reasoning built on it does not hold,
    so the emission stands."""
    row = _one_to_one_row(
        {
            "left_referential_integrity": 24.0,
            "right_referential_integrity": 100.0,
            "cardinality_verified": False,
        }
    )
    assert (row["from_column_id"], row["to_column_id"]) == ("parent_col", "child_col")


def test_one_to_one_correct_emission_stays() -> None:
    row = _one_to_one_row(
        {"left_referential_integrity": 100.0, "right_referential_integrity": 24.0}
    )
    assert (row["from_column_id"], row["to_column_id"]) == ("parent_col", "child_col")


def test_one_to_one_symmetric_keeps_the_judges_emission() -> None:
    """Identical value sets AND no completeness measurement — nothing left to
    orient on, so the judge's semantic emission stands."""
    row = _one_to_one_row(
        {"left_referential_integrity": 100.0, "right_referential_integrity": 100.0}
    )
    assert (row["from_column_id"], row["to_column_id"]) == ("parent_col", "child_col")


def test_one_to_one_completeness_orients_a_sparse_bijection() -> None:
    """The run-#5 pair: containment is silent, completeness is not.

    ``bank_transactions.payment_id`` (~half the rows carry no payment, ratio
    0.47) against ``payments.payment_id`` (a complete key, 1.00). Both value
    sets are identical, so containment says nothing — but the referenced side
    of an FK must be a COMPLETE key, so the sparse side is the child. Emitted
    from the complete side, this is backwards and must swap. The judge was
    shown 1.00 vs 0.47 in its own prompt and still got it wrong in 2 of 5 runs.
    """
    row = _one_to_one_row(
        {
            "left_referential_integrity": 100.0,
            "right_referential_integrity": 100.0,
            "left_uniqueness": 1.0,
            "right_uniqueness": 0.4698,
        }
    )

    assert (row["from_column_id"], row["to_column_id"]) == ("child_col", "parent_col")
    assert row["evidence"]["orientation_swapped"] is True
    # The per-side metrics follow their endpoints.
    assert row["evidence"]["left_uniqueness"] == 0.4698
    assert row["evidence"]["right_uniqueness"] == 1.0


def test_one_to_one_completeness_leaves_a_correct_emission_alone() -> None:
    """Same shape, emitted the right way round: the sparse side is already from."""
    row = Relationship.oriented_row(
        run_id="r1",
        from_table_id="child_tbl",
        from_column_id="child_col",
        to_table_id="parent_tbl",
        to_column_id="parent_col",
        relationship_type="foreign_key",
        cardinality="one-to-one",
        confidence=0.95,
        detection_method="llm",
        confirmation_source="judge",
        evidence={
            "left_referential_integrity": 100.0,
            "right_referential_integrity": 100.0,
            "left_uniqueness": 0.4698,
            "right_uniqueness": 1.0,
        },
    )

    assert (row["from_column_id"], row["to_column_id"]) == ("child_col", "parent_col")
    assert "orientation_swapped" not in row["evidence"]


def test_one_to_one_dense_bijection_is_never_guessed() -> None:
    """Both sides complete over identical value sets — direction is a modelling
    question, not a measurement. The emission stands rather than being decided
    by a hair's-width difference or by table order."""
    for right_uniqueness in (1.0, 0.999):
        row = _one_to_one_row(
            {
                "left_referential_integrity": 100.0,
                "right_referential_integrity": 100.0,
                "left_uniqueness": 1.0,
                "right_uniqueness": right_uniqueness,
            }
        )
        assert (row["from_column_id"], row["to_column_id"]) == ("parent_col", "child_col")


def test_one_to_one_containment_outranks_completeness() -> None:
    """When containment speaks, completeness is not consulted.

    Containment is direct evidence of which side references which; a sparse
    from side is only a proxy. Here containment says the emission is correct
    while completeness would swap it — containment must win.
    """
    row = _one_to_one_row(
        {
            "left_referential_integrity": 100.0,
            "right_referential_integrity": 24.0,
            "left_uniqueness": 1.0,
            "right_uniqueness": 0.47,
        }
    )
    assert (row["from_column_id"], row["to_column_id"]) == ("parent_col", "child_col")


def test_one_to_one_without_both_metrics_stays() -> None:
    """A missing measurement is not a signal — no swap on partial evidence."""
    for evidence in (
        None,
        {"left_referential_integrity": 24.0},
        {"right_referential_integrity": 100.0},
    ):
        row = _one_to_one_row(evidence)
        assert (row["from_column_id"], row["to_column_id"]) == ("parent_col", "child_col")


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
    for cardinality in ("many-to-one", "one-to-one", "many-to-many", None):
        _add(session, cardinality=cardinality, from_column_id="ca", to_column_id="cb")
        session.rollback()  # each is its own attempt on the unique pair
