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


@pytest.mark.parametrize("cardinality", ["many-to-one", "one-to-one", "many-to-many", None])
def test_non_one_to_many_is_left_oriented(cardinality: str | None) -> None:
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
