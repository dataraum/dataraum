"""The DB candidate loader serves the judge the full measured evidence.

DAT-725: the detector persists per-side uniqueness into the evidence JSON
(detector._store_candidates), but ``load_relationship_candidates_for_semantic``
dropped it — so the formatter's ``[uniq: L= R=]`` orientation evidence never
rendered on the DB path, the only path the pipeline uses. The FK side of a real
relationship is the non-unique side; the judge needs the asymmetry to orient.
"""

from __future__ import annotations

from dataraum.analysis.relationships.db_models import Relationship as RelationshipDB
from dataraum.storage import Column, Source, Table
from tests.conftest import baseline_run_id


def _table_with_column(session, table_name: str, column_name: str) -> tuple[Table, Column]:
    src = Source(name=f"src_{table_name}", source_type="csv")
    session.add(src)
    session.flush()
    table = Table(source_id=src.source_id, table_name=table_name, layer="typed", row_count=10)
    session.add(table)
    session.flush()
    col = Column(
        table_id=table.table_id, column_name=column_name, column_position=0, raw_type="VARCHAR"
    )
    session.add(col)
    session.flush()
    return table, col


def test_loader_serves_uniqueness_alongside_ri_metrics(session) -> None:
    from dataraum.analysis.relationships.utils import load_relationship_candidates_for_semantic

    orders, customer_fk = _table_with_column(session, "orders", "customer_id")
    customers, customer_pk = _table_with_column(session, "customers", "id")

    session.add(
        RelationshipDB(
            run_id=baseline_run_id(),
            from_table_id=orders.table_id,
            from_column_id=customer_fk.column_id,
            to_table_id=customers.table_id,
            to_column_id=customer_pk.column_id,
            relationship_type="candidate",
            cardinality="many-to-one",
            confidence=0.92,
            detection_method="candidate",
            evidence={
                "left_uniqueness": 0.02,
                "right_uniqueness": 1.0,
                "left_referential_integrity": 100.0,
                "right_referential_integrity": 85.0,
                "orphan_count": 0,
                "cardinality_verified": True,
            },
        )
    )
    session.flush()

    candidates = load_relationship_candidates_for_semantic(
        session, [orders.table_id, customers.table_id], run_id=baseline_run_id()
    )

    assert len(candidates) == 1
    (jc,) = candidates[0]["join_columns"]
    # The orientation evidence rides through (the regression: it was dropped).
    assert jc["left_uniqueness"] == 0.02
    assert jc["right_uniqueness"] == 1.0
    # The existing metrics still ride.
    assert jc["left_referential_integrity"] == 100.0
    assert jc["cardinality_verified"] is True


def test_loader_omits_uniqueness_when_evidence_lacks_it(session) -> None:
    """Evidence without uniqueness (e.g. an old row) serves no fabricated keys."""
    from dataraum.analysis.relationships.utils import load_relationship_candidates_for_semantic

    orders, customer_fk = _table_with_column(session, "orders2", "customer_id")
    customers, customer_pk = _table_with_column(session, "customers2", "id")

    session.add(
        RelationshipDB(
            run_id=baseline_run_id(),
            from_table_id=orders.table_id,
            from_column_id=customer_fk.column_id,
            to_table_id=customers.table_id,
            to_column_id=customer_pk.column_id,
            relationship_type="candidate",
            cardinality="many-to-one",
            confidence=0.9,
            detection_method="candidate",
            evidence={},
        )
    )
    session.flush()

    candidates = load_relationship_candidates_for_semantic(
        session, [orders.table_id, customers.table_id], run_id=baseline_run_id()
    )

    (jc,) = candidates[0]["join_columns"]
    assert "left_uniqueness" not in jc
    assert "right_uniqueness" not in jc
