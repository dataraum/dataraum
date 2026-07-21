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
                "left_orphan_count": 0,
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


def test_oriented_candidate_serves_uniqueness_in_stored_orientation(session) -> None:
    """detector → oriented_row(swap) → loader composition stays direction-true.

    A ``one-to-many`` candidate flips at the DAT-777 chokepoint (stored
    many→one) and the generic ``left_``/``right_`` evidence swap moves each
    uniqueness value with its endpoint — so the loader must serve the
    asymmetry aligned with the swapped column1/column2, not the detector's
    original emission order.
    """
    from dataraum.analysis.relationships.utils import load_relationship_candidates_for_semantic

    parent, parent_key = _table_with_column(session, "plants", "plant_id")
    child, child_fk = _table_with_column(session, "readings", "plant_id")

    row = RelationshipDB.oriented_row(
        run_id=baseline_run_id(),
        from_table_id=parent.table_id,
        from_column_id=parent_key.column_id,
        to_table_id=child.table_id,
        to_column_id=child_fk.column_id,
        relationship_type="candidate",
        cardinality="one-to-many",  # parent→child emission: the chokepoint swaps
        confidence=0.9,
        detection_method="candidate",
        confirmation_source="unconfirmed",
        evidence={"left_uniqueness": 1.0, "right_uniqueness": 0.02},
    )
    session.add(RelationshipDB(**row))
    session.flush()

    candidates = load_relationship_candidates_for_semantic(
        session, [parent.table_id, child.table_id], run_id=baseline_run_id()
    )

    assert len(candidates) == 1
    assert candidates[0]["table1"] == "readings"  # the many/FK side after the swap
    (jc,) = candidates[0]["join_columns"]
    assert jc["cardinality"] == "many-to-one"
    assert jc["left_uniqueness"] == 0.02  # followed its endpoint through the swap
    assert jc["right_uniqueness"] == 1.0


def _candidate_row(run_id: str, from_table, from_col, to_table, to_col) -> RelationshipDB:
    return RelationshipDB(
        run_id=run_id,
        from_table_id=from_table.table_id,
        from_column_id=from_col.column_id,
        to_table_id=to_table.table_id,
        to_column_id=to_col.column_id,
        relationship_type="candidate",
        cardinality="many-to-one",
        confidence=0.9,
        detection_method="candidate",
        evidence={},
    )


def test_loader_serves_established_column_annotations(session) -> None:
    """DAT-723: each side's per-column annotation rides the candidate as evidence.

    Annotations are object-grain — written under an add_source run, not the
    catalogue run that scopes the candidates — so the loader must serve them
    without run_id agreement. A null field serves NO key (absence, never a
    default).
    """
    from dataraum.analysis.relationships.utils import load_relationship_candidates_for_semantic
    from dataraum.analysis.semantic.db_models import SemanticAnnotation

    facts, period_col = _table_with_column(session, "fact_lines", "fiscal_period")
    invoices, invoice_pk = _table_with_column(session, "invoices", "invoice_id")

    session.add_all(
        [
            SemanticAnnotation(
                column_id=period_col.column_id,
                run_id="object-grain-run",
                semantic_role="timestamp",
                entity_type="fiscal_period",
                confidence=0.9,
            ),
            SemanticAnnotation(
                column_id=invoice_pk.column_id,
                run_id="object-grain-run",
                semantic_role="key",
                entity_type=None,
                confidence=0.95,
            ),
        ]
    )
    session.add(_candidate_row(baseline_run_id(), facts, period_col, invoices, invoice_pk))
    session.flush()

    candidates = load_relationship_candidates_for_semantic(
        session, [facts.table_id, invoices.table_id], run_id=baseline_run_id()
    )

    (jc,) = candidates[0]["join_columns"]
    assert jc["column1_role"] == "timestamp"
    assert jc["column1_entity_type"] == "fiscal_period"
    assert jc["column2_role"] == "key"
    assert "column2_entity_type" not in jc


def test_loader_omits_annotation_fields_when_no_annotation_exists(session) -> None:
    """An unannotated column serves no role fields — absent, not defaulted."""
    from dataraum.analysis.relationships.utils import load_relationship_candidates_for_semantic

    lines, ref_col = _table_with_column(session, "lines3", "doc_ref")
    docs, doc_pk = _table_with_column(session, "docs3", "doc_id")

    session.add(_candidate_row(baseline_run_id(), lines, ref_col, docs, doc_pk))
    session.flush()

    candidates = load_relationship_candidates_for_semantic(
        session, [lines.table_id, docs.table_id], run_id=baseline_run_id()
    )

    (jc,) = candidates[0]["join_columns"]
    assert "column1_role" not in jc
    assert "column1_entity_type" not in jc
    assert "column2_role" not in jc
    assert "column2_entity_type" not in jc


def test_loader_serves_most_recent_annotation_per_column(session) -> None:
    """Coexisting runs' annotations resolve to the most recent row (DAT-413 axis)."""
    from datetime import UTC, datetime

    from dataraum.analysis.relationships.utils import load_relationship_candidates_for_semantic
    from dataraum.analysis.semantic.db_models import SemanticAnnotation

    lines, period_col = _table_with_column(session, "lines4", "period")
    docs, doc_pk = _table_with_column(session, "docs4", "doc_id")

    session.add_all(
        [
            SemanticAnnotation(
                column_id=period_col.column_id,
                run_id="stale-run",
                semantic_role="dimension",
                entity_type="category",
                confidence=0.99,
                annotated_at=datetime(2024, 1, 1, tzinfo=UTC),
            ),
            SemanticAnnotation(
                column_id=period_col.column_id,
                run_id="fresh-run",
                semantic_role="timestamp",
                entity_type="fiscal_period",
                confidence=0.7,
                annotated_at=datetime(2025, 1, 1, tzinfo=UTC),
            ),
        ]
    )
    session.add(_candidate_row(baseline_run_id(), lines, period_col, docs, doc_pk))
    session.flush()

    candidates = load_relationship_candidates_for_semantic(
        session, [lines.table_id, docs.table_id], run_id=baseline_run_id()
    )

    (jc,) = candidates[0]["join_columns"]
    # The fresh run's read wins despite the stale run's higher confidence.
    assert jc["column1_role"] == "timestamp"
    assert jc["column1_entity_type"] == "fiscal_period"


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
