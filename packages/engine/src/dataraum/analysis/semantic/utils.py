"""Shared utility functions for semantic analysis."""

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.semantic.db_models import ColumnConcept, SemanticAnnotation
from dataraum.analysis.typing.db_models import TypeCandidate
from dataraum.storage import Column, Table


def load_column_concepts(
    session: Session,
    table_ids: list[str],
    catalogue_run_id: str,
) -> dict[str, ColumnConcept]:
    """Catalogue-grain per-column semantics for ``table_ids`` at the catalogue head run.

    The ONLY reader of :class:`ColumnConcept` (DAT-637). ``catalogue_run_id`` is
    **mandatory** — the catalogue-grain fields (meaning, ontology hints,
    temporal_behavior, unit_source_column, derived_formula hypothesis) live only
    under the begin_session catalogue head, so a caller MUST hold that run to read
    them. Object-grain code (add_source ``detect``) has no catalogue run and so
    cannot reach these by construction — the cross-grain read is unexpressible.

    Returns ``{column_id: ColumnConcept}`` for the run; columns the table agent did
    not bind are simply absent.
    """
    if not table_ids:
        return {}
    stmt = (
        select(ColumnConcept)
        .join(Column, ColumnConcept.column_id == Column.column_id)
        .where(Column.table_id.in_(table_ids), ColumnConcept.run_id == catalogue_run_id)
    )
    return {cc.column_id: cc for cc in session.execute(stmt).scalars()}


def load_table_mappings(
    session: Session,
    table_ids: list[str],
) -> dict[str, str]:
    """Load mapping of table_name -> table_id.

    Args:
        session: Database session
        table_ids: List of table IDs to load mappings for

    Returns:
        Dictionary mapping table_name to table_id
    """
    stmt = select(Table.table_name, Table.table_id).where(Table.table_id.in_(table_ids))
    result = session.execute(stmt)
    return dict(result.tuples().all())


def load_column_mappings(
    session: Session,
    table_ids: list[str],
) -> dict[tuple[str, str], str]:
    """Load mapping of (table_name, column_name) -> column_id.

    Args:
        session: Database session
        table_ids: List of table IDs to load mappings for

    Returns:
        Dictionary mapping (table_name, column_name) tuples to column_id
    """
    stmt = (
        select(Table.table_name, Column.column_name, Column.column_id)
        .join(Column)
        .where(Table.table_id.in_(table_ids))
    )
    result = session.execute(stmt)
    return {(table_name, col_name): col_id for table_name, col_name, col_id in result.all()}


def load_persisted_annotations(
    session: Session,
    table_ids: list[str],
) -> list[dict[str, Any]]:
    """Load persisted per-column semantic annotations for the given tables.

    The per-table synthesis phase reads these as read-only context — the
    OBJECT-grain column annotations the per-column agent produced (role, entity
    label, term, the stock/flow claim). It does NOT include catalogue-grain ``meaning``:
    that is catalogue-grain and AUTHORED by the table agent itself (DAT-637), so
    feeding it back would be the dual-ownership we removed. Returns one dict per
    annotated column, ordered by table then column.

    Args:
        session: Database session.
        table_ids: Table IDs whose columns' annotations to load.

    Returns:
        List of ``{table_name, column_name, column_id, semantic_role, entity_type,
        confidence, temporal_behavior_claim, detected_unit}`` dicts, ordered by
        table then column. ``detected_unit`` is the value-carried unit the typing
        phase parsed from the column's VALUES (DAT-647) — fed so the table agent
        can record a measure's unit resolution instead of treating it as unknown.
    """
    stmt = (
        select(
            Table.table_name,
            Column.column_name,
            Column.column_id,
            SemanticAnnotation.semantic_role,
            SemanticAnnotation.entity_type,
            SemanticAnnotation.confidence,
            SemanticAnnotation.temporal_behavior_claim,
        )
        .join(Column, SemanticAnnotation.column_id == Column.column_id)
        .join(Table, Column.table_id == Table.table_id)
        .where(Table.table_id.in_(table_ids))
        .order_by(Table.table_name, Column.column_position)
    )
    rows = session.execute(stmt).all()

    # Value-carried unit per column (DAT-647): the CURRENT type candidate's
    # detected_unit. TypeCandidate accumulates across runs (a re-type / teach
    # re-run leaves prior runs' rows in place), so we take the MOST RECENT run's
    # best candidate — mirroring load_typing's run_id=None "most recent" semantics
    # (the promoted re-run after a teach cycle). Ordering by detected_at first
    # avoids a stale prior run's higher-confidence candidate leaking a stale unit.
    # Bulk-loaded once, merged by column_id.
    column_ids = [row.column_id for row in rows]
    detected_units: dict[str, str | None] = {}
    if column_ids:
        unit_rows = session.execute(
            select(TypeCandidate.column_id, TypeCandidate.detected_unit)
            .where(TypeCandidate.column_id.in_(column_ids))
            .order_by(TypeCandidate.detected_at.desc(), TypeCandidate.confidence.desc())
        ).all()
        for column_id, detected_unit in unit_rows:
            detected_units.setdefault(column_id, detected_unit)

    return [
        {
            "table_name": row.table_name,
            "column_name": row.column_name,
            "column_id": row.column_id,
            "semantic_role": row.semantic_role,
            "entity_type": row.entity_type,
            "confidence": row.confidence,
            "temporal_behavior_claim": row.temporal_behavior_claim,
            "detected_unit": detected_units.get(row.column_id),
        }
        for row in rows
    ]
