"""Entropy engine — detector execution + persistence.

Core API:
- run_detector_post_step: Run a single detector by ID as a phase post-step
- persist_records: Add EntropyObjectRecords to session
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, select

from dataraum.core.logging import get_logger
from dataraum.entropy.db_models import EntropyObjectRecord
from dataraum.entropy.models import EntropyObject, relationship_target_key
from dataraum.entropy.snapshot import take_snapshot

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = get_logger(__name__)


def run_detector_post_step(
    session: Session,
    detector_id: str,
    duckdb_conn: Any = None,
    *,
    session_id: str,
    table_ids: list[str],
    run_id: str | None = None,
) -> int:
    """Run a single detector as a phase post-step — source-free (DAT-408).

    Scoped delete-before-insert by ``(detector_id, table_ids)``: deletes this
    detector's stale records for the tables in scope, then runs the detector
    against those typed tables and persists new records. The scope is the table
    set, never a single ``source_id`` — a begin_session run spans sources, and an
    add_source run's tables are exactly its source's tables, so the table-scoped
    delete is byte-identical there. Per-record ``source_id`` is derived from the
    table being measured (provenance only, not a scope key).

    Args:
        session: SQLAlchemy session (caller manages commit).
        detector_id: ID of the detector to run.
        duckdb_conn: DuckDB connection for detectors that query data directly.
        session_id: Per-run FK for the persisted records.
        table_ids: The typed-table scope — the run's table set (delete + scan).
        run_id: Snapshot version axis (DAT-413); stamped on each record. Scopes the
            delete to this run when set so a re-run clears only its OWN prior rows
            (non-destructive); ``None`` (tests) keeps the broad per-table delete.

    Returns:
        Number of records created.
    """
    from dataraum.entropy.detectors.base import get_default_registry
    from dataraum.storage import Column as ColumnModel
    from dataraum.storage import Table

    registry = get_default_registry()
    detector = registry.detectors.get(detector_id)
    if detector is None:
        logger.warning("post_step_detector_not_found", detector_id=detector_id)
        return 0

    # Scoped delete: remove this detector's stale records for the tables in scope.
    # Scoped to ``run_id`` when set (the workflow path, DAT-413) so a re-run clears
    # only its OWN prior rows and leaves earlier runs intact (non-destructive).
    delete_stmt = delete(EntropyObjectRecord).where(
        EntropyObjectRecord.detector_id == detector_id,
        EntropyObjectRecord.table_id.in_(table_ids),
    )
    if run_id is not None:
        delete_stmt = delete_stmt.where(EntropyObjectRecord.run_id == run_id)
    session.execute(delete_stmt)

    # Typed tables in scope — the run's table set (source-free).
    typed_stmt = select(Table).where(Table.table_id.in_(table_ids), Table.layer == "typed")
    typed_tables = list(session.execute(typed_stmt).scalars().all())
    if not typed_tables:
        return 0

    table_id_by_name = {t.table_name: t.table_id for t in typed_tables}
    source_by_table_id = {t.table_id: t.source_id for t in typed_tables}
    all_records: list[EntropyObjectRecord] = []

    if detector.scope == "column":
        # Column-scoped: run on each column of each table
        for table in typed_tables:
            columns = list(
                session.execute(select(ColumnModel).where(ColumnModel.table_id == table.table_id))
                .scalars()
                .all()
            )
            for col in columns:
                target = f"column:{table.table_name}.{col.column_name}"
                snapshot = take_snapshot(
                    target=target,
                    session=session,
                    duckdb_conn=duckdb_conn,
                    dimensions=[detector.sub_dimension],
                    run_id=run_id,
                )
                for obj in snapshot.objects:
                    all_records.append(
                        _make_record(
                            source_id=table.source_id,
                            session_id=session_id,
                            run_id=run_id,
                            entropy_obj=obj,
                            table_id=table.table_id,
                            column_id=col.column_id,
                        )
                    )

    elif detector.scope == "table":
        # Table-scoped: run on each table
        for table in typed_tables:
            target = f"table:{table.table_name}"
            snapshot = take_snapshot(
                target=target,
                session=session,
                duckdb_conn=duckdb_conn,
                dimensions=[detector.sub_dimension],
                run_id=run_id,
            )
            for obj in snapshot.objects:
                resolved_table_id = _resolve_table_id_from_target(
                    obj.target, table_id_by_name, table.table_id
                )
                all_records.append(
                    _make_record(
                        source_id=source_by_table_id.get(resolved_table_id),
                        session_id=session_id,
                        run_id=run_id,
                        entropy_obj=obj,
                        table_id=resolved_table_id,
                        column_id=_extract_column_id(obj),
                    )
                )

    elif detector.scope == "relationship":
        # Relationship-scoped (DAT-408): one pass per distinct directional column
        # pair among the session's tables. The object's ``target`` carries the true
        # identity (relationship:{from}::{to}); ``table_id``/``column_id`` are
        # anchored to the from-endpoint purely so the table-scoped readiness loader
        # picks the object up — the readiness ROW itself keys off ``target``.
        from dataraum.analysis.relationships.db_models import Relationship

        pairs = list(
            session.execute(
                select(
                    Relationship.from_column_id,
                    Relationship.to_column_id,
                    Relationship.from_table_id,
                )
                .where(
                    # Session-grain: only THIS session's relationships (two sessions
                    # can share the same tables/column pairs).
                    Relationship.session_id == session_id,
                    Relationship.from_table_id.in_(table_ids)
                    | Relationship.to_table_id.in_(table_ids),
                )
                .distinct()
            ).tuples()
        )
        seen_pairs: set[tuple[str, str]] = set()
        for from_col, to_col, from_table in pairs:
            if (from_col, to_col) in seen_pairs:
                continue
            seen_pairs.add((from_col, to_col))
            target = relationship_target_key(from_col, to_col)
            snapshot = take_snapshot(
                target=target,
                session=session,
                duckdb_conn=duckdb_conn,
                dimensions=[detector.sub_dimension],
                run_id=run_id,
                session_id=session_id,
            )
            for obj in snapshot.objects:
                all_records.append(
                    _make_record(
                        source_id=source_by_table_id.get(from_table),
                        session_id=session_id,
                        run_id=run_id,
                        entropy_obj=obj,
                        table_id=from_table,
                        column_id=from_col,
                    )
                )

    elif detector.scope == "view":
        # View-scoped: run on enriched views
        from dataraum.analysis.views.db_models import EnrichedView

        enriched_views = list(
            session.execute(
                select(EnrichedView).where(
                    EnrichedView.fact_table_id.in_([t.table_id for t in typed_tables])
                )
            )
            .scalars()
            .all()
        )
        for ev in enriched_views:
            target = f"view:{ev.view_name}"
            snapshot = take_snapshot(
                target=target,
                session=session,
                duckdb_conn=duckdb_conn,
                dimensions=[detector.sub_dimension],
                run_id=run_id,
            )
            for obj in snapshot.objects:
                all_records.append(
                    _make_record(
                        source_id=source_by_table_id.get(ev.fact_table_id),
                        session_id=session_id,
                        run_id=run_id,
                        entropy_obj=obj,
                        table_id=ev.fact_table_id,
                        column_id=_extract_column_id(obj),
                    )
                )

    persist_records(session, all_records)

    if all_records:
        logger.info(
            "post_step_detector_done",
            detector_id=detector_id,
            records=len(all_records),
        )

    return len(all_records)


def persist_records(
    session: Session,
    records: list[EntropyObjectRecord],
) -> None:
    """Add EntropyObjectRecords to session.

    Does not commit — caller is responsible for transaction management.

    Args:
        session: SQLAlchemy session.
        records: Records to persist.
    """
    if records:
        session.add_all(records)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _make_record(
    source_id: str | None,
    entropy_obj: EntropyObject,
    table_id: str | None,
    column_id: str | None,
    *,
    session_id: str,
    run_id: str | None = None,
) -> EntropyObjectRecord:
    """Create an EntropyObjectRecord from an EntropyObject."""
    return EntropyObjectRecord(
        session_id=session_id,
        source_id=source_id,
        table_id=table_id,
        column_id=column_id,
        run_id=run_id,
        target=entropy_obj.target,
        layer=entropy_obj.layer,
        dimension=entropy_obj.dimension,
        sub_dimension=entropy_obj.sub_dimension,
        score=entropy_obj.score,
        evidence=entropy_obj.evidence,
        detector_id=entropy_obj.detector_id,
    )


def _resolve_table_id_from_target(
    target: str,
    table_id_by_name: dict[str, str],
    fallback_table_id: str,
) -> str:
    """Resolve table_id from a target string like 'table:name' or 'column:name.col'."""
    if ":" in target:
        ref = target.split(":", 1)[1]
        table_name = ref.split(".")[0]
        return table_id_by_name.get(table_name, fallback_table_id)
    return fallback_table_id


def _extract_column_id(
    entropy_obj: EntropyObject,
) -> str | None:
    """Extract column_id from an entropy object's evidence.

    For column-level objects produced by table-scoped detectors,
    the evidence may contain column_id.
    """
    for ev in entropy_obj.evidence or []:
        col_id = ev.get("column_id")
        table_id = ev.get("table_id")
        if col_id and table_id:
            return str(col_id)

    return None
