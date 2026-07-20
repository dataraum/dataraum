"""Entropy engine — detector execution + persistence.

Core API:
- run_detector_post_step: Run a single detector by ID as a phase post-step
- persist_records: Add EntropyObjectRecords to session
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, select

from dataraum.core.logging import get_logger
from dataraum.entropy.db_models import ClaimWitnessRecord, EntropyObjectRecord
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
    table_ids: list[str],
    run_id: str | None = None,
    base_runs: dict[str, str] | None = None,
) -> int:
    """Run a single detector as a phase post-step — source-free (DAT-408).

    Scoped delete-before-insert by ``(detector_id, table_ids)``: deletes this
    detector's stale records for the tables in scope, then runs the detector
    against those typed tables and persists new records. The scope is the table
    set, never a single ``source_id`` — a begin_session run spans sources, and an
    add_source run's tables are exactly its source's tables, so the table-scoped
    delete is byte-identical there. Records carry no ``source_id`` (DAT-408) —
    source is reachable via ``table_id`` and was never read off the row.

    Args:
        session: SQLAlchemy session (caller manages commit).
        detector_id: ID of the detector to run.
        duckdb_conn: DuckDB connection for detectors that query data directly.
        table_ids: The typed-table scope — the run's table set (delete + scan).
        run_id: Snapshot version axis (DAT-413); stamped on each record and ALWAYS
            the delete scope, so a re-run clears only its OWN prior rows
            (non-destructive). ``None`` (tests) scopes to ``run_id IS NULL``.
        base_runs: pinned ``table_id → run_id`` generation-head map (DAT-448/506)
            resolved once at detect start (``loaders.resolve_base_runs``);
            threaded onto every snapshot so loader fallbacks read one consistent
            base.

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

    # SANCTIONED form-(b) writer (DAT-502): run-scoped delete-then-insert, NOT
    # a (key, run_id) upsert. Entropy objects are a presence-keyed row-set —
    # rows exist only where a detector fired, and the adjudicating detectors
    # read the un-run-versioned ``config_overlay`` live between attempts, so a
    # success-redelivery's row-set can legitimately SHRINK (a teach landing
    # between attempts resolves a finding). An upsert without the clear would
    # leave the vanished rows behind and corrupt the readiness rollup. The
    # clear is ALWAYS scoped to this exact run (``run_id ==``, which is
    # ``IS NULL`` for the un-versioned test path) so it clears only its OWN
    # prior rows and leaves earlier runs intact — no unscoped branch: a delete
    # with no run_id would wipe every run's objects for this (detector, tables).
    session.execute(
        delete(EntropyObjectRecord).where(
            EntropyObjectRecord.detector_id == detector_id,
            EntropyObjectRecord.table_id.in_(table_ids),
            EntropyObjectRecord.run_id == run_id,
        )
    )
    # Same SANCTIONED form-(b) clear for the pooled-witness provenance
    # (ADR-0009): witness sets shrink when an adjudication resolves
    # differently on redelivery. A no-op for non-adjudication detectors (they
    # write none), so it stays a single generic step rather than a
    # per-detector side effect. ``uq_claim_witness_target_field_witness_run``
    # stays as the grain guard beneath the clear.
    session.execute(
        delete(ClaimWitnessRecord).where(
            ClaimWitnessRecord.detector_id == detector_id,
            ClaimWitnessRecord.table_id.in_(table_ids),
            ClaimWitnessRecord.run_id == run_id,
        )
    )

    # Typed tables in scope — the run's table set (source-free).
    typed_stmt = select(Table).where(Table.table_id.in_(table_ids), Table.layer == "typed")
    typed_tables = list(session.execute(typed_stmt).scalars().all())
    if not typed_tables:
        return 0

    table_id_by_name = {t.table_name: t.table_id for t in typed_tables}
    all_records: list[EntropyObjectRecord] = []
    all_witnesses: list[ClaimWitnessRecord] = []

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
                    base_runs=base_runs,
                )
                for obj in snapshot.objects:
                    all_records.append(
                        _make_record(
                            run_id=run_id,
                            entropy_obj=obj,
                            table_id=table.table_id,
                            column_id=col.column_id,
                        )
                    )
                    all_witnesses.extend(
                        _make_witness_records(
                            obj,
                            run_id=run_id,
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
                base_runs=base_runs,
            )
            for obj in snapshot.objects:
                resolved_table_id = _resolve_table_id_from_target(
                    obj.target, table_id_by_name, table.table_id
                )
                all_records.append(
                    _make_record(
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

        pairs_stmt = (
            select(
                Relationship.from_column_id,
                Relationship.to_column_id,
                Relationship.from_table_id,
            )
            .where(
                # This run's catalog (DAT-408): scoped to the current run_id — rows
                # coexist across runs, so reads pick the current one. Durable
                # manual/keeper rows are materialized into this run (DAT-409), so a
                # single current-run read sees the whole catalog. BOTH endpoints must
                # be in scope: an intra-selection relationship has both, and it keeps
                # the from-endpoint anchor (object table_id) inside table_ids so the
                # run_id-scoped delete reliably clears it on retry.
                # Defined catalog only (DAT-408 contract — see db_models.py /
                # relationships.utils): the LLM in semantic_per_table is the selector,
                # so the relationship detectors measure what it confirmed (llm +
                # materialized manual/keeper), NOT the ephemeral structural
                # ``candidate`` superset. Enumerating bare candidates as focal pairs
                # manufactured join-path ambiguity the schema doesn't have and scored
                # spurious relationship_entropy. Found by DAT-405 calibration.
                Relationship.detection_method != "candidate",
                Relationship.from_table_id.in_(table_ids),
                Relationship.to_table_id.in_(table_ids),
            )
            .distinct()
        )
        if run_id is not None:
            pairs_stmt = pairs_stmt.where(Relationship.run_id == run_id)
        pairs = list(session.execute(pairs_stmt).tuples())
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
                base_runs=base_runs,
            )
            for obj in snapshot.objects:
                all_records.append(
                    _make_record(
                        run_id=run_id,
                        entropy_obj=obj,
                        table_id=from_table,
                        column_id=from_col,
                    )
                )
                # Witness provenance at relationship grain:
                # same from-endpoint anchoring as the object record — without
                # this, a pooled relationship measurement's WitnessClaims were
                # silently discarded and claim_witnesses stayed column-only.
                all_witnesses.extend(
                    _make_witness_records(
                        obj,
                        run_id=run_id,
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
                base_runs=base_runs,
            )
            for obj in snapshot.objects:
                all_records.append(
                    _make_record(
                        run_id=run_id,
                        entropy_obj=obj,
                        table_id=ev.fact_table_id,
                        column_id=_extract_column_id(obj),
                    )
                )

    persist_records(session, all_records)
    if all_witnesses:
        session.add_all(all_witnesses)

    if all_records:
        logger.info(
            "post_step_detector_done",
            detector_id=detector_id,
            records=len(all_records),
            witnesses=len(all_witnesses),
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
    entropy_obj: EntropyObject,
    table_id: str | None,
    column_id: str | None,
    *,
    run_id: str | None = None,
) -> EntropyObjectRecord:
    """Create an EntropyObjectRecord from an EntropyObject."""
    return EntropyObjectRecord(
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


def _make_witness_records(
    entropy_obj: EntropyObject,
    *,
    table_id: str | None,
    column_id: str | None,
    run_id: str | None,
) -> list[ClaimWitnessRecord]:
    """The run-versioned witness rows behind a pooled EntropyObject (ADR-0009).

    Same ``(table_id, column_id, run_id)`` anchoring as the object's record, so
    the head-joined ``current_claim_witnesses`` view resolves them on the same
    grain. Empty for non-adjudication detectors.
    """
    return [
        ClaimWitnessRecord(
            table_id=table_id,
            column_id=column_id,
            run_id=run_id,
            target=entropy_obj.target,
            claim_field=witness.claim_field,
            witness_id=witness.witness_id,
            distribution=witness.distribution,
            reliability=witness.reliability,
            detector_id=entropy_obj.detector_id,
        )
        for witness in entropy_obj.witnesses
    ]


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
