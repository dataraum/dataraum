"""Entropy engine — detector execution, persistence, and network inference.

Core API:
- run_detector_post_step: Run a single detector by ID as a phase post-step
- compute_network: Load records from DB, build Bayesian network
- persist_records: Add EntropyObjectRecords to session
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import delete, select

from dataraum.core.logging import get_logger
from dataraum.entropy.db_models import EntropyObjectRecord
from dataraum.entropy.models import EntropyObject
from dataraum.entropy.snapshot import take_snapshot

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from dataraum.entropy.views.network_context import EntropyForNetwork

logger = get_logger(__name__)


def run_detector_post_step(
    session: Session,
    source_id: str,
    detector_id: str,
) -> int:
    """Run a single detector as a phase post-step.

    Scoped delete-before-insert: deletes existing records for this
    (source_id, detector_id) pair, then runs the detector against all
    typed tables/columns/views and persists new records.

    Args:
        session: SQLAlchemy session (caller manages commit).
        source_id: Source ID for record provenance.
        detector_id: ID of the detector to run.

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

    # Scoped delete: remove stale records for this detector only
    session.execute(
        delete(EntropyObjectRecord).where(
            EntropyObjectRecord.source_id == source_id,
            EntropyObjectRecord.detector_id == detector_id,
        )
    )

    # Get typed tables
    typed_tables = list(
        session.execute(select(Table).where(Table.source_id == source_id, Table.layer == "typed"))
        .scalars()
        .all()
    )
    if not typed_tables:
        return 0

    table_id_by_name = {t.table_name: t.table_id for t in typed_tables}
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
                    dimensions=[detector.sub_dimension],
                )
                for obj in snapshot.objects:
                    all_records.append(
                        _make_record(
                            source_id=source_id,
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
                dimensions=[detector.sub_dimension],
            )
            for obj in snapshot.objects:
                all_records.append(
                    _make_record(
                        source_id=source_id,
                        entropy_obj=obj,
                        table_id=_resolve_table_id_from_target(
                            obj.target, table_id_by_name, table.table_id
                        ),
                        column_id=_extract_column_id(obj),
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
                dimensions=[detector.sub_dimension],
            )
            for obj in snapshot.objects:
                all_records.append(
                    _make_record(
                        source_id=source_id,
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


def compute_network(
    session: Session,
    source_id: str,
) -> EntropyForNetwork | None:
    """Compute entropy network from persisted records.

    Pure function: loads EntropyObjectRecords for the source, converts
    to domain objects, builds Bayesian network inference. Returns None
    if no records exist.

    Args:
        session: SQLAlchemy session.
        source_id: Source to compute network for.

    Returns:
        EntropyForNetwork with per-column results and aggregated summaries,
        or None if no entropy data exists.
    """
    from dataraum.entropy.core.storage import EntropyRepository
    from dataraum.entropy.network.model import EntropyNetwork
    from dataraum.entropy.views.network_context import assemble_network_context
    from dataraum.storage import Table

    table_ids = list(
        session.execute(
            select(Table.table_id).where(Table.source_id == source_id, Table.layer == "typed")
        )
        .scalars()
        .all()
    )

    if not table_ids:
        return None

    repo = EntropyRepository(session)
    entropy_objects = repo.load_for_tables(table_ids, enforce_typed=True)

    if not entropy_objects:
        return None

    network = EntropyNetwork()
    return assemble_network_context(entropy_objects, network)


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
    source_id: str,
    entropy_obj: EntropyObject,
    table_id: str | None,
    column_id: str | None,
) -> EntropyObjectRecord:
    """Create an EntropyObjectRecord from an EntropyObject."""
    resolution_dicts = [
        {
            "action": opt.action,
            "parameters": opt.parameters,
            "effort": opt.effort,
            "description": opt.description,
        }
        for opt in entropy_obj.resolution_options
    ]

    return EntropyObjectRecord(
        source_id=source_id,
        table_id=table_id,
        column_id=column_id,
        target=entropy_obj.target,
        layer=entropy_obj.layer,
        dimension=entropy_obj.dimension,
        sub_dimension=entropy_obj.sub_dimension,
        score=entropy_obj.score,
        evidence=entropy_obj.evidence,
        resolution_options=resolution_dicts if resolution_dicts else None,
        detector_id=entropy_obj.detector_id,
        expected_business_pattern=entropy_obj.expected_business_pattern,
        business_rule=entropy_obj.business_rule,
        filter_confidence=entropy_obj.filter_confidence,
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

    For column-level objects produced by table-scoped detectors (e.g. column_quality),
    the evidence contains column_id.
    """
    for ev in entropy_obj.evidence or []:
        col_id = ev.get("column_id")
        table_id = ev.get("table_id")
        if col_id and table_id:
            return str(col_id)

    return None
