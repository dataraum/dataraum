"""Entropy engine — reusable library for detector execution and network inference.

Extracted from entropy_phase.py so that multiple pipeline phases (entropy,
quality_summary, entropy_interpretation) can run detectors and build
network context without duplicating orchestration logic.

Core API:
- compute_network: Load records from DB, build Bayesian network. Returns None if no data.
- run_detectors: Execute detectors on typed tables, return records + domain objects
- persist_records: Add EntropyObjectRecords to session
- compute_dimension_scores: Aggregate scores by dimension path for gate checking
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sqlalchemy import select

from dataraum.core.logging import get_logger
from dataraum.entropy.db_models import EntropyObjectRecord
from dataraum.entropy.models import EntropyObject
from dataraum.entropy.snapshot import take_snapshot

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from dataraum.entropy.views.network_context import EntropyForNetwork
    from dataraum.storage import Column, Table

logger = get_logger(__name__)


@dataclass
class DetectorResults:
    """Results from running entropy detectors."""

    records: list[EntropyObjectRecord] = field(default_factory=list)
    domain_objects: list[EntropyObject] = field(default_factory=list)
    tables_processed: int = 0


def run_detectors(
    session: Session,
    source_id: str,
    typed_tables: list[Table],
    columns: list[Column],
) -> DetectorResults:
    """Execute entropy detectors on typed tables and their columns.

    Runs column-scoped detectors on each column and table-scoped
    detectors on each table, collecting EntropyObjectRecords and
    in-memory EntropyObjects for network inference.

    Args:
        session: SQLAlchemy session for detector data loading.
        source_id: Source ID for record provenance.
        typed_tables: Typed tables to analyze.
        columns: Columns belonging to those tables.

    Returns:
        DetectorResults with records, domain objects, and tables processed.
    """
    # Group columns by table
    columns_by_table: dict[str, list[Column]] = {}
    for col in columns:
        columns_by_table.setdefault(col.table_id, []).append(col)

    # Build table name -> table_id lookup for target resolution
    table_id_by_name = {t.table_name: t.table_id for t in typed_tables}

    all_records: list[EntropyObjectRecord] = []
    all_domain_objects: list[EntropyObject] = []
    tables_processed = 0

    for table in typed_tables:
        table_columns = columns_by_table.get(table.table_id, [])
        if not table_columns:
            continue

        # --- Column-scoped detectors ---
        for col in table_columns:
            target = f"column:{table.table_name}.{col.column_name}"
            snapshot = take_snapshot(target=target, session=session)

            all_domain_objects.extend(snapshot.objects)

            for entropy_obj in snapshot.objects:
                record = _make_record(
                    source_id=source_id,
                    entropy_obj=entropy_obj,
                    table_id=table.table_id,
                    column_id=col.column_id,
                )
                all_records.append(record)

        # --- Table-scoped detectors ---
        table_snapshot = take_snapshot(target=f"table:{table.table_name}", session=session)
        all_domain_objects.extend(table_snapshot.objects)

        logger.debug(
            "table_scoped_detectors",
            table=table.table_name,
            entropy_objects=len(table_snapshot.objects),
        )

        for entropy_obj in table_snapshot.objects:
            record_column_id = _extract_column_id(entropy_obj)
            record = _make_record(
                source_id=source_id,
                entropy_obj=entropy_obj,
                table_id=_resolve_table_id_from_target(
                    entropy_obj.target, table_id_by_name, table.table_id
                ),
                column_id=record_column_id,
            )
            all_records.append(record)

        tables_processed += 1

    return DetectorResults(
        records=all_records,
        domain_objects=all_domain_objects,
        tables_processed=tables_processed,
    )


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


def compute_dimension_scores(
    domain_objects: list[EntropyObject],
) -> dict[str, float]:
    """Compute averaged scores by dimension path for gate checking.

    Keys use full dimension paths (layer.dimension.sub_dimension) so they
    match contract threshold prefix matching in the scheduler.

    Args:
        domain_objects: EntropyObjects to aggregate.

    Returns:
        Dict mapping dimension paths to average scores.
    """
    scores_by_dim: dict[str, list[float]] = {}
    for obj in domain_objects:
        path = f"{obj.layer}.{obj.dimension}.{obj.sub_dimension}"
        scores_by_dim.setdefault(path, []).append(obj.score)

    return {dim: sum(scores) / len(scores) for dim, scores in scores_by_dim.items() if scores}


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
