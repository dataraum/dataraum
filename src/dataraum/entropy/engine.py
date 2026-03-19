"""Entropy engine — reusable library for detector execution and network inference.

Extracted from entropy_phase.py so that multiple pipeline phases (entropy,
quality_summary, entropy_interpretation) can run detectors and build
network context without duplicating orchestration logic.

Core API:
- run_detectors: Execute detectors on typed tables, return records + domain objects
- build_network_context: Build Bayesian network inference from domain objects
- persist_records: Add EntropyObjectRecords to session
- create_snapshot: Build an EntropySnapshotRecord from results
- compute_dimension_scores: Aggregate scores by dimension path for gate checking
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from dataraum.core.logging import get_logger
from dataraum.entropy.db_models import EntropyObjectRecord, EntropySnapshotRecord
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

    # Build column_id lookup for table-scoped objects that reference columns
    column_id_by_table_col: dict[tuple[str, str], str] = {}
    for col in columns:
        column_id_by_table_col[(col.table_id, col.column_name)] = col.column_id

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
            record_column_id = _extract_column_id(entropy_obj, column_id_by_table_col)
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


def build_network_context(
    domain_objects: list[EntropyObject],
) -> EntropyForNetwork:
    """Build Bayesian network inference from in-memory domain objects.

    Args:
        domain_objects: EntropyObject instances from detector execution.

    Returns:
        EntropyForNetwork with per-column results and aggregated summaries.
    """
    from dataraum.entropy.network.model import EntropyNetwork
    from dataraum.entropy.views.network_context import _assemble_network_context

    network = EntropyNetwork()
    return _assemble_network_context(domain_objects, network)


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


def create_snapshot(
    source_id: str,
    domain_objects: list[EntropyObject],
    network_ctx: EntropyForNetwork,
) -> EntropySnapshotRecord:
    """Create an EntropySnapshotRecord from detector results and network context.

    Args:
        source_id: Source ID this snapshot belongs to.
        domain_objects: All EntropyObjects for avg_entropy computation.
        network_ctx: Network inference results for summary stats.

    Returns:
        EntropySnapshotRecord ready to be added to session.
    """
    high_entropy_count = network_ctx.columns_blocked + network_ctx.columns_investigate
    critical_entropy_count = network_ctx.columns_blocked
    overall_readiness = network_ctx.overall_readiness

    # Average entropy score: per-target max, then mean across targets.
    target_max: dict[str, float] = {}
    for obj in domain_objects:
        if obj.target not in target_max or obj.score > target_max[obj.target]:
            target_max[obj.target] = obj.score
    avg_entropy = sum(target_max.values()) / len(target_max) if target_max else 0.0

    # Serialize Bayesian network state
    snapshot_data: dict[str, Any] = {
        "node_states": {
            intent.intent_name: {
                "worst_p_high": intent.worst_p_high,
                "mean_p_high": intent.mean_p_high,
                "columns_blocked": intent.columns_blocked,
                "columns_investigate": intent.columns_investigate,
                "columns_ready": intent.columns_ready,
                "overall_readiness": intent.overall_readiness,
            }
            for intent in network_ctx.intents
        },
        "total_columns": network_ctx.total_columns,
        "columns_blocked": network_ctx.columns_blocked,
        "columns_investigate": network_ctx.columns_investigate,
        "columns_ready": network_ctx.columns_ready,
    }

    return EntropySnapshotRecord(
        source_id=source_id,
        total_entropy_objects=len(domain_objects),
        high_entropy_count=high_entropy_count,
        critical_entropy_count=critical_entropy_count,
        overall_readiness=overall_readiness,
        avg_entropy_score=avg_entropy,
        snapshot_data=snapshot_data,
    )


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
    column_id_by_table_col: dict[tuple[str, str], str],
) -> str | None:
    """Extract column_id from an entropy object's evidence or target.

    For column-level objects produced by table-scoped detectors (e.g. column_quality),
    the evidence contains column_id and the target is 'column:table.col'.
    """
    for ev in entropy_obj.evidence or []:
        col_id = ev.get("column_id")
        table_id = ev.get("table_id")
        if col_id and table_id:
            return str(col_id)

    return None
