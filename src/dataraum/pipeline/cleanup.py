"""Phase cleanup for --force re-execution.

Deletes a phase's output records so it can be re-run cleanly.
Each phase has a cleanup function that knows which tables to purge.
"""

from __future__ import annotations

from collections.abc import Callable

import duckdb
from sqlalchemy import delete, select
from sqlalchemy.orm import Session
from sqlalchemy.sql.dml import Delete

from dataraum.core.logging import get_logger
from dataraum.pipeline.db_models import PhaseCheckpoint
from dataraum.storage.models import Column, Table

logger = get_logger(__name__)

# Type alias for cleanup functions
CleanupFn = Callable[[Session, str, list[str], list[str]], int]


def _exec_delete(session: Session, stmt: Delete) -> int:  # type: ignore[type-arg]
    """Execute a DELETE statement and return the row count."""
    result = session.execute(stmt)
    rc: int = result.rowcount  # type: ignore[attr-defined]
    return rc


def _get_table_ids(source_id: str, session: Session) -> list[str]:
    """Get all table_ids for a source."""
    stmt = select(Table.table_id).where(Table.source_id == source_id)
    return list(session.execute(stmt).scalars().all())


def _get_column_ids(table_ids: list[str], session: Session) -> list[str]:
    """Get all column_ids for the given tables."""
    if not table_ids:
        return []
    stmt = select(Column.column_id).where(Column.table_id.in_(table_ids))
    return list(session.execute(stmt).scalars().all())


def _get_slice_table_names(source_id: str, session: Session) -> list[str]:
    """Get table_name values for slice-layer tables."""
    stmt = select(Table.table_name).where(
        Table.source_id == source_id,
        Table.layer == "slice",
    )
    return list(session.execute(stmt).scalars().all())


def _cleanup_typing(session: Session, source_id: str, table_ids: list[str], column_ids: list[str]) -> int:
    """Clean up typing phase output."""
    from dataraum.analysis.typing.db_models import TypeCandidate, TypeDecision

    count = 0

    # Delete TypeCandidate and TypeDecision for raw-layer columns
    raw_table_ids = list(
        session.execute(
            select(Table.table_id).where(Table.source_id == source_id, Table.layer == "raw")
        )
        .scalars()
        .all()
    )
    if raw_table_ids:
        raw_col_ids = list(
            session.execute(select(Column.column_id).where(Column.table_id.in_(raw_table_ids)))
            .scalars()
            .all()
        )
        if raw_col_ids:
            count += _exec_delete(
                session, delete(TypeCandidate).where(TypeCandidate.column_id.in_(raw_col_ids))
            )
            count += _exec_delete(
                session, delete(TypeDecision).where(TypeDecision.column_id.in_(raw_col_ids))
            )

    # Delete typed and quarantine layer Tables (CASCADE deletes Columns and their children)
    count += _exec_delete(
        session,
        delete(Table).where(
            Table.source_id == source_id,
            Table.layer.in_(["typed", "quarantine"]),
        ),
    )
    return count


def _cleanup_statistics(
    session: Session, source_id: str, table_ids: list[str], column_ids: list[str]
) -> int:
    from dataraum.analysis.statistics.db_models import StatisticalProfile

    if not column_ids:
        return 0
    return _exec_delete(
        session, delete(StatisticalProfile).where(StatisticalProfile.column_id.in_(column_ids))
    )


def _cleanup_relationships(
    session: Session, source_id: str, table_ids: list[str], column_ids: list[str]
) -> int:
    from dataraum.analysis.relationships.db_models import Relationship

    if not table_ids:
        return 0
    return _exec_delete(
        session,
        delete(Relationship).where(
            Relationship.from_table_id.in_(table_ids) | Relationship.to_table_id.in_(table_ids)
        ),
    )


def _cleanup_correlations(
    session: Session, source_id: str, table_ids: list[str], column_ids: list[str]
) -> int:
    from dataraum.analysis.correlation.db_models import CrossTableCorrelationRecord, DerivedColumn

    count = 0
    if table_ids:
        count += _exec_delete(
            session, delete(DerivedColumn).where(DerivedColumn.table_id.in_(table_ids))
        )
        count += _exec_delete(
            session,
            delete(CrossTableCorrelationRecord).where(
                CrossTableCorrelationRecord.from_table_id.in_(table_ids)
                | CrossTableCorrelationRecord.to_table_id.in_(table_ids)
            ),
        )
    return count


def _cleanup_semantic(
    session: Session, source_id: str, table_ids: list[str], column_ids: list[str]
) -> int:
    from dataraum.analysis.semantic.db_models import SemanticAnnotation, TableEntity

    count = 0
    if column_ids:
        count += _exec_delete(
            session,
            delete(SemanticAnnotation).where(SemanticAnnotation.column_id.in_(column_ids)),
        )
    if table_ids:
        count += _exec_delete(
            session, delete(TableEntity).where(TableEntity.table_id.in_(table_ids))
        )
    return count


def _cleanup_temporal(
    session: Session, source_id: str, table_ids: list[str], column_ids: list[str]
) -> int:
    from dataraum.analysis.temporal.db_models import TemporalColumnProfile

    if not column_ids:
        return 0
    return _exec_delete(
        session,
        delete(TemporalColumnProfile).where(TemporalColumnProfile.column_id.in_(column_ids)),
    )


def _cleanup_slicing(
    session: Session, source_id: str, table_ids: list[str], column_ids: list[str]
) -> int:
    from dataraum.analysis.slicing.db_models import SliceDefinition

    if not table_ids:
        return 0
    return _exec_delete(
        session, delete(SliceDefinition).where(SliceDefinition.table_id.in_(table_ids))
    )


def _cleanup_slice_analysis(
    session: Session, source_id: str, table_ids: list[str], column_ids: list[str]
) -> int:
    # Delete slice-layer tables (CASCADE deletes their Columns and child records)
    return _exec_delete(
        session, delete(Table).where(Table.source_id == source_id, Table.layer == "slice")
    )


def _cleanup_column_eligibility(
    session: Session, source_id: str, table_ids: list[str], column_ids: list[str]
) -> int:
    from dataraum.analysis.eligibility.db_models import ColumnEligibilityRecord

    return _exec_delete(
        session,
        delete(ColumnEligibilityRecord).where(ColumnEligibilityRecord.source_id == source_id),
    )


def _cleanup_enriched_views(
    session: Session, source_id: str, table_ids: list[str], column_ids: list[str]
) -> int:
    from dataraum.analysis.views.db_models import EnrichedView

    if not table_ids:
        return 0
    return _exec_delete(
        session, delete(EnrichedView).where(EnrichedView.fact_table_id.in_(table_ids))
    )


def _cleanup_statistical_quality(
    session: Session, source_id: str, table_ids: list[str], column_ids: list[str]
) -> int:
    from dataraum.analysis.statistics.quality_db_models import StatisticalQualityMetrics

    if not column_ids:
        return 0
    return _exec_delete(
        session,
        delete(StatisticalQualityMetrics).where(StatisticalQualityMetrics.column_id.in_(column_ids)),
    )


def _cleanup_quality_summary(
    session: Session, source_id: str, table_ids: list[str], column_ids: list[str]
) -> int:
    from dataraum.analysis.quality_summary.db_models import ColumnQualityReport, ColumnSliceProfile

    count = 0
    if column_ids:
        count += _exec_delete(
            session,
            delete(ColumnQualityReport).where(ColumnQualityReport.source_column_id.in_(column_ids)),
        )
        count += _exec_delete(
            session,
            delete(ColumnSliceProfile).where(ColumnSliceProfile.source_column_id.in_(column_ids)),
        )
    return count


def _cleanup_business_cycles(
    session: Session, source_id: str, table_ids: list[str], column_ids: list[str]
) -> int:
    from dataraum.analysis.cycles.db_models import DetectedBusinessCycle

    return _exec_delete(
        session,
        delete(DetectedBusinessCycle).where(DetectedBusinessCycle.source_id == source_id),
    )


def _cleanup_validation(
    session: Session, source_id: str, table_ids: list[str], column_ids: list[str]
) -> int:
    from dataraum.analysis.validation.db_models import ValidationResultRecord

    return _exec_delete(session, delete(ValidationResultRecord))


def _cleanup_graph_execution(
    session: Session, source_id: str, table_ids: list[str], column_ids: list[str]
) -> int:
    from dataraum.graphs.db_models import GraphExecutionRecord

    return _exec_delete(session, delete(GraphExecutionRecord))


def _cleanup_entropy(
    session: Session, source_id: str, table_ids: list[str], column_ids: list[str]
) -> int:
    from dataraum.entropy.db_models import EntropyObjectRecord, EntropySnapshotRecord

    count = _exec_delete(
        session,
        delete(EntropyObjectRecord).where(EntropyObjectRecord.source_id == source_id),
    )
    count += _exec_delete(
        session,
        delete(EntropySnapshotRecord).where(EntropySnapshotRecord.source_id == source_id),
    )
    return count


def _cleanup_entropy_interpretation(
    session: Session, source_id: str, table_ids: list[str], column_ids: list[str]
) -> int:
    from dataraum.entropy.interpretation_db_models import EntropyInterpretationRecord

    return _exec_delete(
        session,
        delete(EntropyInterpretationRecord).where(
            EntropyInterpretationRecord.source_id == source_id
        ),
    )


def _cleanup_temporal_slice_analysis(
    session: Session, source_id: str, table_ids: list[str], column_ids: list[str]
) -> int:
    from dataraum.analysis.temporal_slicing.db_models import (
        ColumnDriftSummary,
        TemporalSliceAnalysis,
    )

    slice_names = _get_slice_table_names(source_id, session)
    if not slice_names:
        return 0
    count = _exec_delete(
        session,
        delete(ColumnDriftSummary).where(ColumnDriftSummary.slice_table_name.in_(slice_names)),
    )
    count += _exec_delete(
        session,
        delete(TemporalSliceAnalysis).where(
            TemporalSliceAnalysis.slice_table_name.in_(slice_names)
        ),
    )
    return count


def _cleanup_cross_table_quality(
    session: Session, source_id: str, table_ids: list[str], column_ids: list[str]
) -> int:
    from dataraum.analysis.correlation.db_models import CrossTableCorrelationRecord

    if not table_ids:
        return 0
    return _exec_delete(
        session,
        delete(CrossTableCorrelationRecord).where(
            CrossTableCorrelationRecord.from_table_id.in_(table_ids)
            | CrossTableCorrelationRecord.to_table_id.in_(table_ids)
        ),
    )


# Registry mapping phase names to their cleanup functions.
# Import phase is intentionally excluded — use a full run for that.
_CLEANUP_MAP: dict[str, CleanupFn] = {
    "typing": _cleanup_typing,
    "statistics": _cleanup_statistics,
    "relationships": _cleanup_relationships,
    "correlations": _cleanup_correlations,
    "semantic": _cleanup_semantic,
    "temporal": _cleanup_temporal,
    "slicing": _cleanup_slicing,
    "slice_analysis": _cleanup_slice_analysis,
    "column_eligibility": _cleanup_column_eligibility,
    "enriched_views": _cleanup_enriched_views,
    "statistical_quality": _cleanup_statistical_quality,
    "quality_summary": _cleanup_quality_summary,
    "business_cycles": _cleanup_business_cycles,
    "validation": _cleanup_validation,
    "graph_execution": _cleanup_graph_execution,
    "entropy": _cleanup_entropy,
    "entropy_interpretation": _cleanup_entropy_interpretation,
    "temporal_slice_analysis": _cleanup_temporal_slice_analysis,
    "cross_table_quality": _cleanup_cross_table_quality,
}


# Phases that create DuckDB tables/views, mapped to the layers they own.
_DUCKDB_LAYER_MAP: dict[str, list[str]] = {
    "typing": ["typed", "quarantine"],
    "slice_analysis": ["slice"],
    "enriched_views": ["enriched"],
}


def _collect_duckdb_paths(source_id: str, layers: list[str], session: Session) -> list[str]:
    """Collect DuckDB table names for the given layers before metadata is deleted."""
    stmt = select(Table.duckdb_path).where(
        Table.source_id == source_id,
        Table.layer.in_(layers),
        Table.duckdb_path.is_not(None),
    )
    return [p for p in session.execute(stmt).scalars().all() if p is not None]


def _drop_duckdb_tables(
    duckdb_conn: duckdb.DuckDBPyConnection, paths: list[str], layers: list[str]
) -> None:
    """Drop DuckDB tables/views that were collected before metadata cleanup."""
    # Enriched layer creates VIEWs, other layers create TABLEs
    has_views = "enriched" in layers
    for path in paths:
        kind = "VIEW" if has_views else "TABLE"
        try:
            duckdb_conn.execute(f'DROP {kind} IF EXISTS "{path}"')
        except duckdb.Error:
            logger.debug(f"Could not drop DuckDB {kind} {path}")


def cleanup_phase(
    phase_name: str,
    source_id: str,
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> int:
    """Delete a phase's output records and DuckDB tables for the given source.

    Args:
        phase_name: Name of the phase to clean up.
        source_id: Source identifier to scope deletions.
        session: Active SQLAlchemy session (caller manages transaction).
        duckdb_conn: DuckDB connection for dropping tables created by the phase.

    Returns:
        Total number of records deleted.
    """
    cleanup_fn = _CLEANUP_MAP.get(phase_name)
    if cleanup_fn is None:
        logger.debug(f"No cleanup registered for phase: {phase_name}")
        return 0

    # Gather scope IDs
    table_ids = _get_table_ids(source_id, session)
    column_ids = _get_column_ids(table_ids, session)

    # Collect DuckDB paths BEFORE metadata cleanup deletes the records
    duckdb_layers = _DUCKDB_LAYER_MAP.get(phase_name, [])
    duckdb_paths = _collect_duckdb_paths(source_id, duckdb_layers, session) if duckdb_layers else []

    # Run phase-specific cleanup (deletes SQLite metadata)
    count = cleanup_fn(session, source_id, table_ids, column_ids)

    # Drop orphaned DuckDB tables/views
    if duckdb_paths:
        _drop_duckdb_tables(duckdb_conn, duckdb_paths, duckdb_layers)
        logger.info("duckdb_cleanup", phase=phase_name, tables_dropped=len(duckdb_paths))

    # Always delete checkpoint for this phase + source
    count += _exec_delete(
        session,
        delete(PhaseCheckpoint).where(
            PhaseCheckpoint.source_id == source_id,
            PhaseCheckpoint.phase_name == phase_name,
        ),
    )

    logger.info(
        "phase_cleanup_complete",
        phase=phase_name,
        source_id=source_id,
        records_deleted=count,
    )
    return count
