"""Slice analysis runner.

Functions to register slice tables in metadata and run analysis phases on slices.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import duckdb
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.storage import Column, Table

if TYPE_CHECKING:
    from dataraum.analysis.semantic.agent import SemanticAgent

logger = get_logger(__name__)


@dataclass
class TemporalSlicesResult:
    """Result from running temporal analysis on multiple slices."""

    slices_analyzed: int
    periods_analyzed: int
    incomplete_periods: int
    anomalies_detected: int
    drift_detected_count: int
    errors: list[str]


@dataclass
class SliceTableInfo:
    """Information about a registered slice table."""

    slice_table_id: str
    slice_table_name: str
    source_table_id: str
    source_table_name: str
    slice_column_name: str
    slice_value: str
    row_count: int


@dataclass
class SliceAnalysisResult:
    """Result from analyzing slices."""

    slices_registered: int
    slices_analyzed: int
    statistics_computed: int
    quality_assessed: int
    semantic_enriched: int
    errors: list[str]


def _sanitize_name(value: str) -> str:
    """Sanitize a value for use in table names."""
    safe = re.sub(r"[^a-zA-Z0-9]", "_", str(value))
    safe = re.sub(r"_+", "_", safe).strip("_").lower()
    return safe


def _get_slice_table_name(source_table_name: str, column_name: str, value: str) -> str:
    """Generate slice table name from components.

    Note: The source_table_name parameter is kept for API compatibility but not used
    in the table name. The naming convention matches the SlicingAgent output.
    """
    # Note: source_table_name kept for signature compatibility but not used
    _ = source_table_name  # Suppress unused warning
    safe_column = _sanitize_name(column_name)
    safe_value = _sanitize_name(value)
    return f"slice_{safe_column}_{safe_value}"


def register_slice_tables(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    slice_definitions: list[SliceDefinition] | None = None,
) -> Result[list[SliceTableInfo]]:
    """Register slice tables from DuckDB into metadata database.

    For each slice table found in DuckDB (matching pattern slice_*),
    creates Table and Column entries with layer='slice'.

    Args:
        session: Database session
        duckdb_conn: DuckDB connection
        slice_definitions: Optional list of slice definitions to use.
            If not provided, will query from database.

    Returns:
        Result containing list of registered SliceTableInfo
    """
    try:
        # Get slice definitions if not provided
        if slice_definitions is None:
            stmt = select(SliceDefinition).order_by(SliceDefinition.slice_priority)
            result = session.execute(stmt)
            slice_definitions = list(result.scalars().all())

        if not slice_definitions:
            return Result.ok([])

        # Get all existing slice tables in DuckDB
        tables_result = duckdb_conn.execute("SHOW TABLES").fetchall()
        duckdb_tables = {t[0] for t in tables_result}
        slice_tables_in_duckdb = {t for t in duckdb_tables if t.startswith("slice_")}

        if not slice_tables_in_duckdb:
            return Result.ok([])

        registered: list[SliceTableInfo] = []

        for slice_def in slice_definitions:
            # Load related table and column
            source_table = session.get(Table, slice_def.table_id)
            source_column = session.get(Column, slice_def.column_id)

            if not source_table or not source_column:
                continue

            # Get source columns for copying schema
            source_columns_stmt = (
                select(Column)
                .where(Column.table_id == source_table.table_id)
                .order_by(Column.column_position)
            )
            source_columns_result = session.execute(source_columns_stmt)
            source_columns = list(source_columns_result.scalars().all())

            # Process each slice value
            for value in slice_def.distinct_values or []:
                slice_table_name = _get_slice_table_name(
                    source_table.table_name,
                    source_column.column_name,
                    value,
                )

                # Check if table exists in DuckDB
                if slice_table_name not in slice_tables_in_duckdb:
                    continue

                # Check if already registered (include source_id in query for correct uniqueness)
                existing_stmt = select(Table).where(
                    Table.source_id == source_table.source_id,
                    Table.table_name == slice_table_name,
                    Table.layer == "slice",
                )
                existing_result = session.execute(existing_stmt)
                existing_table = existing_result.scalar_one_or_none()

                # Also check pending objects in session (with autoflush=False, they won't be in DB yet)
                if not existing_table:
                    for obj in session.new:
                        if (
                            isinstance(obj, Table)
                            and obj.source_id == source_table.source_id
                            and obj.table_name == slice_table_name
                            and obj.layer == "slice"
                        ):
                            existing_table = obj
                            break

                if existing_table:
                    # Already registered
                    count_result = duckdb_conn.execute(
                        f"SELECT COUNT(*) FROM {slice_table_name}"
                    ).fetchone()
                    row_count = count_result[0] if count_result else 0
                    registered.append(
                        SliceTableInfo(
                            slice_table_id=existing_table.table_id,
                            slice_table_name=slice_table_name,
                            source_table_id=source_table.table_id,
                            source_table_name=source_table.table_name,
                            slice_column_name=source_column.column_name,
                            slice_value=value,
                            row_count=row_count,
                        )
                    )
                    continue

                # Get row count from DuckDB
                count_result = duckdb_conn.execute(
                    f"SELECT COUNT(*) FROM {slice_table_name}"
                ).fetchone()
                row_count = count_result[0] if count_result else 0

                # Create Table entry for slice
                # Generate table_id explicitly since SQLAlchemy defaults only apply at INSERT time
                slice_table = Table(
                    table_id=str(uuid4()),
                    source_id=source_table.source_id,
                    table_name=slice_table_name,
                    layer="slice",
                    duckdb_path=slice_table_name,
                    row_count=row_count,
                )
                session.add(slice_table)

                # Create Column entries (copy from parent)
                for src_col in source_columns:
                    col = Column(
                        table_id=slice_table.table_id,
                        column_name=src_col.column_name,
                        column_position=src_col.column_position,
                        raw_type=src_col.raw_type,
                        resolved_type=src_col.resolved_type,
                    )
                    session.add(col)

                registered.append(
                    SliceTableInfo(
                        slice_table_id=slice_table.table_id,
                        slice_table_name=slice_table_name,
                        source_table_id=source_table.table_id,
                        source_table_name=source_table.table_name,
                        slice_column_name=source_column.column_name,
                        slice_value=value,
                        row_count=row_count,
                    )
                )

        return Result.ok(registered)

    except Exception as e:
        session.rollback()
        return Result.fail(f"Failed to register slice tables: {e}")


def run_statistics_on_slice(
    slice_info: SliceTableInfo,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: Session,
) -> Result[Any]:
    """Run statistical profiling on a slice table.

    Args:
        slice_info: Information about the slice table
        duckdb_conn: DuckDB connection
        session: Database session

    Returns:
        Result containing profile result
    """
    from dataraum.analysis.statistics import profile_statistics

    # The profile_statistics function expects layer='typed', but we need to
    # temporarily work with slice layer. We'll directly call it with the table_id.
    # Since we registered with layer='slice', we need to handle this.

    # Get the slice table
    table = session.get(Table, slice_info.slice_table_id)
    if not table:
        return Result.fail(f"Slice table not found: {slice_info.slice_table_id}")

    # Temporarily set layer to 'typed' for profiling
    # No flush needed - SQLAlchemy identity map returns modified object to same session
    original_layer = table.layer
    table.layer = "typed"

    try:
        result = profile_statistics(
            table_id=slice_info.slice_table_id,
            duckdb_conn=duckdb_conn,
            session=session,
        )
        return result
    finally:
        # Restore layer - no flush needed, commit happens at session_scope() end
        table.layer = original_layer


def run_quality_on_slice(
    slice_info: SliceTableInfo,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: Session,
) -> Result[Any]:
    """Run statistical quality assessment on a slice table.

    Args:
        slice_info: Information about the slice table
        duckdb_conn: DuckDB connection
        session: Database session

    Returns:
        Result containing quality result
    """
    from dataraum.analysis.statistics import assess_statistical_quality

    result = assess_statistical_quality(
        table_id=slice_info.slice_table_id,
        duckdb_conn=duckdb_conn,
        session=session,
    )
    return result


def run_semantic_on_slices(
    slice_infos: list[SliceTableInfo],
    session: Session,
    agent: SemanticAgent,
) -> Result[Any]:
    """Copy semantic annotations from parent table to slice tables.

    Since slice tables contain the same columns as the parent table,
    we copy semantic annotations rather than re-running LLM analysis.
    This is more efficient and ensures consistency.

    Args:
        slice_infos: List of slice table infos
        session: Database session
        agent: Semantic agent (unused, kept for API compatibility)

    Returns:
        Result containing count of copied annotations
    """
    from dataraum.analysis.semantic.db_models import SemanticAnnotation
    from dataraum.storage import Column

    _ = agent  # Not used - we copy instead of running LLM

    if not slice_infos:
        return Result.ok(0)

    # Get parent table ID from first slice
    parent_table_id = slice_infos[0].source_table_id

    # Load semantic annotations from parent table
    parent_columns = session.execute(
        select(Column.column_id, Column.column_name).where(Column.table_id == parent_table_id)
    )
    parent_col_map = {row.column_name: row.column_id for row in parent_columns}

    # Load existing annotations for parent columns
    parent_annotations = session.execute(
        select(SemanticAnnotation).where(
            SemanticAnnotation.column_id.in_(list(parent_col_map.values()))
        )
    )
    annotation_by_name: dict[str, SemanticAnnotation] = {}
    for ann in parent_annotations.scalars():
        # Find column name for this annotation
        for col_name, col_id in parent_col_map.items():
            if col_id == ann.column_id:
                annotation_by_name[col_name] = ann
                break

    if not annotation_by_name:
        return Result.ok(0)  # No annotations to copy

    copied_count = 0

    # Copy annotations to each slice table
    for slice_info in slice_infos:
        # Load slice columns
        slice_columns = session.execute(
            select(Column.column_id, Column.column_name).where(
                Column.table_id == slice_info.slice_table_id
            )
        )

        for row in slice_columns:
            if row.column_name in annotation_by_name:
                parent_ann = annotation_by_name[row.column_name]
                # Create new annotation for slice column
                slice_ann = SemanticAnnotation(
                    column_id=row.column_id,
                    semantic_role=parent_ann.semantic_role,
                    entity_type=parent_ann.entity_type,
                    business_name=parent_ann.business_name,
                    business_description=parent_ann.business_description,
                    business_concept=parent_ann.business_concept,
                    annotation_source=parent_ann.annotation_source,
                    annotated_by="copied_from_parent",
                    confidence=parent_ann.confidence,
                )
                session.add(slice_ann)
                copied_count += 1

    return Result.ok(copied_count)


def run_analysis_on_slices(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    slice_infos: list[SliceTableInfo],
    semantic_agent: SemanticAgent | None = None,
    run_statistics: bool = True,
    run_quality: bool = True,
    run_semantic: bool = True,
) -> SliceAnalysisResult:
    """Run analysis phases on slice tables.

    Runs Phase 3 (statistics), Phase 3b (quality), and Phase 5 (semantic)
    on each slice table.

    Args:
        session: Database session
        duckdb_conn: DuckDB connection
        slice_infos: List of slice table infos to analyze
        semantic_agent: Semantic agent (required if run_semantic=True)
        run_statistics: Whether to run statistical profiling
        run_quality: Whether to run quality assessment
        run_semantic: Whether to run semantic analysis

    Returns:
        SliceAnalysisResult with counts and errors
    """
    errors: list[str] = []
    stats_count = 0
    quality_count = 0
    semantic_count = 0

    # Run statistics on each slice
    if run_statistics:
        for slice_info in slice_infos:
            result = run_statistics_on_slice(slice_info, duckdb_conn, session)
            if result.success:
                stats_count += 1
            else:
                errors.append(f"Stats {slice_info.slice_table_name}: {result.error}")

    # Run quality on each slice
    if run_quality:
        for slice_info in slice_infos:
            result = run_quality_on_slice(slice_info, duckdb_conn, session)
            if result.success:
                quality_count += 1
            else:
                errors.append(f"Quality {slice_info.slice_table_name}: {result.error}")

    # Run semantic on all slices at once (batched for efficiency)
    if run_semantic and semantic_agent:
        result = run_semantic_on_slices(slice_infos, session, semantic_agent)
        if result.success:
            semantic_count = len(slice_infos)
        else:
            errors.append(f"Semantic analysis: {result.error}")

    return SliceAnalysisResult(
        slices_registered=len(slice_infos),
        slices_analyzed=len(slice_infos),
        statistics_computed=stats_count,
        quality_assessed=quality_count,
        semantic_enriched=semantic_count,
        errors=errors,
    )


def run_temporal_analysis_on_slices(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    slice_infos: list[SliceTableInfo],
    time_column: str,
    period_start: date,
    period_end: date,
    time_grain: str = "monthly",
) -> TemporalSlicesResult:
    """Run temporal analysis on all slice tables.

    Args:
        session: Database session
        duckdb_conn: DuckDB connection
        slice_infos: List of slice tables to analyze
        time_column: Name of the time/date column
        period_start: Start date for analysis
        period_end: End date for analysis
        time_grain: Time granularity (daily, weekly, monthly)

    Returns:
        TemporalSlicesResult with aggregated metrics
    """
    from dataraum.analysis.temporal_slicing import (
        TemporalSliceConfig,
        TimeGrain,
        analyze_temporal_slices,
    )

    # Convert time_grain string to enum
    grain_map = {
        "daily": TimeGrain.DAILY,
        "weekly": TimeGrain.WEEKLY,
        "monthly": TimeGrain.MONTHLY,
    }
    grain = grain_map.get(time_grain, TimeGrain.MONTHLY)

    errors: list[str] = []
    total_periods = 0
    total_incomplete = 0
    total_anomalies = 0
    total_drift = 0
    slices_analyzed = 0

    for slice_info in slice_infos:
        config = TemporalSliceConfig(
            time_column=time_column,
            period_start=period_start,
            period_end=period_end,
            time_grain=grain,
        )

        logger.info(
            "analyzing_slice",
            slice_table=slice_info.slice_table_name,
            slice_column=slice_info.slice_column_name,
            time_column=time_column,
            period_start=str(period_start),
            period_end=str(period_end),
        )

        result = analyze_temporal_slices(
            duckdb_conn=duckdb_conn,
            session=session,
            slice_table_name=slice_info.slice_table_name,
            config=config,
            slice_column_name=slice_info.slice_column_name,
            persist=True,
        )

        if result.success and result.value is not None:
            slices_analyzed += 1
            analysis = result.value
            logger.info(
                "slice_analysis_complete",
                slice_table=slice_info.slice_table_name,
                total_periods=analysis.total_periods,
                drift_results_count=len(analysis.drift_results),
                matrix_entries=len(analysis.slice_time_matrix.data)
                if analysis.slice_time_matrix
                else 0,
            )
            total_periods = max(total_periods, analysis.total_periods)
            total_incomplete += len([c for c in analysis.completeness_results if not c.is_complete])
            total_anomalies += len([v for v in analysis.volume_anomalies if v.is_anomaly])
            total_drift += len([d for d in analysis.drift_results if d.has_significant_drift])
        else:
            logger.warning(
                "slice_analysis_failed",
                slice_table=slice_info.slice_table_name,
                error=result.error,
            )
            errors.append(f"{slice_info.slice_table_name}: {result.error}")

    return TemporalSlicesResult(
        slices_analyzed=slices_analyzed,
        periods_analyzed=total_periods,
        incomplete_periods=total_incomplete,
        anomalies_detected=total_anomalies,
        drift_detected_count=total_drift,
        errors=errors,
    )


@dataclass
class TopologySlicesResult:
    """Result from running topology analysis on multiple slices."""

    slices_analyzed: int
    slices_with_anomalies: int
    topology_by_slice: dict[str, dict[str, Any]]
    structural_drift: list[dict[str, Any]]
    average_topology: dict[str, float] | None
    errors: list[str]


def run_topology_on_slices(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    slice_infos: list[SliceTableInfo],
    correlation_threshold: float = 0.5,  # kept for API compatibility
) -> TopologySlicesResult:
    """Run topological analysis on slice tables to detect structural differences.

    Uses the existing topology module for full TDA analysis including:
    - Betti numbers (components, cycles, voids)
    - Persistence diagrams and entropy
    - Homological stability assessment
    - Anomaly detection

    Args:
        session: SQLAlchemy session
        duckdb_conn: DuckDB connection
        slice_infos: List of slice table info from registration
        correlation_threshold: Unused, kept for API compatibility

    Returns:
        TopologySlicesResult with topology results and cross-slice comparison
    """
    from dataraum.analysis.topology import analyze_topological_quality

    topology_by_slice: dict[str, dict[str, Any]] = {}
    structural_drift: list[dict[str, Any]] = []
    errors: list[str] = []
    slices_with_anomalies = 0

    # Collect topology for all slices
    slice_topologies: list[dict[str, Any]] = []

    for slice_info in slice_infos:
        try:
            # Use the existing topology module for full TDA analysis
            result = analyze_topological_quality(
                table_id=slice_info.slice_table_id,
                duckdb_conn=duckdb_conn,
                session=session,
            )

            if not result.success or result.value is None:
                errors.append(f"{slice_info.slice_table_name}: {result.error}")
                continue

            topo_result = result.value
            betti = topo_result.betti_numbers

            # Build slice topology dict for cross-slice comparison
            slice_topo = {
                "table_name": slice_info.slice_table_name,
                "slice_value": slice_info.slice_value,
                "row_count": slice_info.row_count,
                "betti_0": betti.betti_0,
                "betti_1": betti.betti_1,
                "betti_2": betti.betti_2,
                "complexity": betti.total_complexity,
                "persistent_entropy": topo_result.persistent_entropy,
                "num_cycles": len(topo_result.persistent_cycles),
                "has_anomalies": len(topo_result.anomalies) > 0,
                "anomalies": [a.description for a in topo_result.anomalies],
                "stability": topo_result.stability.is_stable if topo_result.stability else None,
            }

            slice_topologies.append(slice_topo)
            topology_by_slice[slice_info.slice_table_name] = slice_topo

            if slice_topo["has_anomalies"]:
                slices_with_anomalies += 1

        except Exception as e:
            errors.append(f"{slice_info.slice_table_name}: {str(e)}")

    # Compute structural drift (compare topology across slices)
    average_topology: dict[str, float] | None = None

    if len(slice_topologies) >= 2:
        avg_betti_0 = sum(t["betti_0"] for t in slice_topologies) / len(slice_topologies)
        avg_betti_1 = sum(t["betti_1"] for t in slice_topologies) / len(slice_topologies)
        avg_complexity = sum(t["complexity"] for t in slice_topologies) / len(slice_topologies)
        avg_entropy = sum(t["persistent_entropy"] or 0 for t in slice_topologies) / len(
            slice_topologies
        )

        average_topology = {
            "betti_0": avg_betti_0,
            "betti_1": avg_betti_1,
            "complexity": avg_complexity,
            "persistent_entropy": avg_entropy,
        }

        # Find slices that deviate significantly
        for topo in slice_topologies:
            if avg_betti_0 > 0:
                betti_0_diff = abs(topo["betti_0"] - avg_betti_0) / avg_betti_0
                if betti_0_diff > 0.5:
                    structural_drift.append(
                        {
                            "slice": topo["table_name"],
                            "slice_value": topo["slice_value"],
                            "metric": "betti_0",
                            "value": topo["betti_0"],
                            "average": avg_betti_0,
                            "deviation_pct": betti_0_diff * 100,
                        }
                    )

            if avg_complexity > 0:
                complexity_diff = abs(topo["complexity"] - avg_complexity) / avg_complexity
                if complexity_diff > 0.5:
                    structural_drift.append(
                        {
                            "slice": topo["table_name"],
                            "slice_value": topo["slice_value"],
                            "metric": "complexity",
                            "value": topo["complexity"],
                            "average": avg_complexity,
                            "deviation_pct": complexity_diff * 100,
                        }
                    )

            # Also check entropy drift
            if avg_entropy > 0 and topo["persistent_entropy"]:
                entropy_diff = abs(topo["persistent_entropy"] - avg_entropy) / avg_entropy
                if entropy_diff > 0.5:
                    structural_drift.append(
                        {
                            "slice": topo["table_name"],
                            "slice_value": topo["slice_value"],
                            "metric": "persistent_entropy",
                            "value": topo["persistent_entropy"],
                            "average": avg_entropy,
                            "deviation_pct": entropy_diff * 100,
                        }
                    )

    return TopologySlicesResult(
        slices_analyzed=len(slice_topologies),
        slices_with_anomalies=slices_with_anomalies,
        topology_by_slice=topology_by_slice,
        structural_drift=structural_drift,
        average_topology=average_topology,
        errors=errors,
    )


__all__ = [
    "SliceTableInfo",
    "SliceAnalysisResult",
    "TemporalSlicesResult",
    "TopologySlicesResult",
    "register_slice_tables",
    "run_analysis_on_slices",
    "run_statistics_on_slice",
    "run_quality_on_slice",
    "run_semantic_on_slices",
    "run_temporal_analysis_on_slices",
    "run_topology_on_slices",
]
