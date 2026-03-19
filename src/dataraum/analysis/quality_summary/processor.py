"""Quality summary processor.

Orchestrates quality summary analysis: aggregates slice results and
calls the LLM agent to generate summaries per column.

Supports batching multiple columns per LLM call, parallel batch processing,
and skipping existing reports.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import distinct as sql_distinct
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.quality_summary.db_models import (
    ColumnQualityReport,
    ColumnSliceProfile,
)
from dataraum.analysis.quality_summary.models import (
    AggregatedColumnData,
    ColumnQualitySummary,
    QualitySummaryResult,
)
from dataraum.analysis.quality_summary.variance import compute_slice_variance
from dataraum.analysis.semantic.db_models import SemanticAnnotation
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.statistics.db_models import (
    StatisticalProfile,
)
from dataraum.analysis.statistics.quality_db_models import (
    StatisticalQualityMetrics,
)
from dataraum.analysis.temporal_slicing.db_models import ColumnDriftSummary
from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.storage import Column, Table

if TYPE_CHECKING:
    from dataraum.analysis.quality_summary.agent import QualitySummaryAgent
    from dataraum.analysis.quality_summary.variance import SliceVarianceMetrics

logger = get_logger(__name__)

_CONFIG_CACHE: dict[str, Any] | None = None


def _load_config() -> dict[str, Any]:
    """Load quality_summary config from YAML (cached)."""
    global _CONFIG_CACHE
    if _CONFIG_CACHE is None:
        from dataraum.core.config import load_phase_config

        _CONFIG_CACHE = load_phase_config("quality_summary")
    return _CONFIG_CACHE


@dataclass
class BatchResult:
    """Result from processing a single batch."""

    success: bool
    summaries: list[ColumnQualitySummary] = field(default_factory=list)
    error: str | None = None


def _process_batch(
    batch: list[AggregatedColumnData],
    agent: QualitySummaryAgent,
    session_factory: Callable[[], Any],
    source_table_name: str,
    slice_column_name: str,
    total_slices: int,
) -> BatchResult:
    """Process a single batch in a worker thread.

    Each thread gets its own session from the factory for thread safety.

    Args:
        batch: List of column data to process
        agent: Quality summary agent
        session_factory: Factory to create new sessions
        source_table_name: Name of source table
        slice_column_name: Name of slice column
        total_slices: Number of slices

    Returns:
        BatchResult with summaries or error
    """
    try:
        with session_factory() as session:
            batch_result = agent.summarize_columns_batch(
                session=session,
                columns_data=batch,
                source_table_name=source_table_name,
                slice_column_name=slice_column_name,
                total_slices=total_slices,
            )

            if not batch_result.success:
                return BatchResult(success=False, error=batch_result.error)

            return BatchResult(success=True, summaries=batch_result.unwrap())

    except Exception as e:
        return BatchResult(success=False, error=str(e))


def aggregate_slice_results(
    session: Session,
    slice_definition: SliceDefinition,
    min_slice_rows: int = 20,
) -> Result[list[AggregatedColumnData]]:
    """Aggregate analysis results across slices for each column.

    For each column in the source table, collects statistics, quality
    metrics, and semantic info from all slice tables.

    Args:
        session: Database session
        slice_definition: The slice definition to aggregate for

    Returns:
        Result containing list of AggregatedColumnData per column
    """
    try:
        # Get source table and column info
        source_table = session.get(Table, slice_definition.table_id)
        slice_column = session.get(Column, slice_definition.column_id)

        if not source_table or not slice_column:
            return Result.fail("Source table or slice column not found")

        # Prefer slice_definition.column_name (stores the actual LLM-recommended name,
        # which for enriched dim cols is e.g. "kontonummer_des_gegenkontos__land").
        effective_slice_col_name = slice_definition.column_name or slice_column.column_name

        # Get all slice definition column IDs to exclude from analysis
        # These are columns used for slicing - same values in each slice, so redundant
        all_slice_def_cols_stmt = select(sql_distinct(SliceDefinition.column_id)).where(
            SliceDefinition.table_id == source_table.table_id
        )
        all_slice_def_cols_result = session.execute(all_slice_def_cols_stmt)
        slice_definition_column_ids = set(all_slice_def_cols_result.scalars().all())

        # If a slicing_view Table was registered for this fact table, use its columns —
        # they include enriched FK-prefixed dimension columns absent from the typed table.
        # Fall back to the typed source table for dimension tables (no slicing view).
        sv_table = session.execute(
            select(Table).where(
                Table.source_id == source_table.source_id,
                Table.table_name == f"slicing_{source_table.table_name}",
                Table.layer == "slicing_view",
            )
        ).scalar_one_or_none()

        # Use slicing_view table as the authoritative schema when available —
        # it contains enriched FK-prefixed dimension columns absent from the typed table.
        # Use its id/name for AggregatedColumnData so reports reference the view, not the typed table.
        effective_table = sv_table if sv_table else source_table

        if sv_table:
            # Exclude slice-definition columns by name — they are constant within each
            # slice (same value for all rows) so analysing them adds no signal.
            slice_def_col_names = set(
                session.execute(
                    select(Column.column_name).where(
                        Column.column_id.in_(slice_definition_column_ids)
                    )
                )
                .scalars()
                .all()
            )
            source_cols_stmt = (
                select(Column)
                .where(Column.table_id == sv_table.table_id)
                .order_by(Column.column_position)
            )
            source_columns = [
                col
                for col in session.execute(source_cols_stmt).scalars().all()
                if col.column_name not in slice_def_col_names
            ]
        else:
            # Dimension table (or no slicing view registered) — use typed source directly
            source_cols_stmt = (
                select(Column)
                .where(Column.table_id == source_table.table_id)
                .where(Column.column_id.notin_(slice_definition_column_ids))
                .order_by(Column.column_position)
            )
            source_columns = list(session.execute(source_cols_stmt).scalars().all())

        # Get all slice tables for this definition
        slice_values = slice_definition.distinct_values or []

        # Find slice tables in database
        slice_tables = []
        for value in slice_values:
            # Generate expected slice table name (namespaced by source table)
            safe_source = re.sub(r"[^a-zA-Z0-9]", "_", source_table.table_name)
            safe_source = re.sub(r"_+", "_", safe_source).strip("_").lower()
            safe_column = re.sub(r"[^a-zA-Z0-9]", "_", effective_slice_col_name)
            safe_column = re.sub(r"_+", "_", safe_column).strip("_").lower()
            safe_value = re.sub(r"[^a-zA-Z0-9]", "_", str(value))
            safe_value = re.sub(r"_+", "_", safe_value).strip("_").lower()
            # Match SlicingAgent fallback for empty values
            if not safe_value:
                safe_value = "unknown"

            slice_table_name = f"slice_{safe_source}_{safe_column}_{safe_value}"

            # Find in database
            slice_table_stmt = select(Table).where(
                Table.table_name == slice_table_name,
                Table.layer == "slice",
            )
            slice_table_result = session.execute(slice_table_stmt)
            slice_table = slice_table_result.scalar_one_or_none()

            if slice_table:
                slice_tables.append((slice_table, value))

        if not slice_tables:
            return Result.fail("No slice tables found in database")

        # Filter out slices with too few rows for reliable statistics.
        # Small slices remain in SliceDefinition for BI context but are
        # excluded from quality analysis where statistics would be unreliable.
        if min_slice_rows > 0:
            before = len(slice_tables)
            slice_tables = [
                (st, v) for st, v in slice_tables if (st.row_count or 0) >= min_slice_rows
            ]
            dropped = before - len(slice_tables)
            if dropped > 0:
                logger.info(
                    "slice_rows_filter",
                    table=source_table.table_name,
                    slice_column=effective_slice_col_name,
                    dropped=dropped,
                    kept=len(slice_tables),
                    threshold=min_slice_rows,
                )
            if not slice_tables:
                return Result.fail(f"All slice tables below {min_slice_rows} row minimum")

        # Build aggregated data for each source column
        aggregated: list[AggregatedColumnData] = []

        for source_col in source_columns:
            agg_data = AggregatedColumnData(
                column_name=source_col.column_name,
                column_id=source_col.column_id,
                source_table_id=effective_table.table_id,
                source_table_name=effective_table.table_name,
                slice_column_name=effective_slice_col_name,
                resolved_type=source_col.resolved_type,
            )

            # Get semantic info from source column (if exists)
            sem_stmt = select(SemanticAnnotation).where(
                SemanticAnnotation.column_id == source_col.column_id
            )
            sem_result = session.execute(sem_stmt)
            sem_ann = sem_result.scalar_one_or_none()
            if sem_ann:
                agg_data.semantic_role = sem_ann.semantic_role
                agg_data.business_name = sem_ann.business_name
                agg_data.business_description = sem_ann.business_description

            # Collect data from each slice
            for slice_table, slice_value in slice_tables:
                # Find the corresponding column in slice table
                slice_col_stmt = select(Column).where(
                    Column.table_id == slice_table.table_id,
                    Column.column_name == source_col.column_name,
                )
                slice_col_result = session.execute(slice_col_stmt)
                slice_col = slice_col_result.scalar_one_or_none()

                if not slice_col:
                    continue

                slice_data: dict[str, Any] = {
                    "slice_name": slice_table.table_name,
                    "slice_value": slice_value,
                    "row_count": slice_table.row_count or 0,
                }

                # Get statistical profile (use limit(1) in case of duplicates)
                profile_stmt = (
                    select(StatisticalProfile)
                    .where(StatisticalProfile.column_id == slice_col.column_id)
                    .limit(1)
                )
                profile_result = session.execute(profile_stmt)
                profile = profile_result.scalar_one_or_none()

                if profile:
                    slice_data["null_count"] = profile.null_count
                    slice_data["null_ratio"] = (
                        profile.null_count / profile.total_count if profile.total_count else None
                    )
                    slice_data["distinct_count"] = profile.distinct_count
                    slice_data["cardinality_ratio"] = profile.cardinality_ratio

                    # Numeric stats from profile_data (nested under numeric_stats)
                    if profile.profile_data:
                        pd = profile.profile_data
                        numeric_stats = pd.get("numeric_stats") or {}
                        slice_data["min_value"] = numeric_stats.get("min_value")
                        slice_data["max_value"] = numeric_stats.get("max_value")
                        slice_data["mean_value"] = numeric_stats.get("mean")
                        slice_data["stddev"] = numeric_stats.get("stddev")

                # Get quality metrics (use limit(1) in case of duplicates)
                quality_stmt = (
                    select(StatisticalQualityMetrics)
                    .where(StatisticalQualityMetrics.column_id == slice_col.column_id)
                    .limit(1)
                )
                quality_result = session.execute(quality_stmt)
                quality = quality_result.scalar_one_or_none()

                if quality:
                    slice_data["benford_compliant"] = quality.benford_compliant
                    slice_data["has_outliers"] = quality.has_outliers
                    slice_data["outlier_ratio"] = quality.iqr_outlier_ratio

                    if quality.quality_data:
                        qd = quality.quality_data
                        benford = qd.get("benford_analysis") or {}
                        slice_data["benford_p_value"] = benford.get("p_value")

                    # Update aggregation counts
                    if quality.benford_compliant is False:
                        agg_data.benford_violation_count += 1
                    if quality.has_outliers:
                        agg_data.outlier_slice_count += 1

                # Update totals
                agg_data.total_rows += slice_data.get("row_count", 0)
                agg_data.total_nulls += slice_data.get("null_count", 0) or 0

                distinct = slice_data.get("distinct_count")
                if distinct is not None:
                    if agg_data.min_distinct is None or distinct < agg_data.min_distinct:
                        agg_data.min_distinct = distinct
                    if agg_data.max_distinct is None or distinct > agg_data.max_distinct:
                        agg_data.max_distinct = distinct

                agg_data.slice_data.append(slice_data)

            aggregated.append(agg_data)

        # Load drift summaries from DB and attach to aggregated columns
        slice_table_names = [st.table_name for st, _ in slice_tables]
        if slice_table_names:
            drift_stmt = select(ColumnDriftSummary).where(
                ColumnDriftSummary.slice_table_name.in_(slice_table_names)
            )
            drift_summaries = list(session.execute(drift_stmt).scalars().all())

            if drift_summaries:
                # Build per-column drift count and max drift
                drift_counts: dict[str, int] = {}
                for s in drift_summaries:
                    if s.periods_with_drift > 0:
                        drift_counts[s.column_name] = (
                            drift_counts.get(s.column_name, 0) + s.periods_with_drift
                        )

                # Collect temporal issues from drift evidence
                temporal_issues: list[str] = []
                for s in drift_summaries:
                    if s.max_js_divergence > 0.1 and s.drift_evidence_json:
                        evidence = s.drift_evidence_json
                        worst = evidence.get("worst_period", "")
                        worst_js = evidence.get("worst_js", 0)
                        temporal_issues.append(
                            f"Distribution drift in {s.column_name}: JS={worst_js:.3f} in {worst}"
                        )

                max_drift = (
                    max(s.max_js_divergence for s in drift_summaries) if drift_summaries else 0
                )
                drift_detected_total = sum(1 for s in drift_summaries if s.max_js_divergence > 0.1)

                shared_context: dict[str, Any] = {
                    "incomplete_periods": 0,
                    "volume_anomalies": 0,
                    "temporal_issues": temporal_issues,
                    "max_drift": round(max_drift, 4),
                    "drift_detected_total": drift_detected_total,
                }
                for agg in aggregated:
                    agg.temporal_context = {
                        **shared_context,
                        "drift_detected_count": drift_counts.get(agg.column_name, 0),
                    }

        return Result.ok(aggregated)

    except Exception as e:
        return Result.fail(f"Failed to aggregate slice results: {e}")


def _filter_by_network_readiness(
    session: Session,
    columns_with_data: list[AggregatedColumnData],
    source_table_id: str,
    p_high_threshold: float = 0.35,
) -> tuple[list[AggregatedColumnData], dict[str, str]]:
    """Filter columns using Bayesian network readiness.

    A column is "interesting" if:
    - readiness is "investigate" or "blocked", OR
    - any intent P(high) > p_high_threshold

    Args:
        session: Database session.
        columns_with_data: Columns with aggregated slice data.
        source_table_id: Table ID to load entropy for.
        p_high_threshold: Threshold for intent P(high) above which a
            "ready" column is still included.

    Returns:
        Tuple of (filtered columns, readiness_map {col_name: readiness}).
    """
    from dataraum.entropy.core.storage import EntropyRepository
    from dataraum.entropy.network.model import EntropyNetwork
    from dataraum.entropy.views.network_context import assemble_network_context

    repo = EntropyRepository(session)
    typed_table_ids = repo.get_typed_table_ids([source_table_id])
    if not typed_table_ids:
        logger.debug("network_filter_no_typed_tables", table_id=source_table_id)
        return [], {}

    entropy_objects = repo.load_for_tables(typed_table_ids, enforce_typed=True)
    if not entropy_objects:
        logger.debug("network_filter_no_entropy_objects", table_id=source_table_id)
        return [], {}

    network = EntropyNetwork()
    network_ctx = assemble_network_context(entropy_objects, network)

    # Build column name -> readiness map from network results
    readiness_map: dict[str, str] = {}
    filtered: list[AggregatedColumnData] = []

    # Build a lookup: extract column name from target "column:table.col"
    col_results_by_name: dict[str, str] = {}
    col_p_high_by_name: dict[str, float] = {}
    for target, col_result in network_ctx.columns.items():
        # target format: "column:table_name.column_name"
        if "." in target:
            col_name = target.split(".", 1)[1]
        else:
            col_name = target.removeprefix("column:")
        col_results_by_name[col_name] = col_result.readiness
        col_p_high_by_name[col_name] = col_result.worst_intent_p_high

    for col in columns_with_data:
        readiness = col_results_by_name.get(col.column_name)
        if readiness is None:
            # Column has no network result — exclude
            readiness_map[col.column_name] = "no_signal"
            continue

        readiness_map[col.column_name] = readiness

        if readiness in ("investigate", "blocked"):
            filtered.append(col)
        elif (col_p_high_by_name.get(col.column_name, 0.0)) > p_high_threshold:
            # "ready" but some intent P(high) is above threshold
            filtered.append(col)

    logger.debug(
        "network_readiness_filter",
        total=len(columns_with_data),
        filtered=len(filtered),
        blocked=sum(1 for v in readiness_map.values() if v == "blocked"),
        investigate=sum(1 for v in readiness_map.values() if v == "investigate"),
        ready=sum(1 for v in readiness_map.values() if v == "ready"),
        no_signal=sum(1 for v in readiness_map.values() if v == "no_signal"),
    )

    return filtered, readiness_map


def summarize_quality(
    session: Session,
    agent: QualitySummaryAgent,
    slice_definition: SliceDefinition,
    skip_existing: bool = True,
    session_factory: Callable[[], Any] | None = None,
    min_slice_rows: int = 20,
) -> Result[QualitySummaryResult]:
    """Generate quality summaries for all columns across slices.

    Uses batching to process multiple columns per LLM call for efficiency.
    Skips columns that already have reports (unless skip_existing=False).

    When session_factory is provided and there are multiple batches,
    processes batches in parallel for improved throughput.

    Args:
        session: Database session
        agent: Quality summary agent
        slice_definition: The slice definition to summarize
        skip_existing: Whether to skip columns with existing reports
        session_factory: Optional factory for creating sessions (enables parallel batches)
        min_slice_rows: Minimum rows for a slice to be included in quality analysis

    Returns:
        Result containing QualitySummaryResult
    """
    started_at = datetime.now(UTC)

    # Get source info
    source_table = session.get(Table, slice_definition.table_id)
    slice_column = session.get(Column, slice_definition.column_id)

    if not source_table or not slice_column:
        return Result.fail("Source table or slice column not found")

    # Prefer slice_definition.column_name for enriched dim cols (e.g. "kontonummer_des_gegenkontos__land")
    effective_slice_col_name = slice_definition.column_name or slice_column.column_name

    # Resolve the effective table: prefer slicing_view layer (has enriched FK-prefixed columns)
    sv_table = session.execute(
        select(Table).where(
            Table.source_id == source_table.source_id,
            Table.table_name == f"slicing_{source_table.table_name}",
            Table.layer == "slicing_view",
        )
    ).scalar_one_or_none()
    effective_table = sv_table if sv_table else source_table

    try:
        # Aggregate results across slices
        agg_result = aggregate_slice_results(session, slice_definition, min_slice_rows)
        if not agg_result.success:
            return Result.fail(agg_result.error or "Aggregation failed")

        aggregated_columns = agg_result.unwrap()

        # Filter out columns with no slice data
        columns_with_data = [c for c in aggregated_columns if c.slice_data]

        # Compute variance metrics for ColumnSliceProfile.variance_classification
        # (still needed by dimensional_entropy downstream)
        variance_metrics: dict[str, SliceVarianceMetrics] = {}
        for col in columns_with_data:
            variance_metrics[col.column_name] = compute_slice_variance(col)

        # Filter by Bayesian network readiness (entropy is a dependency,
        # so entropy data is guaranteed to exist when this phase runs).
        cfg = _load_config()
        nf_cfg = cfg.get("network_filter", {})
        p_high_threshold = nf_cfg.get("p_high_threshold", 0.35)
        columns_to_process, readiness_map = _filter_by_network_readiness(
            session=session,
            columns_with_data=columns_with_data,
            source_table_id=effective_table.table_id,
            p_high_threshold=p_high_threshold,
        )
        classifications = readiness_map

        # Skip columns with existing reports if requested.
        # Scope by slice_column_name too so enriched dim slice defs sharing the same
        # FK column_id (e.g. land vs geschaeftspartnertyp) don't falsely skip each other.
        if skip_existing:
            existing_stmt = select(ColumnQualityReport.source_column_id).where(
                ColumnQualityReport.slice_column_id == slice_column.column_id,
                ColumnQualityReport.slice_column_name == effective_slice_col_name,
            )
            existing_result = session.execute(existing_stmt)
            existing_column_ids = set(existing_result.scalars().all())

            columns_to_process = [
                c for c in columns_to_process if c.column_id not in existing_column_ids
            ]

        # Always persist slice profiles — even when no columns need LLM summaries.
        # These profiles are computed from aggregated data (no LLM needed) and feed
        # into the query agent context via GraphExecutionContext.
        _save_slice_profiles_from_aggregated(
            session=session,
            aggregated_columns=aggregated_columns,
            slice_definition=slice_definition,
            source_table_name=effective_table.table_name,
            variance_metrics=variance_metrics,
        )

        if not columns_to_process:
            # All columns already have reports or filtered out
            duration = (datetime.now(UTC) - started_at).total_seconds()
            return Result.ok(
                QualitySummaryResult(
                    source_table_id=effective_table.table_id,
                    source_table_name=effective_table.table_name,
                    slice_column_name=effective_slice_col_name,
                    slice_count=len(slice_definition.distinct_values or []),
                    column_summaries=[],
                    column_classifications=classifications,
                    duration_seconds=duration,
                )
            )

        # Load batch config from YAML
        cfg = _load_config()
        batch_size = cfg.get("batch_size", 10)
        max_batch_workers = cfg.get("max_batch_workers", 4)

        # Create batches
        batches = [
            columns_to_process[i : i + batch_size]
            for i in range(0, len(columns_to_process), batch_size)
        ]

        # Process batches
        column_summaries: list[ColumnQualitySummary] = []
        total_slices = len(slice_definition.distinct_values or [])

        # Use parallel processing if session_factory provided and multiple batches
        if session_factory and len(batches) > 1 and max_batch_workers > 1:
            # Parallel batch processing
            with ThreadPoolExecutor(max_workers=max_batch_workers) as pool:
                futures = {
                    pool.submit(
                        _process_batch,
                        batch,
                        agent,
                        session_factory,
                        effective_table.table_name,
                        effective_slice_col_name,
                        total_slices,
                    ): batch
                    for batch in batches
                }

                for future in as_completed(futures):
                    batch_result = future.result()
                    if batch_result.success:
                        column_summaries.extend(batch_result.summaries)
        else:
            # Sequential batch processing
            for batch in batches:
                llm_result = agent.summarize_columns_batch(
                    session=session,
                    columns_data=batch,
                    source_table_name=effective_table.table_name,
                    slice_column_name=effective_slice_col_name,
                    total_slices=total_slices,
                )

                if llm_result.success:
                    column_summaries.extend(llm_result.unwrap())

        # Build column_id lookup
        col_id_map = {c.column_name: c.column_id for c in columns_to_process}

        # Persist reports (always sequential for consistency)
        reports_generated = 0
        for summary in column_summaries:
            col_id = col_id_map.get(summary.column_name)

            report = ColumnQualityReport(
                source_column_id=col_id,
                slice_column_id=slice_column.column_id,
                column_name=summary.column_name,
                source_table_name=summary.source_table_name,
                slice_column_name=summary.slice_column_name,
                slice_count=summary.total_slices,
                overall_quality_score=summary.overall_quality_score,
                quality_grade=summary.quality_grade,
                summary=summary.summary,
                report_data={
                    "key_findings": summary.key_findings,
                    "quality_issues": [qi.model_dump() for qi in summary.quality_issues],
                    "slice_comparisons": [sc.model_dump() for sc in summary.slice_comparisons],
                    "recommendations": summary.recommendations,
                    "slice_values": [sm.slice_value for sm in summary.slice_metrics],
                },
                investigation_views=summary.investigation_views,
            )
            session.add(report)
            reports_generated += 1

        duration = (datetime.now(UTC) - started_at).total_seconds()

        return Result.ok(
            QualitySummaryResult(
                source_table_id=effective_table.table_id,
                source_table_name=effective_table.table_name,
                slice_column_name=effective_slice_col_name,
                slice_count=len(slice_definition.distinct_values or []),
                column_summaries=column_summaries,
                column_classifications=classifications,
                duration_seconds=duration,
            )
        )

    except Exception as e:
        return Result.fail(f"Quality summary failed: {e}")


def _save_slice_profiles_from_aggregated(
    session: Session,
    aggregated_columns: list[AggregatedColumnData],
    slice_definition: SliceDefinition,
    source_table_name: str,
    variance_metrics: dict[str, SliceVarianceMetrics] | None = None,
) -> None:
    """Persist slice profiles from aggregated column data.

    Uses delete + insert strategy: deletes existing profiles for the
    slice definition and inserts fresh data.

    Args:
        session: Database session
        aggregated_columns: List of aggregated column data with slice_data
        slice_definition: The slice definition being processed
        source_table_name: Name of the source table
    """
    # Get slice column name — prefer slice_definition.column_name for enriched dim cols
    slice_column = session.get(Column, slice_definition.column_id)
    slice_column_name = slice_definition.column_name or (
        slice_column.column_name if slice_column else "unknown"
    )

    # Delete existing profiles for this slice definition via ORM.
    # Scope by BOTH slice_column_id AND slice_column_name so that two enriched dim
    # slice defs sharing the same FK column_id (e.g. kontonummer_des_gegenkontos__land
    # and kontonummer_des_gegenkontos__geschaeftspartnertyp) don't clobber each other.
    existing_stmt = select(ColumnSliceProfile).where(
        ColumnSliceProfile.slice_column_id == slice_definition.column_id,
        ColumnSliceProfile.slice_column_name == slice_column_name,
    )
    for existing in session.execute(existing_stmt).scalars().all():
        session.delete(existing)

    # Load quality scoring config
    cfg = _load_config()
    qs = cfg.get("quality_scoring", {})
    high_null_ratio = qs.get("high_null_ratio", 0.5)
    high_null_penalty = qs.get("high_null_penalty", 0.3)
    moderate_null_ratio = qs.get("moderate_null_ratio", 0.2)
    moderate_null_penalty = qs.get("moderate_null_penalty", 0.1)
    outlier_penalty = qs.get("outlier_penalty", 0.2)
    benford_penalty = qs.get("benford_violation_penalty", 0.1)

    # Insert new profiles from aggregated data
    for col_data in aggregated_columns:
        for slice_info in col_data.slice_data:
            quality_score = 1.0
            has_issues = False
            issue_count = 0

            null_ratio = slice_info.get("null_ratio")
            if null_ratio is not None:
                if null_ratio > high_null_ratio:
                    quality_score -= high_null_penalty
                    issue_count += 1
                    has_issues = True
                elif null_ratio > moderate_null_ratio:
                    quality_score -= moderate_null_penalty
                    issue_count += 1

            if slice_info.get("has_outliers"):
                quality_score -= outlier_penalty
                issue_count += 1
                has_issues = True

            if slice_info.get("benford_compliant") is False:
                quality_score -= benford_penalty
                issue_count += 1
                has_issues = True

            quality_score = max(0.0, min(1.0, quality_score))

            # Build profile_data JSON with extended metrics
            profile_data: dict[str, Any] = {}
            for key in [
                "min_value",
                "max_value",
                "mean_value",
                "stddev",
                "benford_p_value",
                "outlier_ratio",
                "cardinality_ratio",
            ]:
                if slice_info.get(key) is not None:
                    profile_data[key] = slice_info[key]

            profile = ColumnSliceProfile(
                source_column_id=col_data.column_id,
                slice_column_id=slice_definition.column_id,
                source_table_name=source_table_name,
                column_name=col_data.column_name,
                slice_column_name=slice_column_name,
                slice_value=str(slice_info.get("slice_value", "")),
                row_count=slice_info.get("row_count", 0),
                null_ratio=null_ratio,
                distinct_count=slice_info.get("distinct_count"),
                variance_classification=(
                    variance_metrics[col_data.column_name].classification.value
                    if variance_metrics and col_data.column_name in variance_metrics
                    else None
                ),
                quality_score=quality_score,
                has_issues=has_issues,
                issue_count=issue_count,
                profile_data=profile_data if profile_data else None,
            )
            session.add(profile)

    # No explicit flush — all adds are batched and sent during session.commit()
    # inside the commit lock. Flushing here would open a SQLite write transaction
    # outside the lock, blocking parallel phases.


__all__ = [
    "aggregate_slice_results",
    "summarize_quality",
]
