"""Temporal analysis processor.

Main entry point for temporal profiling, following the same pattern as statistics:
- profile_temporal(table_id, ...): Profile all temporal columns in a table

This analyzes temporal characteristics like:
- Granularity (daily, hourly, etc.) + confidence
- Span, completeness, and gaps (the served coverage substrate)
- Staleness

Every computed fact has a typed home on ``temporal_column_profiles`` (flat columns
plus the ``gaps`` JSON interior) — there is no write-only ``profile_data`` blob
(DAT-783). All facts derive from the single DISTINCT-timestamp pass in
``analyze_basic_temporal`` (robust to duplicate-per-day fact rows); the duplicate-
corrupted row-interval path and the WRONG fiscal/update-frequency analyzers were
deleted in DAT-783.

Uses parallel processing for large tables to speed up profiling.
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import duckdb
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.temporal.db_models import (
    TemporalColumnProfile,
)
from dataraum.analysis.temporal.detection import (
    analyze_basic_temporal,
)
from dataraum.analysis.temporal.models import (
    TemporalAnalysisResult,
    TemporalProfileResult,
)
from dataraum.core.config import load_yaml_config
from dataraum.core.logging import get_logger
from dataraum.core.models.base import ColumnRef, Result
from dataraum.storage import Column, Table
from dataraum.storage.upsert import upsert

logger = get_logger(__name__)

# Persisted gaps are bounded so a pathological high-cardinality timestamp column
# can't write an unbounded JSON blob. ``gap_count`` keeps the TRUE count; the gaps
# list keeps the N largest (the detection query orders gaps by size, descending).
_MAX_PERSISTED_GAPS = 100


def _profile_temporal_column_parallel(
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_name: str,
    table_duckdb_path: str,
    source_id: str,
    column_id: str,
    column_name: str,
    profiled_at: datetime,
    config: dict[str, Any],
) -> TemporalAnalysisResult | None:
    """Profile a single temporal column in a worker thread.

    Runs in its own thread using a cursor from the shared DuckDB connection.
    DuckDB cursors are thread-safe for read operations.
    Returns TemporalAnalysisResult directly for the main thread to persist.
    """
    with duckdb_conn.cursor() as cursor:
        try:
            # Single DISTINCT-timestamp pass: min/max, span, granularity (+ confidence),
            # completeness/gaps, and staleness. No sampling, no duplicate-corrupted
            # row-interval path (DAT-783).
            basic_result = analyze_basic_temporal(
                cursor, table_duckdb_path, column_name, config=config
            )
            if not basic_result.success or not basic_result.value:
                return None
            b = basic_result.value

            column_ref = ColumnRef(
                source_id=source_id,
                table_name=table_name,
                column_name=column_name,
            )

            return TemporalAnalysisResult(
                metric_id=str(uuid4()),
                column_id=column_id,
                column_ref=column_ref,
                column_name=column_name,
                table_name=table_name,
                computed_at=profiled_at,
                min_timestamp=b["min_timestamp"],
                max_timestamp=b["max_timestamp"],
                span_days=b["span_days"],
                detected_granularity=b["granularity"],
                granularity_confidence=b["granularity_confidence"],
                is_stale=b["is_stale"],
                completeness=b["completeness"],
            )
        except Exception as e:
            logger.warning(
                "temporal_column_profiling_failed",
                column_name=column_name,
                table_name=table_name,
                error=str(e),
            )
            return None


def profile_temporal(
    table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: Session,
    *,
    config: dict[str, Any] | None = None,
    run_id: str,
) -> Result[TemporalProfileResult]:
    """Profile temporal columns in a table.

    This is the main entry point for temporal analysis, following the
    same pattern as profile_statistics(). It:
    1. Gets all temporal columns for the table
    2. Analyzes each column for temporal patterns
    3. Stores per-column profiles

    Uses parallel processing for file-based DBs to speed up profiling.

    REQUIRES table.layer == "typed". Raises error otherwise.

    Args:
        table_id: Table ID to profile
        duckdb_conn: DuckDB connection
        session: SQLAlchemy session
        config: Temporal config dict (from config/phases/temporal.yaml).
            If None, loads from config/phases/temporal.yaml.

    Returns:
        Result containing TemporalProfileResult with all column profiles
    """
    try:
        # Load config if not provided
        if config is None:
            config = load_yaml_config("phases/temporal.yaml")

        max_workers = config["processing"]["max_workers"]
        # Get table from metadata
        table = session.get(Table, str(table_id))
        if not table:
            return Result.fail(f"Table not found: {table_id}")

        if not table.duckdb_path:
            return Result.fail(f"Table has no DuckDB path: {table_id}")

        if table.layer != "typed":
            return Result.fail(f"Temporal profiling requires typed tables. Got: {table.layer}")

        # Get all temporal columns for this table
        temporal_types = ["DATE", "TIMESTAMP", "TIMESTAMPTZ"]
        column_stmt = (
            select(Column)
            .where(
                Column.table_id == table.table_id,
                Column.resolved_type.in_(temporal_types),
            )
            .order_by(Column.column_position)
        )
        column_result = session.execute(column_stmt)
        columns = column_result.scalars().all()

        if not columns:
            return Result.ok(
                TemporalProfileResult(
                    column_profiles=[],
                    duration_seconds=0.0,
                )
            )

        profiled_at = datetime.now(UTC)
        profiles: list[TemporalAnalysisResult] = []
        rows: list[dict[str, Any]] = []
        start_time = time.time()

        # Use parallel processing with cursors from shared connection
        # DuckDB cursors are thread-safe for read operations
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [
                pool.submit(
                    _profile_temporal_column_parallel,
                    duckdb_conn,
                    table.table_name,
                    table.duckdb_path,
                    table.source_id,
                    column.column_id,
                    column.column_name,
                    profiled_at,
                    config,
                )
                for column in columns
            ]

            for future in futures:
                profile = future.result()
                if profile:
                    profiles.append(profile)
                    rows.append(_profile_row(profile, run_id, profiled_at))

        # Upsert on ``(column_id, run_id)`` so a Temporal at-least-once retry
        # (same run_id) updates the row in place instead of duplicating it —
        # which would make the head-resolved loaders' scalar_one_or_none() raise.
        upsert(session, TemporalColumnProfile, rows, index_elements=["column_id", "run_id"])

        # No flush needed - commit happens at session_scope() end
        # The caller (phase) manages the transaction

        duration = time.time() - start_time

        return Result.ok(
            TemporalProfileResult(
                column_profiles=profiles,
                duration_seconds=duration,
            )
        )

    except Exception as e:
        return Result.fail(f"Temporal profiling failed: {e}")


def _profile_row(
    profile: TemporalAnalysisResult,
    run_id: str,
    profiled_at: datetime,
) -> dict[str, Any]:
    """Build the idempotent-upsert row for one column profile.

    Every promoted fact lands in its own typed column — the scalar coverage
    metrics flat, the ``gaps`` list as a bounded JSON interior of strict
    ``TemporalGapInfo`` submodels (DAT-783). ``profile_id`` PK is supplied
    (the model has no default).
    """
    comp = profile.completeness
    gaps = comp.gaps[:_MAX_PERSISTED_GAPS] if comp else []
    return {
        "profile_id": profile.metric_id,
        "column_id": profile.column_id,
        "run_id": run_id,
        "profiled_at": profiled_at,
        "min_timestamp": profile.min_timestamp,
        "max_timestamp": profile.max_timestamp,
        "span_days": profile.span_days,
        "detected_granularity": profile.detected_granularity,
        "granularity_confidence": profile.granularity_confidence,
        "is_stale": profile.is_stale,
        "completeness_ratio": comp.completeness_ratio if comp else None,
        "expected_periods": comp.expected_periods if comp else None,
        "actual_periods": comp.actual_periods if comp else None,
        "gap_count": comp.gap_count if comp else None,
        "largest_gap_days": comp.largest_gap_days if comp else None,
        "gaps": [g.model_dump(mode="json") for g in gaps],
    }


__all__ = [
    "profile_temporal",
]
