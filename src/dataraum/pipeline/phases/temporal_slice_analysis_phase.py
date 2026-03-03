"""Temporal slice analysis phase implementation.

Drift-only analysis on slices:
- Distribution drift detection (JS divergence) per categorical column
- Persists compact ColumnDriftSummary records
"""

from __future__ import annotations

import re
from datetime import date, datetime
from types import ModuleType

from sqlalchemy import select

from dataraum.analysis.semantic.db_models import TableEntity
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.slicing.slice_runner import SliceTableInfo
from dataraum.analysis.temporal import TemporalColumnProfile
from dataraum.analysis.temporal_slicing.analyzer import (
    analyze_column_drift,
    analyze_period_metrics,
    persist_drift_results,
    persist_period_results,
)
from dataraum.analysis.temporal_slicing.models import TemporalSliceConfig, TimeGrain
from dataraum.core.logging import get_logger
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.storage import Column, Table


def _sanitize_name(value: str) -> str:
    """Sanitize a value for matching against slice table names.

    Must match the convention in slice_runner._sanitize_name().
    """
    safe = re.sub(r"[^a-zA-Z0-9]", "_", str(value))
    safe = re.sub(r"_+", "_", safe).strip("_").lower()
    return safe


logger = get_logger(__name__)


@analysis_phase
class TemporalSliceAnalysisPhase(BasePhase):
    """Drift analysis on slices.

    Runs JS divergence drift detection on categorical columns
    within slice tables, producing ColumnDriftSummary records.

    Requires: slice_analysis, temporal phases.
    """

    @property
    def name(self) -> str:
        return "temporal_slice_analysis"

    @property
    def description(self) -> str:
        return "Distribution drift analysis on slices"

    @property
    def dependencies(self) -> list[str]:
        return ["slice_analysis", "temporal"]

    @property
    def outputs(self) -> list[str]:
        return ["drift_summaries", "period_analyses"]

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.analysis.temporal_slicing import db_models

        return [db_models]

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if no slice definitions or no temporal columns."""
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return f"No typed tables found for source {ctx.source_id}"

        table_ids = [t.table_id for t in typed_tables]

        # Check for slice definitions
        slice_stmt = select(SliceDefinition).where(SliceDefinition.table_id.in_(table_ids))
        slice_defs = (ctx.session.execute(slice_stmt)).scalars().all()

        if not slice_defs:
            return "No slice definitions found (slicing phase may have been skipped)"

        # Check for temporal profiles
        column_ids = []
        cols_stmt = select(Column.column_id).where(Column.table_id.in_(table_ids))
        for col_id in (ctx.session.execute(cols_stmt)).scalars().all():
            column_ids.append(col_id)

        if column_ids:
            temp_stmt = select(TemporalColumnProfile).where(
                TemporalColumnProfile.column_id.in_(column_ids)
            )
            temporal_cols = (ctx.session.execute(temp_stmt)).scalars().all()

            if not temporal_cols:
                return "No temporal profiles found (temporal phase may have been skipped or found no temporal columns)"
        else:
            return "No columns found in typed tables"

        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Run drift analysis on slices."""
        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return PhaseResult.failed("No typed tables found. Run typing phase first.")

        table_ids = [t.table_id for t in typed_tables]

        # Get slice definitions
        slice_stmt = select(SliceDefinition).where(SliceDefinition.table_id.in_(table_ids))
        slice_definitions = (ctx.session.execute(slice_stmt)).scalars().all()

        if not slice_definitions:
            return PhaseResult.success(
                outputs={
                    "message": "No slice definitions found",
                    "drift_summaries": 0,
                },
                records_processed=0,
                records_created=0,
            )

        # Find temporal column - from config or auto-detect.
        # The temporal phase already identified temporal columns and stored
        # TemporalColumnProfile records with min/max timestamps. We reuse
        # that data instead of re-querying DuckDB.
        time_column = ctx.config.get("time_column")
        time_profile: TemporalColumnProfile | None = None

        # Load all temporal profiles for this source's typed columns
        column_ids = list(
            (ctx.session.execute(select(Column.column_id).where(Column.table_id.in_(table_ids))))
            .scalars()
            .all()
        )

        all_temporal_profiles: list[TemporalColumnProfile] = []
        if column_ids:
            temp_stmt = select(TemporalColumnProfile).where(
                TemporalColumnProfile.column_id.in_(column_ids)
            )
            all_temporal_profiles = list((ctx.session.execute(temp_stmt)).scalars().all())

        # Build column lookup: column_id → Column
        col_by_id: dict[str, Column] = {}
        if all_temporal_profiles:
            profile_col_ids = [tc.column_id for tc in all_temporal_profiles]
            col_stmt = select(Column).where(Column.column_id.in_(profile_col_ids))
            for col in (ctx.session.execute(col_stmt)).scalars().all():
                col_by_id[col.column_id] = col

        # Verify configured time column exists in temporal profiles
        if time_column:
            for tc in all_temporal_profiles:
                matched_col = col_by_id.get(tc.column_id)
                if matched_col and matched_col.column_name == time_column:
                    time_profile = tc
                    break

            if not time_profile:
                logger.debug(
                    "configured_time_column_not_found",
                    configured_column=time_column,
                    message="Falling back to auto-detection",
                )
                time_column = None

        # Auto-detect: prefer semantic annotation (LLM-identified primary time column),
        # fall back to lowest null ratio among temporal profiles.
        if not time_column and all_temporal_profiles:
            # Check if the semantic phase identified a primary time column for any table
            entity_stmt = select(TableEntity).where(
                TableEntity.table_id.in_(table_ids),
                TableEntity.time_column.isnot(None),
            )
            entities_with_time = list((ctx.session.execute(entity_stmt)).scalars().all())

            # Match semantic time_column against actual temporal profiles
            for entity in entities_with_time:
                for tc in all_temporal_profiles:
                    tc_col = col_by_id.get(tc.column_id)
                    if (
                        tc_col
                        and tc_col.column_name == entity.time_column
                        and tc_col.table_id == entity.table_id
                    ):
                        time_column = entity.time_column
                        time_profile = tc
                        logger.debug(
                            "time_column_from_semantic",
                            time_column=time_column,
                            table_id=entity.table_id,
                        )
                        break
                if time_column:
                    break

        # Fallback: pick the temporal column with the lowest null ratio
        if not time_column and all_temporal_profiles:
            from dataraum.analysis.statistics.db_models import StatisticalProfile

            best_profile = None
            best_null_ratio = 1.0

            for tc in all_temporal_profiles:
                stat_stmt = (
                    select(StatisticalProfile)
                    .where(StatisticalProfile.column_id == tc.column_id)
                    .order_by(StatisticalProfile.profiled_at.desc())
                    .limit(1)
                )
                stat = (ctx.session.execute(stat_stmt)).scalar_one_or_none()
                null_ratio = stat.null_ratio if stat and stat.null_ratio is not None else 1.0

                if null_ratio < best_null_ratio:
                    best_null_ratio = null_ratio
                    best_profile = tc

            if best_profile:
                best_col = col_by_id.get(best_profile.column_id)
                if best_col:
                    time_column = best_col.column_name
                    time_profile = best_profile

        if not time_column:
            return PhaseResult.success(
                outputs={
                    "message": "No temporal column found or specified",
                    "drift_summaries": 0,
                },
                records_processed=0,
                records_created=0,
            )

        # Get time period boundaries
        period_start = ctx.config.get("period_start")
        period_end = ctx.config.get("period_end")
        time_grain = ctx.config.get("time_grain", "monthly")

        if isinstance(period_start, str):
            period_start = date.fromisoformat(period_start)
        if isinstance(period_end, str):
            period_end = date.fromisoformat(period_end)

        # Derive time range from the temporal profile (already computed by temporal phase)
        if (not period_start or not period_end) and time_profile:
            if not period_start:
                ts = time_profile.min_timestamp
                period_start = date(ts.year, ts.month, 1)
            if not period_end:
                ts = time_profile.max_timestamp
                period_end = ts.date() if isinstance(ts, datetime) else ts

        if not period_start:
            period_start = date(date.today().year - 1, 1, 1)
        if not period_end:
            period_end = date.today()

        # Convert time_grain string to enum
        grain_map = {
            "daily": TimeGrain.DAILY,
            "weekly": TimeGrain.WEEKLY,
            "monthly": TimeGrain.MONTHLY,
        }
        grain = grain_map.get(time_grain, TimeGrain.MONTHLY)

        total_drift_summaries = 0
        total_period_analyses = 0
        errors = []

        # Pre-compute which typed tables have the time column
        tables_with_time_col: set[str] = set()
        for tt in typed_tables:
            col_check = select(Column).where(
                Column.table_id == tt.table_id,
                Column.column_name == time_column,
            )
            if ctx.session.execute(col_check).scalar_one_or_none():
                tables_with_time_col.add(tt.table_id)

        for slice_def in slice_definitions:
            if slice_def.table_id not in tables_with_time_col:
                continue

            # Get slice tables for this definition
            slice_tables_stmt = select(Table).where(
                Table.layer == "slice",
                Table.source_id == ctx.source_id,
            )
            slice_tables = (ctx.session.execute(slice_tables_stmt)).scalars().all()

            # Build slice info list
            slice_column_stmt = select(Column).where(Column.column_id == slice_def.column_id)
            slice_col = (ctx.session.execute(slice_column_stmt)).scalar_one_or_none()
            if not slice_col:
                continue

            sanitized_col_name = _sanitize_name(slice_col.column_name)
            prefix = f"slice_{sanitized_col_name}_"
            slice_infos = []
            for st in slice_tables:
                if st.table_name.lower().startswith(prefix):
                    slice_infos.append(
                        SliceTableInfo(
                            slice_table_id=st.table_id,
                            slice_table_name=st.table_name,
                            source_table_id=slice_def.table_id,
                            source_table_name="",
                            slice_column_name=slice_col.column_name,
                            slice_value=st.table_name[len(prefix) :],
                            row_count=st.row_count or 0,
                        )
                    )

            if not slice_infos:
                continue

            # Run drift analysis on each slice table
            config = TemporalSliceConfig(
                time_column=time_column,
                period_start=period_start,
                period_end=period_end,
                time_grain=grain,
            )

            for si in slice_infos:
                try:
                    drift_result = analyze_column_drift(
                        slice_table_name=si.slice_table_name,
                        time_column=time_column,
                        duckdb_conn=ctx.duckdb_conn,
                        session=ctx.session,
                        config=config,
                    )
                    if drift_result.success and drift_result.value is not None:
                        persist_result = persist_drift_results(
                            results=drift_result.value,
                            slice_table_name=si.slice_table_name,
                            time_column=time_column,
                            session=ctx.session,
                        )
                        if persist_result.success and persist_result.value is not None:
                            total_drift_summaries += persist_result.value
                    elif not drift_result.success:
                        errors.append(f"{si.slice_table_name}: {drift_result.error}")

                    # Period-level completeness + volume anomaly analysis
                    period_result = analyze_period_metrics(
                        slice_table_name=si.slice_table_name,
                        time_column=time_column,
                        duckdb_conn=ctx.duckdb_conn,
                        config=config,
                    )
                    if period_result.success and period_result.value is not None:
                        persist_count = persist_period_results(
                            result=period_result.value,
                            session=ctx.session,
                        )
                        if persist_count.success and persist_count.value is not None:
                            total_period_analyses += persist_count.value
                    elif not period_result.success:
                        errors.append(
                            f"Period analysis error for {si.slice_table_name}: {period_result.error}"
                        )

                except Exception as e:
                    errors.append(f"Analysis error for {si.slice_table_name}: {e}")

        outputs = {
            "drift_summaries": total_drift_summaries,
            "period_analyses": total_period_analyses,
            "time_column": time_column,
            "time_grain": time_grain,
            "period_start": str(period_start),
            "period_end": str(period_end),
        }

        if errors:
            outputs["errors"] = errors

        return PhaseResult.success(
            outputs=outputs,
            records_processed=len(slice_definitions),
            records_created=total_drift_summaries + total_period_analyses,
        )
