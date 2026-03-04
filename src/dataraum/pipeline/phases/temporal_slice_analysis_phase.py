"""Temporal slice analysis phase implementation.

Drift-only analysis on slices:
- Distribution drift detection (JS divergence) per categorical column
- Persists compact ColumnDriftSummary records
"""

from __future__ import annotations

import re
from collections.abc import Sequence
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

        # Resolve time column per table. Each typed table may have different
        # temporal columns (or none at all). We scope selection to each table
        # to avoid cross-table type mismatches (e.g. "date" is DATE in one
        # table but VARCHAR in another).
        table_time_columns = self._resolve_time_columns_per_table(
            ctx, table_ids, typed_tables
        )

        if not table_time_columns:
            return PhaseResult.success(
                outputs={
                    "message": "No temporal column found for any table",
                    "drift_summaries": 0,
                },
                records_processed=0,
                records_created=0,
            )

        # Global config overrides
        cfg_period_start = ctx.config.get("period_start")
        cfg_period_end = ctx.config.get("period_end")
        time_grain = ctx.config.get("time_grain", "monthly")

        if isinstance(cfg_period_start, str):
            cfg_period_start = date.fromisoformat(cfg_period_start)
        if isinstance(cfg_period_end, str):
            cfg_period_end = date.fromisoformat(cfg_period_end)

        # Convert time_grain string to enum
        grain_map = {
            "daily": TimeGrain.DAILY,
            "weekly": TimeGrain.WEEKLY,
            "monthly": TimeGrain.MONTHLY,
        }
        grain = grain_map.get(time_grain, TimeGrain.MONTHLY)

        # Pre-load slice tables once (shared across all slice definitions)
        slice_tables_stmt = select(Table).where(
            Table.layer == "slice",
            Table.source_id == ctx.source_id,
        )
        all_slice_tables = list((ctx.session.execute(slice_tables_stmt)).scalars().all())

        total_drift_summaries = 0
        total_period_analyses = 0
        errors: list[str] = []
        time_columns_used: set[str] = set()

        for slice_def in slice_definitions:
            tc_entry = table_time_columns.get(slice_def.table_id)
            if not tc_entry:
                continue

            time_column, time_profile = tc_entry
            time_columns_used.add(time_column)

            # Derive period boundaries from this table's temporal profile
            period_start = cfg_period_start
            period_end = cfg_period_end

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

            # Build slice info list
            slice_column_stmt = select(Column).where(Column.column_id == slice_def.column_id)
            slice_col = (ctx.session.execute(slice_column_stmt)).scalar_one_or_none()
            if not slice_col:
                continue

            sanitized_col_name = _sanitize_name(slice_col.column_name)
            prefix = f"slice_{sanitized_col_name}_"
            slice_infos = []
            for st in all_slice_tables:
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

        outputs: dict[str, object] = {
            "drift_summaries": total_drift_summaries,
            "period_analyses": total_period_analyses,
            "time_columns": sorted(time_columns_used),
            "time_grain": time_grain,
        }

        if errors:
            outputs["errors"] = errors

        return PhaseResult.success(
            outputs=outputs,
            records_processed=len(slice_definitions),
            records_created=total_drift_summaries + total_period_analyses,
            summary=f"{total_drift_summaries} drift summaries, {total_period_analyses} period analyses",
        )

    def _resolve_time_columns_per_table(
        self,
        ctx: PhaseContext,
        table_ids: list[str],
        typed_tables: Sequence[Table],
    ) -> dict[str, tuple[str, TemporalColumnProfile]]:
        """Resolve the best time column for each typed table independently.

        Returns:
            Mapping of table_id → (column_name, TemporalColumnProfile) for
            tables that have a usable temporal column.
        """
        configured_time_column = ctx.config.get("time_column")
        temporal_types = {"DATE", "TIMESTAMP", "TIMESTAMPTZ"}

        # Load all columns and temporal profiles for these tables
        col_stmt = select(Column).where(Column.table_id.in_(table_ids))
        all_columns = list((ctx.session.execute(col_stmt)).scalars().all())

        col_by_id: dict[str, Column] = {c.column_id: c for c in all_columns}

        # Load temporal profiles
        column_ids = [c.column_id for c in all_columns]
        all_temporal_profiles: list[TemporalColumnProfile] = []
        if column_ids:
            temp_stmt = select(TemporalColumnProfile).where(
                TemporalColumnProfile.column_id.in_(column_ids)
            )
            all_temporal_profiles = list((ctx.session.execute(temp_stmt)).scalars().all())

        # Group temporal profiles by table_id, filtering to proper temporal types
        profiles_by_table: dict[str, list[TemporalColumnProfile]] = {}
        for tp in all_temporal_profiles:
            col = col_by_id.get(tp.column_id)
            if col and col.resolved_type in temporal_types:
                profiles_by_table.setdefault(col.table_id, []).append(tp)

        # Load semantic time_column annotations
        entity_stmt = select(TableEntity).where(
            TableEntity.table_id.in_(table_ids),
            TableEntity.time_column.isnot(None),
        )
        semantic_time_by_table: dict[str, str] = {}
        for entity in (ctx.session.execute(entity_stmt)).scalars().all():
            if entity.time_column is not None:
                semantic_time_by_table[entity.table_id] = entity.time_column

        # Resolve per table
        result: dict[str, tuple[str, TemporalColumnProfile]] = {}

        for tt in typed_tables:
            table_profiles = profiles_by_table.get(tt.table_id, [])
            if not table_profiles:
                continue

            chosen_col: str | None = None
            chosen_profile: TemporalColumnProfile | None = None

            # Priority 1: config-specified time_column
            if configured_time_column:
                for tp in table_profiles:
                    col = col_by_id.get(tp.column_id)
                    if col and col.column_name == configured_time_column:
                        chosen_col = configured_time_column
                        chosen_profile = tp
                        break

            # Priority 2: semantic annotation
            if not chosen_col:
                semantic_name = semantic_time_by_table.get(tt.table_id)
                if semantic_name:
                    for tp in table_profiles:
                        col = col_by_id.get(tp.column_id)
                        if col and col.column_name == semantic_name:
                            chosen_col = semantic_name
                            chosen_profile = tp
                            logger.debug(
                                "time_column_from_semantic",
                                time_column=semantic_name,
                                table_id=tt.table_id,
                            )
                            break

            if chosen_col and chosen_profile:
                result[tt.table_id] = (chosen_col, chosen_profile)
            elif table_profiles:
                profile_col_names = [
                    col_by_id[tp.column_id].column_name
                    for tp in table_profiles
                    if tp.column_id in col_by_id
                ]
                logger.warning(
                    "no_time_column_resolved",
                    table_id=tt.table_id,
                    table_name=tt.table_name,
                    candidate_columns=profile_col_names,
                    hint="Set time_column in config or add a semantic annotation",
                )

        return result
