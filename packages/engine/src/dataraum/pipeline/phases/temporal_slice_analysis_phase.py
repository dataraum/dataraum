"""Temporal slice analysis phase implementation.

Drift-only analysis on slices:
- Distribution drift detection (JS divergence) per categorical column
- Persists compact ColumnDriftSummary records
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from types import ModuleType
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from dataraum.analysis.semantic.db_models import TableEntity
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.slicing.naming import slice_table_prefix
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

if TYPE_CHECKING:
    pass


logger = get_logger(__name__)


def _view_time_bounds(
    duckdb_conn: Any, fact_duckdb_path: str, time_column: str
) -> tuple[date, date] | None:
    """MIN/MAX of the time column over the fact's slicing view, as dates.

    The bounds source for an enriched-named axis (DAT-491): the column exists
    only on the slicing view (projected from the enriched view), never as a
    typed Column with a TemporalColumnProfile.
    """
    from dataraum.analysis.slicing.naming import slicing_view_name
    from dataraum.core.duckdb_naming import schema_for_layer
    from dataraum.server.storage import LAKE_CATALOG_ALIAS

    view = slicing_view_name(fact_duckdb_path)
    fqn = f'{LAKE_CATALOG_ALIAS}.{schema_for_layer("slicing_view")}."{view}"'
    try:
        row = duckdb_conn.execute(
            f'SELECT MIN(CAST("{time_column}" AS DATE)), MAX(CAST("{time_column}" AS DATE)) '
            f"FROM {fqn}"
        ).fetchone()
    except Exception:
        return None
    if not row or row[0] is None or row[1] is None:
        return None
    return row[0], row[1]


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
    def db_models(self) -> list[ModuleType]:
        from dataraum.analysis.temporal_slicing import db_models

        return [db_models]

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if no slice definitions or no temporal columns."""
        # Source-free: the session's selected typed tables (DAT-403).
        typed_tables = self._typed_tables(ctx)

        if not typed_tables:
            return "No typed tables found"

        table_ids = [t.table_id for t in typed_tables]

        # Check for THIS run's slice definitions (run-versioned, DAT-448)
        slice_stmt = select(SliceDefinition).where(
            SliceDefinition.table_id.in_(table_ids),
            SliceDefinition.run_id == ctx.run_id,
        )
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
                # An enriched-named time axis (DAT-491) has no profile — the
                # phase can still run on it.
                axis_stmt = select(TableEntity.table_id).where(
                    TableEntity.table_id.in_(table_ids),
                    TableEntity.time_column.isnot(None),
                )
                if ctx.run_id is not None:
                    axis_stmt = axis_stmt.where(TableEntity.run_id == ctx.run_id)
                if ctx.session.execute(axis_stmt).first() is None:
                    return "No temporal profiles found (temporal phase may have been skipped or found no temporal columns)"
        else:
            return "No columns found in typed tables"

        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Run drift analysis on slices."""
        # Source-free: the session's selected typed tables (DAT-403).
        typed_tables = self._typed_tables(ctx)

        if not typed_tables:
            return PhaseResult.failed("No typed tables found. Run typing phase first.")

        table_ids = [t.table_id for t in typed_tables]

        # Get THIS run's slice definitions (run-versioned, DAT-448)
        slice_stmt = select(SliceDefinition).where(
            SliceDefinition.table_id.in_(table_ids),
            SliceDefinition.run_id == ctx.run_id,
        )
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
        table_time_columns = self._resolve_time_columns_per_table(ctx, table_ids, typed_tables)

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

        # Pre-load slice tables once (shared across all slice definitions). Slice
        # tables are derived artifacts carrying their fact table's source_id;
        # scope by the session's source set (DAT-403), not ``ctx.source_id``
        # (None past add_source). The per-slice_def name-prefix match below
        # narrows this set to each definition's own slices.
        source_ids = {t.source_id for t in typed_tables}
        slice_tables_stmt = select(Table).where(
            Table.layer == "slice",
            Table.source_id.in_(source_ids),
        )
        all_slice_tables = list((ctx.session.execute(slice_tables_stmt)).scalars().all())

        total_drift_summaries = 0
        total_period_analyses = 0
        errors: list[str] = []
        time_columns_used: set[str] = set()
        # Each physical slice table is drift-analysed exactly once per run. Two
        # slice definitions for the same fact table + dimension (the slicing
        # agent can emit a column twice, and ``_propagate_enriched_dimensions``
        # adds more — ``slice_definitions`` has no per-run uniqueness guard)
        # resolve to the same source-qualified prefix and so match the same
        # slice tables. Persisting a slice table's drift twice in one activity
        # appends duplicate ``(slice_table_name, column_name, run_id)`` rows
        # under the production session's ``autoflush=False`` (the run-scoped
        # delete in ``persist_drift_results`` does not flush the prior batch),
        # violating ``uq_drift_slice_column_run``. The analysis is independent
        # of which definition routed us here — same source ⇒ same time column
        # and period bounds — so deduping is loss-free. (Root cause — duplicate
        # ``SliceDefinition`` rows at the writer — tracked in DAT-496.)
        processed_slice_tables: set[str] = set()

        for slice_def in slice_definitions:
            tc_entry = table_time_columns.get(slice_def.table_id)
            if not tc_entry:
                continue

            time_column, time_profile = tc_entry
            time_columns_used.add(time_column)

            # Build slice info list
            slice_column_stmt = select(Column).where(Column.column_id == slice_def.column_id)
            slice_col = (ctx.session.execute(slice_column_stmt)).scalar_one_or_none()
            if not slice_col:
                continue

            effective_col_name = slice_def.column_name or slice_col.column_name
            source_table = ctx.session.get(Table, slice_def.table_id)
            if not source_table:
                continue

            # Derive period boundaries from this table's temporal profile, or —
            # for an enriched-named axis with no profile (DAT-491) — from
            # MIN/MAX over the slicing view the slices were cut from.
            period_start = cfg_period_start
            period_end = cfg_period_end

            if time_profile is not None:
                if not period_start:
                    ts = time_profile.min_timestamp
                    period_start = date(ts.year, ts.month, 1)
                if not period_end:
                    ts = time_profile.max_timestamp
                    period_end = ts.date() if isinstance(ts, datetime) else ts
            elif not period_start or not period_end:
                bounds = _view_time_bounds(
                    ctx.duckdb_conn, source_table.duckdb_path or "", time_column
                )
                if bounds is None:
                    logger.warning(
                        "time_bounds_unresolvable",
                        table=source_table.table_name,
                        time_column=time_column,
                    )
                    continue
                lo, hi = bounds
                if not period_start:
                    period_start = date(lo.year, lo.month, 1)
                if not period_end:
                    period_end = hi

            if not period_start:
                period_start = date(date.today().year - 1, 1, 1)
            if not period_end:
                period_end = date.today()
            # Source-qualified prefix (DAT-356): match the slice tables this fact's
            # duckdb_path produced — the single naming source of truth.
            prefix = slice_table_prefix(source_table.duckdb_path or "", effective_col_name)
            slice_infos = []
            for st in all_slice_tables:
                if st.table_name.lower().startswith(prefix):
                    slice_infos.append(
                        SliceTableInfo(
                            slice_table_id=st.table_id,
                            slice_table_name=st.table_name,
                            source_table_id=slice_def.table_id,
                            source_table_name="",
                            slice_column_name=effective_col_name,
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
                if si.slice_table_name in processed_slice_tables:
                    continue
                processed_slice_tables.add(si.slice_table_name)
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
                            session_id=ctx.require_session_id(),
                            run_id=ctx.run_id,
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
                            session_id=ctx.require_session_id(),
                            run_id=ctx.run_id,
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
            summary=(
                f"{total_drift_summaries} drift summaries, {total_period_analyses} period analyses"
            ),
        )

    def _resolve_time_columns_per_table(
        self,
        ctx: PhaseContext,
        table_ids: list[str],
        typed_tables: Sequence[Table],
    ) -> dict[str, tuple[str, TemporalColumnProfile | None]]:
        """Resolve the best time column for each typed table independently.

        The profile is ``None`` when the resolved axis is an enriched
        ``fk__col`` column (DAT-491: the slicing agent named a joined header's
        date as the table's time axis) — those have no typed Column row and no
        TemporalColumnProfile; the caller derives period bounds via MIN/MAX
        over the slicing view instead.

        Returns:
            Mapping of table_id → (column_name, TemporalColumnProfile | None)
            for tables that have a usable temporal column.
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

        # Load semantic time_column annotations. Run-scoped to ``ctx.run_id``
        # (DAT-408/413): ``TableEntity`` coexists across runs, so read only this
        # run's time_column, not a prior run's stale annotation.
        entity_stmt = select(TableEntity).where(
            TableEntity.table_id.in_(table_ids),
            TableEntity.time_column.isnot(None),
        )
        if ctx.run_id is not None:
            entity_stmt = entity_stmt.where(TableEntity.run_id == ctx.run_id)
        semantic_time_by_table: dict[str, str] = {}
        for entity in (ctx.session.execute(entity_stmt)).scalars().all():
            if entity.time_column is not None:
                semantic_time_by_table[entity.table_id] = entity.time_column

        # Resolve per table
        result: dict[str, tuple[str, TemporalColumnProfile | None]] = {}

        for tt in typed_tables:
            table_profiles = profiles_by_table.get(tt.table_id, [])
            semantic_name = semantic_time_by_table.get(tt.table_id)

            # An enriched "fk__col" axis (named by the slicing agent, DAT-491)
            # has no typed Column/profile — accept it as-is; the caller derives
            # bounds from the slicing view.
            if not table_profiles:
                if semantic_name and "__" in semantic_name:
                    result[tt.table_id] = (semantic_name, None)
                    logger.debug(
                        "time_column_from_enriched_axis",
                        time_column=semantic_name,
                        table_id=tt.table_id,
                    )
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
            if not chosen_col and semantic_name:
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
                # Enriched-named axis on a table that also has profiled columns.
                if not chosen_col and "__" in semantic_name:
                    chosen_col = semantic_name

            if chosen_col:
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
