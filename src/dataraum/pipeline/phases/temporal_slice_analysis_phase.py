"""Temporal slice analysis phase implementation.

Non-LLM temporal + topology analysis on slices:
- Period completeness metrics
- Distribution drift detection
- Cross-slice temporal comparison (slice × time matrix)
- Volume anomaly detection
- Per-slice topology (full TDA: Betti numbers, persistence diagrams, entropy)
- Temporal topology drift (via bottleneck distance between periods)
"""

from __future__ import annotations

import re
from datetime import date

from sqlalchemy import select

from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.slicing.slice_runner import (
    SliceTableInfo,
    run_temporal_analysis_on_slices,
    run_topology_on_slices,
)
from dataraum.analysis.temporal import TemporalColumnProfile
from dataraum.analysis.temporal_slicing.analyzer import analyze_temporal_topology
from dataraum.analysis.temporal_slicing.db_models import TemporalTopologyAnalysis
from dataraum.analysis.temporal_slicing.models import PeriodTopology, TopologyDrift
from dataraum.core.logging import get_logger
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.storage import Column, Table


def _sanitize_name(value: str) -> str:
    """Sanitize a value for matching against slice table names.

    Must match the convention in slice_runner._sanitize_name().
    """
    safe = re.sub(r"[^a-zA-Z0-9]", "_", str(value))
    safe = re.sub(r"_+", "_", safe).strip("_").lower()
    return safe


logger = get_logger(__name__)


class TemporalSliceAnalysisPhase(BasePhase):
    """Temporal + topology analysis on slices.

    Combines temporal analysis with TDA topology analysis:
    - Temporal analysis per slice (completeness, drift, volume anomalies)
    - Topology comparison across dimensional slices
    - Temporal topology drift using bottleneck distance

    Requires: slice_analysis, temporal phases.
    """

    @property
    def name(self) -> str:
        return "temporal_slice_analysis"

    @property
    def description(self) -> str:
        return "Temporal + topology analysis on slices"

    @property
    def dependencies(self) -> list[str]:
        return ["slice_analysis", "temporal"]

    @property
    def outputs(self) -> list[str]:
        return ["temporal_slice_profiles", "slice_topology", "topology_drift"]

    @property
    def is_llm_phase(self) -> bool:
        return False

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if no slice definitions or no temporal columns."""

        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return f"No typed tables found for source {ctx.source_id}"

        logger.info(f"TempSlice: Found {len(typed_tables)} typed tables")
        table_ids = [t.table_id for t in typed_tables]

        # Check for slice definitions
        slice_stmt = select(SliceDefinition).where(SliceDefinition.table_id.in_(table_ids))
        slice_defs = (ctx.session.execute(slice_stmt)).scalars().all()

        if not slice_defs:
            return "No slice definitions found (slicing phase may have been skipped)"

        logger.info(f"TempSlice: Found {len(slice_defs)} slice definitions")

        # Check for temporal profiles (created by temporal phase)
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

            logger.info(f"TempSlice: Found {len(temporal_cols)} temporal profiles")
        else:
            return "No columns found in typed tables"

        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Run temporal and topology analysis on slices."""
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
                    "temporal_analyses": 0,
                    "topology_analyses": 0,
                },
                records_processed=0,
                records_created=0,
            )

        # Find temporal column - from config or auto-detect
        time_column = ctx.config.get("time_column")

        if not time_column:
            # Auto-detect from temporal profiles
            column_ids = []
            cols_stmt = select(Column.column_id).where(Column.table_id.in_(table_ids))
            for col_id in (ctx.session.execute(cols_stmt)).scalars().all():
                column_ids.append(col_id)

            if column_ids:
                temp_stmt = select(TemporalColumnProfile).where(
                    TemporalColumnProfile.column_id.in_(column_ids)
                )
                temporal_cols = (ctx.session.execute(temp_stmt)).scalars().all()

                if temporal_cols:
                    # Find the best temporal column - prefer ones with most data coverage
                    from dataraum.analysis.statistics.db_models import StatisticalProfile

                    best_col = None
                    best_null_ratio = 1.0  # Lower is better

                    for tc in temporal_cols:
                        # Get the column's null ratio from statistical profile
                        stat_stmt = (
                            select(StatisticalProfile)
                            .where(StatisticalProfile.column_id == tc.column_id)
                            .order_by(StatisticalProfile.profiled_at.desc())
                            .limit(1)
                        )
                        stat = (ctx.session.execute(stat_stmt)).scalar_one_or_none()

                        null_ratio = (
                            stat.null_ratio if stat and stat.null_ratio is not None else 1.0
                        )

                        col_stmt = select(Column).where(Column.column_id == tc.column_id)
                        col = (ctx.session.execute(col_stmt)).scalar_one_or_none()

                        logger.debug(
                            "temporal_column_candidate",
                            column_name=col.column_name if col else "unknown",
                            null_ratio=null_ratio,
                        )

                        # Prefer column with lowest null ratio (most data)
                        if null_ratio < best_null_ratio:
                            best_null_ratio = null_ratio
                            best_col = col

                    if best_col:
                        time_column = best_col.column_name
                        logger.info(
                            "selected_temporal_column",
                            column_name=time_column,
                            null_ratio=best_null_ratio,
                        )

        if not time_column:
            return PhaseResult.success(
                outputs={
                    "message": "No temporal column found or specified",
                    "temporal_analyses": 0,
                    "topology_analyses": 0,
                },
                records_processed=0,
                records_created=0,
            )

        # Get time period boundaries from config or use defaults
        period_start = ctx.config.get("period_start")
        period_end = ctx.config.get("period_end")
        time_grain = ctx.config.get("time_grain", "monthly")

        # Convert string dates if provided
        if isinstance(period_start, str):
            period_start = date.fromisoformat(period_start)
        if isinstance(period_end, str):
            period_end = date.fromisoformat(period_end)

        # Default to 1 year period if not specified
        if not period_start:
            period_start = date(date.today().year - 1, 1, 1)
        if not period_end:
            period_end = date.today()

        total_temporal_analyses = 0
        total_topology_analyses = 0
        total_topology_drift = 0
        errors = []

        # Pre-compute which typed tables actually have the time column,
        # so we skip slice definitions from tables without it (e.g. dimension tables).
        tables_with_time_col: set[str] = set()
        for tt in typed_tables:
            col_check = select(Column).where(
                Column.table_id == tt.table_id,
                Column.column_name == time_column,
            )
            if ctx.session.execute(col_check).scalar_one_or_none():
                tables_with_time_col.add(tt.table_id)

        for slice_def in slice_definitions:
            # Skip slices from tables that don't have the time column
            if slice_def.table_id not in tables_with_time_col:
                logger.info(
                    "skipping_slice_def_no_time_column",
                    slice_def_id=slice_def.slice_id,
                    table_id=slice_def.table_id,
                    time_column=time_column,
                )
                continue

            # Get slice tables for this definition
            slice_tables_stmt = select(Table).where(
                Table.layer == "slice",
                Table.source_id == ctx.source_id,
            )
            slice_tables = (ctx.session.execute(slice_tables_stmt)).scalars().all()

            logger.info(
                "slice_tables_found",
                slice_def_id=slice_def.slice_id,
                slice_tables_count=len(slice_tables),
                slice_table_names=[t.table_name for t in slice_tables],
            )

            # Build slice info list
            # Look up slice column once (same for all tables in this definition)
            slice_column_stmt = select(Column).where(Column.column_id == slice_def.column_id)
            slice_col = (ctx.session.execute(slice_column_stmt)).scalar_one_or_none()
            if not slice_col:
                logger.warning(
                    "slice_column_not_found",
                    slice_def_id=slice_def.slice_id,
                    column_id=slice_def.column_id,
                )
                continue

            # Sanitize column name to match the slice table naming convention
            # (slice_runner uses _sanitize_name which lowercases and replaces
            # non-alphanumeric chars with underscores)
            sanitized_col_name = _sanitize_name(slice_col.column_name)

            prefix = f"slice_{sanitized_col_name}_"
            slice_infos = []
            for st in slice_tables:
                logger.debug(
                    "checking_slice_table_match",
                    slice_table=st.table_name,
                    slice_column=slice_col.column_name,
                    prefix=prefix,
                    matches=st.table_name.lower().startswith(prefix),
                )

                if st.table_name.lower().startswith(prefix):
                    # Find source table for this slice
                    source_table = next(
                        (t for t in typed_tables if t.table_id == slice_def.table_id), None
                    )
                    slice_infos.append(
                        SliceTableInfo(
                            slice_table_id=st.table_id,
                            slice_table_name=st.table_name,
                            source_table_id=slice_def.table_id,
                            source_table_name=source_table.table_name if source_table else "",
                            slice_column_name=slice_col.column_name,
                            slice_value=st.table_name[len(prefix) :],
                            row_count=st.row_count or 0,
                        )
                    )

            logger.info(
                "slice_infos_built",
                slice_def_id=slice_def.slice_id,
                slice_infos_count=len(slice_infos),
                slice_info_tables=[si.slice_table_name for si in slice_infos],
            )

            if not slice_infos:
                logger.warning(
                    "no_slice_infos_matched",
                    slice_def_id=slice_def.slice_id,
                    slice_column_id=slice_def.column_id,
                )
                continue

            # 1. Run temporal analysis on slices
            try:
                temporal_result = run_temporal_analysis_on_slices(
                    session=ctx.session,
                    duckdb_conn=ctx.duckdb_conn,
                    slice_infos=slice_infos,
                    time_column=time_column,
                    period_start=period_start,
                    period_end=period_end,
                    time_grain=time_grain,
                )
                total_temporal_analyses += temporal_result.slices_analyzed
            except Exception as e:
                errors.append(f"Temporal analysis error: {e}")

            # 2. Run topology on slices (cross-slice comparison)
            try:
                topology_result = run_topology_on_slices(
                    session=ctx.session,
                    duckdb_conn=ctx.duckdb_conn,
                    slice_infos=slice_infos,
                )
                total_topology_analyses += topology_result.slices_analyzed
            except Exception as e:
                errors.append(f"Topology analysis error: {e}")

            # 3. Run temporal topology (bottleneck distance over time)
            # Get the source table for temporal topology
            source_table = next((t for t in typed_tables if t.table_id == slice_def.table_id), None)
            if source_table:
                try:
                    topo_result = analyze_temporal_topology(
                        duck_conn=ctx.duckdb_conn,
                        table_name=source_table.table_name,
                        time_column=time_column,
                        period=time_grain.rstrip("ly"),  # "monthly" -> "month"
                    )
                    total_topology_drift += len(topo_result.topology_drifts)

                    # Persist topology results to DB
                    if topo_result.periods_analyzed > 0:
                        period_granularity = time_grain.rstrip("ly")  # "monthly" -> "month"

                        def _serialize_topology(t: PeriodTopology) -> dict[str, object]:
                            return {
                                "period_start": t.period_start,
                                "period_end": t.period_end,
                                "betti_0": t.betti_0,
                                "betti_1": t.betti_1,
                                "betti_2": t.betti_2,
                                "structural_complexity": t.structural_complexity,
                                "persistent_entropy": t.persistent_entropy,
                                "row_count": t.row_count,
                                "has_anomalies": t.has_anomalies,
                            }

                        def _serialize_drift(d: TopologyDrift) -> dict[str, object]:
                            return {
                                "period_from": d.period_from,
                                "period_to": d.period_to,
                                "metric": d.metric,
                                "value_from": d.value_from,
                                "value_to": d.value_to,
                                "change_pct": d.change_pct,
                                "bottleneck_distance": d.bottleneck_distance,
                                "is_significant": d.is_significant,
                            }

                        topo_record = TemporalTopologyAnalysis(
                            run_id=None,
                            slice_table_name=source_table.table_name,
                            time_column=time_column,
                            period_granularity=period_granularity,
                            periods_analyzed=topo_result.periods_analyzed,
                            avg_complexity=topo_result.avg_complexity,
                            complexity_variance=topo_result.complexity_variance,
                            trend_direction=topo_result.trend_direction,
                            num_drifts_detected=len(topo_result.topology_drifts),
                            num_anomaly_periods=len(topo_result.structural_anomaly_periods),
                            period_topologies_json=[
                                _serialize_topology(t) for t in topo_result.period_topologies
                            ],
                            topology_drifts_json=[
                                _serialize_drift(d) for d in topo_result.topology_drifts
                            ],
                            anomaly_periods_json=topo_result.structural_anomaly_periods,
                        )
                        ctx.session.add(topo_record)
                except Exception as e:
                    errors.append(f"Temporal topology error: {e}")

        outputs = {
            "temporal_analyses": total_temporal_analyses,
            "topology_analyses": total_topology_analyses,
            "topology_drifts_detected": total_topology_drift,
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
            records_created=total_temporal_analyses + total_topology_analyses,
        )
