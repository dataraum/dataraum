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
from dataraum.core.logging import get_logger
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.storage import Column, Table

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
        bottleneck_threshold = ctx.config.get("bottleneck_threshold", 0.5)

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

        for slice_def in slice_definitions:
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
            slice_infos = []
            for st in slice_tables:
                # Check if this slice table is related to the slice definition
                # by matching the naming convention
                slice_column_stmt = select(Column).where(Column.column_id == slice_def.column_id)
                slice_col = (ctx.session.execute(slice_column_stmt)).scalar_one_or_none()

                logger.debug(
                    "checking_slice_table_match",
                    slice_table=st.table_name,
                    slice_column=slice_col.column_name if slice_col else None,
                    matches=slice_col and slice_col.column_name.lower() in st.table_name.lower(),
                )

                if slice_col and slice_col.column_name.lower() in st.table_name.lower():
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
                            slice_value=st.table_name.replace(
                                f"slice_{slice_col.column_name.lower()}_", ""
                            ),
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
                        bottleneck_threshold=bottleneck_threshold,
                    )
                    total_topology_drift += len(topo_result.topology_drifts)
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
