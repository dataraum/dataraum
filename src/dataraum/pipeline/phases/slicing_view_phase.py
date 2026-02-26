"""Slicing view phase implementation.

Creates a DuckDB view per fact table that projects from the enriched view,
keeping all fact table columns but only the dimension columns that correspond
to SliceDefinitions for that table.

The resulting view is named "slicing_{fact_table_name}" and contains:
- All columns from the fact table
- Only the dimension columns (from joined tables) that are slice dimensions

This gives downstream quality analysis a focused view over the slice-relevant
columns without all the noise from non-slice enrichment columns.
"""

from __future__ import annotations

from types import ModuleType
from typing import Any

from sqlalchemy import select

from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.views.db_models import EnrichedView, SlicingView
from dataraum.core.logging import get_logger
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.storage import Column, Table

logger = get_logger(__name__)


@analysis_phase
class SlicingViewPhase(BasePhase):
    """Create slicing views projecting enriched views to slice-relevant columns.

    For each fact table that has SliceDefinitions, creates a DuckDB view that
    keeps all fact table columns but only the dimension columns that are
    slice dimensions. Builds on top of the enriched view (no new JOINs).
    """

    @property
    def name(self) -> str:
        return "slicing_view"

    @property
    def description(self) -> str:
        return "Create slicing views narrowed to slice-relevant dimension columns"

    @property
    def dependencies(self) -> list[str]:
        return ["slicing"]

    @property
    def outputs(self) -> list[str]:
        return ["slicing_views"]

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.analysis.views import db_models

        return [db_models]

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if slicing views already exist for all tables with slice definitions."""
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        typed_tables = ctx.session.execute(stmt).scalars().all()

        if not typed_tables:
            return "No typed tables found"

        table_ids = [t.table_id for t in typed_tables]

        # Tables that have slice definitions
        sliced_stmt = select(SliceDefinition.table_id.distinct()).where(
            SliceDefinition.table_id.in_(table_ids)
        )
        sliced_table_ids = set(ctx.session.execute(sliced_stmt).scalars().all())

        if not sliced_table_ids:
            return "No slice definitions found"

        # Tables that already have slicing views
        view_stmt = select(SlicingView.fact_table_id.distinct()).where(
            SlicingView.fact_table_id.in_(list(sliced_table_ids))
        )
        existing_view_table_ids = set(ctx.session.execute(view_stmt).scalars().all())

        if existing_view_table_ids >= sliced_table_ids:
            return "Slicing views already exist for all tables with slice definitions"

        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Create slicing views for tables with slice definitions."""
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        typed_tables = ctx.session.execute(stmt).scalars().all()

        if not typed_tables:
            return PhaseResult.failed("No typed tables found. Run typing phase first.")

        table_ids = [t.table_id for t in typed_tables]
        tables_by_id = {t.table_id: t for t in typed_tables}

        # Load all slice definitions for these tables
        slice_stmt = select(SliceDefinition).where(SliceDefinition.table_id.in_(table_ids))
        all_slice_defs = ctx.session.execute(slice_stmt).scalars().all()

        if not all_slice_defs:
            return PhaseResult.success(
                outputs={"slicing_views": 0, "message": "No slice definitions found"},
                records_processed=0,
                records_created=0,
            )

        # Group slice defs by table_id
        slice_defs_by_table: dict[str, list[SliceDefinition]] = {}
        for sd in all_slice_defs:
            slice_defs_by_table.setdefault(sd.table_id, []).append(sd)

        # Check which tables already have slicing views
        existing_stmt = select(SlicingView.fact_table_id).where(
            SlicingView.fact_table_id.in_(list(slice_defs_by_table.keys()))
        )
        existing_view_table_ids = set(ctx.session.execute(existing_stmt).scalars().all())

        # Load all columns for fact tables that need processing
        fact_table_ids = [tid for tid in slice_defs_by_table if tid not in existing_view_table_ids]
        if not fact_table_ids:
            return PhaseResult.success(
                outputs={"slicing_views": 0, "message": "All slicing views already exist"},
                records_processed=0,
                records_created=0,
            )

        cols_stmt = select(Column).where(Column.table_id.in_(fact_table_ids + table_ids))
        all_columns = ctx.session.execute(cols_stmt).scalars().all()
        columns_by_id = {col.column_id: col for col in all_columns}
        fact_columns_by_table: dict[str, list[Column]] = {}
        for col in all_columns:
            if col.table_id in fact_table_ids:
                fact_columns_by_table.setdefault(col.table_id, []).append(col)

        # Load enriched views for these fact tables
        ev_stmt = select(EnrichedView).where(
            EnrichedView.fact_table_id.in_(fact_table_ids),
            EnrichedView.is_grain_verified.is_(True),
        )
        enriched_views_by_table = {
            ev.fact_table_id: ev for ev in ctx.session.execute(ev_stmt).scalars().all()
        }

        views_created = 0

        for fact_table_id in fact_table_ids:
            fact_table = tables_by_id.get(fact_table_id)
            if not fact_table or not fact_table.duckdb_path:
                logger.warning("fact_table_missing", table_id=fact_table_id)
                continue

            slice_defs = slice_defs_by_table[fact_table_id]
            enriched_view = enriched_views_by_table.get(fact_table_id)

            # Build the slicing view SQL
            view_sql, slice_dim_cols, slice_def_ids = self._build_slicing_view_sql(
                fact_table=fact_table,
                slice_defs=slice_defs,
                enriched_view=enriched_view,
                tables_by_id=tables_by_id,
                columns_by_id=columns_by_id,
                fact_columns=fact_columns_by_table.get(fact_table_id, []),
            )

            view_name = f"slicing_{fact_table.table_name}"

            # Execute view creation in DuckDB
            try:
                ctx.duckdb_conn.execute(view_sql)
            except Exception as e:
                logger.warning(
                    "slicing_view_creation_failed",
                    view_name=view_name,
                    error=str(e),
                )
                continue

            # Verify grain preservation
            is_grain_verified = self._verify_grain(
                ctx.duckdb_conn,
                view_name=view_name,
                expected_count=fact_table.row_count,
            )

            if not is_grain_verified:
                logger.warning(
                    "slicing_view_grain_failed",
                    view_name=view_name,
                    expected_count=fact_table.row_count,
                )
                try:
                    ctx.duckdb_conn.execute(f'DROP VIEW IF EXISTS "{view_name}"')
                except Exception:
                    pass
                continue

            # Store record
            slicing_view = SlicingView(
                fact_table_id=fact_table_id,
                view_name=view_name,
                view_sql=view_sql,
                slice_definition_ids=slice_def_ids,
                slice_columns=slice_dim_cols,
                is_grain_verified=is_grain_verified,
            )
            ctx.session.add(slicing_view)
            views_created += 1

            logger.info(
                "slicing_view_created",
                view_name=view_name,
                fact_table=fact_table.table_name,
                slice_dim_columns=len(slice_dim_cols),
            )

        return PhaseResult.success(
            outputs={"slicing_views": views_created},
            records_processed=len(fact_table_ids),
            records_created=views_created,
        )

    def _build_slicing_view_sql(
        self,
        fact_table: Table,
        slice_defs: list[SliceDefinition],
        enriched_view: EnrichedView | None,
        tables_by_id: dict[str, Table],
        columns_by_id: dict[str, Column],
        fact_columns: list[Column],
    ) -> tuple[str, list[str], list[str]]:
        """Build SQL for the slicing view.

        Returns:
            Tuple of (view_sql, slice_dimension_columns, slice_definition_ids)
        """
        view_name = f"slicing_{fact_table.table_name}"
        slice_def_ids = [sd.slice_id for sd in slice_defs]

        # Determine which dimension columns from the enriched view are slice-relevant
        # A slice column is slice-relevant if it's from a dimension table and appears
        # in the enriched_view's dimension_columns as "{dim_table}__{col_name}"
        enriched_dim_cols = set(enriched_view.dimension_columns or []) if enriched_view else set()

        slice_dim_cols: list[str] = []
        seen: set[str] = set()
        for sd in slice_defs:
            col = columns_by_id.get(sd.column_id)
            if col is None:
                continue
            # Native fact table columns are already in the explicit fact column list
            if col.table_id == fact_table.table_id:
                continue
            # Dimension column — check if it's in the enriched view
            dim_table = tables_by_id.get(col.table_id)
            if dim_table is None:
                continue
            enriched_col_name = f"{dim_table.table_name}__{col.column_name}"
            if enriched_col_name in enriched_dim_cols and enriched_col_name not in seen:
                slice_dim_cols.append(enriched_col_name)
                seen.add(enriched_col_name)

        # Build explicit SELECT — never SELECT * to avoid pulling all enriched columns
        fact_col_names = [col.column_name for col in fact_columns]

        if enriched_view and (fact_col_names or slice_dim_cols):
            # Project from enriched view: fact cols + slice dim cols only
            select_parts = [f'"{c}"' for c in fact_col_names] + [f'"{c}"' for c in slice_dim_cols]
            source = f'"enriched_{fact_table.table_name}"'
            sql = (
                f'CREATE OR REPLACE VIEW "{view_name}" AS\n'
                f"SELECT {', '.join(select_parts)}\n"
                f"FROM {source}"
            )
        else:
            # No enriched view or no columns to enumerate — fall back to fact table directly
            sql = (
                f'CREATE OR REPLACE VIEW "{view_name}" AS\nSELECT * FROM "{fact_table.duckdb_path}"'
            )

        return sql, slice_dim_cols, slice_def_ids

    @staticmethod
    def _verify_grain(
        duckdb_conn: Any,
        view_name: str,
        expected_count: int | None,
    ) -> bool:
        """Verify that the view preserves the fact table grain."""
        if expected_count is None:
            return True

        try:
            result = duckdb_conn.execute(f'SELECT COUNT(*) FROM "{view_name}"').fetchone()
            actual_count = result[0] if result else 0
            return actual_count == expected_count
        except Exception:
            return False
