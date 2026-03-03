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
from uuid import uuid4

from sqlalchemy import select

from dataraum.analysis.semantic.db_models import TableEntity
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

        # Fact tables that have slice definitions
        sliced_fact_stmt = (
            select(SliceDefinition.table_id.distinct())
            .join(TableEntity, TableEntity.table_id == SliceDefinition.table_id)
            .where(
                SliceDefinition.table_id.in_(table_ids),
                TableEntity.is_fact_table.is_(True),
            )
        )
        sliced_fact_table_ids = set(ctx.session.execute(sliced_fact_stmt).scalars().all())

        if not sliced_fact_table_ids:
            return "No slice definitions found for fact tables"

        # Fact tables that already have slicing views
        view_stmt = select(SlicingView.fact_table_id.distinct()).where(
            SlicingView.fact_table_id.in_(list(sliced_fact_table_ids))
        )
        existing_view_table_ids = set(ctx.session.execute(view_stmt).scalars().all())

        if existing_view_table_ids >= sliced_fact_table_ids:
            return "Slicing views already exist for all fact tables with slice definitions"

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

        # Restrict to actual fact tables only — slicing views are not created for dimension tables
        fact_entity_stmt = select(TableEntity.table_id).where(
            TableEntity.table_id.in_(list(slice_defs_by_table.keys())),
            TableEntity.is_fact_table.is_(True),
        )
        fact_table_id_set = set(ctx.session.execute(fact_entity_stmt).scalars().all())

        # Check which tables already have slicing views
        existing_stmt = select(SlicingView.fact_table_id).where(
            SlicingView.fact_table_id.in_(list(fact_table_id_set))
        )
        existing_view_table_ids = set(ctx.session.execute(existing_stmt).scalars().all())

        # Load all columns for fact tables that need processing
        fact_table_ids = [tid for tid in fact_table_id_set if tid not in existing_view_table_ids]
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

            enriched_view = enriched_views_by_table.get(fact_table_id)

            # Get dimension table IDs from this fact table's enriched view
            dim_table_ids = set()
            if enriched_view and enriched_view.dimension_table_ids:
                dim_table_ids = set(enriched_view.dimension_table_ids)

            # Filter to slice defs relevant to this fact table
            slice_defs = [
                sd
                for sd in all_slice_defs
                if sd.table_id == fact_table_id or sd.table_id in dim_table_ids
            ]

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

            # Store SlicingView record
            slicing_view = SlicingView(
                fact_table_id=fact_table_id,
                view_name=view_name,
                view_sql=view_sql,
                slice_definition_ids=slice_def_ids,
                slice_columns=slice_dim_cols,
                is_grain_verified=is_grain_verified,
            )
            ctx.session.add(slicing_view)

            # Register the slicing view as a Table(layer="slicing_view") so that
            # downstream phases can look up its column schema via standard metadata
            # queries instead of reading from DuckDB or guessing from slice tables.
            sv_table = Table(
                table_id=str(uuid4()),
                source_id=fact_table.source_id,
                table_name=view_name,
                layer="slicing_view",
                duckdb_path=view_name,
                row_count=fact_table.row_count,
            )
            ctx.session.add(sv_table)

            duckdb_cols = ctx.duckdb_conn.execute(f'DESCRIBE "{view_name}"').fetchall()
            for pos, row in enumerate(duckdb_cols):
                ctx.session.add(
                    Column(
                        column_id=str(uuid4()),
                        table_id=sv_table.table_id,
                        column_name=row[0],
                        column_position=pos,
                        raw_type=row[1],
                        resolved_type=row[1],
                    )
                )

            # Rewrite sql_templates so they reference the slicing view instead of
            # the typed table or enriched view the agent originally used.
            # The agent picks enriched view when available, typed path otherwise.
            from_targets = set()
            if fact_table.duckdb_path:
                from_targets.add(fact_table.duckdb_path)
            if enriched_view and enriched_view.view_name:
                from_targets.add(enriched_view.view_name)

            for sd in slice_defs:
                if not sd.sql_template:
                    continue
                for target in from_targets:
                    sd.sql_template = sd.sql_template.replace(
                        f"FROM {target}", f'FROM "{view_name}"'
                    )

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

        # Resolve column names referenced by slice definitions.
        # Prefer sd.column_name (set by slicing_phase, stores the actual LLM-recommended name
        # including enriched dim cols like "kontonummer_des_gegenkontos__land"). Fall back to
        # resolving via columns_by_id for older records without column_name.
        slice_col_names: set[str] = set()
        for sd in slice_defs:
            if sd.column_name:
                slice_col_names.add(sd.column_name)
            else:
                col = columns_by_id.get(sd.column_id)
                if col:
                    slice_col_names.add(col.column_name)

        # Filter enriched dimension columns to only those that are slice dimensions:
        #   - full name match: LLM directly recommended this enriched dim column, OR
        #   - FK prefix match: LLM recommended the fact-table FK column (prefix before "__"),
        #     so include all dim cols from that join for downstream context.
        all_dim_cols: list[str] = (
            list(enriched_view.dimension_columns or []) if enriched_view else []
        )
        slice_dim_cols: list[str] = []
        for dim_col in all_dim_cols:
            if dim_col in slice_col_names:
                slice_dim_cols.append(dim_col)
            elif "__" in dim_col and dim_col.split("__")[0] in slice_col_names:
                slice_dim_cols.append(dim_col)

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
