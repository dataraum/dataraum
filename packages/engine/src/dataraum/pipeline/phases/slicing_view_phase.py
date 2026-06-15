"""Slicing view phase implementation.

Creates a DuckDB view per fact table that projects from the enriched view,
keeping all fact table columns but only the dimension columns that correspond
to SliceDefinitions for that table.

The resulting view is named "slicing_{fact_duckdb_path}" (source-qualified, DAT-356)
and contains:
- All columns from the fact table
- Only the dimension columns (from joined tables) that are slice dimensions

This gives downstream quality analysis a focused view over the slice-relevant
columns without all the noise from non-slice enrichment columns.
"""

from __future__ import annotations

from types import ModuleType
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from sqlalchemy import delete, select

from dataraum.analysis.semantic.db_models import TableEntity
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.slicing.naming import slicing_view_name
from dataraum.analysis.typing.db_models import MaterializationRecipe
from dataraum.analysis.typing.recipe import store_recipe
from dataraum.analysis.views.db_models import EnrichedView, SlicingView
from dataraum.core.duckdb_naming import schema_for_layer
from dataraum.core.logging import get_logger
from dataraum.core.sql_normalize import sql_equivalent
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases._column_cleanup import delete_column_dependents
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.server.storage import LAKE_CATALOG_ALIAS
from dataraum.storage import Column, Table

if TYPE_CHECKING:
    pass

logger = get_logger(__name__)


def _lake_fqn(layer: str, bare: str) -> str:
    """Fully-qualified DuckDB name ``catalog.schema."bare"`` for a lake-layer artifact.

    Mirrors the enriched_views helper. Slicing views, the enriched views they
    project from, and the typed fact all resolve to the ``typed`` schema
    (``schema_for_layer``), so one helper qualifies every reference the recipe
    DDL needs to rebuild standalone.
    """
    return f'{LAKE_CATALOG_ALIAS}.{schema_for_layer(layer)}."{bare}"'


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
    def db_models(self) -> list[ModuleType]:
        from dataraum.analysis.views import db_models

        return [db_models]

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip only on genuine preconditions — never because the session already ran.

        Mirrors ``enriched_views`` (DAT-408/415): there is NO "views already exist
        → skip" bail. A versioned re-run mints a fresh ``run_id`` and re-derives the
        view definitions, reconciling the latest-only ``SlicingView`` in place and
        re-stamping the recipe only when the DDL changed (sqlglot-gated). The phase
        is idempotent on its ``CREATE OR REPLACE`` + reconcile, so re-running is safe.
        """
        # Source-free: the session's selected typed tables (DAT-403).
        typed_tables = self._typed_tables(ctx)

        if not typed_tables:
            return "No typed tables found"

        table_ids = [t.table_id for t in typed_tables]

        # Fact tables that have slice definitions. Run-scoped to ``ctx.run_id``:
        # ``TableEntity`` is run-versioned and coexists across runs (DAT-408/413),
        # so an unscoped fact read would see every prior run's fact row for the
        # same table — exactly the leak ``enriched_views`` guards against.
        sliced_fact_stmt = (
            select(SliceDefinition.table_id.distinct())
            .join(TableEntity, TableEntity.table_id == SliceDefinition.table_id)
            .where(
                SliceDefinition.table_id.in_(table_ids),
                # Run-versioned (DAT-448): only THIS run's definitions count.
                SliceDefinition.run_id == ctx.run_id,
                TableEntity.is_fact_table.is_(True),
            )
        )
        if ctx.run_id is not None:
            sliced_fact_stmt = sliced_fact_stmt.where(TableEntity.run_id == ctx.run_id)
        sliced_fact_table_ids = set(ctx.session.execute(sliced_fact_stmt).scalars().all())

        if not sliced_fact_table_ids:
            return "No slice definitions found for fact tables"

        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Create slicing views for tables with slice definitions."""
        # Source-free: the session's selected typed tables (DAT-403).
        typed_tables = self._typed_tables(ctx)

        if not typed_tables:
            return PhaseResult.failed("No typed tables found. Run typing phase first.")

        table_ids = [t.table_id for t in typed_tables]
        tables_by_id = {t.table_id: t for t in typed_tables}

        # Load THIS run's slice definitions for these tables (run-versioned, DAT-448)
        slice_stmt = select(SliceDefinition).where(
            SliceDefinition.table_id.in_(table_ids),
            SliceDefinition.run_id == ctx.run_id,
        )
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

        # Restrict to actual fact tables only — slicing views are not created for
        # dimension tables. Run-scoped to ``ctx.run_id`` (DAT-408/413): read only
        # this run's fact rows, never a prior run's coexisting classification.
        fact_entity_stmt = select(TableEntity).where(
            TableEntity.table_id.in_(list(slice_defs_by_table.keys())),
            TableEntity.is_fact_table.is_(True),
        )
        if ctx.run_id is not None:
            fact_entity_stmt = fact_entity_stmt.where(TableEntity.run_id == ctx.run_id)
        fact_entities = ctx.session.execute(fact_entity_stmt).scalars().all()
        fact_table_id_set = {e.table_id for e in fact_entities}
        # The agent-named time axis per fact (DAT-491): when it names an enriched
        # "fk__col" column (header date), the projection must keep it so the
        # slice tables cut from this view carry their time column.
        time_col_by_fact = {e.table_id: e.time_column for e in fact_entities}

        # Process every fact table with slice definitions — no "already exists"
        # bail (DAT-415): the latest-only SlicingView is reconciled in place and
        # the recipe re-stamped only on a DDL change, so a re-run re-derives.
        fact_table_ids = list(fact_table_id_set)
        if not fact_table_ids:
            return PhaseResult.success(
                outputs={"slicing_views": 0, "message": "No fact tables with slice definitions"},
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

            # Build the slicing view SQL (fully-qualified; source_fqn is the
            # enriched view / typed fact it projects from — the recipe depends_on).
            view_sql, slice_dim_cols, slice_def_ids, source_fqn = self._build_slicing_view_sql(
                fact_table=fact_table,
                slice_defs=slice_defs,
                enriched_view=enriched_view,
                columns_by_id=columns_by_id,
                fact_columns=fact_columns_by_table.get(fact_table_id, []),
                named_time_column=time_col_by_fact.get(fact_table_id),
            )

            view_name = slicing_view_name(fact_table.duckdb_path or "")
            view_fqn = _lake_fqn("slicing_view", view_name)

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

            # Version the view DDL on the DAT-414 recipe substrate (layer="slicing"),
            # sqlglot-gated exactly like enriched_views: only stamp a NEW
            # run-versioned recipe when the canonical SQL differs from the fact's
            # latest stored slicing recipe — an unchanged re-run adds no redundant
            # version, and a same-run retry sees its own recipe as equivalent and
            # no-ops. depends_on is the enriched view (or typed fact) it projects
            # from, so a multi-layer rebuild orders enriched before slicing.
            latest_recipe = ctx.session.execute(
                select(MaterializationRecipe)
                .where(
                    MaterializationRecipe.table_id == fact_table_id,
                    MaterializationRecipe.layer == "slicing",
                )
                .order_by(MaterializationRecipe.created_at.desc())
                .limit(1)
            ).scalar_one_or_none()
            if latest_recipe is None or not sql_equivalent(latest_recipe.ddl, view_sql):
                store_recipe(
                    ctx.session,
                    table_id=fact_table_id,
                    layer="slicing",
                    run_id=ctx.require_run_id(),
                    target_fqn=view_fqn,
                    ddl=view_sql,
                    depends_on=[source_fqn],
                )

            # The view definition is latest-only substrate (one row per fact,
            # DAT-415): reconcile-in-place, stamping the run that materialized it.
            # The recipe (above) carries the version history; the unique constraint
            # ``uq_slicing_view_fact_table`` is the structural backstop so an errant
            # second insert fails loudly here instead of as ``MultipleResultsFound``
            # downstream.
            slicing_view = ctx.session.execute(
                select(SlicingView).where(SlicingView.fact_table_id == fact_table_id)
            ).scalar_one_or_none()
            if slicing_view is None:
                slicing_view = SlicingView(
                    fact_table_id=fact_table_id,
                )
                ctx.session.add(slicing_view)
            slicing_view.run_id = ctx.require_run_id()
            slicing_view.view_name = view_name
            slicing_view.slice_definition_ids = slice_def_ids
            slicing_view.slice_columns = slice_dim_cols
            slicing_view.is_grain_verified = is_grain_verified

            # Register the slicing view as a Table(layer="slicing_view") so that
            # downstream phases can look up its column schema via standard metadata
            # queries instead of reading from DuckDB or guessing from slice tables.
            # Latest-only substrate (DAT-415): reconcile on the
            # ``(source, view_name, "slicing_view")`` key — reused across runs with
            # its prior columns replaced — so a re-run never mints a duplicate the
            # ``scalar_one_or_none`` lookups in ``slice_runner``/``profiling`` would
            # then trip on.
            sv_table = ctx.session.execute(
                select(Table).where(
                    Table.source_id == fact_table.source_id,
                    Table.table_name == view_name,
                    Table.layer == "slicing_view",
                )
            ).scalar_one_or_none()
            if sv_table is None:
                sv_table = Table(
                    table_id=str(uuid4()),
                    source_id=fact_table.source_id,
                    table_name=view_name,
                    layer="slicing_view",
                    duckdb_path=view_name,
                    row_count=fact_table.row_count,
                )
                ctx.session.add(sv_table)
                ctx.session.flush()
            else:
                sv_table.duckdb_path = view_name
                sv_table.row_count = fact_table.row_count
                # FK children of ``columns`` no longer cascade (DAT-506): delete
                # the prior run's dependents explicitly before the columns go, or
                # the ``delete(Column)`` FK-violates on stale child rows.
                old_col_ids = [
                    cid
                    for (cid,) in ctx.session.execute(
                        select(Column.column_id).where(Column.table_id == sv_table.table_id)
                    ).all()
                ]
                delete_column_dependents(ctx, old_col_ids)
                ctx.session.execute(delete(Column).where(Column.table_id == sv_table.table_id))
                ctx.session.flush()

            duckdb_cols = ctx.duckdb_conn.execute(f'DESCRIBE "{view_name}"').fetchall()
            if not duckdb_cols:
                logger.error(
                    "slicing_view_describe_empty",
                    view_name=view_name,
                    fact_table=fact_table.table_name,
                )
            # Explicit table_id + session.add per Column.
            # Relationship append alone is unreliable under free-threading:
            # cascade may not populate session.new before commit.
            for pos, row in enumerate(duckdb_cols):
                col = Column(
                    column_id=str(uuid4()),
                    table_id=sv_table.table_id,
                    column_name=row[0],
                    column_position=pos,
                    raw_type=row[1],
                    resolved_type=row[1],
                )
                sv_table.columns.append(col)
                ctx.session.add(col)

            # Diagnostic: verify columns are tracked by the session
            sv_pending = sum(
                1
                for obj in ctx.session.new
                if isinstance(obj, Column) and getattr(obj, "table_id", None) == sv_table.table_id
            )
            if sv_pending != len(duckdb_cols):
                logger.error(
                    "slicing_view_column_mismatch",
                    view_name=view_name,
                    describe_count=len(duckdb_cols),
                    session_pending=sv_pending,
                )

            # Rewrite sql_templates so they reference the slicing view instead of
            # the typed table or enriched view the agent originally used.
            # The agent picks enriched view when available, typed path otherwise.
            #
            # Idempotent under run_id (DAT-403): a begin_session re-run mints a
            # fresh run_id but ``SliceDefinition`` is not run-versioned, so the
            # template a prior run already redirected to the slicing view must not
            # be re-patched. Skip a template that already targets this view —
            # ``FROM "{view_name}"`` — and rewrite only the un-redirected sources.
            from_targets = set()
            if fact_table.duckdb_path:
                from_targets.add(fact_table.duckdb_path)
            if enriched_view and enriched_view.view_name:
                from_targets.add(enriched_view.view_name)

            view_from = f'FROM "{view_name}"'
            for sd in slice_defs:
                if not sd.sql_template or view_from in sd.sql_template:
                    continue
                for target in from_targets:
                    sd.sql_template = sd.sql_template.replace(f"FROM {target}", view_from)

            views_created += 1

            logger.info(
                "slicing_view_created",
                view_name=view_name,
                fact_table=fact_table.table_name,
                slice_dim_columns=len(slice_dim_cols),
            )

        # Atomic-publish visibility (DAT-506): force the DuckLake snapshot after
        # this run's COMPLETE set of slicing-view DDL is materialized, so the
        # cockpit's READ_ONLY ATTACH sees the whole batch at once.
        ctx.duckdb_conn.execute("CHECKPOINT")

        return PhaseResult.success(
            outputs={"slicing_views": views_created},
            records_processed=len(fact_table_ids),
            records_created=views_created,
            summary=f"{views_created} slicing views created",
        )

    def _build_slicing_view_sql(
        self,
        fact_table: Table,
        slice_defs: list[SliceDefinition],
        enriched_view: EnrichedView | None,
        columns_by_id: dict[str, Column],
        fact_columns: list[Column],
        named_time_column: str | None = None,
    ) -> tuple[str, list[str], list[str], str]:
        """Build SQL for the slicing view.

        Fully-qualified (DAT-415): the ``CREATE`` target and its source resolve to
        ``lake.typed.<name>`` so the DDL stored as a ``MaterializationRecipe``
        re-executes standalone during a multi-layer rebuild, and the returned
        ``source_fqn`` becomes the recipe's ``depends_on`` (the enriched view it
        projects from — ordering enriched before slicing in the rebuild).

        Returns:
            Tuple of (view_sql, slice_dimension_columns, slice_definition_ids, source_fqn)
        """
        view_name = slicing_view_name(fact_table.duckdb_path or "")
        view_fqn = _lake_fqn("slicing_view", view_name)
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
        # The agent-named time axis rides along when it is an enriched column
        # (DAT-491) — own-column axes are already in fact_col_names.
        kept_time_cols = (
            [named_time_column]
            if named_time_column
            and named_time_column in all_dim_cols
            and named_time_column not in slice_dim_cols
            else []
        )

        if enriched_view and (fact_col_names or slice_dim_cols or kept_time_cols):
            # Project from enriched view: fact cols + slice dim cols + the named
            # time column. Read the view's stored name rather than reconstructing
            # it — the enriched view is now collision-named off the fact's
            # duckdb_path (``enriched_{source}__{table}``, DAT-415).
            select_parts = [f'"{c}"' for c in [*fact_col_names, *slice_dim_cols, *kept_time_cols]]
            source_fqn = _lake_fqn("enriched", enriched_view.view_name)
            sql = (
                f"CREATE OR REPLACE VIEW {view_fqn} AS\n"
                f"SELECT {', '.join(select_parts)}\n"
                f"FROM {source_fqn}"
            )
        else:
            # No enriched view or no columns to enumerate — fall back to fact table directly
            source_fqn = _lake_fqn("typed", fact_table.duckdb_path or "")
            sql = f"CREATE OR REPLACE VIEW {view_fqn} AS\nSELECT * FROM {source_fqn}"

        return sql, slice_dim_cols, slice_def_ids, source_fqn

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
            if actual_count != expected_count:
                logger.warning(
                    "slicing_view_grain_mismatch",
                    view_name=view_name,
                    expected_count=expected_count,
                    actual_count=actual_count,
                )
            return actual_count == expected_count
        except Exception as exc:
            logger.warning(
                "slicing_view_grain_query_failed",
                view_name=view_name,
                error=str(exc),
            )
            return False
