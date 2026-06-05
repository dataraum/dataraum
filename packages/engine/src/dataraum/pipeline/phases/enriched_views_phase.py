"""Enriched views phase implementation.

Creates grain-preserving DuckDB views that LEFT JOIN fact tables with their
confirmed dimension tables. Uses LLM to identify which relationships add
valuable analytical dimensions (geographic, category, reference data).

Only uses relationships that are:
- Confirmed by LLM (detection_method = "llm")
- Cardinality many_to_one or one_to_one (grain-preserving)
- Confidence >= 0.7
- Not flagged as introducing duplicates

Post-creation: verifies row count matches fact table. Drops view if grain violated.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, datetime
from types import ModuleType
from typing import Any
from uuid import uuid4

from sqlalchemy import delete, select

from dataraum.analysis.relationships.db_models import Relationship
from dataraum.analysis.relationships.utils import load_defined_relationships
from dataraum.analysis.semantic.db_models import SemanticAnnotation, TableEntity
from dataraum.analysis.statistics.db_models import StatisticalProfile
from dataraum.analysis.statistics.profiler import _profile_column_stats_parallel
from dataraum.analysis.typing.db_models import MaterializationRecipe
from dataraum.analysis.typing.recipe import store_recipe
from dataraum.analysis.views.builder import DimensionJoin, build_enriched_view_sql
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.analysis.views.enrichment_agent import EnrichmentAgent
from dataraum.analysis.views.enrichment_models import EnrichmentAnalysisResult
from dataraum.core.duckdb_naming import schema_for_layer
from dataraum.core.logging import get_logger
from dataraum.core.sql_normalize import sql_equivalent
from dataraum.llm import PromptRenderer, create_provider, load_llm_config
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.server.storage import LAKE_CATALOG_ALIAS
from dataraum.storage import Column, Table

logger = get_logger(__name__)

_MIN_CONFIDENCE = 0.7


def _lake_fqn(layer: str, bare: str) -> str:
    """Fully-qualified DuckDB name ``catalog.schema."bare"`` for a lake-layer artifact.

    Enriched views resolve to the ``typed`` schema (``schema_for_layer`` default),
    so the view, its fact, and its dimensions all qualify through one helper.
    """
    return f'{LAKE_CATALOG_ALIAS}.{schema_for_layer(layer)}."{bare}"'


@analysis_phase
class EnrichedViewsPhase(BasePhase):
    """Create enriched DuckDB views from semantic output.

    For each fact table, creates a view that LEFT JOINs qualifying
    dimension tables. Uses LLM to identify which confirmed relationships
    add valuable analytical dimensions (geographic, category, reference).

    This materializes semantic relationships as queryable views for
    downstream phases (slicing, correlations).
    """

    @property
    def name(self) -> str:
        return "enriched_views"

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.analysis.views import db_models

        return [db_models]

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip only when the session's selection has no fact table to enrich.

        Source-free (feedback-source-dies-at-addsource): begin_session scopes by
        ``ctx.table_ids`` — the session's selected typed tables, which may span
        sources — never ``source_id``. Structural early-out only (mirrors typing's
        DAT-413 re-seam): there is NO "views already exist → skip" bail — a re-run
        mints a fresh ``run_id`` and re-derives the view definitions under it, and
        ``_run`` is idempotent on the ``run_id`` grain.

        Run-scoped: ``TableEntity`` is run-versioned and coexists across runs
        (DAT-408/413), so the fact lookup filters to ``ctx.run_id`` — same as
        ``_run`` and ``load_defined_relationships`` below. An unscoped read would
        see every prior run's fact row for the same table.
        """
        if not ctx.table_ids:
            return "No tables in session selection"

        fact_stmt = select(TableEntity.table_id).where(
            TableEntity.table_id.in_(ctx.table_ids),
            TableEntity.is_fact_table.is_(True),
        )
        if ctx.run_id is not None:
            fact_stmt = fact_stmt.where(TableEntity.run_id == ctx.run_id)
        if ctx.session.execute(fact_stmt).first() is None:
            return "No fact tables identified"

        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Create enriched views for the session's fact tables using LLM recommendations."""
        # Source-free: scope to the session's selected typed tables (DAT-415).
        if not ctx.table_ids:
            return PhaseResult.failed("No tables in session selection")
        stmt = select(Table).where(Table.table_id.in_(ctx.table_ids), Table.layer == "typed")
        typed_tables = ctx.session.execute(stmt).scalars().all()

        if not typed_tables:
            return PhaseResult.failed("No typed tables found")

        table_ids = [t.table_id for t in typed_tables]
        tables_by_id = {t.table_id: t for t in typed_tables}
        tables_by_name = {t.table_name: t for t in typed_tables}

        # Find this run's fact tables from entity detections. Run-scoped to
        # ``ctx.run_id`` — exactly like the relationship query below and
        # ``load_defined_relationships``. ``TableEntity`` is run-versioned and
        # coexists across runs (DAT-408/413: delete-by-run_id then insert), so an
        # unscoped read iterates every prior run's fact row for the same table and
        # drives a redundant loop iteration per stale row — which, with the
        # production session's ``autoflush=False``, inserts a duplicate
        # ``EnrichedView`` per stale row and breaks the latest-only,
        # one-row-per-fact contract that ``dimension_coverage`` reads via
        # ``scalar_one_or_none``.
        fact_stmt = select(TableEntity).where(
            TableEntity.table_id.in_(table_ids),
            TableEntity.is_fact_table.is_(True),
        )
        if ctx.run_id is not None:
            fact_stmt = fact_stmt.where(TableEntity.run_id == ctx.run_id)
        fact_entities = ctx.session.execute(fact_stmt).scalars().all()

        if not fact_entities:
            return PhaseResult.success(
                outputs={"enriched_views": 0, "message": "No fact tables found"},
                records_processed=0,
                records_created=0,
            )

        # The session's defined relationships (not candidate) touching these tables.
        all_relationships = load_defined_relationships(
            ctx.session,
            table_ids,
            run_id=ctx.run_id,
            both_tables=False,
            min_confidence=_MIN_CONFIDENCE,
        )

        # Build column lookups
        cols_stmt = select(Column).where(Column.table_id.in_(table_ids))
        all_columns = ctx.session.execute(cols_stmt).scalars().all()
        columns_by_table: dict[str, list[Column]] = {}
        for col in all_columns:
            columns_by_table.setdefault(col.table_id, []).append(col)

        # Get LLM recommendations for valuable enrichments. RuntimeError
        # signals an attempted LLM call that failed (transient/permanent);
        # None signals LLM intentionally unavailable (config-disabled).
        try:
            llm_recommendations = self._get_llm_recommendations(
                ctx=ctx,
                typed_tables=typed_tables,
                fact_entities=fact_entities,
                all_relationships=all_relationships,
                columns_by_table=columns_by_table,
                tables_by_id=tables_by_id,
            )
        except RuntimeError as exc:
            return PhaseResult.failed(str(exc))

        if not llm_recommendations:
            return PhaseResult.success(
                outputs={"enriched_views": 0, "message": "LLM unavailable, skipping enrichment"},
                records_processed=0,
                records_created=0,
                summary="skipped (LLM unavailable)",
            )

        views_created = 0
        views_dropped = 0

        for fact_entity in fact_entities:
            fact_table = tables_by_id.get(fact_entity.table_id)
            if not fact_table or not fact_table.duckdb_path:
                continue

            # Get dimension joins from LLM recommendations
            dimension_joins: list[DimensionJoin] = []

            if llm_recommendations:
                for rec in llm_recommendations.recommendations:
                    if rec.fact_table_id == fact_table.table_id:
                        dimension_joins.extend(rec.dimension_joins)

            if not dimension_joins:
                logger.info(
                    "passthrough_enriched_view",
                    fact_table=fact_table.table_name,
                    reason="no qualifying dimension joins",
                )

            # Collision-free identities: name the view off the fact's
            # source-qualified duckdb_path (``enriched_{source}__{table}``), so two
            # sources that each have an ``orders`` fact don't clash on
            # ``enriched_orders``. Every source is fully-qualified, so CREATE OR
            # REPLACE only ever replaces THIS view on a re-run, never a sibling.
            view_name = f"enriched_{fact_table.duckdb_path}"
            view_fqn = _lake_fqn("enriched", view_name)
            fact_fqn = _lake_fqn("typed", fact_table.duckdb_path)
            fqn_joins = [
                replace(join, dim_duckdb_path=_lake_fqn("typed", dim.duckdb_path))
                for join in dimension_joins
                if (dim := tables_by_name.get(join.dim_table_name)) is not None and dim.duckdb_path
            ]

            # Build view SQL
            view_sql, dim_columns = build_enriched_view_sql(view_fqn, fact_fqn, fqn_joins)

            # Create view in DuckDB
            try:
                ctx.duckdb_conn.execute(view_sql)
            except Exception as e:
                logger.warning("view_creation_failed", view_name=view_name, error=str(e))
                continue

            # Verify grain preservation
            is_grain_verified = self._verify_grain(
                ctx.duckdb_conn,
                view_target=view_fqn,
                expected_count=fact_table.row_count,
            )

            if not is_grain_verified:
                # Drop view — it would introduce duplicates
                logger.warning(
                    "grain_verification_failed",
                    view_name=view_name,
                    expected_count=fact_table.row_count,
                )
                try:
                    ctx.duckdb_conn.execute(f"DROP VIEW IF EXISTS {view_fqn}")
                except Exception:
                    pass
                views_dropped += 1
                continue

            # Build evidence with LLM reasoning if available
            evidence: dict[str, Any] = {}
            if llm_recommendations:
                for rec in llm_recommendations.recommendations:
                    if rec.fact_table_id == fact_table.table_id:
                        evidence = {
                            "llm_reasoning": rec.reasoning,
                            "dimension_type": rec.dimension_type,
                            "enrichment_columns": rec.enrichment_columns,
                            "model_name": llm_recommendations.model_name,
                        }
                        break

            # Register and profile dimension columns (latest-only substrate:
            # reconciled by view_name, prior columns/profiles replaced).
            view_table = self._register_and_profile_dim_columns(
                ctx,
                fact_table,
                view_name,
                view_fqn,
                dim_columns,
            )

            # Version the view DDL on the DAT-414 recipe substrate (emit → store →
            # execute; the view was executed above), sqlglot-gated (DAT-415):
            # only stamp a NEW run-versioned recipe when the canonical SQL differs
            # from the fact's latest stored enriched recipe — an unchanged re-run
            # (temp-0 LLM → same joins) adds no redundant version, and a
            # same-run retry sees its own just-stored recipe as equivalent and
            # no-ops. Keyed on the stable fact id + ``layer="enriched"``
            # (collision-free vs typing's typed/quarantine recipes); ``depends_on``
            # is the typed fact/dim FQNs the recipe topo-sort rebuilds after.
            dim_fqns = [j.dim_duckdb_path for j in fqn_joins]
            latest_recipe = ctx.session.execute(
                select(MaterializationRecipe)
                .where(
                    MaterializationRecipe.table_id == fact_table.table_id,
                    MaterializationRecipe.layer == "enriched",
                )
                .order_by(MaterializationRecipe.created_at.desc())
                .limit(1)
            ).scalar_one_or_none()
            if latest_recipe is None or not sql_equivalent(latest_recipe.ddl, view_sql):
                store_recipe(
                    ctx.session,
                    session_id=ctx.require_session_id(),
                    table_id=fact_table.table_id,
                    layer="enriched",
                    run_id=ctx.run_id,
                    target_fqn=view_fqn,
                    ddl=view_sql,
                    depends_on=[fact_fqn, *dim_fqns],
                )

            # The view definition is latest-only substrate (one row per fact,
            # DAT-415): reconcile-in-place, stamping the run that materialized it.
            # The recipe (above) carries the version history; readers resolve the
            # current view here without run-scoping. Keyed on fact_table_id alone
            # (no session filter) and now DB-enforced by ``uq_enriched_view_fact_table``
            # — the physical view is shared in lake.typed, so the metadata is
            # last-writer-wins per fact. The run-scoped fact query above means one
            # iteration per fact; the unique constraint is the structural backstop
            # so any errant second insert fails loudly here instead of silently
            # surfacing as ``MultipleResultsFound`` in a downstream reader.
            view_record = ctx.session.execute(
                select(EnrichedView).where(EnrichedView.fact_table_id == fact_table.table_id)
            ).scalar_one_or_none()
            if view_record is None:
                view_record = EnrichedView(
                    session_id=ctx.require_session_id(),
                    fact_table_id=fact_table.table_id,
                )
                ctx.session.add(view_record)
            view_record.session_id = ctx.require_session_id()
            view_record.run_id = ctx.run_id
            view_record.view_name = view_name
            view_record.relationship_ids = [j.relationship_id for j in dimension_joins]
            view_record.dimension_table_ids = list(
                {
                    tables_by_name[j.dim_table_name].table_id
                    for j in dimension_joins
                    if j.dim_table_name in tables_by_name
                }
            )
            view_record.dimension_columns = dim_columns
            view_record.is_grain_verified = is_grain_verified
            view_record.evidence = evidence if evidence else None
            view_record.view_table_id = view_table.table_id if view_table else None

            views_created += 1

            logger.info(
                "enriched_view_created",
                view_name=view_name,
                fact_table=fact_table.table_name,
                dimension_joins=len(dimension_joins),
                dimension_columns=len(dim_columns),
            )

        return PhaseResult.success(
            outputs={
                "enriched_views": views_created,
                "views_dropped": views_dropped,
                "fact_tables": len(fact_entities),
            },
            records_processed=len(fact_entities),
            records_created=views_created,
            summary=f"{views_created} enriched views created ({len(fact_entities)} fact tables)",
        )

    def _register_and_profile_dim_columns(
        self,
        ctx: PhaseContext,
        fact_table: Table,
        view_name: str,
        view_fqn: str,
        dim_columns: list[str],
    ) -> Table | None:
        """Register enriched-layer Table + Column records for dimension columns and profile them.

        Latest-only substrate (DAT-415): the enriched ``Table`` is reconciled on
        its ``(source, view_name, "enriched")`` unique key — reused across runs,
        with its prior ``Column``s (and cascaded ``StatisticalProfile``s) replaced
        — rather than minting a fresh row each run. The view *definition* is what
        is run-versioned (``EnrichedView`` + recipe DDL); the lake substrate stays
        latest-only and is re-materialized from the versioned recipe on a reset.

        Args:
            ctx: Phase context.
            fact_table: The fact table this view is based on.
            view_name: Bare name of the enriched DuckDB view (stored as duckdb_path).
            view_fqn: Fully-qualified view name, used to query the view (DESCRIBE / profile).
            dim_columns: List of dimension column names in the view.

        Returns:
            The enriched-layer Table record, or None if no dimension columns.
        """
        if not dim_columns:
            return None

        try:
            view_table = ctx.session.execute(
                select(Table).where(
                    Table.source_id == fact_table.source_id,
                    Table.table_name == view_name,
                    Table.layer == "enriched",
                )
            ).scalar_one_or_none()
            if view_table is None:
                view_table = Table(
                    table_id=str(uuid4()),
                    source_id=fact_table.source_id,
                    table_name=view_name,
                    layer="enriched",
                    duckdb_path=view_name,
                    row_count=fact_table.row_count,
                )
                ctx.session.add(view_table)
                ctx.session.flush()
            else:
                # Reuse the row, drop its prior columns (profiles cascade) so the
                # latest run's dimension set replaces the last one's.
                view_table.duckdb_path = view_name
                view_table.row_count = fact_table.row_count
                ctx.session.execute(delete(Column).where(Column.table_id == view_table.table_id))
                ctx.session.flush()

            # Get DuckDB types for dimension columns
            duckdb_cols = ctx.duckdb_conn.execute(f"DESCRIBE {view_fqn}").fetchall()
            type_by_name = {row[0]: row[1] for row in duckdb_cols}

            registered_columns: list[Column] = []
            for pos, col_name in enumerate(dim_columns):
                col_type = type_by_name.get(col_name, "VARCHAR")
                col = Column(
                    column_id=str(uuid4()),
                    table_id=view_table.table_id,
                    column_name=col_name,
                    column_position=pos,
                    raw_type=col_type,
                    resolved_type=col_type,
                )
                ctx.session.add(col)
                registered_columns.append(col)

            # Profile each dimension column inline
            profiled_at = datetime.now(UTC)
            profiled_count = 0
            for col in registered_columns:
                profile = _profile_column_stats_parallel(
                    duckdb_conn=ctx.duckdb_conn,
                    table_name=view_name,
                    table_duckdb_path=view_fqn,
                    column_id=col.column_id,
                    column_name=col.column_name,
                    resolved_type=col.resolved_type or "VARCHAR",
                    profiled_at=profiled_at,
                    top_k=10,
                )
                if profile:
                    non_null = profile.total_count - profile.null_count
                    is_unique = profile.distinct_count == non_null if non_null > 0 else False
                    db_profile = StatisticalProfile(
                        profile_id=str(uuid4()),
                        session_id=ctx.require_session_id(),
                        column_id=col.column_id,
                        profiled_at=profiled_at,
                        layer="enriched",
                        total_count=profile.total_count,
                        null_count=profile.null_count,
                        distinct_count=profile.distinct_count,
                        null_ratio=profile.null_ratio,
                        cardinality_ratio=profile.cardinality_ratio,
                        is_unique=is_unique,
                        is_numeric=profile.numeric_stats is not None,
                        profile_data=profile.model_dump(mode="json"),
                    )
                    ctx.session.add(db_profile)
                    profiled_count += 1

            logger.info(
                "dim_columns_profiled",
                view_name=view_name,
                columns=len(registered_columns),
                profiles=profiled_count,
            )
            return view_table

        except Exception as e:
            logger.warning(
                "dim_column_registration_failed",
                view_name=view_name,
                error=str(e),
            )
            return None

    def _get_llm_recommendations(
        self,
        ctx: PhaseContext,
        typed_tables: Sequence[Table],
        fact_entities: Sequence[TableEntity],
        all_relationships: Sequence[Relationship],
        columns_by_table: dict[str, list[Column]],
        tables_by_id: dict[str, Table],
    ) -> EnrichmentAnalysisResult | None:
        """Get LLM recommendations for valuable dimension joins.

        Returns None when LLM is intentionally unavailable (no config,
        feature disabled, no provider configured) — pipeline proceeds
        without enriched views, which is a documented operating mode.

        Raises ``RuntimeError`` when an LLM call was attempted and failed
        (transient or permanent). The phase translates this into
        ``PhaseResult.failed`` so the pipeline halts loudly instead of
        silently producing degraded output.
        """
        # Try to load LLM config
        try:
            config = load_llm_config()
        except FileNotFoundError:
            logger.info("llm_config_not_found", result="skipped")
            return None

        # Check if enrichment analysis is enabled
        if (
            not config.features.enrichment_analysis
            or not config.features.enrichment_analysis.enabled
        ):
            logger.info("enrichment_analysis_disabled", result="skipped")
            return None

        # Create provider. Missing provider config and provider-creation
        # failures are configuration errors, not intentional skips —
        # surface them as runtime failures so the pipeline halts loudly.
        provider_config = config.providers.get(config.active_provider)
        if not provider_config:
            raise RuntimeError(
                f"Provider '{config.active_provider}' not configured — "
                "enrichment analysis is enabled but cannot run."
            )

        try:
            provider = create_provider(config.active_provider, provider_config.model_dump())
        except Exception as e:
            raise RuntimeError(f"Failed to create LLM provider for enrichment: {e}") from e

        # Build context data for the agent
        context_data = self._build_context_data(
            ctx=ctx,
            typed_tables=typed_tables,
            fact_entities=fact_entities,
            all_relationships=all_relationships,
            columns_by_table=columns_by_table,
            tables_by_id=tables_by_id,
        )

        # Create and call the enrichment agent
        renderer = PromptRenderer()
        agent = EnrichmentAgent(
            config=config,
            provider=provider,
            prompt_renderer=renderer,
        )

        result = agent.analyze(
            session=ctx.session,
            context_data=context_data,
        )

        if not result.success:
            logger.error(
                "enrichment_analysis_failed",
                error=result.error,
            )
            raise RuntimeError(f"Enrichment analysis failed: {result.error}")

        return result.value

    def _build_context_data(
        self,
        ctx: PhaseContext,
        typed_tables: Sequence[Table],
        fact_entities: Sequence[TableEntity],
        all_relationships: Sequence[Relationship],
        columns_by_table: dict[str, list[Column]],
        tables_by_id: dict[str, Table],
    ) -> dict[str, Any]:
        """Build context data for the enrichment agent."""
        table_ids = [t.table_id for t in typed_tables]

        # Build tables with entity info
        fact_table_ids = {e.table_id for e in fact_entities}
        tables_data = []
        for table in typed_tables:
            columns_list = [
                {
                    "column_id": col.column_id,
                    "column_name": col.column_name,
                    "resolved_type": col.resolved_type,
                }
                for col in columns_by_table.get(table.table_id, [])
            ]
            tables_data.append(
                {
                    "table_id": table.table_id,
                    "table_name": table.table_name,
                    "duckdb_path": table.duckdb_path,
                    "row_count": table.row_count,
                    "is_fact_table": table.table_id in fact_table_ids,
                    "columns": columns_list,
                }
            )

        # Build semantic annotations
        annotations_data = []
        ann_stmt = select(SemanticAnnotation).where(
            SemanticAnnotation.column_id.in_(
                [col.column_id for cols in columns_by_table.values() for col in cols]
            )
        )
        annotations = ctx.session.execute(ann_stmt).scalars().all()

        # Map column_id to column info for lookup
        column_id_to_info: dict[str, dict[str, str]] = {}
        for table in typed_tables:
            for col in columns_by_table.get(table.table_id, []):
                column_id_to_info[col.column_id] = {
                    "table_name": table.table_name,
                    "column_name": col.column_name,
                }

        for ann in annotations:
            col_info = column_id_to_info.get(ann.column_id, {})
            annotations_data.append(
                {
                    "table_name": col_info.get("table_name", ""),
                    "column_name": col_info.get("column_name", ""),
                    "semantic_role": ann.semantic_role,
                    "entity_type": ann.entity_type,
                    "business_name": ann.business_name,
                }
            )

        # Build confirmed relationships
        relationships_data = []
        for rel in all_relationships:
            from_table = tables_by_id.get(rel.from_table_id)
            to_table = tables_by_id.get(rel.to_table_id)

            # Get column names
            from_col_name = ""
            for col in columns_by_table.get(rel.from_table_id, []):
                if col.column_id == rel.from_column_id:
                    from_col_name = col.column_name
                    break

            to_col_name = ""
            for col in columns_by_table.get(rel.to_table_id, []):
                if col.column_id == rel.to_column_id:
                    to_col_name = col.column_name
                    break

            if from_table and to_table:
                relationships_data.append(
                    {
                        "from_table": from_table.table_name,
                        "from_column": from_col_name,
                        "to_table": to_table.table_name,
                        "to_column": to_col_name,
                        "cardinality": rel.cardinality,
                        "confidence": rel.confidence,
                    }
                )

        # Get existing enriched views
        existing_views_data = []
        existing_stmt = select(EnrichedView).where(EnrichedView.fact_table_id.in_(table_ids))
        existing_views = ctx.session.execute(existing_stmt).scalars().all()
        for ev in existing_views:
            fact_table = tables_by_id.get(ev.fact_table_id)
            existing_views_data.append(
                {
                    "view_name": ev.view_name,
                    "fact_table": fact_table.table_name if fact_table else "",
                    "dimension_columns": ev.dimension_columns or [],
                }
            )

        return {
            "tables": tables_data,
            "annotations": annotations_data,
            "confirmed_relationships": relationships_data,
            "existing_views": existing_views_data,
        }

    @staticmethod
    def _verify_grain(
        duckdb_conn: Any,
        view_target: str,
        expected_count: int | None,
    ) -> bool:
        """Verify that the view preserves the fact table grain.

        ``view_target`` is the fully-qualified view name. Returns True if
        COUNT(*) of the view matches the expected fact row count.
        """
        if expected_count is None:
            return True  # Can't verify without expected count

        try:
            result = duckdb_conn.execute(f"SELECT COUNT(*) FROM {view_target}").fetchone()
            actual_count = result[0] if result else 0
            return actual_count == expected_count
        except Exception:
            return False
