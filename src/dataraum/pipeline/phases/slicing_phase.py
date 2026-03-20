"""Slicing phase implementation.

LLM-powered analysis to identify optimal data slicing dimensions:
- Identifies categorical columns suitable for creating data subsets
- Generates SQL for creating slice tables
- Considers semantic meaning and statistical properties
"""

from __future__ import annotations

from types import ModuleType
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, select

from dataraum.analysis.slicing.agent import SlicingAgent
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.slicing.models import (
    SliceRecommendation,
    SliceSQL,
    SlicingAnalysisResult,
)
from dataraum.core.logging import get_logger
from dataraum.llm import PromptRenderer, create_provider, load_llm_config
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.cleanup import exec_delete
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.storage import Column, Table

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = get_logger(__name__)


@analysis_phase
class SlicingPhase(BasePhase):
    """LLM-powered slicing analysis phase.

    Analyzes tables to identify the best categorical dimensions for
    creating data subsets (slices). Uses statistical profiles,
    semantic annotations, and correlation data as context.

    Requires: statistics, semantic phases.
    """

    @property
    def name(self) -> str:
        return "slicing"

    def cleanup(
        self,
        session: Session,
        source_id: str,
        table_ids: list[str],
        column_ids: list[str],
    ) -> int:
        if not table_ids:
            return 0
        return exec_delete(
            session, delete(SliceDefinition).where(SliceDefinition.table_id.in_(table_ids))
        )

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.analysis.slicing import db_models

        return [db_models]

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if all fact tables already have slice definitions."""
        fact_tables = self._get_fact_tables(ctx)

        if not fact_tables:
            return "No fact tables with enriched views found"

        table_ids = [t.table_id for t in fact_tables]

        # Check which tables already have slice definitions
        sliced_stmt = select(SliceDefinition.table_id.distinct()).where(
            SliceDefinition.table_id.in_(table_ids)
        )
        sliced_ids = set((ctx.session.execute(sliced_stmt)).scalars().all())

        if len(sliced_ids) >= len(table_ids):
            return "All fact tables already have slice definitions"

        return None

    def _get_fact_tables(self, ctx: PhaseContext) -> list[Table]:
        """Return only typed tables that have an enriched view (fact tables)."""
        from dataraum.analysis.views.db_models import EnrichedView

        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        typed_tables = list(ctx.session.execute(stmt).scalars().all())

        if not typed_tables:
            return []

        # Keep only tables that are fact tables in at least one verified enriched view
        fact_table_ids = set(
            ctx.session.execute(
                select(EnrichedView.fact_table_id.distinct()).where(
                    EnrichedView.fact_table_id.in_([t.table_id for t in typed_tables]),
                    EnrichedView.is_grain_verified.is_(True),
                )
            )
            .scalars()
            .all()
        )

        return [t for t in typed_tables if t.table_id in fact_table_ids]

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Run slicing analysis using LLM."""
        # Get only fact tables (those with enriched views) for this source
        fact_tables = self._get_fact_tables(ctx)

        if not fact_tables:
            return PhaseResult.failed(
                "No fact tables with enriched views found. Run enriched_views phase first."
            )

        table_ids = [t.table_id for t in fact_tables]

        # Check which tables already have slice definitions
        sliced_stmt = select(SliceDefinition.table_id.distinct()).where(
            SliceDefinition.table_id.in_(table_ids)
        )
        sliced_ids = set((ctx.session.execute(sliced_stmt)).scalars().all())
        unsliced_tables = [t for t in fact_tables if t.table_id not in sliced_ids]

        if not unsliced_tables:
            return PhaseResult.success(
                outputs={
                    "slice_definitions": 0,
                    "slice_queries": 0,
                    "message": "All tables already have slice definitions",
                },
                records_processed=0,
                records_created=0,
            )

        # Initialize LLM infrastructure
        try:
            config = load_llm_config()
        except FileNotFoundError as e:
            return PhaseResult.failed(f"LLM config not found: {e}")

        # Check if slicing analysis is enabled
        if not config.features.slicing_analysis or not config.features.slicing_analysis.enabled:
            return PhaseResult.failed(
                "Slicing analysis is disabled in config. "
                "Enable it in config/llm.yaml or use --skip-llm to skip LLM phases."
            )

        # Create provider
        provider_config = config.providers.get(config.active_provider)
        if not provider_config:
            return PhaseResult.failed(f"Provider '{config.active_provider}' not configured")

        try:
            provider = create_provider(config.active_provider, provider_config.model_dump())
        except Exception as e:
            return PhaseResult.failed(f"Failed to create LLM provider: {e}")

        # Create other components
        renderer = PromptRenderer()

        # Create slicing agent
        agent = SlicingAgent(
            config=config,
            provider=provider,
            prompt_renderer=renderer,
        )

        # Build context data for the agent
        context_data = self._build_context_data(ctx, unsliced_tables)

        # Pre-filter columns: remove objectively bad slice candidates
        # before sending to LLM (saves tokens, prevents bad recommendations)
        self._pre_filter_columns(context_data)

        # Pass config constraints so the prompt can reference them
        context_data["constraints"] = {
            "max_recommendations": ctx.config.get("max_recommendations", 6),
        }

        # Run slicing analysis
        analysis_result = agent.analyze(
            session=ctx.session,
            table_ids=[t.table_id for t in unsliced_tables],
            context_data=context_data,
        )

        if not analysis_result.success:
            return PhaseResult.failed(analysis_result.error or "Slicing analysis failed")

        slicing = analysis_result.unwrap()

        # Propagate enriched FK dimension recommendations to other tables
        # that share the same dimension column
        slicing = self._propagate_enriched_dimensions(slicing, context_data, agent)

        # Store slice definitions
        for rec in slicing.recommendations:
            slice_def = SliceDefinition(
                table_id=rec.table_id,
                column_id=rec.column_id,
                column_name=rec.column_name,
                slice_priority=rec.slice_priority,
                slice_type="categorical",
                distinct_values=rec.distinct_values,
                value_count=rec.value_count,
                reasoning=rec.reasoning,
                business_context=rec.business_context,
                confidence=rec.confidence,
                sql_template=rec.sql_template,
                detection_source="llm",
            )
            ctx.session.add(slice_def)

        return PhaseResult.success(
            outputs={
                "slice_definitions": len(slicing.recommendations),
                "slice_queries": len(slicing.slice_queries),
                "tables_analyzed": [t.table_name for t in unsliced_tables],
            },
            records_processed=len(unsliced_tables),
            records_created=len(slicing.recommendations),
            summary=f"{len(slicing.recommendations)} definitions, {len(slicing.slice_queries)} queries",
        )

    def _pre_filter_columns(self, context_data: dict[str, Any]) -> None:
        """Remove columns that are objectively bad slice candidates.

        Mutates context_data in place, removing columns with:
        - distinct_count > 50 (too high cardinality for slicing)
        - null_ratio > 0.5 (majority NULL)
        - cardinality_ratio > 0.5 (approaching identifier territory)

        Enriched dimension columns are exempt from the cardinality_ratio
        check since they are specifically designed for analytical grouping.
        """
        for table_data in context_data.get("tables", []):
            original = table_data.get("columns", [])
            filtered = []
            for col in original:
                distinct = col.get("distinct_count")
                null_ratio = col.get("null_ratio")
                card_ratio = col.get("cardinality_ratio")
                is_enriched = col.get("is_enriched_dimension", False)

                if distinct is not None and distinct > 50:
                    continue
                if null_ratio is not None and null_ratio > 0.5:
                    continue
                if not is_enriched and card_ratio is not None and card_ratio > 0.5:
                    continue

                filtered.append(col)

            if len(filtered) < len(original):
                logger.debug(
                    "pre_filtered_columns",
                    table=table_data.get("table_name"),
                    removed=len(original) - len(filtered),
                    kept=len(filtered),
                )
            table_data["columns"] = filtered

    def _propagate_enriched_dimensions(
        self,
        result: SlicingAnalysisResult,
        context_data: dict[str, Any],
        agent: SlicingAgent,
    ) -> SlicingAnalysisResult:
        """Copy enriched FK dim recommendations to all tables sharing the same dimension column.

        When the LLM recommends an enriched dimension (e.g. ``account_id__account_type``)
        for one fact table, this method finds other fact tables that also have that
        enriched column and creates matching recommendations + SQL for them.

        Args:
            result: LLM slicing analysis result.
            context_data: Context data with table/column metadata.
            agent: Slicing agent (for SQL generation helpers).

        Returns:
            Updated result with propagated recommendations.
        """
        tables_data = context_data.get("tables", [])
        if len(tables_data) < 2:
            return result

        # Build lookup: dim_column_name → list of table dicts that have it
        dim_col_to_tables: dict[str, list[dict[str, Any]]] = {}
        for tdata in tables_data:
            for col in tdata.get("columns", []):
                if col.get("is_enriched_dimension") and "__" in col.get("column_name", ""):
                    dim_col_to_tables.setdefault(col["column_name"], []).append(tdata)

        # Track which (table_name, column_name) combos already have recommendations
        existing_recs: set[tuple[str, str]] = set()
        for rec in result.recommendations:
            existing_recs.add((rec.table_name, rec.column_name))

        new_recs: list[SliceRecommendation] = []
        new_queries: list[SliceSQL] = []

        for rec in result.recommendations:
            col_name = rec.column_name
            if "__" not in col_name:
                continue

            candidate_tables = dim_col_to_tables.get(col_name, [])
            for tdata in candidate_tables:
                target_table_name = tdata["table_name"]
                if (target_table_name, col_name) in existing_recs:
                    continue

                # Find the FK column_id in the target table (prefix before __)
                fk_prefix = col_name.split("__")[0]
                target_col_id = ""
                for col in tdata.get("columns", []):
                    if col["column_name"] == fk_prefix:
                        target_col_id = col.get("column_id", "")
                        break

                # Build SQL using target table's enriched view
                enriched_view = tdata.get("enriched_duckdb_path")
                duckdb_table = enriched_view or tdata.get(
                    "duckdb_path", f"typed_{target_table_name}"
                )

                safe_source = agent._sanitize_for_table_name(target_table_name)
                sql_template = agent._build_sql_template(
                    duckdb_table,
                    col_name,
                    rec.distinct_values,
                    source_table_name=target_table_name,
                )

                new_rec = SliceRecommendation(
                    table_id=tdata.get("table_id", ""),
                    table_name=target_table_name,
                    column_id=target_col_id,
                    column_name=col_name,
                    slice_priority=rec.slice_priority,
                    distinct_values=rec.distinct_values,
                    value_count=rec.value_count,
                    reasoning=f"Propagated from {rec.table_name}: {rec.reasoning}",
                    business_context=rec.business_context,
                    confidence=rec.confidence,
                    sql_template=sql_template,
                )
                new_recs.append(new_rec)
                existing_recs.add((target_table_name, col_name))

                # Generate slice queries for the new table
                for value in rec.distinct_values:
                    safe_value = agent._sanitize_for_table_name(str(value))
                    safe_column = agent._sanitize_for_table_name(col_name)
                    slice_table_name = f"slice_{safe_source}_{safe_column}_{safe_value}"

                    sql_query = agent._build_slice_sql(
                        duckdb_table, col_name, value, slice_table_name
                    )
                    new_queries.append(
                        SliceSQL(
                            slice_name=f"{col_name}={value}",
                            slice_value=str(value),
                            table_name=slice_table_name,
                            sql_query=sql_query,
                        )
                    )

                logger.info(
                    "propagated_enriched_dimension",
                    column=col_name,
                    from_table=rec.table_name,
                    to_table=target_table_name,
                )

        if new_recs:
            result.recommendations.extend(new_recs)
            result.slice_queries.extend(new_queries)

        return result

    def _build_context_data(self, ctx: PhaseContext, tables: list[Table]) -> dict[str, Any]:
        """Build context data for the slicing agent.

        Statistics and semantic annotations are merged directly into each column dict
        to eliminate cross-referencing in the prompt and reduce token usage.

        Enriched FK-prefixed dimension columns (e.g. ``fk_col__dim_col``) are appended
        to the fact table's column list so the LLM can recommend them as slice candidates.
        Their ``column_id`` is set to the FK column's column_id (the prefix part) because
        enriched dim columns are not yet individually registered as Column records.
        """
        from dataraum.analysis.semantic.db_models import SemanticAnnotation
        from dataraum.analysis.statistics.db_models import StatisticalProfile
        from dataraum.analysis.views.db_models import EnrichedView

        table_ids = [t.table_id for t in tables]
        tables_data = []
        column_count = 0

        # Pre-load enriched views for all tables so we can merge dim cols per-table
        ev_by_fact: dict[str, EnrichedView | None] = {}
        try:
            ev_stmt = select(EnrichedView).where(
                EnrichedView.fact_table_id.in_(table_ids),
                EnrichedView.is_grain_verified.is_(True),
            )
            for ev in ctx.session.execute(ev_stmt).scalars().all():
                ev_by_fact[ev.fact_table_id] = ev
        except Exception:
            pass  # Enriched views not available, proceed without

        for table in tables:
            # Get columns for this table
            col_stmt = select(Column).where(Column.table_id == table.table_id)
            columns = list((ctx.session.execute(col_stmt)).scalars().all())
            column_count += len(columns)

            col_ids = [c.column_id for c in columns]

            columns_list: list[dict[str, Any]] = [
                {
                    "column_id": col.column_id,
                    "column_name": col.column_name,
                    "raw_type": col.raw_type,
                    "resolved_type": col.resolved_type,
                }
                for col in columns
            ]

            # Build lookup from column_id -> column dict
            col_dict_by_id = {
                col.column_id: col_dict for col, col_dict in zip(columns, columns_list, strict=True)
            }
            # Also build lookup from column_name -> column_id (for FK prefix resolution)
            col_id_by_name = {col.column_name: col.column_id for col in columns}

            # Merge statistical profiles into columns
            stats_stmt = select(StatisticalProfile).where(StatisticalProfile.column_id.in_(col_ids))
            for profile in (ctx.session.execute(stats_stmt)).scalars().all():
                col_dict = col_dict_by_id.get(profile.column_id)
                if col_dict:
                    profile_data = profile.profile_data or {}
                    col_dict["total_count"] = profile.total_count
                    col_dict["null_count"] = profile.null_count
                    col_dict["null_ratio"] = profile.null_ratio
                    col_dict["distinct_count"] = profile.distinct_count
                    col_dict["cardinality_ratio"] = profile.cardinality_ratio
                    col_dict["top_values"] = profile_data.get("top_values", [])

            # Merge semantic annotations into columns
            sem_stmt = select(SemanticAnnotation).where(SemanticAnnotation.column_id.in_(col_ids))
            for ann in (ctx.session.execute(sem_stmt)).scalars().all():
                col_dict = col_dict_by_id.get(ann.column_id)
                if col_dict:
                    col_dict["semantic_role"] = ann.semantic_role
                    col_dict["entity_type"] = ann.entity_type
                    col_dict["business_name"] = ann.business_name
                    col_dict["business_description"] = ann.business_description

            # Append enriched dimension columns from the enriched view's
            # registered Table + Column records (persisted during enriched_views phase).
            # Stats come from StatisticalProfile — no ad-hoc DuckDB queries needed.
            table_ev = ev_by_fact.get(table.table_id)
            if table_ev and table_ev.view_table_id and table_ev.dimension_columns:
                # Load Column records for dimension columns
                dim_col_stmt = select(Column).where(Column.table_id == table_ev.view_table_id)
                dim_cols = list(ctx.session.execute(dim_col_stmt).scalars().all())
                dim_col_ids = [c.column_id for c in dim_cols]

                # Load their StatisticalProfiles
                dim_profiles: dict[str, StatisticalProfile] = {}
                if dim_col_ids:
                    prof_stmt = select(StatisticalProfile).where(
                        StatisticalProfile.column_id.in_(dim_col_ids)
                    )
                    for prof in ctx.session.execute(prof_stmt).scalars().all():
                        dim_profiles[prof.column_id] = prof

                for dim_col in dim_cols:
                    fk_prefix = (
                        dim_col.column_name.split("__")[0] if "__" in dim_col.column_name else None
                    )
                    fk_col_id = col_id_by_name.get(fk_prefix) if fk_prefix else None
                    dim_entry: dict[str, Any] = {
                        "column_id": fk_col_id or dim_col.column_id,
                        "column_name": dim_col.column_name,
                        "is_enriched_dimension": True,
                        "fk_column_name": fk_prefix,
                    }
                    dim_prof = dim_profiles.get(dim_col.column_id)
                    if dim_prof:
                        profile_data = dim_prof.profile_data or {}
                        dim_entry["total_count"] = dim_prof.total_count
                        dim_entry["null_count"] = dim_prof.null_count
                        dim_entry["null_ratio"] = dim_prof.null_ratio
                        dim_entry["distinct_count"] = dim_prof.distinct_count
                        dim_entry["cardinality_ratio"] = dim_prof.cardinality_ratio
                        dim_entry["top_values"] = profile_data.get("top_values", [])
                    columns_list.append(dim_entry)
                    column_count += 1

            enriched_view_name = table_ev.view_name if table_ev else None

            tables_data.append(
                {
                    "table_id": table.table_id,
                    "table_name": table.table_name,
                    "duckdb_path": table.duckdb_path,
                    "row_count": table.row_count,
                    "columns": columns_list,
                    # Use enriched view if available, otherwise use typed table
                    "enriched_view_name": enriched_view_name,
                    "enriched_duckdb_path": enriched_view_name if table_ev else None,
                }
            )

        return {
            "tables": tables_data,
            "column_count": column_count,
        }
