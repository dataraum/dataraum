"""Slicing phase implementation.

LLM-powered analysis to identify optimal data slicing dimensions:
- Identifies categorical columns suitable for creating data subsets
- Generates SQL for creating slice tables
- Considers semantic meaning and statistical properties
"""

from __future__ import annotations

from types import ModuleType
from typing import Any

from sqlalchemy import select

from dataraum.analysis.slicing.agent import SlicingAgent
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.llm import PromptRenderer, create_provider, load_llm_config
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.storage import Column, Table


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

    @property
    def description(self) -> str:
        return "LLM-powered slice dimension identification"

    @property
    def dependencies(self) -> list[str]:
        return ["enriched_views"]

    @property
    def outputs(self) -> list[str]:
        return ["slice_definitions", "slice_queries"]

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

        # Pass config constraints so the prompt can reference them
        context_data["constraints"] = {
            "max_cardinality": ctx.config.get("max_cardinality", 15),
            "max_recommendations": ctx.config.get("max_recommendations", 4),
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
        )

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

            # Append enriched FK-prefixed dimension columns so the LLM can consider them.
            # Format: "{fk_col}__{dim_col}" — we resolve column_id via the FK prefix part.
            # Stats are queried directly from DuckDB since no StatisticalProfile records
            # exist for enriched dim columns (they are not registered as Column records).
            # Batch all stats into 2 queries (one pass for counts, one for top values)
            # instead of N queries per column to avoid scanning the enriched view 40+ times.
            table_ev = ev_by_fact.get(table.table_id)
            if table_ev and table_ev.dimension_columns:
                ev_view_name = table_ev.view_name  # e.g. "enriched_kontobuchungen"
                dim_col_names = list(table_ev.dimension_columns)

                # Batch stats query: one scan for all dim columns
                dim_stats: dict[str, dict[str, Any]] = {}
                try:
                    select_parts = ["COUNT(*) AS total_count"]
                    for dcn in dim_col_names:
                        q = f'"{dcn}"'
                        safe = dcn.replace('"', "")
                        select_parts.append(f"COUNT(DISTINCT {q}) AS d_{safe}")
                        select_parts.append(f"COUNT(*) - COUNT({q}) AS n_{safe}")
                    stats_row = ctx.duckdb_conn.execute(
                        f'SELECT {", ".join(select_parts)} FROM "{ev_view_name}"'
                    ).fetchone()
                    if stats_row:
                        total = stats_row[0] or 0
                        for i, dcn in enumerate(dim_col_names):
                            distinct_c = stats_row[1 + i * 2] or 0
                            null_c = stats_row[2 + i * 2] or 0
                            dim_stats[dcn] = {
                                "total_count": total,
                                "null_count": null_c,
                                "null_ratio": round(null_c / total, 4) if total else 0.0,
                                "distinct_count": distinct_c,
                                "cardinality_ratio": round(distinct_c / total, 4) if total else 0.0,
                            }
                except Exception:
                    pass

                # Batch top values query: one UNION ALL for all low-cardinality dim columns
                low_card_cols = [
                    dcn
                    for dcn in dim_col_names
                    if dim_stats.get(dcn, {}).get("distinct_count", 999) <= 20
                ]
                top_values_by_col: dict[str, list[dict[str, Any]]] = {}
                if low_card_cols:
                    try:
                        union_parts = [
                            f"SELECT '{dcn}' AS col_name, \"{dcn}\"::VARCHAR AS val,"
                            f' COUNT(*) AS cnt FROM "{ev_view_name}"'
                            f' WHERE "{dcn}" IS NOT NULL GROUP BY "{dcn}"'
                            for dcn in low_card_cols
                        ]
                        top_rows = ctx.duckdb_conn.execute(
                            " UNION ALL ".join(union_parts) + " ORDER BY col_name, cnt DESC"
                        ).fetchall()
                        for col_name, val, cnt in top_rows:
                            top_values_by_col.setdefault(col_name, [])
                            if len(top_values_by_col[col_name]) < 10:
                                top_values_by_col[col_name].append(
                                    {"value": str(val), "count": cnt}
                                )
                    except Exception:
                        pass

                for dim_col_name in dim_col_names:
                    fk_prefix = dim_col_name.split("__")[0] if "__" in dim_col_name else None
                    fk_col_id = col_id_by_name.get(fk_prefix) if fk_prefix else None
                    dim_entry: dict[str, Any] = {
                        "column_id": fk_col_id or "",
                        "column_name": dim_col_name,
                        "is_enriched_dimension": True,
                        "fk_column_name": fk_prefix,
                    }
                    if dim_col_name in dim_stats:
                        dim_entry.update(dim_stats[dim_col_name])
                    if dim_col_name in top_values_by_col:
                        dim_entry["top_values"] = top_values_by_col[dim_col_name]
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
