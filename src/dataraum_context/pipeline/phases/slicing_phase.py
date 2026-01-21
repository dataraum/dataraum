"""Slicing phase implementation.

LLM-powered analysis to identify optimal data slicing dimensions:
- Identifies categorical columns suitable for creating data subsets
- Generates SQL for creating slice tables
- Considers semantic meaning and statistical properties
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select

from dataraum_context.analysis.slicing.agent import SlicingAgent
from dataraum_context.analysis.slicing.db_models import SliceDefinition, SlicingAnalysisRun
from dataraum_context.llm import LLMCache, PromptRenderer, create_provider, load_llm_config
from dataraum_context.pipeline.base import PhaseContext, PhaseResult
from dataraum_context.pipeline.phases.base import BasePhase
from dataraum_context.storage import Column, Table


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
        return ["semantic"]

    @property
    def outputs(self) -> list[str]:
        return ["slice_definitions", "slice_queries"]

    @property
    def is_llm_phase(self) -> bool:
        return True

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if all tables already have slice definitions."""
        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return "No typed tables found"

        table_ids = [t.table_id for t in typed_tables]

        # Check which tables already have slice definitions
        sliced_stmt = select(SliceDefinition.table_id.distinct()).where(
            SliceDefinition.table_id.in_(table_ids)
        )
        sliced_ids = set((ctx.session.execute(sliced_stmt)).scalars().all())

        if len(sliced_ids) >= len(table_ids):
            return "All tables already have slice definitions"

        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Run slicing analysis using LLM."""
        start_time = datetime.now(UTC)

        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return PhaseResult.failed("No typed tables found. Run typing phase first.")

        table_ids = [t.table_id for t in typed_tables]

        # Check which tables already have slice definitions
        sliced_stmt = select(SliceDefinition.table_id.distinct()).where(
            SliceDefinition.table_id.in_(table_ids)
        )
        sliced_ids = set((ctx.session.execute(sliced_stmt)).scalars().all())
        unsliced_tables = [t for t in typed_tables if t.table_id not in sliced_ids]

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
        cache = LLMCache()
        renderer = PromptRenderer()

        # Create slicing agent
        agent = SlicingAgent(
            config=config,
            provider=provider,
            prompt_renderer=renderer,
            cache=cache,
        )

        # Build context data for the agent
        context_data = self._build_context_data(ctx, unsliced_tables)

        # Create analysis run record
        run_record = SlicingAnalysisRun(
            table_ids=[t.table_id for t in unsliced_tables],
            tables_analyzed=len(unsliced_tables),
            columns_considered=context_data.get("column_count", 0),
            started_at=start_time,
            status="running",
        )
        ctx.session.add(run_record)
        # No flush needed - run_id is client-generated UUID, commit happens at session_scope() end

        # Run slicing analysis
        analysis_result = agent.analyze(
            session=ctx.session,
            table_ids=[t.table_id for t in unsliced_tables],
            context_data=context_data,
        )

        if not analysis_result.success:
            run_record.status = "failed"
            run_record.error_message = analysis_result.error
            run_record.completed_at = datetime.now(UTC)
            # Note: commit handled by session_scope() in orchestrator
            return PhaseResult.failed(analysis_result.error or "Slicing analysis failed")

        slicing = analysis_result.unwrap()

        # Store slice definitions
        for rec in slicing.recommendations:
            slice_def = SliceDefinition(
                table_id=rec.table_id,
                column_id=rec.column_id,
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

        # Update run record
        run_record.status = "completed"
        run_record.completed_at = datetime.now(UTC)
        run_record.duration_seconds = (
            run_record.completed_at - run_record.started_at
        ).total_seconds()
        run_record.recommendations_count = len(slicing.recommendations)
        run_record.slices_generated = len(slicing.slice_queries)

        # Note: commit handled by session_scope() in orchestrator

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

        Loads statistics, semantic annotations, correlations, and quality metrics.
        """
        from dataraum_context.analysis.correlation.db_models import (
            ColumnCorrelation,
            DerivedColumn,
            FunctionalDependency,
        )
        from dataraum_context.analysis.semantic.db_models import SemanticAnnotation
        from dataraum_context.analysis.statistics.db_models import StatisticalProfile

        table_ids = [t.table_id for t in tables]
        tables_data = []
        statistics_data = []
        semantic_data = []
        correlations_data = []
        column_count = 0

        for table in tables:
            # Get columns for this table
            col_stmt = select(Column).where(Column.table_id == table.table_id)
            columns = (ctx.session.execute(col_stmt)).scalars().all()
            column_count += len(columns)

            columns_list = []
            for col in columns:
                columns_list.append(
                    {
                        "column_id": col.column_id,
                        "column_name": col.column_name,
                        "raw_type": col.raw_type,
                        "resolved_type": col.resolved_type,
                    }
                )

            tables_data.append(
                {
                    "table_id": table.table_id,
                    "table_name": table.table_name,
                    "duckdb_path": table.duckdb_path,
                    "row_count": table.row_count,
                    "columns": columns_list,
                }
            )

            # Get statistical profiles
            stats_stmt = select(StatisticalProfile).where(
                StatisticalProfile.column_id.in_([c.column_id for c in columns])
            )
            profiles = (ctx.session.execute(stats_stmt)).scalars().all()

            for profile in profiles:
                profile_col: Column | None = next(
                    (c for c in columns if c.column_id == profile.column_id), None
                )
                if not profile_col:
                    continue

                profile_data = profile.profile_data or {}
                top_values = profile_data.get("top_values", [])

                statistics_data.append(
                    {
                        "table_name": table.table_name,
                        "column_name": profile_col.column_name,
                        "total_count": profile.total_count,
                        "null_count": profile.null_count,
                        "null_ratio": profile.null_ratio,
                        "distinct_count": profile.distinct_count,
                        "cardinality_ratio": profile.cardinality_ratio,
                        "top_values": top_values,
                    }
                )

            # Get semantic annotations
            sem_stmt = select(SemanticAnnotation).where(
                SemanticAnnotation.column_id.in_([c.column_id for c in columns])
            )
            annotations = (ctx.session.execute(sem_stmt)).scalars().all()

            for ann in annotations:
                ann_col: Column | None = next(
                    (c for c in columns if c.column_id == ann.column_id), None
                )
                if not ann_col:
                    continue

                semantic_data.append(
                    {
                        "table_name": table.table_name,
                        "column_name": ann_col.column_name,
                        "semantic_role": ann.semantic_role,
                        "entity_type": ann.entity_type,
                        "business_name": ann.business_name,
                        "business_description": ann.business_description,
                    }
                )

        # Get correlations for all tables
        # Functional dependencies
        fd_stmt = select(FunctionalDependency).where(FunctionalDependency.table_id.in_(table_ids))
        fds = (ctx.session.execute(fd_stmt)).scalars().all()

        for fd in fds:
            correlations_data.append(
                {
                    "type": "functional_dependency",
                    "table_id": fd.table_id,
                    "determinant": fd.determinant_column_ids,
                    "dependent": fd.dependent_column_id,
                    "confidence": fd.confidence,
                }
            )

        # Column correlations (numeric)
        cc_stmt = select(ColumnCorrelation).where(ColumnCorrelation.table_id.in_(table_ids))
        ccs = (ctx.session.execute(cc_stmt)).scalars().all()

        for cc in ccs:
            correlations_data.append(
                {
                    "type": "numeric_correlation",
                    "table_id": cc.table_id,
                    "column1": cc.column1_id,
                    "column2": cc.column2_id,
                    "pearson_r": cc.pearson_r,
                    "spearman_rho": cc.spearman_rho,
                }
            )

        # Derived columns
        dc_stmt = select(DerivedColumn).where(DerivedColumn.table_id.in_(table_ids))
        dcs = (ctx.session.execute(dc_stmt)).scalars().all()

        for dc in dcs:
            correlations_data.append(
                {
                    "type": "derived_column",
                    "table_id": dc.table_id,
                    "derived_column": dc.derived_column_id,
                    "formula": dc.formula,
                    "match_rate": dc.match_rate,
                }
            )

        return {
            "tables": tables_data,
            "statistics": statistics_data,
            "semantic": semantic_data,
            "correlations": correlations_data,
            "quality": [],  # Quality metrics would come from statistical_quality phase
            "column_count": column_count,
        }
