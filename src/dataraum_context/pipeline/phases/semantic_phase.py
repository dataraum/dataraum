"""Semantic phase implementation.

LLM-powered semantic analysis of tables and columns:
- Semantic roles (measure, dimension, identifier, etc.)
- Entity types (customer, product, transaction, etc.)
- Business names and descriptions
- Relationship confirmation from candidates
"""

from __future__ import annotations

from sqlalchemy import func, select

from dataraum_context.analysis.relationships.utils import load_relationship_candidates_for_semantic
from dataraum_context.analysis.semantic.agent import SemanticAgent
from dataraum_context.analysis.semantic.db_models import SemanticAnnotation
from dataraum_context.analysis.semantic.processor import enrich_semantic
from dataraum_context.llm import LLMCache, PromptRenderer, create_provider, load_llm_config
from dataraum_context.pipeline.base import PhaseContext, PhaseResult
from dataraum_context.pipeline.phases.base import BasePhase
from dataraum_context.storage import Column, Table


class SemanticPhase(BasePhase):
    """LLM-powered semantic analysis phase.

    Analyzes tables and columns using LLM to determine:
    - Semantic roles (measure, dimension, identifier, attribute)
    - Entity types (customer, product, transaction, etc.)
    - Business names and descriptions
    - Confirmed relationships between tables

    Requires: statistics, relationships, correlations phases.
    """

    @property
    def name(self) -> str:
        return "semantic"

    @property
    def description(self) -> str:
        return "LLM-powered semantic analysis"

    @property
    def dependencies(self) -> list[str]:
        return ["statistics", "relationships", "correlations"]

    @property
    def outputs(self) -> list[str]:
        return ["annotations", "entities", "confirmed_relationships"]

    @property
    def is_llm_phase(self) -> bool:
        return True

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if all columns already have semantic annotations."""
        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return "No typed tables found"

        table_ids = [t.table_id for t in typed_tables]

        # Count columns in these tables
        col_count_stmt = select(func.count(Column.column_id)).where(Column.table_id.in_(table_ids))
        total_columns = ctx.session.execute(col_count_stmt).scalar() or 0

        if total_columns == 0:
            return "No columns found in typed tables"

        # Count columns with LLM annotations
        annotated_stmt = (
            select(func.count(SemanticAnnotation.annotation_id))
            .join(Column)
            .where(
                Column.table_id.in_(table_ids),
                SemanticAnnotation.annotation_source == "llm",
            )
        )
        annotated_count = ctx.session.execute(annotated_stmt).scalar() or 0

        if annotated_count >= total_columns:
            return "All columns already have semantic annotations"

        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Run semantic analysis using LLM."""
        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return PhaseResult.failed("No typed tables found. Run typing phase first.")

        table_ids = [t.table_id for t in typed_tables]

        # Check which columns already have annotations
        annotated_cols_stmt = (
            select(SemanticAnnotation.column_id)
            .join(Column)
            .where(
                Column.table_id.in_(table_ids),
                SemanticAnnotation.annotation_source == "llm",
            )
        )
        annotated_col_ids = set(ctx.session.execute(annotated_cols_stmt).scalars().all())

        # Get columns needing analysis
        cols_stmt = select(Column).where(Column.table_id.in_(table_ids))
        all_columns = ctx.session.execute(cols_stmt).scalars().all()
        unannotated_columns = [c for c in all_columns if c.column_id not in annotated_col_ids]

        if not unannotated_columns:
            return PhaseResult.success(
                outputs={
                    "annotations": 0,
                    "entities": 0,
                    "confirmed_relationships": 0,
                    "message": "All columns already annotated",
                },
                records_processed=0,
                records_created=0,
            )

        # Initialize LLM infrastructure
        try:
            config = load_llm_config()
        except FileNotFoundError as e:
            return PhaseResult.failed(f"LLM config not found: {e}")

        # Check if semantic analysis is enabled
        if not config.features.semantic_analysis.enabled:
            return PhaseResult.failed(
                "Semantic analysis is disabled in config. "
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

        # Create semantic agent
        agent = SemanticAgent(
            config=config,
            provider=provider,
            prompt_renderer=renderer,
            cache=cache,
        )

        # Load relationship candidates from relationships phase
        relationship_candidates = load_relationship_candidates_for_semantic(
            session=ctx.session,
            table_ids=table_ids,
            detection_method="candidate",
        )

        # Get ontology from config (default to 'financial_reporting')
        ontology = ctx.config.get("ontology", "financial_reporting")

        # Run semantic enrichment
        enrich_result = enrich_semantic(
            session=ctx.session,
            agent=agent,
            table_ids=table_ids,
            ontology=ontology,
            relationship_candidates=relationship_candidates,
        )

        if not enrich_result.success:
            return PhaseResult.failed(enrich_result.error or "Semantic enrichment failed")

        enrichment = enrich_result.unwrap()

        return PhaseResult.success(
            outputs={
                "annotations": len(enrichment.annotations),
                "entities": len(enrichment.entity_detections),
                "confirmed_relationships": len(enrichment.relationships),
                "tables_analyzed": [t.table_name for t in typed_tables],
            },
            records_processed=len(unannotated_columns),
            records_created=len(enrichment.annotations)
            + len(enrichment.entity_detections)
            + len(enrichment.relationships),
        )
