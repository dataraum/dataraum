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
from types import ModuleType
from typing import Any

from sqlalchemy import select

from dataraum.analysis.relationships.db_models import Relationship
from dataraum.analysis.semantic.db_models import SemanticAnnotation, TableEntity
from dataraum.analysis.views.builder import DimensionJoin, build_enriched_view_sql
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.analysis.views.enrichment_agent import EnrichmentAgent
from dataraum.analysis.views.enrichment_models import EnrichmentAnalysisResult
from dataraum.core.logging import get_logger
from dataraum.llm import PromptRenderer, create_provider, load_llm_config
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.storage import Column, Table

logger = get_logger(__name__)

# Only these cardinalities preserve fact table grain
_GRAIN_SAFE_CARDINALITIES = {"many-to-one", "one-to-one"}
_MIN_CONFIDENCE = 0.7

_CARDINALITY_FLIP = {
    "one-to-many": "many-to-one",
    "many-to-one": "one-to-many",
    "one-to-one": "one-to-one",
    "many-to-many": "many-to-many",
}


def _flip_cardinality(cardinality: str | None) -> str | None:
    """Return the cardinality as seen from the opposite join direction."""
    return _CARDINALITY_FLIP.get(cardinality or "", cardinality)


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
    def description(self) -> str:
        return "Create enriched views joining fact + dimension tables"

    @property
    def dependencies(self) -> list[str]:
        return ["semantic"]

    @property
    def outputs(self) -> list[str]:
        return ["enriched_views"]

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.analysis.views import db_models

        return [db_models]

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if enriched views already exist for all fact tables."""
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        typed_tables = ctx.session.execute(stmt).scalars().all()

        if not typed_tables:
            return "No typed tables found"

        table_ids = [t.table_id for t in typed_tables]

        # Check for fact tables
        fact_stmt = select(TableEntity).where(
            TableEntity.table_id.in_(table_ids),
            TableEntity.is_fact_table.is_(True),
        )
        fact_entities = ctx.session.execute(fact_stmt).scalars().all()

        if not fact_entities:
            return "No fact tables identified"

        # Check if views already exist
        existing_stmt = select(EnrichedView).where(
            EnrichedView.fact_table_id.in_([e.table_id for e in fact_entities])
        )
        existing = ctx.session.execute(existing_stmt).scalars().all()

        if len(existing) >= len(fact_entities):
            return "Enriched views already exist for all fact tables"

        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Create enriched views for fact tables using LLM recommendations."""
        # Get typed tables
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        typed_tables = ctx.session.execute(stmt).scalars().all()

        if not typed_tables:
            return PhaseResult.failed("No typed tables found")

        table_ids = [t.table_id for t in typed_tables]
        tables_by_id = {t.table_id: t for t in typed_tables}
        tables_by_name = {t.table_name: t for t in typed_tables}

        # Find fact tables from entity detections
        fact_stmt = select(TableEntity).where(
            TableEntity.table_id.in_(table_ids),
            TableEntity.is_fact_table.is_(True),
        )
        fact_entities = ctx.session.execute(fact_stmt).scalars().all()

        if not fact_entities:
            return PhaseResult.success(
                outputs={"enriched_views": 0, "message": "No fact tables found"},
                records_processed=0,
                records_created=0,
            )

        # Load all LLM-confirmed relationships for these tables
        rel_stmt = select(Relationship).where(
            Relationship.from_table_id.in_(table_ids) | Relationship.to_table_id.in_(table_ids),
            Relationship.detection_method == "llm",
            Relationship.confidence >= _MIN_CONFIDENCE,
        )
        all_relationships = ctx.session.execute(rel_stmt).scalars().all()

        # Build column lookups
        cols_stmt = select(Column).where(Column.table_id.in_(table_ids))
        all_columns = ctx.session.execute(cols_stmt).scalars().all()
        columns_by_table: dict[str, list[Column]] = {}
        for col in all_columns:
            columns_by_table.setdefault(col.table_id, []).append(col)

        # Try to get LLM recommendations for valuable enrichments
        llm_recommendations = self._get_llm_recommendations(
            ctx=ctx,
            typed_tables=typed_tables,
            fact_entities=fact_entities,
            all_relationships=all_relationships,
            columns_by_table=columns_by_table,
            tables_by_id=tables_by_id,
        )

        views_created = 0
        views_dropped = 0

        for fact_entity in fact_entities:
            fact_table = tables_by_id.get(fact_entity.table_id)
            if not fact_table or not fact_table.duckdb_path:
                continue

            # Get dimension joins - prefer LLM recommendations if available
            dimension_joins: list[DimensionJoin] = []

            if llm_recommendations:
                # Use LLM-recommended joins
                for rec in llm_recommendations.recommendations:
                    if rec.fact_table_id == fact_table.table_id:
                        dimension_joins.extend(rec.dimension_joins)
            else:
                # Fallback to deterministic join finding
                dimension_joins = self._find_dimension_joins(
                    fact_table=fact_table,
                    relationships=all_relationships,
                    tables_by_id=tables_by_id,
                    columns_by_table=columns_by_table,
                )

            if not dimension_joins:
                logger.debug(
                    "no_qualifying_joins",
                    fact_table=fact_table.table_name,
                )
                continue

            # Build view SQL
            view_name, view_sql, dim_columns = build_enriched_view_sql(
                fact_table_name=fact_table.table_name,
                fact_duckdb_path=fact_table.duckdb_path,
                dimension_joins=dimension_joins,
            )

            # Create view in DuckDB
            try:
                ctx.duckdb_conn.execute(view_sql)
            except Exception as e:
                logger.warning(
                    "view_creation_failed",
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
                # Drop view — it would introduce duplicates
                logger.warning(
                    "grain_verification_failed",
                    view_name=view_name,
                    expected_count=fact_table.row_count,
                )
                try:
                    ctx.duckdb_conn.execute(f'DROP VIEW IF EXISTS "{view_name}"')
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

            # Check if view already exists - update or create
            existing_view_stmt = select(EnrichedView).where(
                EnrichedView.fact_table_id == fact_table.table_id
            )
            existing_view = ctx.session.execute(existing_view_stmt).scalar_one_or_none()

            if existing_view:
                # Update existing view record
                existing_view.view_name = view_name
                existing_view.view_sql = view_sql
                existing_view.relationship_ids = [j.relationship_id for j in dimension_joins]
                existing_view.dimension_table_ids = list(
                    {
                        tables_by_name[j.dim_table_name].table_id
                        for j in dimension_joins
                        if j.dim_table_name in tables_by_name
                    }
                )
                existing_view.dimension_columns = dim_columns
                existing_view.is_grain_verified = is_grain_verified
                existing_view.evidence = evidence if evidence else None
                logger.info(
                    "enriched_view_updated",
                    view_name=view_name,
                    fact_table=fact_table.table_name,
                )
            else:
                # Create new view record
                view_record = EnrichedView(
                    fact_table_id=fact_table.table_id,
                    view_name=view_name,
                    view_sql=view_sql,
                    relationship_ids=[j.relationship_id for j in dimension_joins],
                    dimension_table_ids=list(
                        {
                            tables_by_name[j.dim_table_name].table_id
                            for j in dimension_joins
                            if j.dim_table_name in tables_by_name
                        }
                    ),
                    dimension_columns=dim_columns,
                    is_grain_verified=is_grain_verified,
                    evidence=evidence if evidence else None,
                )
                ctx.session.add(view_record)

            views_created += 1

            logger.info(
                "enriched_view_created",
                view_name=view_name,
                fact_table=fact_table.table_name,
                dimension_joins=len(dimension_joins),
                dimension_columns=len(dim_columns),
                llm_powered=llm_recommendations is not None,
            )

        return PhaseResult.success(
            outputs={
                "enriched_views": views_created,
                "views_dropped": views_dropped,
                "fact_tables": len(fact_entities),
                "llm_powered": llm_recommendations is not None,
            },
            records_processed=len(fact_entities),
            records_created=views_created,
        )

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

        Returns None if LLM is disabled or fails, allowing fallback to
        deterministic join finding.
        """
        # Try to load LLM config
        try:
            config = load_llm_config()
        except FileNotFoundError:
            logger.info("llm_config_not_found", fallback="deterministic")
            return None

        # Check if enrichment analysis is enabled
        if (
            not config.features.enrichment_analysis
            or not config.features.enrichment_analysis.enabled
        ):
            logger.info("enrichment_analysis_disabled", fallback="deterministic")
            return None

        # Create provider
        provider_config = config.providers.get(config.active_provider)
        if not provider_config:
            logger.warning("llm_provider_not_configured", fallback="deterministic")
            return None

        try:
            provider = create_provider(config.active_provider, provider_config.model_dump())
        except Exception as e:
            logger.warning("llm_provider_creation_failed", error=str(e), fallback="deterministic")
            return None

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
            logger.warning(
                "enrichment_analysis_failed",
                error=result.error,
                fallback="deterministic",
            )
            return None

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

    def _find_dimension_joins(
        self,
        fact_table: Table,
        relationships: Sequence[Relationship],
        tables_by_id: dict[str, Table],
        columns_by_table: dict[str, list[Column]],
    ) -> list[DimensionJoin]:
        """Find qualifying dimension joins for a fact table (fallback).

        Only includes relationships where:
        - Cardinality is many_to_one or one_to_one
        - Confidence >= threshold
        - The relationship doesn't introduce duplicates
        """
        joins: list[DimensionJoin] = []

        for rel in relationships:
            # Determine which side is fact, which is dimension.
            # Cardinality is stored as "from_table → to_table", so when traversing
            # in reverse (fact is to_table) we must flip it:
            #   stored one-to-many (dim→fact) = many-to-one (fact→dim) = grain-safe
            if rel.from_table_id == fact_table.table_id:
                fk_column_id = rel.from_column_id
                dim_table_id = rel.to_table_id
                dim_pk_column_id = rel.to_column_id
                cardinality = rel.cardinality
            elif rel.to_table_id == fact_table.table_id:
                fk_column_id = rel.to_column_id
                dim_table_id = rel.from_table_id
                dim_pk_column_id = rel.from_column_id
                # Flip cardinality for the reversed direction
                cardinality = _flip_cardinality(rel.cardinality)
            else:
                continue

            # Check cardinality is grain-safe
            if cardinality not in _GRAIN_SAFE_CARDINALITIES:
                continue

            # Check for duplicate flag in evidence
            evidence = rel.evidence or {}
            if evidence.get("introduces_duplicates"):
                continue

            dim_table = tables_by_id.get(dim_table_id)
            if not dim_table or not dim_table.duckdb_path:
                continue

            # Find FK column name
            fk_col_name = None
            for col in columns_by_table.get(fact_table.table_id, []):
                if col.column_id == fk_column_id:
                    fk_col_name = col.column_name
                    break

            # Find dimension PK column name
            dim_pk_col_name = None
            for col in columns_by_table.get(dim_table_id, []):
                if col.column_id == dim_pk_column_id:
                    dim_pk_col_name = col.column_name
                    break

            if not fk_col_name or not dim_pk_col_name:
                continue

            # Get dimension columns to include (exclude PK/FK columns)
            include_columns = [
                col.column_name
                for col in columns_by_table.get(dim_table_id, [])
                if col.column_name != dim_pk_col_name and col.column_name != fk_col_name
            ]

            if not include_columns:
                continue  # No useful columns to add

            joins.append(
                DimensionJoin(
                    dim_table_name=dim_table.table_name,
                    dim_duckdb_path=dim_table.duckdb_path,
                    fact_fk_column=fk_col_name,
                    dim_pk_column=dim_pk_col_name,
                    include_columns=include_columns,
                    relationship_id=rel.relationship_id,
                )
            )

        return joins

    @staticmethod
    def _verify_grain(
        duckdb_conn: Any,
        view_name: str,
        expected_count: int | None,
    ) -> bool:
        """Verify that the view preserves the fact table grain.

        Returns True if COUNT(*) of view matches expected row count.
        """
        if expected_count is None:
            return True  # Can't verify without expected count

        try:
            result = duckdb_conn.execute(f'SELECT COUNT(*) FROM "{view_name}"').fetchone()
            actual_count = result[0] if result else 0
            return actual_count == expected_count
        except Exception:
            return False
