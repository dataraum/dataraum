"""Semantic phase implementation.

Two-tier LLM-powered semantic analysis:
  Tier 1 (fast): Column annotation — roles, entity types, business terms, concepts
  Tier 2 (capable): Table classification, relationship evaluation, unit detection

The fast model handles column-level pattern recognition cheaply.
The capable model receives tier 1 annotations as context and focuses on
reasoning-heavy tasks: relationships, fact/dim classification, cross-column
unit detection, and reviewing low-confidence annotations.
"""

from __future__ import annotations

from types import ModuleType
from typing import TYPE_CHECKING

from sqlalchemy import delete, func, select

from dataraum.analysis.relationships.utils import load_relationship_candidates_for_semantic
from dataraum.analysis.semantic.agent import SemanticAgent
from dataraum.analysis.semantic.column_agent import ColumnAnnotationAgent
from dataraum.analysis.semantic.db_models import SemanticAnnotation
from dataraum.analysis.semantic.processor import enrich_semantic
from dataraum.core.logging import get_logger
from dataraum.entropy.dimensions import AnalysisKey
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
class SemanticPhase(BasePhase):
    """Two-tier LLM-powered semantic analysis phase.

    Tier 1 (fast model): Annotate columns with semantic roles, entity types,
        business terms, and ontology concept mappings.
    Tier 2 (capable model): Table classification, relationship evaluation,
        cross-column unit detection, and annotation review/upgrade.

    Requires: statistics, relationships phases.
    """

    @property
    def name(self) -> str:
        return "semantic"

    @property
    def description(self) -> str:
        return "LLM-powered semantic analysis"

    @property
    def dependencies(self) -> list[str]:
        return ["relationships"]

    @property
    def produces_analyses(self) -> set[AnalysisKey]:
        return {AnalysisKey.SEMANTIC}

    def cleanup(
        self,
        session: Session,
        source_id: str,
        table_ids: list[str],
        column_ids: list[str],
    ) -> int:
        from dataraum.analysis.relationships.db_models import Relationship
        from dataraum.analysis.semantic.db_models import TableEntity

        count = 0
        if column_ids:
            count += exec_delete(
                session,
                delete(SemanticAnnotation).where(SemanticAnnotation.column_id.in_(column_ids)),
            )
        if table_ids:
            count += exec_delete(
                session, delete(TableEntity).where(TableEntity.table_id.in_(table_ids))
            )
            # Delete LLM-confirmed relationships created by the semantic phase
            count += exec_delete(
                session,
                delete(Relationship).where(
                    Relationship.detection_method == "llm",
                    Relationship.from_table_id.in_(table_ids)
                    | Relationship.to_table_id.in_(table_ids),
                ),
            )
        return count

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.analysis.semantic import db_models

        return [db_models]

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
        """Run two-tier semantic analysis using LLM."""
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

        renderer = PromptRenderer()

        # Get vertical from pipeline config
        ontology = ctx.config.get("vertical")
        if not ontology:
            return PhaseResult.failed(
                "No vertical configured. Set 'vertical' in config/phases/semantic.yaml."
            )

        # Load standard_fields required by metric graphs so the semantic phase
        # can prioritize mapping those concepts to actual dataset columns.
        from dataraum.graphs.loader import GraphLoader

        metric_loader = GraphLoader(vertical=ontology)
        metric_loader.load_all()
        required_standard_fields = sorted(metric_loader.get_all_abstract_fields())

        # ========================================================
        # Step 1: Fast column annotation (tier 1)
        # ========================================================
        column_annotations = None
        col_annotation_config = config.features.column_annotation

        if col_annotation_config and col_annotation_config.enabled:
            col_agent = ColumnAnnotationAgent(
                config=config,
                provider=provider,
                prompt_renderer=renderer,
            )

            annotation_result = col_agent.annotate(
                session=ctx.session,
                table_ids=table_ids,
                ontology=ontology,
            )

            if annotation_result.success and annotation_result.value:
                column_annotations = annotation_result.value
                total_cols = sum(len(t.columns) for t in column_annotations.tables)
                logger.info(
                    "tier1_column_annotation_complete",
                    tables=len(column_annotations.tables),
                    columns=total_cols,
                )
            else:
                # Tier 1 failure is non-fatal — tier 2 can still work without it
                logger.warning(
                    "tier1_column_annotation_failed",
                    error=annotation_result.error,
                )
        else:
            logger.info("tier1_column_annotation_skipped", reason="disabled in config")

        # ========================================================
        # Step 2: Capable semantic analysis (tier 2)
        # ========================================================
        agent = SemanticAgent(
            config=config,
            provider=provider,
            prompt_renderer=renderer,
        )

        # Load relationship candidates from relationships phase
        relationship_candidates = load_relationship_candidates_for_semantic(
            session=ctx.session,
            table_ids=table_ids,
            detection_method="candidate",
        )

        # Run semantic enrichment with tier 1 annotations as context
        enrich_result = enrich_semantic(
            session=ctx.session,
            agent=agent,
            table_ids=table_ids,
            ontology=ontology,
            relationship_candidates=relationship_candidates,
            duckdb_conn=ctx.duckdb_conn,
            column_annotations=column_annotations,
            required_standard_fields=required_standard_fields,
        )

        if not enrich_result.success:
            return PhaseResult.failed(enrich_result.error or "Semantic enrichment failed")

        enrichment = enrich_result.unwrap()

        # Apply config overrides (e.g. set_timestamp_role fix)
        _apply_semantic_overrides(ctx.session, ctx.config, table_ids)

        # Apply relationship confirmations from semantic config.
        # This is the correct place: all relationships exist now, including
        # self-referential ones discovered by the semantic agent.
        _apply_relationship_confirmations(ctx.session, ctx.config, table_ids)

        annotations_count = len(enrichment.annotations)
        entities_count = len(enrichment.entity_detections)
        relationships_count = len(enrichment.relationships)

        # Surface entity discoveries as preview lines
        previews: list[str] = []
        for ent in enrichment.entity_detections:
            kind = "FACT" if ent.is_fact_table else "DIMENSION" if ent.is_dimension_table else ""
            label = f"{ent.table_name}: {ent.entity_type}"
            if kind:
                label += f" ({kind})"
            previews.append(label)
        for r in enrichment.relationships:
            previews.append(f"{r.from_table}.{r.from_column} \u2192 {r.to_table}.{r.to_column}")

        return PhaseResult.success(
            outputs={
                "annotations": annotations_count,
                "entities": entities_count,
                "confirmed_relationships": relationships_count,
                "tables_analyzed": [t.table_name for t in typed_tables],
                "tier1_annotations": column_annotations is not None,
            },
            records_processed=len(unannotated_columns),
            records_created=annotations_count + entities_count + relationships_count,
            warnings=previews,
            summary=f"{annotations_count} annotations, {entities_count} entities, {relationships_count} relationships",
        )


def _apply_semantic_overrides(
    session: Session,
    config: dict,  # type: ignore[type-arg]
    table_ids: list[str],
) -> None:
    """Apply semantic overrides from config.

    Reads all override sections under ``overrides`` in the semantic
    phase config (``semantic_roles``, ``units``, ``business_meaning``).
    Each section maps ``"table.column"`` to a dict of field values
    that are patched onto the existing SemanticAnnotation.

    The fix schemas upstream constrain which fields get written to the
    config YAML, so we trust the field names here.
    """
    overrides = config.get("overrides", {})
    if not isinstance(overrides, dict):
        return

    # Merge all override sections into a single col_ref -> fields dict.
    merged: dict[str, dict[str, object]] = {}
    for section in overrides.values():
        if not isinstance(section, dict):
            continue
        for col_ref, values in section.items():
            if not isinstance(values, dict):
                continue
            merged.setdefault(col_ref, {}).update(values)

    if not merged:
        return

    # Build column lookup: "table.column" -> column_id
    cols = session.execute(
        select(Column, Table.table_name)
        .join(Table, Column.table_id == Table.table_id)
        .where(Column.table_id.in_(table_ids))
    ).all()
    col_lookup: dict[str, str] = {}
    for col, tbl_name in cols:
        col_lookup[f"{tbl_name}.{col.column_name}"] = col.column_id

    for col_ref, field_values in merged.items():
        col_id = col_lookup.get(col_ref)
        if col_id is None:
            logger.debug("semantic_override_skip", column=col_ref, reason="not found")
            continue

        annotation = session.execute(
            select(SemanticAnnotation).where(SemanticAnnotation.column_id == col_id)
        ).scalar_one_or_none()
        if annotation is None:
            logger.debug("semantic_override_skip", column=col_ref, reason="no annotation")
            continue

        changed = False
        for field_name, value in field_values.items():
            if hasattr(annotation, field_name) and getattr(annotation, field_name) != value:
                setattr(annotation, field_name, value)
                changed = True

        if changed:
            annotation.annotation_source = "config_override"
            logger.info("semantic_override_applied", column=col_ref)

    session.flush()


def _apply_relationship_confirmations(
    session: Session,
    config: dict,  # type: ignore[type-arg]
    table_ids: list[str],
) -> None:
    """Apply confirmed_relationships overrides from the semantic phase config.

    Called after enrich_semantic so all relationships exist (including
    self-referential ones the finder can't discover). Sets is_confirmed=True
    and patches field values onto matching Relationship records.

    Keys are ``"from_table->to_table"``; values are field dicts patched
    onto ALL matching Relationship rows for that table pair.
    """
    from datetime import UTC, datetime

    from dataraum.analysis.relationships.db_models import Relationship

    overrides = config.get("overrides", {})
    if not isinstance(overrides, dict):
        return

    confirmed = overrides.get("confirmed_relationships", {})
    if not isinstance(confirmed, dict) or not confirmed:
        return

    # Build table name lookup
    tables = session.execute(select(Table).where(Table.table_id.in_(table_ids))).scalars().all()
    name_to_id = {t.table_name: t.table_id for t in tables}

    for key, field_values in confirmed.items():
        if not isinstance(field_values, dict):
            continue
        if "->" not in key:
            continue
        from_name, to_name = key.split("->", 1)
        from_tid = name_to_id.get(from_name)
        to_tid = name_to_id.get(to_name)
        if not from_tid or not to_tid:
            logger.debug("relationship_confirm_skip", key=key, reason="table not found")
            continue

        rels = (
            session.execute(
                select(Relationship).where(
                    Relationship.from_table_id == from_tid,
                    Relationship.to_table_id == to_tid,
                )
            )
            .scalars()
            .all()
        )
        if not rels:
            logger.debug("relationship_confirm_skip", key=key, reason="no relationship")
            continue

        for rel in rels:
            for field_name, value in field_values.items():
                if hasattr(rel, field_name) and getattr(rel, field_name) != value:
                    setattr(rel, field_name, value)

            if not rel.is_confirmed:
                rel.is_confirmed = True
                rel.confirmed_at = datetime.now(UTC)
                rel.confirmed_by = "config_override"
                logger.info("relationship_confirm_applied", key=key)

    session.flush()
