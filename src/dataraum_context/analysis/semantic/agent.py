"""Semantic Agent - LLM-powered column and table analysis.

This agent follows the same pattern as graphs/agent.py:
- It extends LLMFeature from the llm module
- It depends on llm module, but llm module does not depend on it
- Used directly by analysis/semantic/processor.py
"""

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.analysis.semantic.models import (
    EntityDetection,
    Relationship,
    SemanticAnnotation,
    SemanticEnrichmentResult,
)
from dataraum_context.analysis.semantic.ontology import OntologyLoader
from dataraum_context.analysis.statistics.models import ColumnProfile
from dataraum_context.core.models.base import (
    Cardinality,
    ColumnRef,
    DecisionSource,
    RelationshipType,
    Result,
    SemanticRole,
)
from dataraum_context.llm.features._base import LLMFeature
from dataraum_context.llm.privacy import DataSampler
from dataraum_context.storage import Column, Table

if TYPE_CHECKING:
    from dataraum_context.llm.cache import LLMCache
    from dataraum_context.llm.config import LLMConfig
    from dataraum_context.llm.prompts import PromptRenderer
    from dataraum_context.llm.providers.base import LLMProvider


class SemanticAgent(LLMFeature):
    """LLM-powered semantic analysis agent.

    Analyzes tables and columns to determine:
    - Semantic roles (measure, dimension, key, etc.)
    - Entity types (customer, product, transaction, etc.)
    - Business names and descriptions
    - Relationships between tables

    This agent follows the same pattern as GraphAgent:
    - Extends LLMFeature for LLM infrastructure access
    - Can be instantiated directly with LLM config, provider, renderer, cache
    - Does not depend on LLMService facade
    """

    def __init__(
        self,
        config: LLMConfig,
        provider: LLMProvider,
        prompt_renderer: PromptRenderer,
        cache: LLMCache,
        ontologies_dir: Path | None = None,
    ) -> None:
        """Initialize semantic agent.

        Args:
            config: LLM configuration
            provider: LLM provider instance
            prompt_renderer: Prompt template renderer
            cache: Response cache
            ontologies_dir: Directory containing ontology YAML files.
                          If None, uses config/ontologies/
        """
        super().__init__(config, provider, prompt_renderer, cache)
        self._ontology_loader = OntologyLoader(ontologies_dir)

    async def analyze(
        self,
        session: AsyncSession,
        table_ids: list[str],
        ontology: str = "general",
        relationship_candidates: list[dict[str, Any]] | None = None,
    ) -> Result[SemanticEnrichmentResult]:
        """Analyze semantic meaning of tables and columns.

        Args:
            session: Database session
            table_ids: List of table IDs to analyze
            ontology: Ontology name to use for context
            relationship_candidates: Pre-computed relationship candidates from
                analysis/relationships module. Each candidate contains:
                - table1, table2: Table names
                - join_columns: List of column pairs with confidence scores
                - topology_similarity: TDA-based structural similarity

        Returns:
            Result containing SemanticEnrichmentResult or error
        """
        # Check if feature is enabled
        feature_config = self.config.features.semantic_analysis
        if not feature_config.enabled:
            return Result.fail("Semantic analysis is disabled in config")

        # Load column profiles from metadata
        profiles_result = await self._load_profiles(session, table_ids)
        if not profiles_result.success or not profiles_result.value:
            return Result.fail(profiles_result.error if profiles_result.error else "Unknown Error")

        profiles = profiles_result.value

        # Prepare sample data with privacy controls
        sampler = DataSampler(self.config.privacy)
        samples = sampler.prepare_samples(profiles)

        # Build context for prompt
        tables_json = self._build_tables_json(profiles, samples)
        ontology_def = self._ontology_loader.load(ontology)

        context = {
            "tables_json": json.dumps(tables_json, indent=2),
            "ontology_name": ontology,
            "ontology_concepts": self._ontology_loader.format_concepts_for_prompt(ontology_def),
            "relationship_candidates": self._format_relationship_candidates(
                relationship_candidates
            ),
        }

        # Render prompt
        try:
            prompt, temperature = self.renderer.render("semantic_analysis", context)
        except Exception as e:
            return Result.fail(f"Failed to render prompt: {e}")

        # Call LLM
        response_result = await self._call_llm(
            session=session,
            feature_name="semantic_analysis",
            prompt=prompt,
            temperature=temperature,
            model_tier=feature_config.model_tier,
            table_ids=table_ids,
            ontology=ontology,
        )

        if not response_result.success or not response_result.value:
            return Result.fail(response_result.error if response_result.error else "Unknown Error")

        response = response_result.value

        try:
            parsed = json.loads(response.content)
            return self._parse_semantic_response(parsed, response.model)
        except json.JSONDecodeError as e:
            # Log the problematic content for debugging
            return Result.fail(f"Failed to parse LLM response as JSON: {e}")
        except Exception as e:
            return Result.fail(f"Failed to parse semantic response: {e}")

    async def _load_profiles(
        self, session: AsyncSession, table_ids: list[str]
    ) -> Result[list[ColumnProfile]]:
        """Load column profiles from metadata.

        Args:
            session: Database session
            table_ids: Table IDs

        Returns:
            Result containing list of column profiles
        """
        try:
            # Get latest profile for each column in these tables
            # We use a subquery to get the most recent profile per column
            from sqlalchemy import func

            from dataraum_context.analysis.statistics.db_models import (
                StatisticalProfile as ColumnProfileModel,
            )
            from dataraum_context.analysis.statistics.models import (
                NumericStats,
                StringStats,
                ValueCount,
            )

            subq = (
                select(
                    ColumnProfileModel.column_id,
                    func.max(ColumnProfileModel.profiled_at).label("max_profiled_at"),
                )
                .join(Column)
                .join(Table)
                .where(Table.table_id.in_(table_ids))
                .group_by(ColumnProfileModel.column_id)
                .subquery()
            )

            stmt = (
                select(ColumnProfileModel, Column, Table)
                .join(Column, ColumnProfileModel.column_id == Column.column_id)
                .join(Table, Column.table_id == Table.table_id)
                .join(
                    subq,
                    (ColumnProfileModel.column_id == subq.c.column_id)
                    & (ColumnProfileModel.profiled_at == subq.c.max_profiled_at),
                )
                .where(Table.table_id.in_(table_ids))
            )

            result = await session.execute(stmt)
            rows = result.all()

            profiles = []
            for profile_model, col, table in rows:
                # Convert storage model to core model
                # StatisticalProfile uses hybrid storage: stats are in profile_data JSONB field
                profile_data = profile_model.profile_data or {}

                numeric_stats = None
                numeric_data = profile_data.get("numeric_stats")
                if numeric_data is not None:
                    numeric_stats = NumericStats(
                        min_value=numeric_data.get("min", 0.0),
                        max_value=numeric_data.get("max", 0.0),
                        mean=numeric_data.get("mean", 0.0),
                        stddev=numeric_data.get("std", 0.0),
                        percentiles=numeric_data.get("percentiles", {}),
                    )

                string_stats = None
                string_data = profile_data.get("string_stats")
                if string_data is not None:
                    string_stats = StringStats(
                        min_length=string_data.get("min_length", 0),
                        max_length=string_data.get("max_length", 0),
                        avg_length=string_data.get("avg_length", 0.0),
                    )

                # Convert top values
                top_values = []
                top_values_data = profile_data.get("top_values")
                if top_values_data:
                    for val_data in top_values_data:
                        top_values.append(
                            ValueCount(
                                value=val_data.get("value"),
                                count=val_data.get("count", 0),
                                percentage=val_data.get("percentage", 0.0),
                            )
                        )

                # Note: patterns are stored in SchemaProfileResult.detected_patterns
                # and are only available during schema profiling, not statistics profiling

                profile = ColumnProfile(
                    column_id=col.column_id,
                    column_ref=ColumnRef(table_name=table.table_name, column_name=col.column_name),
                    profiled_at=profile_model.profiled_at,
                    total_count=profile_model.total_count,
                    null_count=profile_model.null_count,
                    distinct_count=profile_model.distinct_count or 0,
                    null_ratio=profile_model.null_ratio or 0.0,
                    cardinality_ratio=profile_model.cardinality_ratio or 0.0,
                    numeric_stats=numeric_stats,
                    string_stats=string_stats,
                    top_values=top_values,
                )
                profiles.append(profile)

            if not profiles:
                # If no profiles found, create placeholder profiles
                # This allows semantic analysis to work even without profiling
                placeholder_stmt = (
                    select(Column, Table).join(Table).where(Table.table_id.in_(table_ids))
                )
                placeholder_result = await session.execute(placeholder_stmt)
                placeholder_rows = placeholder_result.all()

                for col, table in placeholder_rows:
                    profile = ColumnProfile(
                        column_id=col.column_id,
                        column_ref=ColumnRef(
                            table_name=table.table_name, column_name=col.column_name
                        ),
                        profiled_at=table.created_at,
                        total_count=table.row_count or 0,
                        null_count=0,
                        distinct_count=0,
                        null_ratio=0.0,
                        cardinality_ratio=0.0,
                        top_values=[],
                    )
                    profiles.append(profile)

            return Result.ok(profiles)

        except Exception as e:
            return Result.fail(f"Failed to load profiles: {e}")

    def _format_relationship_candidates(self, candidates: list[dict[str, Any]] | None) -> str:
        """Format relationship candidates for the prompt.

        Args:
            candidates: List of relationship candidates from analysis/relationships

        Returns:
            Formatted string for the prompt
        """
        if not candidates:
            return "No pre-computed relationship candidates available."

        lines = []
        for rel in candidates:
            table1 = rel.get("table1", "?")
            table2 = rel.get("table2", "?")

            lines.append(f"\n### {table1} <-> {table2}")

            # Add relationship-level evaluation metrics if available
            join_success = rel.get("join_success_rate")
            introduces_dups = rel.get("introduces_duplicates")
            if join_success is not None:
                lines.append(f"Join success rate: {join_success:.1f}%")
            if introduces_dups is not None:
                lines.append(f"Introduces duplicates (fan trap): {introduces_dups}")

            lines.append("Column pairs with value overlap:")

            join_cols = rel.get("join_columns", [])
            if not join_cols:
                lines.append("  (none detected)")
            else:
                for jc in join_cols:
                    col1 = jc.get("column1", "?")
                    col2 = jc.get("column2", "?")
                    join_conf = jc.get("join_confidence", 0.0)
                    card = jc.get("cardinality", "unknown")

                    # Basic info with value overlap score
                    line = f"  - {col1} <-> {col2}: overlap={join_conf:.2f} ({card})"

                    # Add uniqueness ratios (helps identify keys vs measures)
                    left_uniq = jc.get("left_uniqueness")
                    right_uniq = jc.get("right_uniqueness")
                    if left_uniq is not None and right_uniq is not None:
                        line += f" [uniq: L={left_uniq:.2f} R={right_uniq:.2f}]"

                    # Add evaluation metrics if available
                    left_ri = jc.get("left_referential_integrity")
                    right_ri = jc.get("right_referential_integrity")
                    orphans = jc.get("orphan_count")
                    verified = jc.get("cardinality_verified")

                    metrics = []
                    if left_ri is not None and right_ri is not None:
                        metrics.append(f"RI: L={left_ri:.0f}% R={right_ri:.0f}%")
                    if orphans is not None and orphans > 0:
                        metrics.append(f"orphans={orphans}")
                    if verified is not None:
                        metrics.append(f"verified={verified}")

                    if metrics:
                        line += f" [{', '.join(metrics)}]"

                    lines.append(line)

        return "\n".join(lines)

    def _build_tables_json(
        self, profiles: list[ColumnProfile], samples: dict[str, list[Any]]
    ) -> list[dict[str, Any]]:
        """Build JSON representation of tables for prompt.

        Args:
            profiles: Column profiles
            samples: Sample values per column

        Returns:
            List of table dicts for JSON serialization
        """
        # Group by table
        tables_data: dict[str, dict[str, Any]] = {}

        for profile in profiles:
            table_name = profile.column_ref.table_name

            if table_name not in tables_data:
                tables_data[table_name] = {
                    "table_name": table_name,
                    "row_count": profile.total_count,
                    "columns": [],
                }

            col_data = {
                "column_name": profile.column_ref.column_name,
                "null_ratio": round(profile.null_ratio, 4),
                "distinct_count": profile.distinct_count,
                "cardinality_ratio": round(profile.cardinality_ratio, 4),  # Helps identify keys
                "sample_values": samples.get(profile.column_ref.column_name, []),
            }

            # Add numeric stats if available
            if profile.numeric_stats:
                col_data["min"] = profile.numeric_stats.min_value
                col_data["max"] = profile.numeric_stats.max_value
                col_data["mean"] = round(profile.numeric_stats.mean, 4)

            # Add string stats if available
            if profile.string_stats:
                col_data["avg_length"] = round(profile.string_stats.avg_length, 1)

            tables_data[table_name]["columns"].append(col_data)

        return list(tables_data.values())

    def _parse_semantic_response(
        self, parsed: dict[str, Any], model_name: str
    ) -> Result[SemanticEnrichmentResult]:
        """Parse LLM response into structured result.

        Args:
            parsed: Parsed JSON response
            model_name: Model that generated the response

        Returns:
            Result containing SemanticEnrichmentResult
        """
        try:
            annotations = []
            entity_detections = []
            relationships = []

            # Parse table-level entities
            for table_data in parsed.get("tables", []):
                # Create entity detection
                entity = EntityDetection(
                    table_id="",  # Filled by caller
                    table_name=table_data["table_name"],
                    entity_type=table_data.get("entity_type", "unknown"),
                    description=table_data.get("description"),
                    confidence=0.8,
                    evidence={},
                    grain_columns=table_data.get("grain", []),
                    is_fact_table=table_data.get("is_fact_table", False),
                    is_dimension_table=not table_data.get("is_fact_table", False),
                    time_column=table_data.get("time_column"),
                )
                entity_detections.append(entity)

                # Parse column annotations
                for col_data in table_data.get("columns", []):
                    # Parse semantic role
                    role_str = col_data.get("semantic_role", "unknown")
                    try:
                        semantic_role = SemanticRole(role_str)
                    except ValueError:
                        semantic_role = SemanticRole.UNKNOWN

                    annotation = SemanticAnnotation(
                        column_id="",  # Filled by caller
                        column_ref=ColumnRef(
                            table_name=table_data["table_name"],
                            column_name=col_data["column_name"],
                        ),
                        semantic_role=semantic_role,
                        entity_type=col_data.get("entity_type"),
                        business_name=col_data.get("business_term"),
                        business_description=col_data.get("description"),
                        annotation_source=DecisionSource.LLM,
                        annotated_by=model_name,
                        confidence=col_data.get("confidence", 0.8),
                    )
                    annotations.append(annotation)

            # Parse relationships
            for rel_data in parsed.get("relationships", []):
                # Parse relationship type
                rel_type_str = rel_data.get("relationship_type", "foreign_key")
                try:
                    rel_type = RelationshipType(rel_type_str)
                except ValueError:
                    rel_type = RelationshipType.FOREIGN_KEY

                # Parse cardinality
                card_str = rel_data.get("cardinality", "many_to_one")
                cardinality = None
                if card_str == "one_to_one":
                    cardinality = Cardinality.ONE_TO_ONE
                elif card_str == "one_to_many":
                    cardinality = Cardinality.ONE_TO_MANY
                elif card_str == "many_to_one":
                    cardinality = Cardinality.ONE_TO_MANY  # Flip perspective
                elif card_str == "many_to_many":
                    cardinality = Cardinality.MANY_TO_MANY

                # Build evidence dict with reasoning if provided
                evidence = {"source": "semantic_analysis"}
                if "reasoning" in rel_data:
                    evidence["reasoning"] = rel_data["reasoning"]

                relationship = Relationship(
                    relationship_id=str(uuid4()),
                    from_table=rel_data["from_table"],
                    from_column=rel_data["from_column"],
                    to_table=rel_data["to_table"],
                    to_column=rel_data["to_column"],
                    relationship_type=rel_type,
                    cardinality=cardinality,
                    confidence=rel_data.get("confidence", 0.8),
                    detection_method="llm",
                    evidence=evidence,
                )
                relationships.append(relationship)

            return Result.ok(
                SemanticEnrichmentResult(
                    annotations=annotations,
                    entity_detections=entity_detections,
                    relationships=relationships,
                    source="llm",
                )
            )

        except Exception as e:
            return Result.fail(f"Failed to parse semantic response: {e}")
