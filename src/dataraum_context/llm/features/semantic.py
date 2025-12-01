"""Semantic analysis feature - LLM-powered column and table analysis."""

import json
from typing import Any
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models.base import (
    Cardinality,
    ColumnRef,
    DecisionSource,
    RelationshipType,
    Result,
    SemanticRole,
)
from dataraum_context.enrichment.models import (
    EntityDetection,
    Relationship,
    SemanticAnnotation,
    SemanticEnrichmentResult,
)
from dataraum_context.llm.features._base import LLMFeature
from dataraum_context.llm.privacy import DataSampler
from dataraum_context.profiling.models import ColumnProfile
from dataraum_context.storage.models_v2 import Column, Ontology, Table


class SemanticAnalysisFeature(LLMFeature):
    """LLM-powered semantic analysis.

    Analyzes tables and columns to determine:
    - Semantic roles (measure, dimension, key, etc.)
    - Entity types (customer, product, transaction, etc.)
    - Business names and descriptions
    - Relationships between tables
    """

    async def analyze(
        self,
        session: AsyncSession,
        table_ids: list[str],
        ontology: str = "general",
    ) -> Result[SemanticEnrichmentResult]:
        """Analyze semantic meaning of tables and columns.

        Args:
            session: Database session
            table_ids: List of table IDs to analyze
            ontology: Ontology name to use for context

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
        ontology_data = await self._load_ontology(session, ontology)

        context = {
            "tables_json": json.dumps(tables_json, indent=2),
            "ontology_name": ontology,
            "ontology_concepts": self._format_ontology_concepts(ontology_data),
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

        # Parse response
        try:
            parsed = json.loads(response.content)
            return self._parse_semantic_response(parsed, response.model)
        except json.JSONDecodeError as e:
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

            from dataraum_context.profiling.models import (
                NumericStats,
                StringStats,
                ValueCount,
            )
            from dataraum_context.storage.models_v2 import (
                StatisticalProfile as ColumnProfileModel,
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
                numeric_stats = None
                if profile_model.min_value is not None:
                    numeric_stats = NumericStats(
                        min_value=profile_model.min_value,
                        max_value=profile_model.max_value or 0.0,
                        mean=profile_model.mean_value or 0.0,
                        stddev=profile_model.stddev_value or 0.0,
                        percentiles=profile_model.percentiles or {},
                    )

                string_stats = None
                if profile_model.min_length is not None:
                    string_stats = StringStats(
                        min_length=profile_model.min_length,
                        max_length=profile_model.max_length or 0,
                        avg_length=profile_model.avg_length or 0.0,
                    )

                # Convert top values
                top_values = []
                if profile_model.top_values:
                    for val_data in profile_model.top_values.get("values", []):
                        top_values.append(
                            ValueCount(
                                value=val_data.get("value"),
                                count=val_data.get("count", 0),
                                percentage=val_data.get("percentage", 0.0),
                            )
                        )

                # Convert detected patterns
                # TODO: this should be implemented
                _detected_patterns: list[str] = []
                # Note: patterns are stored in type_candidates table in Phase 2B
                # For now, leave empty - will be populated when profiling is integrated

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
                    detected_patterns=[],
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
                        detected_patterns=[],
                    )
                    profiles.append(profile)

            return Result.ok(profiles)

        except Exception as e:
            return Result.fail(f"Failed to load profiles: {e}")

    async def _load_ontology(self, session: AsyncSession, ontology_name: str) -> dict[str, Any]:
        """Load ontology from database.

        Args:
            session: Database session
            ontology_name: Ontology name

        Returns:
            Ontology data dict
        """
        try:
            stmt = select(Ontology).where(Ontology.name == ontology_name)
            result = await session.execute(stmt)
            ontology = result.scalar_one_or_none()

            if ontology:
                return {
                    "name": ontology.name,
                    "concepts": ontology.concepts or {},
                    "metrics": ontology.metrics or {},
                }
            else:
                # Return empty ontology
                return {"name": ontology_name, "concepts": {}, "metrics": {}}

        except Exception:
            return {"name": ontology_name, "concepts": {}, "metrics": {}}

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
                "null_ratio": profile.null_ratio,
                "distinct_count": profile.distinct_count,
                "sample_values": samples.get(profile.column_ref.column_name, []),
            }

            # Add numeric stats if available
            if profile.numeric_stats:
                col_data["min"] = profile.numeric_stats.min_value
                col_data["max"] = profile.numeric_stats.max_value
                col_data["mean"] = profile.numeric_stats.mean

            # Add detected patterns
            if profile.detected_patterns:
                col_data["patterns"] = [p.name for p in profile.detected_patterns]

            tables_data[table_name]["columns"].append(col_data)

        return list(tables_data.values())

    def _format_ontology_concepts(self, ontology_data: dict[str, Any]) -> str:
        """Format ontology concepts for prompt.

        Args:
            ontology_data: Ontology data dict

        Returns:
            Formatted string describing concepts
        """
        concepts = ontology_data.get("concepts", {})

        if not concepts:
            return "No specific ontology concepts defined"

        # Format as bullet list
        lines = []
        for concept_name, concept_data in concepts.items():
            if isinstance(concept_data, dict):
                indicators = concept_data.get("indicators", [])
                lines.append(f"- {concept_name}: {', '.join(indicators)}")
            else:
                lines.append(f"- {concept_name}")

        return "\n".join(lines)

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
                    evidence={"source": "semantic_analysis"},
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
