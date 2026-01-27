"""Semantic Agent - LLM-powered column and table analysis.

This agent follows the same pattern as graphs/agent.py:
- It extends LLMFeature from the llm module
- It depends on llm module, but llm module does not depend on it
- Used directly by analysis/semantic/processor.py

Uses Pydantic tool for structured output via Anthropic tool use API.
"""

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.relationships.graph_topology import (
    GraphStructure,
    analyze_graph_topology,
)
from dataraum.analysis.semantic.models import (
    EntityDetection,
    Relationship,
    SemanticAnalysisOutput,
    SemanticAnnotation,
    SemanticEnrichmentResult,
)
from dataraum.analysis.semantic.ontology import OntologyLoader
from dataraum.analysis.semantic.utils import load_correlations_for_semantic
from dataraum.analysis.statistics.models import ColumnProfile
from dataraum.core.logging import get_logger
from dataraum.core.models.base import (
    Cardinality,
    ColumnRef,
    DecisionSource,
    RelationshipType,
    Result,
    SemanticRole,
)
from dataraum.llm.features._base import LLMFeature
from dataraum.llm.privacy import DataSampler
from dataraum.llm.providers.base import (
    ConversationRequest,
    Message,
    ToolDefinition,
)
from dataraum.storage import Column, Table

if TYPE_CHECKING:
    from dataraum.llm.cache import LLMCache
    from dataraum.llm.config import LLMConfig
    from dataraum.llm.prompts import PromptRenderer
    from dataraum.llm.providers.base import LLMProvider

logger = get_logger(__name__)


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

    def analyze(
        self,
        session: Session,
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
        profiles_result = self._load_profiles(session, table_ids)
        if not profiles_result.success or not profiles_result.value:
            return Result.fail(profiles_result.error if profiles_result.error else "Unknown Error")

        profiles = profiles_result.value

        # Prepare sample data with privacy controls
        sampler = DataSampler(self.config.privacy)
        samples = sampler.prepare_samples(profiles)

        # Load within-table correlation data (if available from Phase 4b)
        correlations = load_correlations_for_semantic(session, table_ids)

        # Log correlation context usage
        if correlations:
            total_fds = sum(
                len(d.get("functional_dependencies", [])) for d in correlations.values()
            )
            total_corrs = sum(len(d.get("numeric_correlations", [])) for d in correlations.values())
            total_derived = sum(len(d.get("derived_columns", [])) for d in correlations.values())
            if total_fds or total_corrs or total_derived:
                logger.info(
                    "within_table_correlations",
                    functional_dependencies=total_fds,
                    numeric_correlations=total_corrs,
                    derived_columns=total_derived,
                )
            else:
                logger.debug("no_significant_correlations")
        else:
            logger.debug("no_correlation_data")

        # Build context for prompt
        tables_json = self._build_tables_json(profiles, samples)
        ontology_def = self._ontology_loader.load(ontology)

        # Ontology is required for business_concept mapping
        if ontology_def is None:
            available = self._ontology_loader.list_ontologies()
            return Result.fail(
                f"Ontology '{ontology}' not found. "
                f"Available ontologies: {available}. "
                f"Create config/ontologies/{ontology}.yaml or use an existing ontology."
            )

        # Compute graph topology from relationship candidates
        graph_structure: GraphStructure | None = None
        if relationship_candidates:
            # Extract table names from candidates
            table_names_from_candidates = set()
            for cand in relationship_candidates:
                if cand.get("table1"):
                    table_names_from_candidates.add(cand["table1"])
                if cand.get("table2"):
                    table_names_from_candidates.add(cand["table2"])

            if table_names_from_candidates:
                # Use table names as IDs since candidates use names
                graph_structure = analyze_graph_topology(
                    table_ids=list(table_names_from_candidates),
                    relationships=relationship_candidates,
                )

        context = {
            "tables_json": json.dumps(tables_json),
            "ontology_name": ontology,
            "ontology_concepts": self._ontology_loader.format_concepts_for_prompt(ontology_def),
            "relationship_candidates": self._format_relationship_candidates(
                relationship_candidates, graph_structure=graph_structure
            ),
            "within_table_correlations": self._format_correlations(correlations),
        }

        # Render prompt with system/user split
        try:
            system_prompt, user_prompt, temperature = self.renderer.render_split(
                "semantic_analysis", context
            )
        except Exception as e:
            return Result.fail(f"Failed to render prompt: {e}")

        # Create tool definition from Pydantic model
        tool = self._create_tool_definition()

        # Call LLM with tool use
        response_result = self._call_with_tool(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            tool=tool,
            temperature=temperature,
            model_tier=feature_config.model_tier,
        )

        if not response_result.success or not response_result.value:
            return Result.fail(response_result.error or "Unknown Error")

        tool_output, model_name = response_result.value

        try:
            # Parse tool output into our internal models
            return self._parse_tool_output(tool_output, model_name)
        except Exception as e:
            return Result.fail(f"Failed to parse semantic response: {e}")

    def _load_profiles(self, session: Session, table_ids: list[str]) -> Result[list[ColumnProfile]]:
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

            from dataraum.analysis.statistics.db_models import (
                StatisticalProfile as ColumnProfileModel,
            )
            from dataraum.analysis.statistics.models import (
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

            result = session.execute(stmt)
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
                placeholder_result = session.execute(placeholder_stmt)
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

    def _format_relationship_candidates(
        self,
        candidates: list[dict[str, Any]] | None,
        *,
        graph_structure: GraphStructure | None = None,
    ) -> str:
        """Format relationship candidates for the prompt.

        Args:
            candidates: List of relationship candidates from analysis/relationships
            graph_structure: Optional graph topology analysis result.
                When provided, a compact topology summary is prepended.

        Returns:
            Formatted string for the prompt
        """
        lines: list[str] = []

        # Prepend compact topology summary if available
        if graph_structure is not None:
            lines.append(
                f"Topology: {graph_structure.pattern} — {graph_structure.pattern_description}"
            )
            role_parts: list[str] = []
            if graph_structure.hub_tables:
                role_parts.append(f"hubs: {', '.join(graph_structure.hub_tables)}")
            if graph_structure.leaf_tables:
                role_parts.append(f"leaves: {', '.join(graph_structure.leaf_tables)}")
            if graph_structure.bridge_tables:
                role_parts.append(f"bridges: {', '.join(graph_structure.bridge_tables)}")
            if graph_structure.isolated_tables:
                role_parts.append(f"isolated: {', '.join(graph_structure.isolated_tables)}")
            if role_parts:
                lines.append("Roles: " + "; ".join(role_parts))
            if graph_structure.schema_cycles:
                cycle_strs = [
                    " → ".join(c.tables) + " → " + c.tables[0]
                    for c in graph_structure.schema_cycles[:5]
                ]
                lines.append(f"Cycles: {'; '.join(cycle_strs)}")
            lines.append("")

        if not candidates:
            lines.append("No pre-computed relationship candidates available.")
            return "\n".join(lines)

        _MAX_JOIN_COLS = 10

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
                # Sort by confidence descending, take top N
                sorted_cols = sorted(
                    join_cols,
                    key=lambda jc: jc.get("join_confidence", 0.0),
                    reverse=True,
                )
                total_cols = len(sorted_cols)
                display_cols = sorted_cols[:_MAX_JOIN_COLS]

                if total_cols > _MAX_JOIN_COLS:
                    lines.append(f"  (showing top {_MAX_JOIN_COLS} of {total_cols} candidates)")

                for jc in display_cols:
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

    def _format_correlations(self, correlations: dict[str, dict[str, Any]]) -> str:
        """Format within-table correlation data for the prompt.

        Args:
            correlations: Dict mapping table_name to correlation data

        Returns:
            Formatted string for the prompt
        """
        if not correlations:
            return "No within-table correlation data available."

        # Check if we have any actual data
        has_data = False
        for table_data in correlations.values():
            if (
                table_data.get("functional_dependencies")
                or table_data.get("numeric_correlations")
                or table_data.get("derived_columns")
            ):
                has_data = True
                break

        if not has_data:
            return "No significant within-table correlations detected."

        lines = []

        for table_name, data in correlations.items():
            fds = data.get("functional_dependencies", [])
            corrs = data.get("numeric_correlations", [])
            derived = data.get("derived_columns", [])

            if not fds and not corrs and not derived:
                continue

            lines.append(f"\n### {table_name}")

            # Functional dependencies (key indicators)
            if fds:
                lines.append("Functional dependencies (determinant → dependent):")
                for fd in fds:
                    det = ", ".join(fd["determinant"])
                    dep = fd["dependent"]
                    conf = fd["confidence"]
                    lines.append(f"  - {det} → {dep} (conf: {conf:.2f})")

            # Strong numeric correlations
            if corrs:
                lines.append("Strong numeric correlations:")
                for c in corrs:
                    r = c.get("pearson_r") or c.get("spearman_rho") or 0
                    lines.append(
                        f"  - {c['column1']} <-> {c['column2']}: r={r:.2f} ({c['strength']})"
                    )

            # Derived columns
            if derived:
                lines.append("Derived columns:")
                for d in derived:
                    lines.append(
                        f"  - {d['derived_column']} = {d['formula']} (match: {d['match_rate']:.0%})"
                    )

        return "\n".join(lines) if lines else "No significant within-table correlations detected."

    @staticmethod
    def _truncate_sample(value: Any, max_length: int = 100) -> Any:
        """Truncate a sample value if it exceeds max_length.

        Args:
            value: Sample value (any type)
            max_length: Maximum string length before truncation

        Returns:
            Original value or truncated string
        """
        if isinstance(value, str) and len(value) > max_length:
            return value[:max_length] + "..."
        return value

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

            col_data: dict[str, Any] = {
                "column_name": profile.column_ref.column_name,
                "distinct_count": profile.distinct_count,
                "cardinality_ratio": round(profile.cardinality_ratio, 4),  # Helps identify keys
                "sample_values": [
                    self._truncate_sample(v)
                    for v in samples.get(profile.column_ref.column_name, [])
                ],
            }

            # Only include null_ratio when non-zero to save tokens
            null_ratio = round(profile.null_ratio, 4)
            if null_ratio > 0.0:
                col_data["null_ratio"] = null_ratio

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

    def _create_tool_definition(self) -> ToolDefinition:
        """Create tool definition from SemanticAnalysisOutput Pydantic model.

        Returns:
            ToolDefinition with JSON schema from the Pydantic model
        """
        # Generate JSON schema from Pydantic model
        schema = SemanticAnalysisOutput.model_json_schema()

        return ToolDefinition(
            name="analyze_schema",
            description=(
                "Provide semantic analysis results for the database schema. "
                "Analyze each table and column, map to business concepts, "
                "and identify relationships."
            ),
            input_schema=schema,
        )

    def _call_with_tool(
        self,
        system_prompt: str | None,
        user_prompt: str,
        tool: ToolDefinition,
        temperature: float,
        model_tier: str,
    ) -> Result[tuple[dict[str, Any], str]]:
        """Call LLM with tool use and extract tool output.

        Args:
            system_prompt: System message (role/instructions)
            user_prompt: User message (data/context)
            tool: Tool definition for structured output
            temperature: Sampling temperature
            model_tier: Model tier to use

        Returns:
            Result containing (tool_output_dict, model_name)
        """
        # Get model for tier
        model = self.provider.get_model_for_tier(model_tier)

        # Build conversation request
        request = ConversationRequest(
            messages=[Message(role="user", content=user_prompt)],
            system=system_prompt,
            tools=[tool],
            max_tokens=8192,  # Semantic analysis can produce large output
            temperature=temperature,
            model=model,
        )

        # Call LLM
        response_result = self.provider.converse(request)

        if not response_result.success or not response_result.value:
            return Result.fail(response_result.error or "LLM call failed")

        response = response_result.value

        # Extract tool call result
        if not response.tool_calls:
            # LLM didn't use the tool - try to parse text response as fallback
            if response.content:
                try:
                    parsed = json.loads(response.content)
                    return Result.ok((parsed, response.model))
                except json.JSONDecodeError:
                    pass
            return Result.fail(
                f"LLM did not use the analyze_schema tool. Response: {response.content[:500]}"
            )

        # Get the first tool call (should be analyze_schema)
        tool_call = response.tool_calls[0]
        if tool_call.name != "analyze_schema":
            return Result.fail(f"Unexpected tool call: {tool_call.name}")

        return Result.ok((tool_call.input, response.model))

    def _parse_tool_output(
        self, tool_output: dict[str, Any], model_name: str
    ) -> Result[SemanticEnrichmentResult]:
        """Parse tool output into SemanticEnrichmentResult.

        Args:
            tool_output: Raw tool output dict from LLM
            model_name: Model that generated the response

        Returns:
            Result containing SemanticEnrichmentResult
        """
        try:
            # Validate with Pydantic model
            analysis = SemanticAnalysisOutput.model_validate(tool_output)

            annotations = []
            entity_detections = []
            relationships = []

            # Convert tool output to internal models
            for table in analysis.tables:
                # Create entity detection
                entity = EntityDetection(
                    table_id="",  # Filled by caller
                    table_name=table.table_name,
                    entity_type=table.entity_type,
                    description=table.description,
                    confidence=0.9,  # Tool-based output has higher confidence
                    evidence={},
                    grain_columns=table.grain,
                    is_fact_table=table.is_fact_table,
                    is_dimension_table=not table.is_fact_table,
                    time_column=table.time_column,
                )
                entity_detections.append(entity)

                # Parse column annotations
                for col in table.columns:
                    try:
                        semantic_role = SemanticRole(col.semantic_role)
                    except ValueError:
                        semantic_role = SemanticRole.UNKNOWN

                    annotation = SemanticAnnotation(
                        column_id="",  # Filled by caller
                        column_ref=ColumnRef(
                            table_name=table.table_name,
                            column_name=col.column_name,
                        ),
                        semantic_role=semantic_role,
                        entity_type=col.entity_type,
                        business_name=col.business_term,
                        business_description=col.description,
                        business_concept=col.business_concept,
                        annotation_source=DecisionSource.LLM,
                        annotated_by=model_name,
                        confidence=col.confidence,
                    )
                    annotations.append(annotation)

            # Parse relationships
            for rel in analysis.relationships:
                try:
                    rel_type = RelationshipType(rel.relationship_type)
                except ValueError:
                    rel_type = RelationshipType.FOREIGN_KEY

                cardinality = None
                if rel.cardinality == "one_to_one":
                    cardinality = Cardinality.ONE_TO_ONE
                elif rel.cardinality == "one_to_many":
                    cardinality = Cardinality.ONE_TO_MANY
                elif rel.cardinality == "many_to_one":
                    cardinality = Cardinality.ONE_TO_MANY  # Flip perspective
                elif rel.cardinality == "many_to_many":
                    cardinality = Cardinality.MANY_TO_MANY

                relationship = Relationship(
                    relationship_id=str(uuid4()),
                    from_table=rel.from_table,
                    from_column=rel.from_column,
                    to_table=rel.to_table,
                    to_column=rel.to_column,
                    relationship_type=rel_type,
                    cardinality=cardinality,
                    confidence=rel.confidence,
                    detection_method="llm_tool",
                    evidence={"source": "semantic_analysis", "reasoning": rel.reasoning},
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
            return Result.fail(f"Failed to parse tool output: {e}")
