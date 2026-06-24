"""Semantic Agent - LLM-powered column and table analysis.

This agent follows the same pattern as graphs/agent.py:
- It extends LLMFeature from the llm module
- It depends on llm module, but llm module does not depend on it
- Used directly by analysis/semantic/processor.py

Uses Pydantic tool for structured output via Anthropic tool use API.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from dataraum.analysis.relationships.graph_topology import (
    GraphStructure,
    analyze_graph_topology,
)
from dataraum.analysis.semantic.models import (
    EntityDetection,
    Relationship,
    SemanticEnrichmentResult,
    TableSynthesisOutput,
)
from dataraum.analysis.semantic.ontology import OntologyLoader
from dataraum.analysis.semantic.utils import load_persisted_annotations
from dataraum.analysis.statistics.db_models import (
    StatisticalProfile as ColumnProfileModel,
)
from dataraum.analysis.statistics.models import (
    ColumnProfile,
    NumericStats,
    StringStats,
    ValueCount,
)
from dataraum.core.logging import get_logger
from dataraum.core.models.base import (
    ColumnRef,
    RelationshipType,
    Result,
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
    - Can be instantiated directly with LLM config, provider, renderer
    - Does not depend on LLMService facade
    """

    def __init__(
        self,
        config: LLMConfig,
        provider: LLMProvider,
        prompt_renderer: PromptRenderer,
        verticals_dir: Path | None = None,
    ) -> None:
        """Initialize semantic agent.

        Args:
            config: LLM configuration
            provider: LLM provider instance
            prompt_renderer: Prompt template renderer
            verticals_dir: Root verticals directory.
                          If None, uses config/verticals/
        """
        super().__init__(config, provider, prompt_renderer)
        self._ontology_loader = OntologyLoader(verticals_dir)

    def synthesize_tables(
        self,
        session: Session,
        table_ids: list[str],
        ontology: str = "general",
        relationship_candidates: list[dict[str, Any]] | None = None,
    ) -> Result[SemanticEnrichmentResult]:
        """Classify tables + confirm relationships over persisted column annotations.

        The per-table synthesis tier (DAT-362 Option B). Reads the already-persisted
        per-column annotations (post-teach) as read-only context and produces only
        table entity classifications + cross-table relationships — it does NOT
        re-emit per-column annotations.

        Args:
            session: Database session.
            table_ids: Table IDs to synthesize.
            ontology: Ontology name for context.
            relationship_candidates: Pre-computed relationship candidates from the
                relationships phase (TDA + join detection).

        Returns:
            Result with a ``SemanticEnrichmentResult`` carrying ``entity_detections``
            and ``relationships`` (``annotations`` is always empty).
        """
        feature_config = self.config.features.semantic_analysis
        if not feature_config.enabled:
            return Result.fail("Semantic analysis is disabled in config")

        profiles_result = self._load_profiles(session, table_ids)
        if not profiles_result.success or not profiles_result.value:
            return Result.fail(profiles_result.error if profiles_result.error else "Unknown Error")
        profiles = profiles_result.value

        sampler = DataSampler(self.config.privacy)
        samples = sampler.prepare_samples(profiles)
        tables_json = self._build_tables_json(profiles, samples)

        ontology_def = self._ontology_loader.load(ontology)
        if ontology_def is None:
            available = self._ontology_loader.list_verticals()
            return Result.fail(
                f"Vertical '{ontology}' not found. Available verticals: {available}."
            )

        graph_structure: GraphStructure | None = None
        if relationship_candidates:
            table_names_from_candidates = set()
            for cand in relationship_candidates:
                if cand.get("table1"):
                    table_names_from_candidates.add(cand["table1"])
                if cand.get("table2"):
                    table_names_from_candidates.add(cand["table2"])
            if table_names_from_candidates:
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
            "column_annotations": self._format_persisted_annotations(
                load_persisted_annotations(session, table_ids)
            ),
        }

        try:
            system_prompt, user_prompt, temperature = self.renderer.render_split(
                "semantic_per_table", context
            )
        except Exception as e:
            return Result.fail(f"Failed to render semantic_per_table prompt: {e}")

        tool = ToolDefinition(
            name="analyze_tables",
            description=(
                "Classify each table as a business entity (fact/dimension, grain, "
                "time column) and confirm relationships between tables. Do NOT "
                "annotate individual columns — those are already decided."
            ),
            input_schema=TableSynthesisOutput.model_json_schema(),
        )

        model = self.provider.get_model_for_tier(feature_config.model_tier)
        request = ConversationRequest(
            messages=[Message(role="user", content=user_prompt)],
            system=system_prompt,
            tools=[tool],
            tool_choice={"type": "tool", "name": "analyze_tables"},
            label="semantic_per_table",
            max_tokens=self.config.limits.max_output_tokens_per_request,
            temperature=temperature,
            model=model,
        )

        # converse raises a typed ProviderError on an API failure (DAT-503) —
        # retryability rides the exception to the worker's durable boundary, so
        # we don't re-wrap it. A returned Result is always a success.
        response = self.provider.converse(request).unwrap()

        if not response.tool_calls or response.tool_calls[0].name != "analyze_tables":
            return Result.fail("LLM did not use the analyze_tables tool")

        try:
            return self._parse_table_synthesis_output(response.tool_calls[0].input, response.model)
        except Exception as e:
            return Result.fail(f"Failed to parse table synthesis response: {e}")

    @staticmethod
    def _format_persisted_annotations(annotations: list[dict[str, Any]]) -> str:
        """Format persisted per-column annotations as read-only per-table context."""
        if not annotations:
            return "No prior column annotations available."

        by_table: dict[str, list[dict[str, Any]]] = {}
        for ann in annotations:
            by_table.setdefault(ann["table_name"], []).append(ann)

        lines: list[str] = []
        for table_name, cols in by_table.items():
            lines.append(f"\n### {table_name}")
            for col in cols:
                concept = col.get("business_concept") or "(none)"
                role = col.get("semantic_role") or "(unknown)"
                conf = col.get("confidence")
                conf_str = f"{conf:.2f}" if isinstance(conf, (int, float)) else "n/a"
                lines.append(
                    f"  - {col['column_name']}: role={role}, concept={concept}, "
                    f"confidence={conf_str}"
                )
        return "\n".join(lines)

    def _parse_table_synthesis_output(
        self,
        tool_output: dict[str, Any],
        model_name: str,
    ) -> Result[SemanticEnrichmentResult]:
        """Parse ``analyze_tables`` output into entities + relationships (no annotations)."""
        synthesis = TableSynthesisOutput.model_validate(tool_output)

        entity_detections = [
            EntityDetection(
                table_id="",  # Filled by caller
                table_name=table.table_name,
                entity_type=table.entity_type,
                description=table.description,
                confidence=0.9,
                grain_columns=table.grain,
                is_fact_table=table.is_fact_table,
                is_dimension_table=not table.is_fact_table,
                time_columns=table.time_columns,
                identity_columns=table.identity_columns,
            )
            for table in synthesis.tables
        ]

        relationships = []
        for rel in synthesis.relationships:
            try:
                rel_type = RelationshipType(rel.relationship_type)
            except ValueError:
                rel_type = RelationshipType.FOREIGN_KEY
            relationships.append(
                Relationship(
                    relationship_id=str(uuid4()),
                    from_table=rel.from_table,
                    from_column=rel.from_column,
                    to_table=rel.to_table,
                    to_column=rel.to_column,
                    relationship_type=rel_type,
                    cardinality=None,  # Set by processor from actual data
                    confidence=rel.confidence,
                    detection_method="llm_tool",
                    evidence={"source": "table_synthesis", "reasoning": rel.reasoning},
                )
            )

        return Result.ok(
            SemanticEnrichmentResult(
                annotations=[],
                entity_detections=entity_detections,
                relationships=relationships,
                source="llm",
            )
        )

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
                    original_name=col.original_name,
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
        self, profiles: list[ColumnProfile], samples: dict[tuple[str, str], list[Any]]
    ) -> list[dict[str, Any]]:
        """Build JSON representation of tables for prompt.

        Args:
            profiles: Column profiles
            samples: Sample values keyed by (table_name, column_name)

        Returns:
            List of table dicts for JSON serialization
        """
        # Group by table
        tables_data: dict[str, dict[str, Any]] = {}

        for profile in profiles:
            table_name = profile.column_ref.table_name
            column_name = profile.column_ref.column_name

            if table_name not in tables_data:
                tables_data[table_name] = {
                    "table_name": table_name,
                    "row_count": profile.total_count,
                    "columns": [],
                }

            col_data: dict[str, Any] = {
                "column_name": column_name,
                "distinct_count": profile.distinct_count,
                "cardinality_ratio": round(profile.cardinality_ratio, 4),  # Helps identify keys
                "sample_values": [
                    self._truncate_sample(v) for v in samples.get((table_name, column_name), [])
                ],
            }

            # Include original column name when it differs from normalized name
            if profile.original_name and profile.original_name != column_name:
                col_data["original_name"] = profile.original_name

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
