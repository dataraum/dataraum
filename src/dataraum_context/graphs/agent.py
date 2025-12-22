"""Unified Graph Agent for SQL generation and execution.

This agent handles ALL graph types (filters and metrics) through a unified approach:
1. Load graph specification (YAML with accounting context)
2. Analyze actual data schema (columns, types, samples)
3. Use LLM to generate executable SQL
4. Cache generated SQL for deterministic re-execution (in-memory + DB)
5. Execute SQL and capture results

See docs/CALCULATION_ENGINE_DESIGN.md for full architecture.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import duckdb
import yaml
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models.base import Result
from dataraum_context.graphs.db_models import GeneratedCodeRecord
from dataraum_context.llm.cache import LLMCache
from dataraum_context.llm.config import LLMConfig
from dataraum_context.llm.features._base import LLMFeature
from dataraum_context.llm.prompts import PromptRenderer
from dataraum_context.llm.providers.base import LLMProvider

from .models import (
    DatasetSchemaMapping,
    GraphExecution,
    StepResult,
    StepType,
    TransformationGraph,
)


@dataclass
class GeneratedCode:
    """LLM-generated SQL for a specific graph + schema combination."""

    code_id: str
    graph_id: str
    graph_version: str
    schema_mapping_id: str

    # Generated SQL
    steps: list[dict[str, str]]  # List of {step_id, sql, description}
    final_sql: str
    column_mappings: dict[str, str]  # abstract_field -> concrete_column

    # Generation metadata
    llm_model: str
    prompt_hash: str
    generated_at: datetime

    # Validation
    is_validated: bool = False
    validation_errors: list[str] = field(default_factory=list)


@dataclass
class ExecutionContext:
    """Context for graph execution."""

    duckdb_conn: duckdb.DuckDBPyConnection
    table_name: str
    schema_mapping: DatasetSchemaMapping | None = None
    schema_mapping_id: str | None = None
    period: str | None = None
    is_period_final: bool = False
    filter_execution_id: str | None = None

    # Rich metadata context (optional)
    # When provided, gives the LLM additional information about:
    # - Column semantics (roles, entity types)
    # - Statistical profiles (null ratios, outliers)
    # - Table relationships and topology
    # - Quality flags
    rich_context: Any | None = None  # GraphExecutionContext from graphs.context

    @classmethod
    async def with_rich_context(
        cls,
        session: Any,  # AsyncSession
        duckdb_conn: duckdb.DuckDBPyConnection,
        table_name: str,
        table_ids: list[str],
        **kwargs: Any,
    ) -> ExecutionContext:
        """Create ExecutionContext with rich metadata loaded from analysis modules.

        This is the recommended way to create an ExecutionContext when you want
        the LLM to have access to semantic, statistical, and relational metadata.

        Args:
            session: SQLAlchemy async session
            duckdb_conn: DuckDB connection for queries
            table_name: Primary table name for execution
            table_ids: List of table IDs to include in context
            **kwargs: Additional ExecutionContext arguments

        Returns:
            ExecutionContext with rich_context populated
        """
        from dataraum_context.graphs.context import build_execution_context

        rich_context = await build_execution_context(
            session=session,
            table_ids=table_ids,
            duckdb_conn=duckdb_conn,
            slice_column=kwargs.pop("slice_column", None),
            slice_value=kwargs.pop("slice_value", None),
        )

        return cls(
            duckdb_conn=duckdb_conn,
            table_name=table_name,
            rich_context=rich_context,
            **kwargs,
        )


@dataclass
class TableSchema:
    """Schema information for a table."""

    table_name: str
    columns: list[dict[str, Any]]  # name, type, sample_values
    row_count: int


class GraphAgent(LLMFeature):
    """Unified agent for executing any graph type.

    The agent:
    1. Takes a graph specification and data schema
    2. Uses LLM to generate executable SQL
    3. Caches generated SQL for reuse
    4. Executes SQL deterministically
    5. Returns traced results
    """

    def __init__(
        self,
        config: LLMConfig,
        provider: LLMProvider,
        prompt_renderer: PromptRenderer,
        cache: LLMCache,
    ):
        """Initialize the graph agent."""
        super().__init__(config, provider, prompt_renderer, cache)
        self._code_cache: dict[str, GeneratedCode] = {}  # In-memory cache

    async def execute(
        self,
        session: AsyncSession,
        graph: TransformationGraph,
        context: ExecutionContext,
        parameters: dict[str, Any] | None = None,
        force_regenerate: bool = False,
    ) -> Result[GraphExecution]:
        """Execute a graph by generating and running SQL.

        Args:
            session: Database session for LLM cache
            graph: The graph specification to execute
            context: Execution context with data connection
            parameters: Parameter values for the graph
            force_regenerate: If True, regenerate SQL even if cached

        Returns:
            Result containing GraphExecution with results
        """
        parameters = parameters or {}

        # Resolve parameters with defaults
        resolved_params = self._resolve_parameters(graph, parameters)

        # Generate cache key
        schema_mapping_id = context.schema_mapping_id or "default"
        cache_key = self._cache_key(graph, schema_mapping_id)

        # Check caches for generated code (in-memory first, then DB)
        generated_code: GeneratedCode | None = None

        if not force_regenerate:
            # Check in-memory cache
            generated_code = self._code_cache.get(cache_key)

            # Check database cache if not in memory
            if generated_code is None:
                db_code = await self._load_from_db(
                    session, graph.graph_id, graph.version, schema_mapping_id
                )
                if db_code:
                    generated_code = db_code
                    self._code_cache[cache_key] = generated_code

        if generated_code is None:
            # Generate SQL using LLM
            gen_result = await self._generate_sql(session, graph, context, resolved_params)
            if not gen_result.success or not gen_result.value:
                return Result.fail(gen_result.error or "SQL generation failed")

            generated_code = gen_result.value
            self._code_cache[cache_key] = generated_code

            # Persist to database
            await self._save_to_db(session, generated_code)

        # Execute the generated SQL
        exec_result = self._execute_sql(generated_code, context, graph, resolved_params)
        if not exec_result.success or not exec_result.value:
            return Result.fail(exec_result.error or "SQL execution failed")

        return exec_result

    async def _generate_sql(
        self,
        session: AsyncSession,
        graph: TransformationGraph,
        context: ExecutionContext,
        parameters: dict[str, Any],
    ) -> Result[GeneratedCode]:
        """Use LLM to generate SQL from graph specification."""
        # Get table schema
        schema_result = self._get_table_schema(context)
        if not schema_result.success or not schema_result.value:
            return Result.fail(schema_result.error or "Failed to get schema")

        table_schema = schema_result.value

        # Serialize graph to YAML for LLM context
        graph_yaml = self._graph_to_yaml(graph)

        # Build prompt context
        prompt_context = {
            "graph_yaml": graph_yaml,
            "graph_type": graph.graph_type.value,
            "table_schema": json.dumps(
                {
                    "table_name": table_schema.table_name,
                    "columns": table_schema.columns,
                    "row_count": table_schema.row_count,
                },
                indent=2,
            ),
            "parameters": json.dumps(parameters, indent=2),
        }

        # Add rich context if available (provides semantic, statistical, relational metadata)
        if context.rich_context is not None:
            from dataraum_context.graphs.context import format_context_for_prompt

            prompt_context["rich_context"] = format_context_for_prompt(context.rich_context)

            # Add field mappings if available (for resolving standard_field references)
            if (
                hasattr(context.rich_context, "field_mappings")
                and context.rich_context.field_mappings
            ):
                from dataraum_context.graphs.field_mapping import format_mappings_for_prompt

                prompt_context["field_mappings"] = format_mappings_for_prompt(
                    context.rich_context.field_mappings
                )
            else:
                prompt_context["field_mappings"] = ""
        else:
            prompt_context["rich_context"] = ""
            prompt_context["field_mappings"] = ""

        # Render prompt
        try:
            prompt, temperature = self.renderer.render("graph_sql_generation", prompt_context)
        except Exception as e:
            return Result.fail(f"Failed to render prompt: {e}")

        # Compute prompt hash for reproducibility
        prompt_hash = hashlib.sha256(prompt.encode()).hexdigest()[:16]

        # Call LLM
        response_result = await self._call_llm(
            session=session,
            feature_name="graph_sql_generation",
            prompt=prompt,
            temperature=temperature,
            model_tier="balanced",
        )

        if not response_result.success or not response_result.value:
            return Result.fail(response_result.error or "LLM call failed")

        # Parse response
        try:
            response_data = json.loads(response_result.value.content)
        except json.JSONDecodeError as e:
            return Result.fail(f"Failed to parse LLM response: {e}")

        # Create GeneratedCode
        generated_code = GeneratedCode(
            code_id=str(uuid4()),
            graph_id=graph.graph_id,
            graph_version=graph.version,
            schema_mapping_id=context.schema_mapping_id or "default",
            steps=response_data.get("steps", []),
            final_sql=response_data.get("final_sql", ""),
            column_mappings=response_data.get("column_mappings", {}),
            llm_model=response_result.value.model,
            prompt_hash=prompt_hash,
            generated_at=datetime.now(UTC),
        )

        # Validate generated SQL
        validation_result = self._validate_sql(generated_code, context)
        generated_code.is_validated = validation_result.success
        if not validation_result.success:
            generated_code.validation_errors = [validation_result.error or "Unknown"]

        return Result.ok(generated_code)

    def _execute_sql(
        self,
        generated_code: GeneratedCode,
        context: ExecutionContext,
        graph: TransformationGraph,
        parameters: dict[str, Any],
    ) -> Result[GraphExecution]:
        """Execute generated SQL and capture results."""
        # Create execution record
        execution = GraphExecution.create(graph, parameters, context.period)
        execution.is_period_final = context.is_period_final

        if context.filter_execution_id:
            execution.depends_on_executions.append(context.filter_execution_id)

        # Execute each step
        step_values: dict[str, Any] = {}

        for step_info in generated_code.steps:
            step_id = step_info.get("step_id", "unknown")
            sql = step_info.get("sql", "")

            try:
                result = context.duckdb_conn.execute(sql).fetchone()
                value = result[0] if result else None
                step_values[step_id] = value

                # Create step result
                step_result = StepResult(
                    step_id=step_id,
                    level=1,  # All generated steps are level 1 for now
                    step_type=StepType.FORMULA,
                    source_query=sql,
                    inputs_used={"sql": sql},
                )

                if isinstance(value, (int, float)):
                    step_result.value_scalar = float(value)
                elif isinstance(value, bool):
                    step_result.value_boolean = value
                elif isinstance(value, str):
                    step_result.value_string = value

                execution.step_results.append(step_result)

            except Exception as e:
                return Result.fail(f"Step {step_id} failed: {e}")

        # Execute final SQL
        try:
            final_result = context.duckdb_conn.execute(generated_code.final_sql).fetchone()
            execution.output_value = final_result[0] if final_result else None
        except Exception as e:
            return Result.fail(f"Final SQL failed: {e}")

        # Add interpretation if available
        if graph.interpretation and execution.output_value is not None:
            execution.output_interpretation = self._interpret_value(execution.output_value, graph)

        # Generate execution hash
        execution.execution_hash = self._generate_execution_hash(
            graph, parameters, context, generated_code.code_id
        )

        return Result.ok(execution)

    def _get_table_schema(self, context: ExecutionContext) -> Result[TableSchema]:
        """Get schema information from the actual table."""
        try:
            # Get column info
            columns_result = context.duckdb_conn.execute(
                f"DESCRIBE {context.table_name}"
            ).fetchall()

            columns = []
            for col in columns_result:
                col_name = col[0]
                col_type = col[1]

                # Get sample values
                sample_result = context.duckdb_conn.execute(
                    f"SELECT DISTINCT {col_name} FROM {context.table_name} LIMIT 5"
                ).fetchall()
                samples = [str(r[0]) for r in sample_result if r[0] is not None]

                columns.append(
                    {
                        "name": col_name,
                        "type": col_type,
                        "sample_values": samples,
                    }
                )

            # Get row count
            count_result = context.duckdb_conn.execute(
                f"SELECT COUNT(*) FROM {context.table_name}"
            ).fetchone()
            row_count = count_result[0] if count_result else 0

            return Result.ok(
                TableSchema(
                    table_name=context.table_name,
                    columns=columns,
                    row_count=row_count,
                )
            )

        except Exception as e:
            return Result.fail(f"Failed to get table schema: {e}")

    def _graph_to_yaml(self, graph: TransformationGraph) -> str:
        """Serialize graph to YAML for LLM context."""
        # Convert graph to dict for YAML serialization
        graph_dict: dict[str, Any] = {
            "graph_id": graph.graph_id,
            "graph_type": graph.graph_type.value,
            "version": graph.version,
            "metadata": {
                "name": graph.metadata.name,
                "description": graph.metadata.description,
                "category": graph.metadata.category,
            },
            "output": {
                "type": graph.output.output_type.value if graph.output else None,
                "metric_id": graph.output.metric_id if graph.output else None,
                "unit": graph.output.unit if graph.output else None,
            },
            "parameters": [
                {
                    "name": p.name,
                    "type": p.param_type,
                    "default": p.default,
                    "description": p.description,
                }
                for p in graph.parameters
            ],
            "dependencies": {
                step_id: {
                    "level": step.level,
                    "type": step.step_type.value,
                    "source": {
                        "standard_field": step.source.standard_field if step.source else None,
                        "statement": step.source.statement if step.source else None,
                    }
                    if step.source
                    else None,
                    "expression": step.expression,
                    "condition": step.condition,
                    "aggregation": step.aggregation,
                    "depends_on": step.depends_on,
                }
                for step_id, step in graph.steps.items()
            },
        }

        if graph.interpretation:
            graph_dict["interpretation"] = {
                "ranges": [
                    {
                        "min": r.min_value,
                        "max": r.max_value,
                        "label": r.label,
                        "description": r.description,
                    }
                    for r in graph.interpretation.ranges
                ]
            }

        return yaml.dump(graph_dict, default_flow_style=False, allow_unicode=True)

    def _validate_sql(
        self, generated_code: GeneratedCode, context: ExecutionContext
    ) -> Result[bool]:
        """Validate generated SQL syntax and column references."""
        # Try to explain the SQL (validates syntax without executing)
        try:
            context.duckdb_conn.execute(f"EXPLAIN {generated_code.final_sql}")
            return Result.ok(True)
        except Exception as e:
            return Result.fail(f"SQL validation failed: {e}")

    def _resolve_parameters(
        self, graph: TransformationGraph, provided: dict[str, Any]
    ) -> dict[str, Any]:
        """Merge provided parameters with graph defaults."""
        resolved = {}
        for param in graph.parameters:
            if param.name in provided:
                resolved[param.name] = provided[param.name]
            elif param.default is not None:
                resolved[param.name] = param.default
        return resolved

    def _interpret_value(self, value: Any, graph: TransformationGraph) -> str | None:
        """Interpret a metric value based on defined ranges."""
        if not graph.interpretation or not graph.interpretation.ranges:
            return None

        if not isinstance(value, (int, float)):
            return None

        for range_def in graph.interpretation.ranges:
            if range_def.min_value <= value <= range_def.max_value:
                return range_def.label

        return None

    def _cache_key(self, graph: TransformationGraph, schema_mapping_id: str) -> str:
        """Generate cache key for generated code."""
        return f"{graph.graph_id}:{graph.version}:{schema_mapping_id}"

    def _generate_execution_hash(
        self,
        graph: TransformationGraph,
        parameters: dict[str, Any],
        context: ExecutionContext,
        code_id: str,
    ) -> str:
        """Generate hash for reproducibility verification."""
        hash_input = {
            "graph": f"{graph.graph_id}@{graph.version}",
            "params": sorted(parameters.items()),
            "schema_mapping_id": context.schema_mapping_id,
            "filter_execution_id": context.filter_execution_id,
            "period": context.period,
            "code_id": code_id,
        }

        return hashlib.sha256(
            json.dumps(hash_input, sort_keys=True, default=str).encode()
        ).hexdigest()[:16]

    async def _load_from_db(
        self,
        session: AsyncSession,
        graph_id: str,
        graph_version: str,
        schema_mapping_id: str,
    ) -> GeneratedCode | None:
        """Load generated code from database cache."""
        stmt = select(GeneratedCodeRecord).where(
            GeneratedCodeRecord.graph_id == graph_id,
            GeneratedCodeRecord.graph_version == graph_version,
            GeneratedCodeRecord.schema_mapping_id == schema_mapping_id,
        )
        result = await session.execute(stmt)
        record = result.scalar_one_or_none()

        if record is None:
            return None

        return GeneratedCode(
            code_id=record.code_id,
            graph_id=record.graph_id,
            graph_version=record.graph_version,
            schema_mapping_id=record.schema_mapping_id,
            steps=record.steps_json,
            final_sql=record.final_sql,
            column_mappings=record.column_mappings,
            llm_model=record.llm_model,
            prompt_hash=record.prompt_hash,
            generated_at=record.generated_at,
            is_validated=record.is_validated,
            validation_errors=record.validation_errors,
        )

    async def _save_to_db(
        self,
        session: AsyncSession,
        generated_code: GeneratedCode,
    ) -> None:
        """Save generated code to database cache."""
        record = GeneratedCodeRecord(
            code_id=generated_code.code_id,
            graph_id=generated_code.graph_id,
            graph_version=generated_code.graph_version,
            schema_mapping_id=generated_code.schema_mapping_id,
            steps_json=generated_code.steps,
            final_sql=generated_code.final_sql,
            column_mappings=generated_code.column_mappings,
            llm_model=generated_code.llm_model,
            prompt_hash=generated_code.prompt_hash,
            generated_at=generated_code.generated_at,
            is_validated=generated_code.is_validated,
            validation_errors=generated_code.validation_errors,
        )
        session.add(record)
        await session.flush()
