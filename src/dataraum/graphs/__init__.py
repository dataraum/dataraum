"""Unified transformation graphs for filters and metrics.

This module provides a unified schema for defining both filter graphs
(data quality and scope filtering) and metric graphs (business calculations).

ARCHITECTURE NOTE:
    Graphs are SPECIFICATIONS, not executable code. They define WHAT to calculate
    with rich accounting context. The GraphAgent uses LLM to interpret graphs +
    data schemas and generate executable SQL.

    See docs/CALCULATION_ENGINE_DESIGN.md for full architecture.

Usage:
    from dataraum.graphs import (
        GraphLoader,
        GraphAgent,
        ExecutionContext,
    )

    # Load graphs
    loader = GraphLoader(vertical="finance")
    loader.load_all()
    graph = loader.get_graph("dso")

    # Execute using agent (requires LLM infrastructure)
    agent = GraphAgent(config, provider, renderer)
    context = ExecutionContext.with_rich_context(
        session=session,
        duckdb_conn=conn,
        table_ids=table_ids,
    )
    result = agent.execute(session, graph, context)
"""

# Re-export DB models for convenience
from dataraum.graphs.db_models import (
    GraphExecutionRecord,
    StepResultRecord,
)

from .agent import ExecutionContext, GeneratedCode, GraphAgent
from .context import (
    ColumnContext,
    GraphExecutionContext,
    RelationshipContext,
    TableContext,
    build_execution_context,
    format_context_for_prompt,
)
from .entropy_behavior import (
    BehaviorMode,
    DimensionBehavior,
    EntropyAction,
    EntropyBehaviorConfig,
    get_default_config,
)
from .export import export_graph_definition, export_to_react_flow
from .loader import GraphLoader, GraphLoadError
from .models import (
    AggregationDefinition,
    AppliesTo,
    AssumptionBasis,
    Classification,
    ClassificationSummary,
    ColumnMapping,
    DatasetSchemaMapping,
    FilterRequirement,
    GraphExecution,
    GraphMetadata,
    GraphSource,
    GraphStep,
    GraphType,
    Interpretation,
    InterpretationRange,
    MetricScope,
    OutputDef,
    OutputType,
    ParameterDef,
    QueryAssumption,
    SchemaMapping,
    StepResult,
    StepSource,
    StepType,
    StepValidation,
    TransformationGraph,
)
from .persistence import GraphExecutionRepository

__all__ = [
    # Loader
    "GraphLoader",
    "GraphLoadError",
    # Agent (unified execution)
    "GraphAgent",
    "ExecutionContext",
    "GeneratedCode",
    # TableSchema removed — schema is now built as dict by _build_schema_info
    # Context builder
    "GraphExecutionContext",
    "TableContext",
    "ColumnContext",
    "RelationshipContext",
    "build_execution_context",
    "format_context_for_prompt",
    # Entropy behavior
    "BehaviorMode",
    "EntropyAction",
    "EntropyBehaviorConfig",
    "DimensionBehavior",
    "get_default_config",
    # Enums
    "GraphType",
    "GraphSource",
    "StepType",
    "Classification",
    "OutputType",
    "MetricScope",
    # Graph definition models
    "TransformationGraph",
    "GraphMetadata",
    "AppliesTo",
    "GraphStep",
    "StepSource",
    "StepValidation",
    "ParameterDef",
    "OutputDef",
    "FilterRequirement",
    "Interpretation",
    "InterpretationRange",
    # Execution models
    "GraphExecution",
    "StepResult",
    "ClassificationSummary",
    # Assumption tracking
    "QueryAssumption",
    "AssumptionBasis",
    # Schema mapping models
    "ColumnMapping",
    "AggregationDefinition",
    "SchemaMapping",
    "DatasetSchemaMapping",
    # Persistence
    "GraphExecutionRecord",
    "StepResultRecord",
    "GraphExecutionRepository",
    # Export/Visualization
    "export_to_react_flow",
    "export_graph_definition",
]
