"""Transformation graphs for metric computation.

Graphs are SPECIFICATIONS, not executable code. They define WHAT to calculate
with rich accounting context. The GraphAgent uses LLM to interpret graphs +
data schemas and generate executable SQL.

Usage:
    from dataraum.graphs import GraphLoader, GraphAgent, ExecutionContext

    loader = GraphLoader(vertical="finance")
    loader.load_all()
    graph = loader.get_graph("dso")

    agent = GraphAgent(config, provider, renderer)
    context = ExecutionContext.with_rich_context(
        session=session,
        duckdb_conn=conn,
        table_ids=table_ids,
    )
    result = agent.execute(session, graph, context)
"""

from .agent import ExecutionContext, GeneratedCode, GraphAgent
from .context import (
    ColumnContext,
    GraphExecutionContext,
    RelationshipContext,
    TableContext,
    build_execution_context,
    format_metadata_document,
)
from .entropy_behavior import (
    BehaviorMode,
    DimensionBehavior,
    EntropyAction,
    EntropyBehaviorConfig,
    get_default_config,
)
from .loader import GraphLoader, GraphLoadError
from .models import (
    AssumptionBasis,
    GraphExecution,
    GraphMetadata,
    GraphSource,
    GraphStep,
    Interpretation,
    InterpretationRange,
    MetricScope,
    OutputDef,
    OutputType,
    ParameterDef,
    QueryAssumption,
    StepResult,
    StepSource,
    StepType,
    StepValidation,
    TransformationGraph,
)

__all__ = [
    # Loader
    "GraphLoader",
    "GraphLoadError",
    # Agent (unified execution)
    "GraphAgent",
    "ExecutionContext",
    "GeneratedCode",
    # Context builder
    "GraphExecutionContext",
    "TableContext",
    "ColumnContext",
    "RelationshipContext",
    "build_execution_context",
    "format_metadata_document",
    # Entropy behavior
    "BehaviorMode",
    "EntropyAction",
    "EntropyBehaviorConfig",
    "DimensionBehavior",
    "get_default_config",
    # Enums
    "GraphSource",
    "StepType",
    "OutputType",
    "MetricScope",
    # Graph definition models
    "TransformationGraph",
    "GraphMetadata",
    "GraphStep",
    "StepSource",
    "StepValidation",
    "ParameterDef",
    "OutputDef",
    "Interpretation",
    "InterpretationRange",
    # Execution models
    "GraphExecution",
    "StepResult",
    # Assumption tracking
    "QueryAssumption",
    "AssumptionBasis",
]
