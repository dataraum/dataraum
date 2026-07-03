"""Transformation graphs for metric computation.

Graphs are SPECIFICATIONS, not executable code. They define WHAT to calculate
with rich accounting context. The GraphAgent grounds each EXTRACT leaf to SQL
via the LLM and composes metrics deterministically from those groundings.

How a metric actually runs (DAT-646/DAT-603 — there is no whole-graph LLM
authoring): the metrics phase warms each unique EXTRACT leaf once via
``agent.execute`` on a single-extract mini-graph (``node_warming.build_mini_
graph``), then assembles every metric from the recorded bindings with NO LLM
(``agent.assemble``). ``execute`` fails loud on anything but a single-extract
mini-graph. See ``pipeline/phases/metrics_phase.py`` for the driving loop —
that is the usage example.
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
    OutputDef,
    OutputType,
    ParameterDef,
    QueryAssumption,
    StepResult,
    StepSource,
    StepType,
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
    # Graph definition models
    "TransformationGraph",
    "GraphMetadata",
    "GraphStep",
    "StepSource",
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
