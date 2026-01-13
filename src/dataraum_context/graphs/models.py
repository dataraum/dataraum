"""Unified transformation graph models.

Supports both filter graphs and metric graphs with a single schema.

Filter graphs:
    - Output: classification (clean/exclude/quarantine/flag)
    - Steps: predicates that evaluate to boolean

Metric graphs:
    - Output: scalar/series values with units
    - Steps: extractions, formulas, aggregations
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any
from uuid import uuid4

# =============================================================================
# Enums
# =============================================================================


class GraphType(str, Enum):
    """Type of transformation graph."""

    FILTER = "filter"
    METRIC = "metric"


class GraphSource(str, Enum):
    """Source of the graph definition."""

    SYSTEM = "system"  # Built-in system graphs
    USER = "user"  # User-defined graphs
    LLM = "llm"  # LLM-generated graphs


class StepType(str, Enum):
    """Type of graph step."""

    EXTRACT = "extract"  # Pull data from source
    CONSTANT = "constant"  # Fixed or parameterized value
    PREDICATE = "predicate"  # Boolean condition (for filters)
    FORMULA = "formula"  # Calculate derived value (for metrics)
    COMPOSITE = "composite"  # Combine multiple steps


class Classification(str, Enum):
    """Classification result for filter graphs."""

    CLEAN = "clean"  # Passes all checks, include in analysis
    EXCLUDE = "exclude"  # Outside scope, skip silently
    QUARANTINE = "quarantine"  # Quality issue, save for review
    FLAG = "flag"  # Include but marked for attention


class OutputType(str, Enum):
    """Type of graph output."""

    CLASSIFICATION = "classification"  # For filters
    SCALAR = "scalar"  # Single value
    SERIES = "series"  # Time series or array
    TABLE = "table"  # Multi-column result


class MetricScope(str, Enum):
    """Scope at which a metric can be computed.

    - GLOBAL: Computed once for the entire dataset
    - SLICE: Computed once per slice value (requires slice dimension)
    - BOTH: Computed at both global and per-slice levels
    """

    GLOBAL = "global"
    SLICE = "slice"
    BOTH = "both"


# =============================================================================
# Graph Definition Models
# =============================================================================


@dataclass
class AppliesTo:
    """Criteria for when a filter graph applies to a column.

    Used by rule-based filters to auto-match columns based on:
    - semantic_role: key, timestamp, measure, foreign_key
    - data_type: DOUBLE, DATE, VARCHAR, etc.
    - column_pattern: regex pattern for column names
    - column_pairs: patterns for cross-column rules
    - has_profile: whether statistical profile exists
    """

    semantic_role: str | None = None
    data_type: str | None = None
    column_pattern: str | None = None
    column_pairs: dict[str, str] | None = None
    has_profile: bool | None = None


@dataclass
class GraphMetadata:
    """Metadata about a transformation graph."""

    name: str
    description: str
    category: str  # quality, scope, working_capital, profitability, liquidity
    source: GraphSource
    created_by: str | None = None
    created_at: str | None = None
    tags: list[str] = field(default_factory=list)
    applies_to: AppliesTo | None = None  # For rule-based filters


@dataclass
class ParameterDef:
    """Definition of a user-configurable parameter."""

    name: str
    param_type: str  # integer, float, date, boolean, string
    default: Any
    description: str | None = None
    options: list[Any] | None = None  # For enum-like parameters


@dataclass
class StepSource:
    """Data source for an extract step."""

    table: str | None = None  # Concrete table name
    column: str | None = None  # Concrete column name
    standard_field: str | None = None  # Abstract field (resolved by schema mapping)
    statement: str | None = None  # balance_sheet, income_statement


@dataclass
class StepValidation:
    """Validation rule for a step."""

    condition: str
    severity: str  # error, warning
    message: str


@dataclass
class GraphStep:
    """A single step in a transformation graph."""

    step_id: str
    level: int
    step_type: StepType

    # For extract steps
    source: StepSource | None = None
    aggregation: str | None = None  # sum, avg, min, max, count, end_of_period

    # For constant steps
    value: Any | None = None
    parameter: str | None = None  # Reference to a parameter

    # For predicate steps (filters)
    condition: str | None = None
    on_false: Classification | None = None  # What to do when predicate fails
    on_true: Classification | None = None  # What to do when predicate passes (for flags)
    reason: str | None = None

    # For formula steps (metrics)
    expression: str | None = None

    # For composite steps
    logic: str | None = None  # Boolean expression combining other steps

    # Dependencies
    depends_on: list[str] = field(default_factory=list)

    # Validation
    validations: list[StepValidation] = field(default_factory=list)

    # Control
    enabled: str | None = None  # Conditional enable (e.g., "{param}")
    output_step: bool = False  # Is this the final output step?
    severity: str | None = None  # For predicates: critical, high, medium, low


@dataclass
class OutputDef:
    """Definition of graph output."""

    output_type: OutputType

    # For classification (filters)
    categories: dict[str, str] | None = None  # category -> description

    # For scalar/series (metrics)
    metric_id: str | None = None
    unit: str | None = None  # days, currency, ratio, count, percentage
    decimal_places: int | None = None


@dataclass
class InterpretationRange:
    """Interpretation range for metric values."""

    min_value: float
    max_value: float
    label: str
    description: str


@dataclass
class Interpretation:
    """Interpretation rules for metric output."""

    ranges: list[InterpretationRange] = field(default_factory=list)


@dataclass
class FilterRequirement:
    """A filter graph required by a metric graph."""

    graph_id: str
    required: bool = True
    required_classification: Classification = Classification.CLEAN


@dataclass
class TransformationGraph:
    """A unified transformation graph (filter or metric)."""

    graph_id: str
    graph_type: GraphType
    version: str

    metadata: GraphMetadata
    output: OutputDef
    steps: dict[str, GraphStep]

    # Optional
    parameters: list[ParameterDef] = field(default_factory=list)
    requires_filters: list[FilterRequirement] = field(default_factory=list)
    interpretation: Interpretation | None = None

    # Scope for metrics (global vs per-slice)
    scope: MetricScope = MetricScope.GLOBAL
    slice_dimension: str | None = None  # Column to slice by (for SLICE or BOTH scope)

    def get_execution_order(self) -> list[tuple[int, list[GraphStep]]]:
        """Get steps grouped by level for ordered execution."""
        levels: dict[int, list[GraphStep]] = {}
        for step in self.steps.values():
            if step.level not in levels:
                levels[step.level] = []
            levels[step.level].append(step)

        return [(level, levels[level]) for level in sorted(levels.keys())]

    def get_output_step(self) -> GraphStep | None:
        """Get the final output step."""
        for step in self.steps.values():
            if step.output_step:
                return step
        return None


# =============================================================================
# Execution Result Models
# =============================================================================


@dataclass
class StepResult:
    """Result of executing a single step."""

    step_id: str
    level: int
    step_type: StepType

    # Value (polymorphic based on step type)
    value_scalar: float | None = None
    value_boolean: bool | None = None
    value_string: str | None = None
    value_list: list[Any] | None = None

    # Classification (for predicate steps)
    classification: Classification | None = None
    rows_passed: int | None = None
    rows_failed: int | None = None

    # Trace information
    inputs_used: dict[str, Any] = field(default_factory=dict)
    expression_evaluated: str | None = None
    source_query: str | None = None
    rows_affected: int | None = None

    @property
    def value(self) -> Any:
        """Get the value in its native type."""
        if self.value_scalar is not None:
            return self.value_scalar
        if self.value_boolean is not None:
            return self.value_boolean
        if self.value_string is not None:
            return self.value_string
        if self.value_list is not None:
            return self.value_list
        return None


@dataclass
class ClassificationSummary:
    """Summary of filter classification results."""

    clean_count: int = 0
    exclude_count: int = 0
    quarantine_count: int = 0
    flag_count: int = 0
    total_count: int = 0

    @property
    def clean_ratio(self) -> float:
        """Ratio of clean rows."""
        return self.clean_count / self.total_count if self.total_count > 0 else 0.0


class AssumptionBasis(str, Enum):
    """Basis for an assumption made during query execution."""

    SYSTEM_DEFAULT = "system_default"  # Default from system configuration
    INFERRED = "inferred"  # Inferred from context or data patterns
    USER_SPECIFIED = "user_specified"  # Explicitly set by user


@dataclass
class QueryAssumption:
    """An assumption made during query execution due to data entropy.

    Tracks assumptions the agent makes when data has uncertainty,
    allowing them to be reviewed, corrected, or promoted to permanent rules.
    """

    assumption_id: str
    execution_id: str

    # What was assumed
    dimension: str  # e.g., "semantic.units", "structural.relations"
    target: str  # e.g., "column:orders.amount", "relationship:orders->customers"
    assumption: str  # Human-readable: "Currency is EUR"

    # Basis for assumption
    basis: AssumptionBasis
    confidence: float  # 0.0 to 1.0

    # For promotion to permanent rule
    can_promote: bool = True
    promoted_at: datetime | None = None
    promoted_by: str | None = None

    @classmethod
    def create(
        cls,
        execution_id: str,
        dimension: str,
        target: str,
        assumption: str,
        basis: AssumptionBasis,
        confidence: float,
    ) -> QueryAssumption:
        """Create a new assumption with generated ID."""
        return cls(
            assumption_id=str(uuid4()),
            execution_id=execution_id,
            dimension=dimension,
            target=target,
            assumption=assumption,
            basis=basis,
            confidence=confidence,
        )


@dataclass
class GraphExecution:
    """Result of executing a transformation graph."""

    execution_id: str
    graph_id: str
    graph_type: GraphType
    graph_version: str

    # Source tracking
    source: GraphSource

    # Parameters used
    parameters: dict[str, Any]

    # Period (optional)
    period: str | None = None
    is_period_final: bool = False

    # Results
    step_results: list[StepResult] = field(default_factory=list)

    # Output (depends on graph type)
    output_value: Any = None  # Metric value or ClassificationSummary
    output_interpretation: str | None = None

    # Traceability
    execution_hash: str = ""
    executed_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    # Links to other executions
    depends_on_executions: list[str] = field(default_factory=list)

    # Entropy-related tracking
    assumptions: list[QueryAssumption] = field(default_factory=list)
    max_entropy_score: float = 0.0  # Highest entropy encountered
    entropy_warnings: list[str] = field(default_factory=list)

    @classmethod
    def create(
        cls,
        graph: TransformationGraph,
        parameters: dict[str, Any],
        period: str | None = None,
    ) -> GraphExecution:
        """Create a new execution for a graph."""
        return cls(
            execution_id=str(uuid4()),
            graph_id=graph.graph_id,
            graph_type=graph.graph_type,
            graph_version=graph.version,
            source=graph.metadata.source,
            parameters=parameters,
            period=period,
        )


# =============================================================================
# Schema Mapping Models (integrated from calculations/)
# =============================================================================


@dataclass
class ColumnMapping:
    """Mapping from a concrete column to an abstract field.

    Represents one source column that contributes to an abstract field.
    Multiple columns can map to the same abstract field.
    """

    table: str
    column: str
    confidence: float  # 0.0 to 1.0
    reasoning: str | None = None
    filter_condition: str | None = None  # Optional WHERE clause
    sign_adjustment: int = 1  # Multiply by -1 for contra accounts


@dataclass
class AggregationDefinition:
    """How to aggregate origin columns into the target field."""

    method: str  # sum, end_of_period, average, count, min, max
    group_by: list[str] = field(default_factory=list)
    time_column: str | None = None
    period_type: str | None = None  # month, quarter, year


@dataclass
class SchemaMapping:
    """Mapping from abstract field to concrete columns."""

    abstract_field: str
    origin_mappings: list[ColumnMapping]
    aggregation: AggregationDefinition
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    created_by: str | None = None  # "llm", "user", "system"


@dataclass
class DatasetSchemaMapping:
    """Complete schema mapping for a dataset."""

    dataset_id: str
    mappings: dict[str, SchemaMapping]  # abstract_field -> mapping
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    version: str = "1.0"
