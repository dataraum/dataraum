"""Transformation graph models.

Metric graphs:
    - Output: scalar/series values with units
    - Steps: extractions, formulas, aggregations
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field

# =============================================================================
# Enums
# =============================================================================


class GraphSource(StrEnum):
    """Source of the graph definition."""

    SYSTEM = "system"  # Built-in system graphs
    USER = "user"  # User-defined graphs
    LLM = "llm"  # LLM-generated graphs
    TEACH = "teach"  # Created via teach(type="metric")


class StepType(StrEnum):
    """Type of graph step."""

    EXTRACT = "extract"  # Pull data from source
    CONSTANT = "constant"  # Fixed or parameterized value
    FORMULA = "formula"  # Calculate derived value


class OutputType(StrEnum):
    """Type of graph output."""

    SCALAR = "scalar"  # Single value
    SERIES = "series"  # Time series or array
    TABLE = "table"  # Multi-column result


class MetricScope(StrEnum):
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
class GraphMetadata:
    """Metadata about a transformation graph."""

    name: str
    description: str
    category: str  # working_capital, profitability, liquidity, ...
    source: GraphSource
    created_by: str | None = None
    created_at: str | None = None
    tags: list[str] = field(default_factory=list)
    inspiration_snippet_id: str | None = None  # For snippet promotion via teach


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
    step_type: StepType

    # For extract steps
    source: StepSource | None = None
    aggregation: str | None = None  # sum, avg, min, max, count, end_of_period

    # For constant steps
    value: Any | None = None
    parameter: str | None = None  # Reference to a parameter

    # For formula steps
    expression: str | None = None

    # Dependencies
    depends_on: list[str] = field(default_factory=list)

    # Output marker
    output_step: bool = False


@dataclass
class OutputDef:
    """Definition of graph output."""

    output_type: OutputType
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
class TransformationGraph:
    """A metric transformation graph."""

    graph_id: str
    version: str

    metadata: GraphMetadata
    output: OutputDef
    steps: dict[str, GraphStep]

    # Optional
    parameters: list[ParameterDef] = field(default_factory=list)
    interpretation: Interpretation | None = None

    # Scope (global vs per-slice)
    scope: MetricScope = MetricScope.GLOBAL
    slice_dimension: str | None = None  # Column to slice by (for SLICE or BOTH scope)

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

    # Value (polymorphic based on step type)
    value_scalar: float | None = None
    value_boolean: bool | None = None
    value_string: str | None = None
    value_list: list[Any] | None = None

    # Trace information
    inputs_used: dict[str, Any] = field(default_factory=dict)
    source_query: str | None = None

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


class AssumptionBasis(StrEnum):
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
    source: GraphSource

    # Step results (used internally for snippet saving)
    step_results: list[StepResult] = field(default_factory=list)

    # Output
    output_value: Any = None
    output_interpretation: str | None = None

    # Assumptions made during execution (populated from LLM output)
    assumptions: list[QueryAssumption] = field(default_factory=list)

    @classmethod
    def create(cls, graph: TransformationGraph) -> GraphExecution:
        """Create a new execution for a graph."""
        return cls(
            execution_id=str(uuid4()),
            graph_id=graph.graph_id,
            source=graph.metadata.source,
        )


# =============================================================================
# Pydantic models for LLM tool output
# =============================================================================


class SQLStepOutput(BaseModel):
    """Pydantic model for a SQL step in LLM tool output."""

    step_id: str = Field(description="Identifier for this step")
    sql: str = Field(
        description="Standalone DuckDB SQL query for this step. "
        "Executed as: CREATE TEMP VIEW {step_id} AS {this_sql}. "
        "Should return a single scalar value or simple row."
    )
    description: str = Field(description="What this step does")


class GraphAssumptionOutput(BaseModel):
    """An assumption made during graph SQL generation."""

    dimension: str = Field(description="Entropy dimension (e.g., 'semantic.units', 'value.nulls')")
    target: str = Field(description="What the assumption applies to (e.g., 'column:orders.amount')")
    assumption: str = Field(description="Human-readable assumption (e.g., 'Currency is EUR')")
    basis: str = Field(
        description="Basis for assumption: 'system_default', 'inferred', or 'user_specified'"
    )
    confidence: float = Field(
        ge=0.0, le=1.0, description="Confidence in this assumption (0.0 to 1.0)"
    )


class GraphProvenanceOutput(BaseModel):
    """Provenance of how the LLM grounded business concepts to SQL."""

    field_resolution: str = Field(
        description="How fields were resolved: 'direct' (taught concept, deterministic mapping) "
        "or 'inferred' (LLM bridged vocabulary gap using enriched views)"
    )
    column_mappings_basis: dict[str, dict[str, str]] = Field(
        default_factory=dict,
        description="Per-concept grounding: {concept: {column, filter, resolution}}",
    )
    llm_reasoning: str = Field(
        default="",
        description="Brief explanation of how business concepts were mapped to columns",
    )


class GraphSQLGenerationOutput(BaseModel):
    """Pydantic model for LLM tool output - graph SQL generation.

    Used as a tool definition for structured LLM output via tool use API.
    """

    summary: str = Field(
        description="One sentence describing what this query calculates in plain English, "
        "e.g., 'Calculates Days Sales Outstanding (DSO) by dividing accounts receivable "
        "by average daily sales over the period.'"
    )
    steps: list[SQLStepOutput] = Field(
        default_factory=list,
        description="List of SQL steps, each with step_id, sql, and description",
    )
    final_sql: str = Field(
        description="SQL that combines step results to produce the final output. "
        "Steps are available as temp views — reference via: "
        "SELECT (SELECT value FROM step_1) / (SELECT value FROM step_2)."
    )
    column_mappings: dict[str, str] = Field(
        default_factory=dict,
        description="Mapping from abstract field names to concrete column names",
    )
    assumptions: list[GraphAssumptionOutput] = Field(
        default_factory=list,
        description="Assumptions made due to data uncertainty during SQL generation",
    )
    provenance: GraphProvenanceOutput | None = Field(
        default=None,
        description="How business concepts were grounded to concrete columns",
    )
