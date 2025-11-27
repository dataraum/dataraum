"""Core data models used across all modules.

This module defines the fundamental data structures that form the
contract between modules. All inter-module communication uses these types.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Generic, TypeVar
from uuid import UUID

from pydantic import BaseModel, Field

# Generic type for Result
T = TypeVar("T")


class Result(BaseModel, Generic[T]):
    """Result type for operations that can fail.

    Use this instead of exceptions for expected failures.
    Exceptions are reserved for unexpected/programming errors.
    """

    success: bool
    value: T | None = None
    error: str | None = None
    warnings: list[str] = Field(default_factory=list)

    @classmethod
    def ok(cls, value: T, warnings: list[str] | None = None) -> Result[T]:
        """Create a successful result."""
        return cls(success=True, value=value, warnings=warnings or [])

    @classmethod
    def fail(cls, error: str) -> Result[T]:
        """Create a failed result."""
        return cls(success=False, error=error)

    def unwrap(self) -> T:
        """Get the value or raise if failed."""
        if not self.success:
            raise ValueError(f"Result failed: {self.error}")
        assert self.value is not None
        return self.value

    def map(self, fn: callable[[T], Any]) -> Result[Any]:
        """Transform the value if successful."""
        if self.success:
            return Result.ok(fn(self.value), self.warnings)
        return self  # type: ignore


# === Enums ===


class DataType(str, Enum):
    """Supported data types."""

    VARCHAR = "VARCHAR"
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    DOUBLE = "DOUBLE"
    DECIMAL = "DECIMAL"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMPTZ = "TIMESTAMPTZ"
    TIME = "TIME"
    INTERVAL = "INTERVAL"
    JSON = "JSON"
    BLOB = "BLOB"


class SemanticRole(str, Enum):
    """Semantic role of a column."""

    MEASURE = "measure"  # Numeric value to aggregate
    DIMENSION = "dimension"  # Categorical grouping
    KEY = "key"  # Primary/business key
    FOREIGN_KEY = "foreign_key"  # Reference to another table
    ATTRIBUTE = "attribute"  # Descriptive non-key column
    TIMESTAMP = "timestamp"  # Time dimension
    UNKNOWN = "unknown"


class RelationshipType(str, Enum):
    """Type of relationship between tables."""

    FOREIGN_KEY = "foreign_key"
    HIERARCHY = "hierarchy"
    CORRELATION = "correlation"
    SEMANTIC = "semantic"


class Cardinality(str, Enum):
    """Relationship cardinality."""

    ONE_TO_ONE = "1:1"
    ONE_TO_MANY = "1:n"
    MANY_TO_MANY = "n:m"


class QualitySeverity(str, Enum):
    """Severity of quality issues."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class DecisionSource(str, Enum):
    """Source of a decision (type, annotation, etc)."""

    AUTO = "auto"  # Automatic heuristic
    LLM = "llm"  # LLM-generated
    MANUAL = "manual"  # Human-provided
    OVERRIDE = "override"  # Manual override of LLM
    RULE = "rule"  # Rule-based
    ONTOLOGY = "ontology"  # From ontology config
    DEFAULT = "default"  # From default config


# === Identifiers ===


class ColumnRef(BaseModel):
    """Reference to a column by name."""

    table_name: str
    column_name: str

    def __str__(self) -> str:
        return f"{self.table_name}.{self.column_name}"

    def __hash__(self) -> int:
        return hash((self.table_name, self.column_name))


class TableRef(BaseModel):
    """Reference to a table by name."""

    table_name: str
    schema_name: str | None = None

    def __str__(self) -> str:
        if self.schema_name:
            return f"{self.schema_name}.{self.table_name}"
        return self.table_name


# === Staging Models ===


class SourceConfig(BaseModel):
    """Configuration for a data source."""

    name: str
    source_type: str  # 'csv', 'parquet', 'postgres', 'duckdb', 'api'

    # For files
    path: str | None = None
    file_pattern: str | None = None

    # For databases
    connection_string: str | None = None
    schema_name: str | None = None  # Renamed from 'schema' to avoid shadowing BaseModel.schema
    tables: list[str] | None = None

    # Options
    sample_size: int | None = None


class StagedColumn(BaseModel):
    """A column in a staged table."""

    column_id: UUID
    name: str
    position: int
    sample_values: list[str] = Field(default_factory=list)


class StagedTable(BaseModel):
    """A staged table."""

    table_id: UUID
    table_name: str
    raw_table_name: str
    row_count: int
    columns: list[StagedColumn]


class StagingResult(BaseModel):
    """Result of staging operation."""

    source_id: UUID
    tables: list[StagedTable]
    total_rows: int
    duration_seconds: float


# === Profiling Models ===


class NumericStats(BaseModel):
    """Statistics for numeric columns."""

    min_value: float
    max_value: float
    mean: float
    stddev: float
    percentiles: dict[str, float] = Field(default_factory=dict)


class StringStats(BaseModel):
    """Statistics for string columns."""

    min_length: int
    max_length: int
    avg_length: float


class HistogramBucket(BaseModel):
    """A histogram bucket."""

    bucket_min: float | str
    bucket_max: float | str
    count: int


class ValueCount(BaseModel):
    """A value with its count."""

    value: Any
    count: int
    percentage: float


class DetectedPattern(BaseModel):
    """A detected pattern in column values."""

    name: str
    match_rate: float
    semantic_type: str | None = None


class TypeCandidate(BaseModel):
    """A candidate type for a column."""

    column_id: UUID
    column_ref: ColumnRef

    data_type: DataType
    confidence: float
    parse_success_rate: float
    failed_examples: list[str] = Field(default_factory=list)

    detected_pattern: str | None = None
    pattern_match_rate: float | None = None

    detected_unit: str | None = None
    unit_confidence: float | None = None


class ColumnProfile(BaseModel):
    """Statistical profile of a column."""

    column_id: UUID
    column_ref: ColumnRef
    profiled_at: datetime

    total_count: int
    null_count: int
    distinct_count: int

    null_ratio: float
    cardinality_ratio: float

    numeric_stats: NumericStats | None = None
    string_stats: StringStats | None = None

    histogram: list[HistogramBucket] | None = None
    top_values: list[ValueCount] | None = None
    detected_patterns: list[DetectedPattern] = Field(default_factory=list)


class ProfileResult(BaseModel):
    """Result of profiling operation."""

    profiles: list[ColumnProfile]
    type_candidates: list[TypeCandidate]
    duration_seconds: float


# === Type Resolution Models ===


class TypeDecision(BaseModel):
    """A type decision for a column."""

    column_id: UUID
    decided_type: DataType
    decision_source: DecisionSource = DecisionSource.AUTO
    decision_reason: str | None = None


class ColumnCastResult(BaseModel):
    """Cast result for a single column."""

    column_id: UUID
    column_ref: ColumnRef
    source_type: str
    target_type: DataType
    success_count: int
    failure_count: int
    success_rate: float
    failure_samples: list[str] = Field(default_factory=list)


class TypeResolutionResult(BaseModel):
    """Result of type resolution."""

    typed_table_name: str
    quarantine_table_name: str

    total_rows: int
    typed_rows: int
    quarantined_rows: int

    column_results: list[ColumnCastResult]


# === Enrichment Models ===


class SemanticAnnotation(BaseModel):
    """Semantic annotation for a column (LLM-generated or manual)."""

    column_id: UUID
    column_ref: ColumnRef

    semantic_role: SemanticRole
    entity_type: str | None = None
    business_name: str | None = None
    business_description: str | None = None  # LLM-generated description

    annotation_source: DecisionSource
    annotated_by: str | None = None  # e.g., 'claude-sonnet-4-20250514' or 'user@example.com'
    confidence: float


class EntityDetection(BaseModel):
    """Entity type detection for a table."""

    table_id: UUID
    table_name: str

    entity_type: str
    description: str | None = None  # LLM-generated table description
    confidence: float
    evidence: dict[str, Any] = Field(default_factory=dict)

    grain_columns: list[str] = Field(default_factory=list)
    is_fact_table: bool = False
    is_dimension_table: bool = False
    time_column: str | None = None  # Primary time column


class Relationship(BaseModel):
    """A detected relationship between tables."""

    relationship_id: UUID

    from_table: str
    from_column: str
    to_table: str
    to_column: str

    relationship_type: RelationshipType
    cardinality: Cardinality | None = None

    confidence: float
    detection_method: str
    evidence: dict[str, Any] = Field(default_factory=dict)

    is_confirmed: bool = False


class JoinStep(BaseModel):
    """A single step in a join path."""

    from_column: str
    to_table: str
    to_column: str
    confidence: float


class JoinPath(BaseModel):
    """A computed join path between tables."""

    from_table: str
    to_table: str
    steps: list[JoinStep]
    total_confidence: float


class TemporalGap(BaseModel):
    """A gap in temporal data."""

    start: datetime
    end: datetime
    missing_periods: int


class TemporalProfile(BaseModel):
    """Temporal profile for a time column."""

    column_id: UUID
    column_ref: ColumnRef

    min_timestamp: datetime
    max_timestamp: datetime

    detected_granularity: str
    granularity_confidence: float

    expected_periods: int
    actual_periods: int
    completeness_ratio: float

    gap_count: int
    gaps: list[TemporalGap] = Field(default_factory=list)

    has_seasonality: bool = False
    seasonality_period: str | None = None
    trend_direction: str | None = None


# === Quality Models ===


class QualityRule(BaseModel):
    """A quality rule."""

    rule_id: UUID

    table_name: str
    column_name: str | None = None

    rule_name: str
    rule_type: str
    rule_expression: str
    parameters: dict[str, Any] = Field(default_factory=dict)

    severity: QualitySeverity
    source: DecisionSource
    description: str | None = None


class RuleResult(BaseModel):
    """Result of a single rule execution."""

    rule_id: UUID
    rule_name: str

    total_records: int
    passed_records: int
    failed_records: int
    pass_rate: float

    failure_samples: list[dict[str, Any]] = Field(default_factory=list)


class QualityScore(BaseModel):
    """Aggregate quality score."""

    scope: str  # 'table' or 'column'
    scope_id: UUID
    scope_name: str

    completeness: float
    validity: float
    consistency: float
    uniqueness: float
    timeliness: float

    overall: float


class Anomaly(BaseModel):
    """A detected anomaly."""

    table_name: str
    column_name: str | None = None

    anomaly_type: str
    description: str
    severity: QualitySeverity
    evidence: dict[str, Any] = Field(default_factory=dict)


# === Context Models ===


class ColumnContext(BaseModel):
    """Context for a single column."""

    name: str
    data_type: str
    description: str | None = None

    null_ratio: float
    cardinality_ratio: float

    semantic_role: SemanticRole
    business_name: str | None = None
    detected_unit: str | None = None

    quality_score: float


class TableContext(BaseModel):
    """Context for a single table."""

    name: str
    description: str | None = None

    row_count: int
    columns: list[ColumnContext]

    entity_type: str | None = None
    grain_columns: list[str] = Field(default_factory=list)
    is_fact_table: bool = False
    is_dimension_table: bool = False

    time_columns: list[str] = Field(default_factory=list)
    granularity: str | None = None
    date_range: tuple[datetime, datetime] | None = None

    quality_score: float


class MetricDefinition(BaseModel):
    """A metric that can be computed."""

    name: str
    formula: str
    description: str
    required_columns: list[str] = Field(default_factory=list)
    applicable_tables: list[str] = Field(default_factory=list)
    output_type: str = "number"


class DomainConcept(BaseModel):
    """A domain concept from the ontology."""

    name: str
    description: str
    mapped_columns: list[ColumnRef] = Field(default_factory=list)


class QualitySummary(BaseModel):
    """Summary of data quality."""

    overall_score: float
    tables_assessed: int
    rules_executed: int
    issues_found: int
    critical_issues: list[str] = Field(default_factory=list)


class SuggestedQuery(BaseModel):
    """A suggested SQL query for the context (LLM-generated)."""

    name: str
    description: str
    category: str = "overview"  # 'overview', 'metrics', 'trends', 'segments', 'quality'
    sql: str
    complexity: str = "simple"  # 'simple', 'moderate', 'complex'


class ContextSummary(BaseModel):
    """Natural language summary of the data context (LLM-generated)."""

    summary: str
    key_facts: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class ContextDocument(BaseModel):
    """The main context document for AI consumption.

    This is the primary output of the context engine - a comprehensive
    document that gives AI everything it needs to understand and query
    the data effectively.

    Several fields are LLM-generated when enabled in config/llm.yaml.
    """

    # Data inventory
    tables: list[TableContext]
    relationships: list[Relationship]

    # Ontology interpretation
    ontology: str
    relevant_metrics: list[MetricDefinition]
    domain_concepts: list[DomainConcept]

    # Quality summary
    quality_summary: QualitySummary

    # LLM-generated content
    suggested_queries: list[SuggestedQuery] = Field(default_factory=list)
    context_summary: ContextSummary | None = None

    # Metadata
    llm_features_used: list[str] = Field(default_factory=list)  # which LLM features contributed
