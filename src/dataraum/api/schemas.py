"""Pydantic schemas for API request/response models."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

# --- Source schemas ---


class SourceBase(BaseModel):
    """Base schema for source data."""

    name: str
    source_type: str = Field(description="csv, parquet, database, etc.")
    path: str | None = Field(default=None, description="File path or connection string")


class SourceCreate(SourceBase):
    """Schema for creating a new source."""

    pass


class SourceResponse(SourceBase):
    """Schema for source response."""

    model_config = ConfigDict(from_attributes=True)

    source_id: str
    created_at: datetime
    table_count: int = 0


class SourceListResponse(BaseModel):
    """Schema for list of sources."""

    sources: list[SourceResponse]
    total: int


# --- Table schemas ---


class ColumnResponse(BaseModel):
    """Schema for column metadata."""

    model_config = ConfigDict(from_attributes=True)

    column_id: str
    name: str
    position: int
    resolved_type: str | None = None
    nullable: bool = True
    business_description: str | None = None
    semantic_role: str | None = None
    entity_type: str | None = None


class TableResponse(BaseModel):
    """Schema for table metadata."""

    model_config = ConfigDict(from_attributes=True)

    table_id: str
    name: str
    source_id: str
    row_count: int | None = None
    columns: list[ColumnResponse] = Field(default_factory=list)


class TableListResponse(BaseModel):
    """Schema for list of tables."""

    tables: list[TableResponse]
    total: int


# --- Pipeline schemas ---


class PhaseStatus(BaseModel):
    """Schema for phase execution status."""

    name: str
    description: str
    status: str = Field(description="pending, running, completed, failed, skipped")
    duration_seconds: float | None = None
    completed_at: datetime | None = None
    error: str | None = None
    records_processed: int = 0
    records_created: int = 0


class PipelineStatusResponse(BaseModel):
    """Schema for pipeline status."""

    source_id: str
    last_run_id: str | None = None
    last_run_status: str | None = None
    last_run_at: datetime | None = None
    completed: int
    total: int
    progress_percent: float
    phases: list[PhaseStatus]


class PipelineRunRequest(BaseModel):
    """Schema for triggering a pipeline run."""

    target_phase: str | None = Field(
        default=None, description="Run up to this phase (None = all phases)"
    )
    skip_llm: bool = Field(default=False, description="Skip LLM-dependent phases")
    force: bool = Field(default=False, description="Force re-run even if already completed")


class PipelineRunResponse(BaseModel):
    """Schema for pipeline run response."""

    run_id: str
    source_id: str
    status: str
    message: str


# --- Entropy schemas ---


class ColumnEntropyResponse(BaseModel):
    """Schema for column entropy profile."""

    column_id: str
    column_name: str
    composite_score: float = Field(ge=0, le=1)
    readiness: str = Field(description="ready, investigate, blocked")
    high_entropy_dimensions: list[str] = Field(default_factory=list)
    resolution_hints: list[str] = Field(default_factory=list)
    layer_scores: dict[str, float] = Field(default_factory=dict)


class CompoundRiskResponse(BaseModel):
    """Schema for compound risk."""

    risk_id: str
    dimensions: list[str]
    risk_level: str
    impact: str
    combined_score: float


class TableEntropyResponse(BaseModel):
    """Schema for table entropy profile."""

    table_id: str
    table_name: str
    avg_composite_score: float
    max_composite_score: float
    blocked_column_count: int
    total_columns: int
    readiness: str
    compound_risks: list[CompoundRiskResponse] = Field(default_factory=list)


class EntropyDashboardResponse(BaseModel):
    """Schema for entropy dashboard."""

    source_id: str
    overall_readiness: str
    tables: list[TableEntropyResponse]
    high_priority_resolutions: list[str] = Field(default_factory=list)


# --- Context schemas ---


class ContextResponse(BaseModel):
    """Schema for context document."""

    source_id: str
    tables: list[dict[str, Any]]
    relationships: list[dict[str, Any]]
    entropy_summary: dict[str, Any] | None = None
    prompt_text: str = Field(description="Formatted context for LLM consumption")


# --- Graph schemas ---


class GraphResponse(BaseModel):
    """Schema for calculation graph."""

    graph_id: str
    name: str
    description: str | None = None
    target_table: str
    metrics: list[str]
    created_at: datetime


class GraphListResponse(BaseModel):
    """Schema for list of graphs."""

    graphs: list[GraphResponse]
    total: int


class GraphExecuteRequest(BaseModel):
    """Schema for executing a graph."""

    filters: dict[str, Any] = Field(default_factory=dict)
    parameters: dict[str, Any] = Field(default_factory=dict)


class GraphExecuteResponse(BaseModel):
    """Schema for graph execution result."""

    execution_id: str
    graph_id: str
    sql: str
    result: list[dict[str, Any]] | None = None
    row_count: int = 0
    assumptions: list[str] = Field(default_factory=list)
    entropy_warnings: list[str] = Field(default_factory=list)


# --- Query schemas ---


class QueryRequest(BaseModel):
    """Schema for SQL query execution."""

    sql: str = Field(description="SQL query to execute (read-only)")
    limit: int = Field(default=1000, ge=1, le=10000, description="Maximum rows to return")


class QueryResponse(BaseModel):
    """Schema for query result."""

    columns: list[str]
    rows: list[dict[str, Any]]
    row_count: int
    truncated: bool = Field(description="True if result was limited")


class QueryAgentRequest(BaseModel):
    """Request for Query Agent natural language question."""

    question: str = Field(description="Natural language question to answer")
    source_id: str = Field(description="Source ID to query against")
    contract: str | None = Field(
        default=None, description="Contract to evaluate against (e.g., 'executive_dashboard')"
    )
    auto_contract: bool = Field(
        default=False, description="Automatically select the strictest passing contract"
    )


class QueryAssumptionResponse(BaseModel):
    """An assumption made during query execution."""

    dimension: str = Field(description="Entropy dimension (e.g., 'semantic.units')")
    target: str = Field(description="What the assumption applies to")
    assumption: str = Field(description="Human-readable assumption")
    basis: str = Field(description="Basis: system_default, inferred, user_specified")
    confidence: float = Field(description="Confidence in this assumption (0.0 to 1.0)")


class QueryAgentResponse(BaseModel):
    """Response from Query Agent."""

    execution_id: str
    question: str
    answer: str = Field(description="Natural language answer")
    sql: str | None = Field(default=None, description="Generated SQL")
    data: list[dict[str, Any]] | None = Field(default=None, description="Query results")
    columns: list[str] | None = Field(default=None, description="Column names")

    # Confidence
    confidence_level: str = Field(description="green, yellow, orange, red")
    confidence_emoji: str = Field(description="Traffic light emoji")
    confidence_label: str = Field(description="GOOD, MARGINAL, ISSUES, BLOCKED")
    entropy_score: float | None = Field(
        default=None, description="Overall entropy score (None if not computed)"
    )

    # Assumptions
    assumptions: list[QueryAssumptionResponse] = Field(default_factory=list)

    # Contract
    contract: str | None = Field(default=None, description="Contract used")

    # Analysis details
    interpreted_question: str = Field(default="", description="How the question was understood")
    metric_type: str = Field(default="table", description="scalar, table, time_series, comparison")
    validation_notes: list[str] = Field(default_factory=list)

    # Status
    success: bool = True
    error: str | None = None


# --- SSE Progress schemas ---


class PipelineProgressEvent(BaseModel):
    """Schema for pipeline progress SSE event."""

    event: str = Field(
        description="Event type: start, phase_start, phase_complete, phase_failed, complete, error"
    )
    run_id: str
    source_id: str
    phase: str | None = None
    phase_status: str | None = None
    phases_completed: int = 0
    phases_total: int = 0
    progress_percent: float = 0.0
    duration_seconds: float | None = None
    error: str | None = None
    message: str | None = None


# --- Contract schemas ---


class ContractSummary(BaseModel):
    """Summary of a contract for listing."""

    name: str
    display_name: str
    description: str
    overall_threshold: float


class ContractListResponse(BaseModel):
    """Response for listing contracts."""

    contracts: list[ContractSummary]


class ViolationResponse(BaseModel):
    """A contract violation."""

    type: str = Field(description="Violation type: dimension, overall, blocking_condition")
    severity: str = Field(description="Severity: warning, blocking")
    dimension: str | None = None
    max_allowed: float | None = None
    actual: float | None = None
    details: str
    affected_columns: list[str] = Field(default_factory=list)


class ContractEvaluationResponse(BaseModel):
    """Result of evaluating a contract against data."""

    contract_name: str
    contract_display_name: str
    is_compliant: bool

    # Traffic light confidence
    confidence_level: str = Field(description="green, yellow, orange, red")
    confidence_emoji: str = Field(description="Traffic light emoji")
    confidence_label: str = Field(description="GOOD, MARGINAL, ISSUES, BLOCKED")

    # Scores
    overall_score: float
    dimension_scores: dict[str, float]

    # Violations and warnings
    violations: list[ViolationResponse]
    warnings: list[ViolationResponse]

    # Summary
    compliance_percentage: float = Field(description="Percentage of dimensions within threshold")
    worst_dimension: str | None = None
    worst_dimension_score: float = 0.0
    estimated_effort_to_comply: str

    evaluated_at: str


class AllContractsEvaluationResponse(BaseModel):
    """Result of evaluating all contracts."""

    source_id: str
    evaluations: dict[str, ContractEvaluationResponse]
    strictest_passing: str | None = Field(description="Name of strictest passing contract")
    passing_count: int
    total_count: int


# --- Query Library schemas ---


class QueryLibraryEntryResponse(BaseModel):
    """Response for a query library entry."""

    query_id: str
    source_id: str
    original_question: str | None = Field(
        default=None, description="NL question (for user queries)"
    )
    graph_id: str | None = Field(default=None, description="Graph ID (for seeded metrics)")
    name: str | None = None
    description: str | None = None
    final_sql: str
    column_mappings: dict[str, str] = Field(default_factory=dict)
    assumptions: list[dict[str, Any]] = Field(default_factory=list)
    confidence_level: str = "GREEN"
    usage_count: int = 0
    created_at: datetime
    last_used_at: datetime | None = None


class QueryLibraryListResponse(BaseModel):
    """Response for listing query library entries."""

    entries: list[QueryLibraryEntryResponse]
    total: int


class QueryLibrarySaveRequest(BaseModel):
    """Request to save a query to the library."""

    question: str = Field(description="Natural language question")
    summary: str = Field(description="One-sentence description of what the query calculates")
    sql: str = Field(description="SQL query")
    steps: list[dict[str, str]] = Field(
        default_factory=list,
        description="Calculation steps [{step_id, sql, description}]",
    )
    name: str | None = Field(default=None, description="Optional name for the query")
    description: str | None = Field(default=None, description="Optional description")
    assumptions: list[dict[str, Any]] = Field(
        default_factory=list, description="Assumptions made during query"
    )
    column_mappings: dict[str, str] = Field(
        default_factory=dict, description="Column name mappings"
    )
    confidence_level: str = Field(default="GREEN", description="Confidence level")


class QueryLibrarySaveResponse(BaseModel):
    """Response after saving a query to the library."""

    query_id: str
    message: str


class QueryLibrarySearchRequest(BaseModel):
    """Request to search the query library."""

    question: str = Field(description="Natural language question to search for")
    min_similarity: float = Field(
        default=0.5, ge=0.0, le=1.0, description="Minimum similarity threshold"
    )
    limit: int = Field(default=5, ge=1, le=20, description="Maximum results to return")


class QueryLibrarySearchResult(BaseModel):
    """A single search result from the library."""

    entry: QueryLibraryEntryResponse
    similarity: float = Field(description="Cosine similarity score (0.0 to 1.0)")


class QueryLibrarySearchResponse(BaseModel):
    """Response for library search."""

    results: list[QueryLibrarySearchResult]
    query: str = Field(description="Original query text")
