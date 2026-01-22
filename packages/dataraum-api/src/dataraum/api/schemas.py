"""Pydantic schemas for API request/response models."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

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

    source_id: str
    created_at: datetime
    table_count: int = 0

    class Config:
        from_attributes = True


class SourceListResponse(BaseModel):
    """Schema for list of sources."""

    sources: list[SourceResponse]
    total: int


# --- Table schemas ---


class ColumnResponse(BaseModel):
    """Schema for column metadata."""

    column_id: str
    name: str
    position: int
    resolved_type: str | None = None
    nullable: bool = True
    business_description: str | None = None
    semantic_role: str | None = None
    entity_type: str | None = None

    class Config:
        from_attributes = True


class TableResponse(BaseModel):
    """Schema for table metadata."""

    table_id: str
    name: str
    source_id: str
    row_count: int | None = None
    columns: list[ColumnResponse] = Field(default_factory=list)

    class Config:
        from_attributes = True


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
