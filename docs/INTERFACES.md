# Module Interfaces

## Overview

This document defines the interfaces between modules. All modules communicate 
via well-defined data classes (Pydantic models) and functions.

## Core Models

```python
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel


# Result type for operations that can fail
class Result[T](BaseModel):
    success: bool
    value: T | None = None
    error: str | None = None
    warnings: list[str] = []

    @classmethod
    def ok(cls, value: T, warnings: list[str] = []) -> "Result[T]":
        return cls(success=True, value=value, warnings=warnings)

    @classmethod
    def fail(cls, error: str) -> "Result[T]":
        return cls(success=False, error=error)


# Column identifier
class ColumnRef(BaseModel):
    table_name: str
    column_name: str


# Type system
class DataType(str, Enum):
    VARCHAR = "VARCHAR"
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    DOUBLE = "DOUBLE"
    DECIMAL = "DECIMAL"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    TIME = "TIME"
    INTERVAL = "INTERVAL"
    JSON = "JSON"
    BINARY = "BINARY"


class SemanticRole(str, Enum):
    MEASURE = "measure"
    DIMENSION = "dimension"
    KEY = "key"
    FOREIGN_KEY = "foreign_key"
    ATTRIBUTE = "attribute"
    TIMESTAMP = "timestamp"
    UNKNOWN = "unknown"
```

## Staging Module

### Input
```python
class SourceConfig(BaseModel):
    """Configuration for a data source."""
    name: str
    source_type: str  # 'csv', 'parquet', 'postgres', 'duckdb', 'api'
    
    # For files
    path: str | None = None
    file_pattern: str | None = None  # glob pattern for multiple files
    
    # For databases
    connection_string: str | None = None
    schema: str | None = None
    tables: list[str] | None = None  # None = all tables
    
    # Options
    sample_size: int | None = None  # rows to sample, None = all
```

### Output
```python
class StagingResult(BaseModel):
    """Result of staging operation."""
    source_id: UUID
    tables: list[StagedTable]
    total_rows: int
    duration_seconds: float


class StagedTable(BaseModel):
    """A single staged table."""
    table_id: UUID
    table_name: str
    raw_table_name: str  # actual DuckDB table name (raw_{name})
    column_count: int
    row_count: int
    columns: list[StagedColumn]


class StagedColumn(BaseModel):
    """A column in a staged table."""
    column_id: UUID
    name: str
    position: int
    sample_values: list[str]
```

### Interface
```python
# staging/loaders.py

async def ingest_source(
    config: SourceConfig,
    duckdb_conn: duckdb.DuckDBPyConnection,
    metadata_conn: asyncpg.Connection,
) -> Result[StagingResult]:
    """
    Load data from source into DuckDB staging tables.
    
    - All columns loaded as VARCHAR
    - Source metadata recorded in PostgreSQL
    - Returns table and column IDs for downstream processing
    """
    ...
```

## Profiling Module

### Input
```python
class ProfileRequest(BaseModel):
    """Request to profile tables."""
    table_ids: list[UUID]
    columns: list[ColumnRef] | None = None  # None = all columns
    options: ProfileOptions = ProfileOptions()


class ProfileOptions(BaseModel):
    """Profiling options."""
    compute_histograms: bool = True
    histogram_buckets: int = 20
    top_k_values: int = 10
    sample_for_patterns: int = 10000
    detect_units: bool = True
```

### Output
```python
class ProfileResult(BaseModel):
    """Result of profiling operation."""
    profiles: list[ColumnProfile]
    type_candidates: list[TypeCandidate]
    duration_seconds: float


class ColumnProfile(BaseModel):
    """Statistical profile of a column."""
    column_id: UUID
    column_ref: ColumnRef
    profiled_at: datetime
    
    # Counts
    total_count: int
    null_count: int
    distinct_count: int
    
    # Derived
    null_ratio: float
    cardinality_ratio: float
    
    # Numeric stats (if applicable)
    numeric_stats: NumericStats | None = None
    
    # String stats (if applicable)
    string_stats: StringStats | None = None
    
    # Distribution
    histogram: list[HistogramBucket] | None = None
    top_values: list[ValueCount] | None = None


class NumericStats(BaseModel):
    min_value: float
    max_value: float
    mean: float
    stddev: float
    percentiles: dict[str, float]  # p25, p50, p75, p95, p99


class StringStats(BaseModel):
    min_length: int
    max_length: int
    avg_length: float


class HistogramBucket(BaseModel):
    bucket_min: float | str
    bucket_max: float | str
    count: int


class ValueCount(BaseModel):
    value: Any
    count: int
    percentage: float


class TypeCandidate(BaseModel):
    """A candidate type for a column."""
    column_id: UUID
    column_ref: ColumnRef
    
    data_type: DataType
    confidence: float
    parse_success_rate: float
    failed_examples: list[str]
    
    # Pattern detection
    detected_pattern: str | None = None
    pattern_match_rate: float | None = None
    
    # Unit detection (from Pint prototype)
    detected_unit: str | None = None
    unit_confidence: float | None = None
```

### Interface
```python
# profiling/statistical.py

async def profile_columns(
    request: ProfileRequest,
    duckdb_conn: duckdb.DuckDBPyConnection,
    metadata_conn: asyncpg.Connection,
) -> Result[ProfileResult]:
    """
    Extract statistical profiles and type candidates.
    
    Uses pattern detection prototype for type inference.
    Uses Pint prototype for unit detection.
    Stores results in PostgreSQL metadata tables.
    """
    ...


# profiling/patterns.py (wrapper around prototype)

def detect_patterns(
    arrow_table: pa.Table,
    column_name: str,
) -> PatternResult:
    """
    Wrapper around existing prototype.
    DO NOT REIMPLEMENT - call prototype code.
    """
    from prototypes.pattern_detection import detect_patterns as _detect
    return _detect(arrow_table, column_name)
```

## Type Resolution Module

### Input
```python
class TypeDecisionRequest(BaseModel):
    """Request to apply type decisions."""
    table_id: UUID
    decisions: list[TypeDecision]


class TypeDecision(BaseModel):
    """A type decision for a column."""
    column_id: UUID
    decided_type: DataType
    decision_source: str = "auto"  # 'auto', 'manual', 'rule'
    decision_reason: str | None = None
```

### Output
```python
class TypeResolutionResult(BaseModel):
    """Result of type resolution."""
    typed_table_name: str
    quarantine_table_name: str
    
    total_rows: int
    typed_rows: int
    quarantined_rows: int
    
    column_results: list[ColumnCastResult]


class ColumnCastResult(BaseModel):
    """Cast result for a single column."""
    column_id: UUID
    column_ref: ColumnRef
    source_type: str
    target_type: DataType
    success_count: int
    failure_count: int
    success_rate: float
    failure_samples: list[str]
```

### Interface
```python
# profiling/types.py

async def resolve_types(
    request: TypeDecisionRequest,
    duckdb_conn: duckdb.DuckDBPyConnection,
    metadata_conn: asyncpg.Connection,
) -> Result[TypeResolutionResult]:
    """
    Apply type casting with quarantine for failures.
    
    - Creates typed_{table} with cast columns
    - Creates quarantine_{table} with failed rows
    - Updates metadata with results
    """
    ...


async def auto_decide_types(
    table_id: UUID,
    metadata_conn: asyncpg.Connection,
    min_confidence: float = 0.95,
) -> list[TypeDecision]:
    """
    Generate automatic type decisions from candidates.
    Only decides if confidence >= min_confidence.
    """
    ...
```

## Enrichment Module

### Semantic Enrichment (LLM-Powered)

```python
class SemanticEnrichmentRequest(BaseModel):
    """Request for semantic enrichment."""
    table_ids: list[UUID]
    ontology: str | None = None  # apply ontology for domain context
    use_llm: bool = True         # enable LLM analysis


class SemanticEnrichmentResult(BaseModel):
    """Result of semantic enrichment."""
    annotations: list[SemanticAnnotation]
    entity_detections: list[EntityDetection]
    relationships: list[Relationship]  # LLM-detected relationships
    source: str  # 'llm', 'manual', 'override'


class SemanticAnnotation(BaseModel):
    """Semantic annotation for a column."""
    column_id: UUID
    column_ref: ColumnRef
    
    semantic_role: SemanticRole
    entity_type: str | None = None
    business_name: str | None = None
    business_description: str | None = None  # LLM-generated description
    
    annotation_source: str  # 'llm', 'manual', 'override'
    annotated_by: str | None = None  # model name or user
    confidence: float


class EntityDetection(BaseModel):
    """Entity type detection for a table."""
    table_id: UUID
    table_name: str
    
    entity_type: str
    description: str | None = None  # LLM-generated table description
    confidence: float
    evidence: dict[str, Any]
    
    grain_columns: list[str]
    is_fact_table: bool
    is_dimension_table: bool
    time_column: str | None = None  # primary time column
```

### Topological Enrichment

```python
class TopologyEnrichmentRequest(BaseModel):
    """Request for topology analysis."""
    table_ids: list[UUID]
    include_semantic_similarity: bool = True


class TopologyEnrichmentResult(BaseModel):
    """Result of topology analysis."""
    relationships: list[Relationship]
    join_paths: list[JoinPath]


class Relationship(BaseModel):
    """A detected relationship."""
    relationship_id: UUID
    
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    
    relationship_type: str  # 'foreign_key', 'hierarchy', 'correlation', 'semantic'
    cardinality: str | None = None  # '1:1', '1:n', 'n:m'
    
    confidence: float
    detection_method: str
    evidence: dict[str, Any]


class JoinPath(BaseModel):
    """A computed join path between tables."""
    from_table: str
    to_table: str
    steps: list[JoinStep]
    total_confidence: float


class JoinStep(BaseModel):
    from_column: str
    to_table: str
    to_column: str
    confidence: float
```

### Temporal Enrichment

```python
class TemporalEnrichmentRequest(BaseModel):
    """Request for temporal analysis."""
    table_ids: list[UUID]


class TemporalEnrichmentResult(BaseModel):
    """Result of temporal analysis."""
    profiles: list[TemporalProfile]


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
    gaps: list[TemporalGap]
    
    has_seasonality: bool
    seasonality_period: str | None = None
    trend_direction: str | None = None


class TemporalGap(BaseModel):
    start: datetime
    end: datetime
    missing_periods: int
```

### Interfaces
```python
# enrichment/semantic.py

async def enrich_semantic(
    request: SemanticEnrichmentRequest,
    duckdb_conn: duckdb.DuckDBPyConnection,
    metadata_conn: asyncpg.Connection,
) -> Result[SemanticEnrichmentResult]:
    """Extract semantic metadata from typed tables."""
    ...


# enrichment/topology.py (wrapper around TDA prototype)

async def enrich_topology(
    request: TopologyEnrichmentRequest,
    duckdb_conn: duckdb.DuckDBPyConnection,
    metadata_conn: asyncpg.Connection,
) -> Result[TopologyEnrichmentResult]:
    """
    Detect relationships using TDA prototype.
    DO NOT REIMPLEMENT TDA - call prototype code.
    """
    from prototypes.topology import analyze_topology
    ...


# enrichment/temporal.py

async def enrich_temporal(
    request: TemporalEnrichmentRequest,
    duckdb_conn: duckdb.DuckDBPyConnection,
    metadata_conn: asyncpg.Connection,
) -> Result[TemporalEnrichmentResult]:
    """Extract temporal patterns from time columns."""
    ...
```

## Quality Module

The quality module provides **context-focused output** for LLM/API consumption.
Instead of computing quality scores, it surfaces raw metrics, flags, and issues
that consumers can interpret within their own context.

### Output Models
```python
class ColumnQualityContext(BaseModel):
    """Quality context for a single column - optimized for LLM consumption."""
    column_id: str
    column_name: str
    table_id: str
    table_name: str

    # Raw metrics (not scores)
    null_ratio: float | None = None
    cardinality_ratio: float | None = None
    outlier_ratio: float | None = None
    parse_success_rate: float | None = None

    # Temporal metrics
    is_stale: bool | None = None
    data_freshness_days: float | None = None
    has_seasonality: bool | None = None
    has_trend: bool | None = None

    # Statistical metrics
    benford_compliant: bool | None = None

    # Actionable flags for quick filtering decisions
    flags: list[str] = []  # e.g., ["high_nulls", "benford_violation", "stale_data"]

    # Issues with evidence
    issues: list[QualitySynthesisIssue] = []

    # Filter hints (LLM-generated when available)
    filter_hints: list[str] = []


class TableQualityContext(BaseModel):
    """Quality context for a table."""
    table_id: str
    table_name: str
    row_count: int | None = None
    column_count: int = 0

    # Column contexts
    columns: list[ColumnQualityContext] = []

    # Table-level issues (topological, domain)
    issues: list[QualitySynthesisIssue] = []

    # Topological summary
    betti_0: int | None = None  # Connected components
    betti_1: int | None = None  # Cycles
    orphaned_components: int | None = None

    # Flags for quick assessment
    flags: list[str] = []  # e.g., ["fragmented", "has_anomalous_cycles"]


class DatasetQualityContext(BaseModel):
    """Quality context for entire dataset."""
    tables: list[TableQualityContext] = []
    cross_table_issues: list[QualitySynthesisIssue] = []

    # Summary counts
    total_tables: int = 0
    total_columns: int = 0
    total_issues: int = 0
    critical_issue_count: int = 0

    # Issues by category (for quick overview)
    issues_by_severity: dict[str, int] = {}
    issues_by_dimension: dict[str, int] = {}

    # LLM-generated content (when available)
    summary: str | None = None
    filter_recommendations: list[dict[str, Any]] = []

    computed_at: datetime | None = None


class QualitySynthesisIssue(BaseModel):
    """A quality issue detected during analysis."""
    issue_id: str
    issue_type: str  # e.g., 'benford_violation', 'large_gap', 'high_nulls'
    severity: str  # 'info', 'warning', 'error', 'critical'
    dimension: str  # Quality dimension: completeness, validity, consistency, etc.

    table_id: str | None = None
    column_id: str | None = None
    column_name: str | None = None

    description: str
    recommendation: str | None = None
    evidence: dict[str, Any] = {}

    source_pillar: int  # Which pillar detected this (1-5)
    source_module: str
    detected_at: datetime


class QualityRule(BaseModel):
    """A quality rule for validation (generated by LLM or config)."""
    rule_id: str
    table_name: str
    column_name: str | None = None
    rule_name: str
    rule_type: str
    rule_expression: str
    parameters: dict[str, Any] = {}
    severity: str = "warning"
    source: str = "auto"  # 'auto', 'llm', 'manual', 'rule', 'ontology'
    description: str | None = None
```

### Interface
```python
# quality/context.py

async def format_dataset_quality_context(
    table_ids: list[str],
    session: AsyncSession,
    duckdb_conn: duckdb.DuckDBPyConnection | None = None,
    llm_service: LLMService | None = None,
) -> DatasetQualityContext:
    """
    Format quality context for a dataset.

    Aggregates quality metrics from all pillars into a context-focused
    output optimized for LLM/API consumption.

    Args:
        table_ids: List of table IDs to include
        session: Database session for metadata queries
        duckdb_conn: Optional DuckDB connection for row counts
        llm_service: Optional LLM service for filter recommendations

    Returns:
        DatasetQualityContext with metrics, flags, and issues
    """
    ...


async def format_table_quality_context(
    table_id: str,
    session: AsyncSession,
    duckdb_conn: duckdb.DuckDBPyConnection | None = None,
) -> TableQualityContext | None:
    """Format quality context for a single table."""
    ...


async def format_column_quality_context(
    column: Column,
    table_name: str,
    session: AsyncSession,
) -> ColumnQualityContext:
    """Format quality context for a single column."""
    ...
```

## Context Module

### Input
```python
class ContextRequest(BaseModel):
    """Request for context document."""
    tables: list[str]
    ontology: str = "general"
    include_modules: list[str] = ["statistical", "semantic", "topological", "temporal", "quality"]
    include_suggested_queries: bool = True


class MetricRequest(BaseModel):
    """Request for available metrics."""
    ontology: str
    tables: list[str] | None = None  # filter to applicable tables
```

### Output
```python
class ContextDocument(BaseModel):
    """The main context document for AI consumption."""
    generated_at: datetime
    
    # Data inventory
    tables: list[TableContext]
    relationships: list[Relationship]
    
    # Ontology interpretation
    ontology: str
    relevant_metrics: list[MetricDefinition]
    domain_concepts: list[DomainConcept]
    
    # Quality summary
    quality_summary: QualitySummary
    warnings: list[str]
    
    # For AI
    suggested_queries: list[SuggestedQuery]


class TableContext(BaseModel):
    """Context for a single table."""
    name: str
    description: str | None
    
    # Statistical
    row_count: int
    columns: list[ColumnContext]
    
    # Semantic
    entity_type: str | None
    grain_columns: list[str]
    is_fact_table: bool
    is_dimension_table: bool
    
    # Temporal
    time_columns: list[str]
    granularity: str | None
    date_range: tuple[datetime, datetime] | None

    # Quality context (flags and issues, not scores)
    quality_flags: list[str] = []  # e.g., ["has_orphaned_keys", "high_null_columns"]
    quality_issue_count: int = 0


class ColumnContext(BaseModel):
    """Context for a single column."""
    name: str
    data_type: str
    description: str | None

    # Statistical
    null_ratio: float
    cardinality_ratio: float

    # Semantic
    semantic_role: SemanticRole
    business_name: str | None
    detected_unit: str | None

    # Quality context (flags and issues, not scores)
    quality_flags: list[str] = []  # e.g., ["high_nulls", "outliers", "stale_data"]
    quality_issue_count: int = 0


class MetricDefinition(BaseModel):
    """A metric that can be computed."""
    name: str
    formula: str
    description: str
    required_columns: list[str]
    applicable_tables: list[str]
    output_type: str  # 'number', 'percentage', 'currency'


class DomainConcept(BaseModel):
    """A domain concept from the ontology."""
    name: str
    description: str
    mapped_columns: list[ColumnRef]


class QualitySummary(BaseModel):
    """Summary of data quality (context-focused, no scores)."""
    tables_assessed: int
    columns_assessed: int
    rules_executed: int
    issues_found: int
    critical_issue_count: int
    issues_by_severity: dict[str, int] = {}  # {"critical": 2, "warning": 5, ...}
    issues_by_dimension: dict[str, int] = {}  # {"completeness": 3, "validity": 2, ...}
    critical_issues: list[str]  # descriptions of critical issues


class SuggestedQuery(BaseModel):
    """A suggested SQL query for the context (LLM-generated)."""
    name: str
    description: str
    category: str  # 'overview', 'metrics', 'trends', 'segments', 'quality'
    sql: str
    complexity: str  # 'simple', 'moderate', 'complex'


class ContextSummary(BaseModel):
    """Natural language summary of the data context (LLM-generated)."""
    summary: str           # 2-3 paragraph overview
    key_facts: list[str]   # bullet points
    warnings: list[str]    # important caveats
```

### Context Document

```python
class ContextDocument(BaseModel):
    """
    The primary output for AI consumption.
    Combines all metadata with LLM-generated content.
    """
    # Core metadata
    tables: list[TableContext]
    relationships: list[Relationship]
    
    # Ontology
    ontology: str
    relevant_metrics: list[MetricDefinition]
    domain_concepts: list[DomainConcept]
    
    # Quality
    quality_summary: QualitySummary
    
    # LLM-generated content
    suggested_queries: list[SuggestedQuery]  # LLM-generated
    context_summary: ContextSummary | None   # LLM-generated
    
    # Metadata
    generated_at: datetime
    llm_features_used: list[str]  # which LLM features contributed
```

### Interface
```python
# context/assembly.py

async def get_context(
    request: ContextRequest,
    duckdb_conn: duckdb.DuckDBPyConnection,
    metadata_conn: asyncpg.Connection,
) -> Result[ContextDocument]:
    """
    Assemble context document from metadata.
    This is the primary interface for AI consumption.
    """
    ...


# context/ontologies.py

async def get_metrics(
    request: MetricRequest,
    metadata_conn: asyncpg.Connection,
) -> Result[list[MetricDefinition]]:
    """Get applicable metrics for ontology and tables."""
    ...


async def load_ontology(
    name: str,
    metadata_conn: asyncpg.Connection,
) -> Result[Ontology]:
    """Load ontology definition."""
    ...
```

## LLM Module

### Configuration
```python
class LLMConfig(BaseModel):
    """LLM configuration from config/llm.yaml."""
    active_provider: str  # 'anthropic', 'openai', 'local'
    
    features: LLMFeatures
    limits: LLMLimits
    privacy: LLMPrivacy


class LLMFeatures(BaseModel):
    """LLM feature toggles."""
    semantic_analysis: FeatureConfig
    quality_rule_generation: FeatureConfig
    suggested_queries: FeatureConfig
    context_summary: FeatureConfig


class FeatureConfig(BaseModel):
    enabled: bool = True
    model_tier: str = "balanced"  # 'fast', 'balanced'
    prompt_file: str
```

### Provider Interface
```python
# llm/providers/base.py

class LLMRequest(BaseModel):
    """Request to LLM provider."""
    prompt: str
    max_tokens: int = 4000
    temperature: float = 0.0
    response_format: str = "json"


class LLMResponse(BaseModel):
    """Response from LLM provider."""
    content: str
    model: str
    input_tokens: int
    output_tokens: int
    cached: bool = False


class LLMProvider(ABC):
    """Abstract base for LLM providers."""
    
    @abstractmethod
    async def complete(self, request: LLMRequest) -> LLMResponse:
        """Send completion request."""
        pass
```

### Feature Functions
```python
# llm/features/semantic.py

async def llm_analyze_semantics(
    tables: list[TableProfile],
    ontology: Ontology,
    llm_config: LLMConfig,
    session: AsyncSession,
) -> Result[SemanticEnrichmentResult]:
    """
    Run LLM-based semantic analysis.
    
    Returns semantic annotations, entity detections, and relationships
    for all tables and columns.
    """
    ...


# llm/features/quality.py

async def llm_generate_rules(
    schema: SemanticEnrichmentResult,
    ontology: Ontology,
    llm_config: LLMConfig,
    session: AsyncSession,
) -> Result[list[QualityRule]]:
    """
    Generate quality rules using LLM.
    
    Rules are domain-appropriate based on semantic understanding.
    """
    ...


# llm/features/queries.py

async def llm_generate_queries(
    schema: SemanticEnrichmentResult,
    ontology: Ontology,
    llm_config: LLMConfig,
    session: AsyncSession,
) -> Result[list[SuggestedQuery]]:
    """
    Generate suggested SQL queries using LLM.
    
    Queries cover: overview, metrics, trends, segments, quality.
    """
    ...


# llm/features/summary.py

async def llm_generate_summary(
    schema: SemanticEnrichmentResult,
    quality_summary: QualitySummary,
    llm_config: LLMConfig,
    session: AsyncSession,
) -> Result[ContextSummary]:
    """
    Generate natural language context summary using LLM.
    
    Returns overview, key facts, and warnings.
    """
    ...
```

### Privacy (SDV Integration)
```python
# llm/privacy.py

async def prepare_samples_for_llm(
    table: TableProfile,
    llm_config: LLMConfig,
) -> list[dict]:
    """
    Prepare sample data for LLM analysis.
    
    If use_synthetic_samples is enabled, generates synthetic data
    using SDV for columns matching sensitive_patterns.
    """
    if not llm_config.privacy.use_synthetic_samples:
        return table.sample_values[:llm_config.privacy.max_sample_values]
    
    # Use SDV to generate synthetic samples for sensitive columns
    synthetic = await generate_synthetic_samples(
        table, 
        llm_config.privacy.sensitive_patterns,
        count=llm_config.privacy.synthetic_sample_count,
    )
    return synthetic
```

## API Module (FastAPI)

```python
# api/routes/context.py

@router.post("/context")
async def get_context(
    request: ContextRequest,
    duckdb: DuckDBDep,
    metadata: MetadataDep,
) -> ContextDocument:
    """Get context document for tables."""
    result = await assembly.get_context(request, duckdb, metadata)
    if not result.success:
        raise HTTPException(500, result.error)
    return result.value


@router.get("/metrics/{ontology}")
async def get_metrics(
    ontology: str,
    tables: list[str] | None = Query(None),
    metadata: MetadataDep,
) -> list[MetricDefinition]:
    """Get available metrics for ontology."""
    ...


# api/routes/sources.py

@router.post("/sources")
async def add_source(
    config: SourceConfig,
    duckdb: DuckDBDep,
    metadata: MetadataDep,
) -> StagingResult:
    """Ingest a new data source."""
    ...


@router.get("/sources/{source_id}/tables")
async def get_tables(
    source_id: UUID,
    metadata: MetadataDep,
) -> list[TableSummary]:
    """Get tables for a source."""
    ...


# api/routes/metadata.py

@router.get("/tables/{table_id}/profile")
async def get_profile(
    table_id: UUID,
    metadata: MetadataDep,
) -> list[ColumnProfile]:
    """Get column profiles for table."""
    ...


@router.put("/columns/{column_id}/annotation")
async def update_annotation(
    column_id: UUID,
    annotation: SemanticAnnotationUpdate,
    metadata: MetadataDep,
) -> SemanticAnnotation:
    """Update semantic annotation (human-in-loop)."""
    ...
```

## MCP Module

```python
# mcp/tools.py

from mcp.server import Server
from mcp.types import Tool, TextContent

server = Server("dataraum-context")


@server.tool()
async def get_context(
    tables: list[str],
    ontology: str = "general",
) -> TextContent:
    """
    Get rich metadata context for tables.
    
    Returns a structured context document including:
    - Statistical metadata (row counts, distributions)
    - Semantic metadata (business terms, entity types)
    - Topological metadata (relationships, join paths)
    - Temporal metadata (granularity, date ranges)
    - Quality context (flags, issues, warnings)
    - Applicable metrics from the ontology
    
    Args:
        tables: List of table names to get context for
        ontology: Ontological context ('financial_reporting', 'marketing', 'general')
    
    Returns:
        Formatted context document for AI consumption
    """
    ...


@server.tool()
async def query(sql: str) -> TextContent:
    """
    Execute a SQL query against the data.
    
    Args:
        sql: SQL query to execute (read-only)
    
    Returns:
        Query results as formatted text
    """
    ...


@server.tool()
async def get_metrics(ontology: str) -> TextContent:
    """
    Get available metrics for an ontological context.
    
    Args:
        ontology: Ontological context name
    
    Returns:
        List of metrics with formulas and required columns
    """
    ...


@server.tool()
async def annotate(
    table: str,
    column: str,
    business_name: str | None = None,
    description: str | None = None,
    semantic_role: str | None = None,
) -> TextContent:
    """
    Update semantic annotation for a column.
    
    This enables human-in-the-loop refinement of metadata.
    
    Args:
        table: Table name
        column: Column name
        business_name: Human-friendly name
        description: Description of the column
        semantic_role: One of 'measure', 'dimension', 'key', 'attribute', 'timestamp'
    
    Returns:
        Confirmation of update
    """
    ...
```

## Hamilton Dataflows

```python
# dataflows/ingest.py

"""
Ingest dataflow using Apache Hamilton.

Hamilton automatically builds the DAG from function dependencies.
Each function's parameters define its upstream dependencies.
"""

from hamilton import driver
from hamilton.function_modifiers import check_output, tag

# === Staging Functions ===

def source_config(config: SourceConfig) -> SourceConfig:
    """Pass-through for source configuration (entry point)."""
    return config


def staging_result(
    source_config: SourceConfig,
    duckdb_conn: duckdb.DuckDBPyConnection,
    metadata_conn: asyncpg.Connection,
) -> StagingResult:
    """Stage raw data from source."""
    return stage_source(source_config, duckdb_conn, metadata_conn)


# === Profiling Functions ===

@check_output(data_type=ProfileResult)
def profile_result(
    staging_result: StagingResult,
    duckdb_conn: duckdb.DuckDBPyConnection,
    metadata_conn: asyncpg.Connection,
) -> ProfileResult:
    """Profile all columns in staged tables."""
    return profile_tables(staging_result.table_ids, duckdb_conn, metadata_conn)


def auto_type_decisions(
    profile_result: ProfileResult,
    metadata_conn: asyncpg.Connection,
    min_confidence: float = 0.95,
) -> list[TypeDecision]:
    """Generate automatic type decisions from candidates."""
    return auto_decide_types(profile_result.type_candidates, metadata_conn, min_confidence)


def needs_type_review(auto_type_decisions: list[TypeDecision]) -> bool:
    """Check if any type decisions need human review."""
    return any(d.confidence < 0.95 for d in auto_type_decisions)


# === Checkpoint: Type Review ===
# When needs_type_review is True, the orchestrator should:
# 1. Save checkpoint to metadata.checkpoints
# 2. Create review items in metadata.review_queue
# 3. Wait for human approval
# 4. Resume with approved decisions


def final_type_decisions(
    auto_type_decisions: list[TypeDecision],
    needs_type_review: bool,
    human_decisions: list[TypeDecision] | None = None,  # Injected after review
) -> list[TypeDecision]:
    """Merge auto and human type decisions."""
    if needs_type_review and human_decisions:
        return human_decisions
    return auto_type_decisions


# === Type Resolution ===

def type_resolution_result(
    staging_result: StagingResult,
    final_type_decisions: list[TypeDecision],
    duckdb_conn: duckdb.DuckDBPyConnection,
    metadata_conn: asyncpg.Connection,
) -> TypeResolutionResult:
    """Apply type casting with quarantine for failures."""
    return resolve_types(staging_result.table_ids[0], final_type_decisions, duckdb_conn, metadata_conn)


def needs_quarantine_review(type_resolution_result: TypeResolutionResult) -> bool:
    """Check if quarantined rows need review."""
    return type_resolution_result.quarantined_rows > 0


# === Enrichment Functions ===

@tag(module="enrichment")
def semantic_result(
    staging_result: StagingResult,
    type_resolution_result: TypeResolutionResult,  # Ensures ordering
    duckdb_conn: duckdb.DuckDBPyConnection,
    metadata_conn: asyncpg.Connection,
) -> SemanticEnrichmentResult:
    """Extract semantic metadata."""
    return enrich_semantic(staging_result.table_ids, duckdb_conn, metadata_conn)


@tag(module="enrichment")
def topology_result(
    staging_result: StagingResult,
    type_resolution_result: TypeResolutionResult,
    duckdb_conn: duckdb.DuckDBPyConnection,
    metadata_conn: asyncpg.Connection,
) -> TopologyEnrichmentResult:
    """Detect relationships using TDA."""
    return enrich_topology(staging_result.table_ids, duckdb_conn, metadata_conn)


@tag(module="enrichment")
def temporal_result(
    staging_result: StagingResult,
    type_resolution_result: TypeResolutionResult,
    duckdb_conn: duckdb.DuckDBPyConnection,
    metadata_conn: asyncpg.Connection,
) -> TemporalEnrichmentResult:
    """Extract temporal patterns."""
    return enrich_temporal(staging_result.table_ids, duckdb_conn, metadata_conn)


# === Quality Functions ===

@tag(module="quality")
def quality_rules(
    staging_result: StagingResult,
    semantic_result: SemanticEnrichmentResult,
    metadata_conn: asyncpg.Connection,
    ontology: str | None = None,
) -> list[QualityRule]:
    """Generate quality rules from metadata."""
    return generate_quality_rules(staging_result.table_ids, ontology, metadata_conn)


@tag(module="quality")
def quality_assessment(
    staging_result: StagingResult,
    quality_rules: list[QualityRule],
    duckdb_conn: duckdb.DuckDBPyConnection,
    metadata_conn: asyncpg.Connection,
) -> QualityAssessmentResult:
    """Execute quality rules and collect issues."""
    return assess_quality(staging_result.table_ids, quality_rules, duckdb_conn, metadata_conn)


# === Context Assembly ===

def context_document(
    staging_result: StagingResult,
    semantic_result: SemanticEnrichmentResult,
    topology_result: TopologyEnrichmentResult,
    temporal_result: TemporalEnrichmentResult,
    quality_assessment: QualityAssessmentResult,
    metadata_conn: asyncpg.Connection,
    ontology: str = "general",
) -> ContextDocument:
    """Assemble final context document."""
    return assemble_context(
        staging_result.table_ids,
        ontology,
        metadata_conn,
    )


# === Driver Setup ===

def create_ingest_driver() -> driver.Driver:
    """Create Hamilton driver for ingest dataflow."""
    import dataflows.ingest as ingest_module
    
    return (
        driver.Builder()
        .with_modules(ingest_module)
        .build()
    )


# === Orchestration with Checkpoints ===

async def run_ingest_with_checkpoints(
    source_config: SourceConfig,
    duckdb_conn: duckdb.DuckDBPyConnection,
    metadata_conn: asyncpg.Connection,
) -> ContextDocument:
    """
    Run ingest dataflow with human-in-loop checkpoints.
    
    Checkpoints are persisted to PostgreSQL for resume capability.
    """
    dr = create_ingest_driver()
    
    # Phase 1: Stage and Profile
    phase1_results = dr.execute(
        ["staging_result", "profile_result", "auto_type_decisions", "needs_type_review"],
        inputs={
            "config": source_config,
            "duckdb_conn": duckdb_conn,
            "metadata_conn": metadata_conn,
        }
    )
    
    # Checkpoint: Type Review
    if phase1_results["needs_type_review"]:
        checkpoint_id = await save_checkpoint(
            metadata_conn,
            checkpoint_type="type_review",
            state=phase1_results,
        )
        await create_type_review_items(metadata_conn, phase1_results["auto_type_decisions"])
        
        # Wait for human approval (caller handles this)
        raise CheckpointPending(checkpoint_id, "type_review")
    
    # Phase 2: Type Resolution through Context
    final_results = dr.execute(
        ["context_document"],
        inputs={
            "config": source_config,
            "duckdb_conn": duckdb_conn,
            "metadata_conn": metadata_conn,
            "human_decisions": None,  # No overrides needed
        }
    )
    
    return final_results["context_document"]
```
