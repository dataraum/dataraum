# Metadata Data Model

## Overview

Metadata is stored via SQLAlchemy, supporting both SQLite (development) and
PostgreSQL (production). DuckDB is used for data compute only.
This separation allows metadata to be queried independently of data files.

**Database backends:**
- **SQLite** (default): Zero-config local development, file-based
- **PostgreSQL**: Production deployments, concurrent access

The schema below is shown in SQL for clarity. Implementation uses SQLAlchemy
ORM models with async support via `aiosqlite` and `asyncpg`.

## Model Organization

SQLAlchemy models are **co-located with their business logic** for better maintainability:

| Domain | Location | Models |
|--------|----------|--------|
| Core | `storage/models_v2/core.py` | Source, Table, Column |
| Ontology | `storage/models_v2/ontology.py` | Ontology, OntologyApplication |
| Profiling | `profiling/db_models.py` | StatisticalProfile, TypeCandidate, TypeDecision, correlations |
| Enrichment | `enrichment/db_models.py` | SemanticAnnotation, TableEntity, Relationship, JoinPath, topological/temporal metrics |
| Quality | `quality/db_models.py` | QualityRule, QualityResult |
| Domain Quality | `quality/domains/db_models.py` | DomainQualityMetrics, FinancialQualityMetrics, detail tables |
| LLM | `llm/db_models.py` | LLMCache |
| Graphs | `graphs/db_models.py` | GeneratedCodeRecord, GraphExecutionRecord, StepResultRecord |

All models inherit from `storage/models_v2/base.py:Base` and are registered via imports in `storage/schema.py`.

## Schema: `metadata`

> Note: For SQLite, UUID columns use TEXT with Python-generated UUIDs.
> JSONB columns use JSON. TIMESTAMPTZ uses TEXT with ISO format.

### Core Tables

```sql
-- Track all known data sources
CREATE TABLE metadata.sources (
    source_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR NOT NULL UNIQUE,
    source_type VARCHAR NOT NULL,  -- 'csv', 'parquet', 'postgres', 'api'
    connection_config JSONB,        -- encrypted connection details
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Track all tables (raw and typed)
CREATE TABLE metadata.tables (
    table_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id UUID REFERENCES metadata.sources(source_id),
    table_name VARCHAR NOT NULL,
    layer VARCHAR NOT NULL,  -- 'raw', 'typed', 'quarantine'
    duckdb_path VARCHAR,     -- path to parquet/duckdb file
    row_count BIGINT,
    created_at TIMESTAMPTZ DEFAULT now(),
    last_profiled_at TIMESTAMPTZ,
    UNIQUE(source_id, table_name, layer)
);

-- Column inventory
CREATE TABLE metadata.columns (
    column_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    table_id UUID REFERENCES metadata.tables(table_id) ON DELETE CASCADE,
    column_name VARCHAR NOT NULL,
    column_position INTEGER NOT NULL,
    raw_type VARCHAR,        -- original inferred type
    resolved_type VARCHAR,   -- final decided type
    UNIQUE(table_id, column_name)
);
```

### Statistical Metadata

```sql
-- Statistical profile per column (versioned)
CREATE TABLE metadata.column_profiles (
    profile_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    column_id UUID REFERENCES metadata.columns(column_id) ON DELETE CASCADE,
    profiled_at TIMESTAMPTZ DEFAULT now(),
    
    -- Counts
    total_count BIGINT NOT NULL,
    null_count BIGINT NOT NULL,
    distinct_count BIGINT,
    
    -- For numeric columns
    min_value DOUBLE PRECISION,
    max_value DOUBLE PRECISION,
    mean_value DOUBLE PRECISION,
    stddev_value DOUBLE PRECISION,
    percentiles JSONB,  -- {p25, p50, p75, p95, p99}
    
    -- For string columns
    min_length INTEGER,
    max_length INTEGER,
    avg_length DOUBLE PRECISION,
    
    -- Distribution
    histogram JSONB,     -- [{bucket, count}, ...]
    top_values JSONB,    -- [{value, count}, ...]
    
    -- Computed metrics
    cardinality_ratio DOUBLE PRECISION,  -- distinct/total
    null_ratio DOUBLE PRECISION          -- null/total
);

-- Create index for latest profile lookup
CREATE INDEX idx_column_profiles_latest 
ON metadata.column_profiles(column_id, profiled_at DESC);
```

### Pattern Detection Results

```sql
-- Type candidates from pattern detection
CREATE TABLE metadata.type_candidates (
    candidate_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    column_id UUID REFERENCES metadata.columns(column_id) ON DELETE CASCADE,
    detected_at TIMESTAMPTZ DEFAULT now(),
    
    data_type VARCHAR NOT NULL,      -- 'INTEGER', 'DOUBLE', 'DATE', etc.
    confidence DOUBLE PRECISION NOT NULL,
    parse_success_rate DOUBLE PRECISION,
    failed_examples JSONB,           -- sample of unparseable values
    
    -- Pattern info
    detected_pattern VARCHAR,        -- 'iso_date', 'uuid', 'email', etc.
    pattern_match_rate DOUBLE PRECISION,
    
    -- Unit detection (from Pint)
    detected_unit VARCHAR,           -- 'kg', 'm/s', 'USD', etc.
    unit_confidence DOUBLE PRECISION
);

-- Type decisions (human-reviewable)
CREATE TABLE metadata.type_decisions (
    decision_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    column_id UUID REFERENCES metadata.columns(column_id) ON DELETE CASCADE,
    
    decided_type VARCHAR NOT NULL,
    decision_source VARCHAR NOT NULL,  -- 'auto', 'manual', 'rule'
    decided_at TIMESTAMPTZ DEFAULT now(),
    decided_by VARCHAR,               -- 'system' or user identifier
    
    -- Audit trail
    previous_type VARCHAR,
    decision_reason VARCHAR,
    
    UNIQUE(column_id)  -- one active decision per column
);
```

### Semantic Metadata

```sql
-- Semantic annotations (LLM-generated or manual)
CREATE TABLE metadata.semantic_annotations (
    annotation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    column_id UUID REFERENCES metadata.columns(column_id) ON DELETE CASCADE,
    
    -- Classification
    semantic_role VARCHAR,      -- 'measure', 'dimension', 'key', 'foreign_key', 'attribute', 'timestamp'
    entity_type VARCHAR,        -- 'customer', 'transaction', 'product', etc.
    
    -- Business terms
    business_name VARCHAR,      -- human-friendly name
    business_description TEXT,  -- LLM-generated or manual description
    business_domain VARCHAR,    -- 'finance', 'marketing', 'operations'
    
    -- Ontology mapping
    ontology_term VARCHAR,      -- mapped term from ontology
    ontology_uri VARCHAR,       -- URI if using formal ontology
    
    -- Provenance
    annotation_source VARCHAR,  -- 'llm', 'manual', 'override'
    annotated_at TIMESTAMPTZ DEFAULT now(),
    annotated_by VARCHAR,       -- 'claude-sonnet-4-20250514', 'user@example.com', etc.
    confidence DOUBLE PRECISION,
    
    UNIQUE(column_id)
);

-- Entity detection at table level
CREATE TABLE metadata.table_entities (
    entity_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    table_id UUID REFERENCES metadata.tables(table_id) ON DELETE CASCADE,
    
    detected_entity_type VARCHAR NOT NULL,
    description TEXT,            -- LLM-generated table description
    confidence DOUBLE PRECISION,
    evidence JSONB,              -- columns/patterns that led to detection
    
    -- Grain
    grain_columns VARCHAR[],     -- columns that define table grain
    is_fact_table BOOLEAN,
    is_dimension_table BOOLEAN,
    
    -- Provenance
    detection_source VARCHAR,    -- 'llm', 'manual', 'override'
    detected_at TIMESTAMPTZ DEFAULT now()
);
```

### Topological Metadata

```sql
-- Detected relationships (from TDA)
CREATE TABLE metadata.relationships (
    relationship_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Source side
    from_table_id UUID REFERENCES metadata.tables(table_id),
    from_column_id UUID REFERENCES metadata.columns(column_id),
    
    -- Target side  
    to_table_id UUID REFERENCES metadata.tables(table_id),
    to_column_id UUID REFERENCES metadata.columns(column_id),
    
    -- Classification
    relationship_type VARCHAR NOT NULL,  -- 'foreign_key', 'hierarchy', 'correlation', 'semantic'
    cardinality VARCHAR,                 -- '1:1', '1:n', 'n:m'
    
    -- Confidence
    confidence DOUBLE PRECISION NOT NULL,
    detection_method VARCHAR,            -- 'tda', 'value_overlap', 'name_similarity'
    evidence JSONB,                      -- TDA features, overlap stats
    
    -- Verification
    is_confirmed BOOLEAN DEFAULT FALSE,
    confirmed_at TIMESTAMPTZ,
    confirmed_by VARCHAR,
    
    detected_at TIMESTAMPTZ DEFAULT now(),
    
    UNIQUE(from_column_id, to_column_id)
);

-- Join paths (computed from relationships)
CREATE TABLE metadata.join_paths (
    path_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    from_table_id UUID REFERENCES metadata.tables(table_id),
    to_table_id UUID REFERENCES metadata.tables(table_id),
    
    path_steps JSONB NOT NULL,  -- [{from_col, to_table, to_col}, ...]
    path_length INTEGER NOT NULL,
    total_confidence DOUBLE PRECISION,
    
    computed_at TIMESTAMPTZ DEFAULT now(),
    
    UNIQUE(from_table_id, to_table_id, path_steps)
);
```

### Temporal Metadata

```sql
-- Temporal profiles for time columns
CREATE TABLE metadata.temporal_profiles (
    temporal_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    column_id UUID REFERENCES metadata.columns(column_id) ON DELETE CASCADE,
    
    -- Range
    min_timestamp TIMESTAMPTZ,
    max_timestamp TIMESTAMPTZ,
    
    -- Granularity
    detected_granularity VARCHAR,  -- 'second', 'minute', 'hour', 'day', 'week', 'month', 'quarter', 'year'
    granularity_confidence DOUBLE PRECISION,
    dominant_gap INTERVAL,
    
    -- Completeness
    expected_periods INTEGER,
    actual_periods INTEGER,
    completeness_ratio DOUBLE PRECISION,
    
    -- Gaps
    gap_count INTEGER,
    largest_gap INTERVAL,
    gap_details JSONB,  -- [{start, end, missing_periods}, ...]
    
    -- Patterns
    has_seasonality BOOLEAN,
    seasonality_period VARCHAR,
    trend_direction VARCHAR,  -- 'increasing', 'decreasing', 'stable'
    
    profiled_at TIMESTAMPTZ DEFAULT now(),
    
    UNIQUE(column_id)
);
```

### Quality Metadata

```sql
-- Quality rules (LLM-generated or manual)
CREATE TABLE metadata.quality_rules (
    rule_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Scope
    table_id UUID REFERENCES metadata.tables(table_id),
    column_id UUID REFERENCES metadata.columns(column_id),  -- NULL for table-level
    
    -- Rule definition
    rule_name VARCHAR NOT NULL,
    rule_type VARCHAR NOT NULL,  -- 'not_null', 'unique', 'range', 'pattern', 'referential', 'custom'
    rule_expression TEXT,        -- SQL expression (DuckDB syntax)
    rule_parameters JSONB,       -- {min, max, pattern, etc.}
    
    -- Metadata
    severity VARCHAR DEFAULT 'warning',  -- 'error', 'warning', 'info'
    source VARCHAR NOT NULL,             -- 'llm', 'ontology', 'default', 'manual'
    description TEXT,                    -- LLM-generated rationale
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT now(),
    created_by VARCHAR                   -- 'claude-sonnet-4-20250514', 'user@example.com', etc.
);

-- Quality rule execution results
CREATE TABLE metadata.quality_results (
    result_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    rule_id UUID REFERENCES metadata.quality_rules(rule_id) ON DELETE CASCADE,
    
    executed_at TIMESTAMPTZ DEFAULT now(),
    
    -- Results
    total_records BIGINT,
    passed_records BIGINT,
    failed_records BIGINT,
    pass_rate DOUBLE PRECISION,
    
    -- Failure details
    failure_samples JSONB,  -- sample of failing records
    
    -- Trend
    previous_pass_rate DOUBLE PRECISION,
    trend_direction VARCHAR  -- 'improving', 'degrading', 'stable'
);

-- Aggregate quality scores
CREATE TABLE metadata.quality_scores (
    score_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Scope (one of these is set)
    table_id UUID REFERENCES metadata.tables(table_id),
    column_id UUID REFERENCES metadata.columns(column_id),
    
    -- Scores by dimension (0-1)
    completeness_score DOUBLE PRECISION,
    validity_score DOUBLE PRECISION,
    consistency_score DOUBLE PRECISION,
    uniqueness_score DOUBLE PRECISION,
    timeliness_score DOUBLE PRECISION,
    
    -- Overall
    overall_score DOUBLE PRECISION,
    
    computed_at TIMESTAMPTZ DEFAULT now()
);
```

### Ontology Definitions

```sql
-- Ontological contexts
CREATE TABLE metadata.ontologies (
    ontology_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    name VARCHAR NOT NULL UNIQUE,
    description TEXT,
    version VARCHAR,
    
    -- Content
    concepts JSONB,       -- [{name, indicators, temporal_behavior}, ...]
    metrics JSONB,        -- [{name, formula, required_concepts}, ...]
    quality_rules JSONB,  -- default rules for this ontology
    semantic_hints JSONB, -- column name patterns
    
    -- Metadata
    is_builtin BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Ontology application log
CREATE TABLE metadata.ontology_applications (
    application_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    table_id UUID REFERENCES metadata.tables(table_id),
    ontology_id UUID REFERENCES metadata.ontologies(ontology_id),
    
    -- Results
    matched_concepts JSONB,   -- which concepts were found
    applicable_metrics JSONB, -- which metrics can be computed
    applied_rules JSONB,      -- which quality rules were added
    
    applied_at TIMESTAMPTZ DEFAULT now()
);
```

### Dataflow Checkpoints (Human-in-Loop)

```sql
-- Dataflow execution checkpoints for resume capability
CREATE TABLE metadata.checkpoints (
    checkpoint_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    dataflow_name VARCHAR NOT NULL,
    source_id UUID REFERENCES metadata.sources(source_id),
    
    -- Status
    status VARCHAR NOT NULL,  -- 'pending_review', 'approved', 'completed', 'failed'
    checkpoint_type VARCHAR NOT NULL,  -- 'type_review', 'quarantine_review', 'semantic_review'
    
    -- Timing
    created_at TIMESTAMPTZ DEFAULT now(),
    resumed_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    
    -- State for resume (Hamilton inputs/outputs)
    checkpoint_state JSONB,  -- serialized intermediate results
    
    -- Results
    result_summary JSONB,
    error_message TEXT
);

-- Human review queue
CREATE TABLE metadata.review_queue (
    review_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    checkpoint_id UUID REFERENCES metadata.checkpoints(checkpoint_id),
    
    review_type VARCHAR NOT NULL,  -- 'type_decision', 'quarantine', 'semantic', 'relationship'
    item_id UUID NOT NULL,         -- reference to the item needing review
    
    -- Context
    context_data JSONB,  -- relevant info for reviewer
    suggested_action JSONB,
    
    -- Status
    status VARCHAR DEFAULT 'pending',  -- 'pending', 'approved', 'rejected', 'modified'
    reviewed_at TIMESTAMPTZ,
    reviewed_by VARCHAR,
    review_notes TEXT
);
```

### LLM Response Cache

```sql
-- Cache LLM responses to avoid redundant API calls
CREATE TABLE metadata.llm_cache (
    cache_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Cache key (hash of inputs)
    cache_key VARCHAR NOT NULL UNIQUE,
    feature VARCHAR NOT NULL,  -- 'semantic_analysis', 'quality_rules', 'suggested_queries', 'context_summary'
    
    -- Request context
    source_id UUID REFERENCES metadata.sources(source_id),
    table_ids JSONB,           -- list of table IDs included
    ontology VARCHAR,
    
    -- LLM details
    provider VARCHAR NOT NULL,  -- 'anthropic', 'openai', 'local'
    model VARCHAR NOT NULL,
    prompt_hash VARCHAR,        -- hash of prompt template used
    
    -- Response
    response_json JSONB NOT NULL,
    input_tokens INTEGER,
    output_tokens INTEGER,
    
    -- Timing
    created_at TIMESTAMPTZ DEFAULT now(),
    expires_at TIMESTAMPTZ,     -- based on cache_ttl_seconds config
    
    -- Invalidation
    is_valid BOOLEAN DEFAULT TRUE
);

CREATE INDEX idx_llm_cache_key ON metadata.llm_cache(cache_key);
CREATE INDEX idx_llm_cache_feature ON metadata.llm_cache(feature, source_id) WHERE is_valid = TRUE;
```

## Indexes

```sql
-- Performance indexes
CREATE INDEX idx_columns_table ON metadata.columns(table_id);
CREATE INDEX idx_type_candidates_column ON metadata.type_candidates(column_id);
CREATE INDEX idx_relationships_from ON metadata.relationships(from_table_id);
CREATE INDEX idx_relationships_to ON metadata.relationships(to_table_id);
CREATE INDEX idx_quality_rules_table ON metadata.quality_rules(table_id);
CREATE INDEX idx_quality_rules_column ON metadata.quality_rules(column_id);
CREATE INDEX idx_checkpoints_status ON metadata.checkpoints(status);
CREATE INDEX idx_review_queue_status ON metadata.review_queue(status) WHERE status = 'pending';
```

## Views

```sql
-- Latest profile per column
CREATE VIEW metadata.v_latest_profiles AS
SELECT DISTINCT ON (column_id) *
FROM metadata.column_profiles
ORDER BY column_id, profiled_at DESC;

-- Full column metadata (denormalized for context assembly)
CREATE VIEW metadata.v_column_metadata AS
SELECT 
    c.column_id,
    c.column_name,
    c.resolved_type,
    t.table_name,
    s.name as source_name,
    p.total_count,
    p.null_ratio,
    p.cardinality_ratio,
    tc.detected_pattern,
    tc.detected_unit,
    sa.semantic_role,
    sa.entity_type,
    sa.business_name,
    tp.detected_granularity,
    qs.overall_score as quality_score
FROM metadata.columns c
JOIN metadata.tables t ON c.table_id = t.table_id
JOIN metadata.sources s ON t.source_id = s.source_id
LEFT JOIN metadata.v_latest_profiles p ON c.column_id = p.column_id
LEFT JOIN metadata.type_candidates tc ON c.column_id = tc.column_id
LEFT JOIN metadata.semantic_annotations sa ON c.column_id = sa.column_id
LEFT JOIN metadata.temporal_profiles tp ON c.column_id = tp.column_id
LEFT JOIN metadata.quality_scores qs ON c.column_id = qs.column_id;

-- Relationship graph for context
CREATE VIEW metadata.v_relationship_graph AS
SELECT 
    r.relationship_id,
    ft.table_name as from_table,
    fc.column_name as from_column,
    tt.table_name as to_table,
    tc.column_name as to_column,
    r.relationship_type,
    r.cardinality,
    r.confidence,
    r.is_confirmed
FROM metadata.relationships r
JOIN metadata.columns fc ON r.from_column_id = fc.column_id
JOIN metadata.tables ft ON fc.table_id = ft.table_id
JOIN metadata.columns tc ON r.to_column_id = tc.column_id
JOIN metadata.tables tt ON tc.table_id = tt.table_id;
```
