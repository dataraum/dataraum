# Architecture Overview

## Table of Contents

1. [Vision & Core Insight](#vision--core-insight)
2. [System Architecture](#system-architecture)
3. [Data Processing Layers](#data-processing-layers)
4. [Metadata Generation](#metadata-generation)
5. [Context Assembly & Delivery](#context-assembly--delivery)
6. [Key Algorithms](#key-algorithms)
7. [Deployment & Scaling](#deployment--scaling)
8. [LLM Integration](#llm-integration)
9. [Extension Points](#extension-points)

## Vision & Core Insight

Traditional semantic layers tell BI tools "what things are called." This system tells AI
"what the data means, how it behaves, how it relates, and what you can compute from it."

**Core insight**: AI agents don't need tools to discover metadata at runtime. They need
**rich, pre-computed context** delivered in a format optimized for LLM consumption.

## System Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           AI CONSUMERS                                       │
│                                                                             │
│   Claude / GPT / Local LLM  ←──  MCP Server  ←──  ContextDocument           │
│                                   (4 tools)       (pre-assembled)           │
└─────────────────────────────────────────────────────────────────────────────┘
                                       ↑
┌─────────────────────────────────────────────────────────────────────────────┐
│                           CONTEXT LAYER                                      │
│                                                                             │
│   ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐        │
│   │   Ontologies    │    │    Assembly     │    │   LLM Features  │        │
│   │                 │    │                 │    │                 │        │
│   │ • financial     │───▶│ Combine all     │◀───│ • Semantic      │        │
│   │ • marketing     │    │ metadata into   │    │   analysis      │        │
│   │ • operations    │    │ ContextDocument │    │ • Quality rules │        │
│   │ • custom        │    │ + Summary       │    │ • Suggested     │        │
│   └─────────────────┘    └─────────────────┘    │   queries       │        │
│                                                  │ • Summary       │        │
│                                                  └─────────────────┘        │
└─────────────────────────────────────────────────────────────────────────────┘
                                       ↑
┌─────────────────────────────────────────────────────────────────────────────┐
│                           METADATA MODULES                                   │
│                                                                             │
│   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐      │
│   │ Statistical │  │  Semantic   │  │ Topological │  │  Temporal   │      │
│   │             │  │   (LLM)     │  │             │  │             │      │
│   │ • profiles  │  │ • roles     │  │ • FKs (TDA) │  │ • granular- │      │
│   │ • distribs  │  │ • entities  │  │ • hierarchs │  │   ity       │      │
│   │ • patterns  │  │ • terms     │  │ • join paths│  │ • gaps      │      │
│   │ • units     │  │ • relations │  │ • graph     │  │ • trends    │      │
│   └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘      │
│          │                │                │                │              │
│          └────────────────┴────────────────┴────────────────┘              │
│                                    │                                        │
│                           ┌────────▼────────┐                              │
│                           │     Quality     │                              │
│                           │     (LLM)       │                              │
│                           │ • rules (gen'd) │                              │
│                           │ • scores        │                              │
│                           │ • anomalies     │                              │
│                           └─────────────────┘                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                       ↑
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DATA LAYER                                         │
│                                                                             │
│   ┌─────────────────────────────────────────────────────────────────┐      │
│   │                         DuckDB                                   │      │
│   │                                                                  │      │
│   │   raw_{table}  ──▶  typed_{table}  ──▶  (queries)               │      │
│   │        │                  │                                      │      │
│   │        │                  ▼                                      │      │
│   │        │         quarantine_{table}                              │      │
│   │        │                                                         │      │
│   │        ▼                                                         │      │
│   │   Source files (CSV, Parquet, etc.)                             │      │
│   └─────────────────────────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────────────────────┘
                                       ↑
┌─────────────────────────────────────────────────────────────────────────────┐
│                           ORCHESTRATION                                      │
│                                                                             │
│   Apache Hamilton Dataflows                                                 │
│   ┌────────┐   ┌─────────┐   ┌──────────┐   ┌─────────┐   ┌─────────┐     │
│   │ Stage  │──▶│ Profile │──▶│ Resolve  │──▶│ Enrich  │──▶│ Quality │     │
│   │        │   │         │   │ Types    │   │         │   │         │     │
│   └────────┘   └────┬────┘   └────┬─────┘   └─────────┘   └─────────┘     │
│                     │             │                                         │
│                     ▼             ▼                                         │
│              [CHECKPOINT]  [CHECKPOINT]                                     │
│              Human review  Quarantine                                       │
│              of types      review                                           │
│              (SQLAlchemy)  (SQLAlchemy)                                     │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
┌─────────────────────────────────────────────────────────────────────────────┐
│                           METADATA STORAGE                                   │
│                                                                             │
│   SQLAlchemy (SQLite dev / PostgreSQL prod)                                 │
│   ┌─────────────────────────────────────────────────────────────────┐      │
│   │ metadata.sources          metadata.column_profiles              │      │
│   │ metadata.tables           metadata.type_candidates              │      │
│   │ metadata.columns          metadata.semantic_annotations         │      │
│   │ metadata.relationships    metadata.temporal_profiles            │      │
│   │ metadata.quality_rules    metadata.quality_scores               │      │
│   │ metadata.ontologies       metadata.checkpoints                  │      │
│   └─────────────────────────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Technology Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| **AI Interface** | MCP Server | 4 tools for AI agents |
| **API Layer** | FastAPI | HTTP REST endpoints |
| **Orchestration** | Apache Hamilton | Functional DAG with lineage |
| **Metadata Store** | SQLAlchemy | SQLite (dev) / PostgreSQL (prod) |
| **Data Compute** | DuckDB | Query and transform data |
| **Data Exchange** | PyArrow | Efficient in-memory data |
| **Pattern Detection** | Pint + Regex | Units and patterns |
| **Topology Analysis** | TDA (prototype) | Relationship detection |
| **LLM (optional)** | Anthropic/OpenAI | Semantic analysis & rules |

## Data Processing Layers

### 1. Staging Layer - VARCHAR-First Loading

**Purpose**: Preserve all raw data without information loss.

**Key Principle**: Load everything as VARCHAR to avoid irreversible type decisions at import time.

**Why VARCHAR-first?**
- CSV readers make irreversible decisions: `"N/A"` → `NULL`, locale-dependent number parsing
- Original values preserved exactly for explicit type inference later
- Failed type casts go to quarantine tables, not oblivion
- Human review possible before data loss occurs

```sql
-- What we create
CREATE TABLE raw_sales (
    _source_file VARCHAR,      -- provenance
    _source_row BIGINT,        -- for quarantine linkage
    _loaded_at TIMESTAMP,      -- audit
    date VARCHAR,              -- all actual columns as VARCHAR
    amount VARCHAR,
    customer_id VARCHAR,
    ...
);
```

### 2. Profiling Layer - Statistical Analysis & Type Inference

**Purpose**: Extract statistical metadata and infer column types with confidence scores.

**Components**:
1. **Statistical Profiler** - Distributions, cardinality, null rates
2. **Pattern Detector** - Regex patterns, unit detection (Pint), semantic hints
3. **Type Inferencer** - Cast testing with confidence scoring

**Type Inference Pipeline**:

```python
# For each column, try casting to each type
type_candidates = []
for dtype in [BIGINT, DOUBLE, DATE, TIMESTAMP, BOOLEAN]:
    success_rate = try_cast_success_rate(column, dtype)
    if success_rate > 0.5:  # At least half parse
        type_candidates.append(TypeCandidate(
            dtype=dtype,
            confidence=success_rate,
            failed_examples=get_failures(column, dtype)
        ))

# Pattern detection enriches this
pattern_result = detect_patterns(arrow_table, column)
if pattern_result.detected_unit:
    # "kg", "USD", "m/s" → suggests numeric type + semantic info
```

**Output**: `type_candidates` table with multiple ranked options per column.

### 3. Type Resolution Layer - Quarantine Pattern

**Purpose**: Apply type decisions with graceful failure handling.

**Key Innovation**: Failed casts don't break the pipeline - they go to quarantine for review.

```sql
-- Create typed table with cast attempts
CREATE TABLE typed_sales AS
SELECT 
    _source_row,
    TRY_CAST(date AS DATE) as date,
    TRY_CAST(amount AS DOUBLE) as amount,
    TRY_CAST(customer_id AS BIGINT) as customer_id,
    -- Track what failed
    CASE WHEN date IS NOT NULL AND TRY_CAST(date AS DATE) IS NULL 
         THEN TRUE ELSE FALSE END as _date_failed,
    ...
FROM raw_sales;

-- Extract failures for review
CREATE TABLE quarantine_sales AS
SELECT raw.*, 'date_cast_failed' as _reason
FROM raw_sales raw
JOIN typed_sales typed ON raw._source_row = typed._source_row
WHERE typed._date_failed;
```

**Workflow Checkpoint**: Quarantine rows trigger human review before proceeding.

## Metadata Generation

### Enrichment Overview

The enrichment layer adds four types of metadata to profiled data:

| Type | Purpose | Technology | Required |
|------|---------|------------|----------|
| **Semantic** | Column roles, entity types, relationships | LLM or YAML config | Yes |
| **Topological** | Relationship graph, FK detection | TDA prototype | No |
| **Temporal** | Time granularity, gaps, completeness | Statistical analysis | For time data |
| **Quality** | Rules, scores, anomaly detection | LLM or YAML config | No |

### Semantic Enrichment

**Purpose**: Understand what data *means* in business terms.

**Analyzes**:
- Column roles (measure, dimension, key, foreign_key, timestamp, attribute)
- Entity types (what real-world concept does this represent?)
- Business terminology mapping to ontology concepts
- Relationships between tables and columns

**LLM Mode** (when enabled):
```python
result = await llm_analyze_semantics(
    tables=table_profiles,
    ontology=selected_ontology,
    config=llm_config,
)
# Returns: SemanticAnalysisResult with roles, entities, relationships
```

**Manual Mode** (LLM disabled):
- Define annotations in `config/semantic_overrides.yaml`
- Suitable for small datasets or when privacy is critical

### Topological Enrichment (TDA)

**Purpose**: Detect relationships through structural analysis, not just value overlap.

**Approach**:
- Traditional FK detection (value overlap)
- TDA persistent homology (structural similarity)
- Semantic similarity (column name embeddings)

**Output**: Relationship graph with confidence scores and supporting evidence.

### Temporal Enrichment

**Purpose**: Understand time-series behavior and data completeness.

**For time columns**:

```python
# Granularity detection
gaps = compute_consecutive_gaps(time_column)
dominant_gap = mode(gaps)

granularity = match_granularity(dominant_gap)  # day, week, month, etc.

# Completeness analysis
expected = date_range / granularity_interval
actual = distinct_periods
completeness = actual / expected

# Gap detection
gaps = find_gaps(time_column, granularity)
```

### Quality Assessment

**Purpose**: Generate domain-appropriate rules and compute quality scores.

**Rule Generation**:

*LLM Mode* - Intelligent rule generation:
```python
result = await llm_generate_rules(
    schema=semantic_analysis,
    ontology=selected_ontology,
    config=llm_config,
)
```
Considers: column semantics, ontology constraints, cross-column logic, statistical baselines

*Manual Mode* - Configuration-based:
- `config/rules/default.yaml` - Standard rules by type/role
- `config/ontologies/*.yaml` - Domain-specific rules

**Quality Dimensions** (industry standard):

| Dimension | Metrics | Example |
|-----------|---------|---------|
| **Completeness** | Null rates, missing records | "90% of rows have email" |
| **Validity** | Range/pattern conformance | "All amounts > 0" |
| **Consistency** | Referential integrity, cross-column | "start_date <= end_date" |
| **Uniqueness** | Duplicate detection | "order_id is unique" |
| **Timeliness** | Freshness, gap analysis | "No gaps in daily data" |

## Context Assembly & Delivery

### Ontology Application

**Purpose**: Map generic metadata to domain-specific business concepts.

**Ontology Structure** (`config/ontologies/*.yaml`):
```yaml
name: financial_reporting
concepts:
  - name: revenue
    indicators: ["revenue", "sales", "income"]
    temporal_behavior: additive
metrics:
  - name: gross_margin
    formula: "(revenue - cogs) / revenue"
    required_concepts: [revenue, cogs]
quality_rules:
  - revenue >= 0
  - date should have no gaps
```

**Application Flow**:
1. Map columns to ontology concepts (pattern matching + semantic analysis)
2. Identify applicable computable metrics
3. Apply domain-specific quality rules
4. Annotate context with business semantics

### Context Document Structure

```python
ContextDocument(
    tables=[
        TableContext(
            name="sales",
            description="Daily sales transactions",  # LLM-generated
            row_count=1_000_000,
            entity_type="transaction",
            grain_columns=["sale_id"],
            time_columns=["sale_date"],
            granularity="daily",
            quality_score=0.94,
            columns=[
                ColumnContext(
                    name="amount",
                    data_type="DOUBLE",
                    semantic_role="measure",
                    business_name="Sale Amount",
                    description="Transaction value in USD",  # LLM-generated
                    detected_unit="USD",
                    quality_score=0.98,
                ),
                ...
            ]
        )
    ],
    relationships=[...],
    ontology="financial_reporting",
    relevant_metrics=[
        MetricDefinition(
            name="Total Revenue",
            formula="SUM(amount)",
            applicable_tables=["sales"],
        ),
        ...
    ],
    quality_summary=QualitySummary(
        overall_score=0.92,
        critical_issues=["Missing data in Q3 2024"],
    ),
    suggested_queries=[  # LLM-generated
        SuggestedQuery(
            name="Revenue by Month",
            description="Monthly revenue trend analysis",
            category="trends",
            sql="SELECT DATE_TRUNC('month', sale_date), SUM(amount) FROM sales GROUP BY 1",
        ),
        ...
    ],
    summary="This dataset contains 3 years of e-commerce transactions...",  # LLM-generated
    key_facts=["1M transactions", "4 related tables", "Daily granularity"],
    warnings=["Data gaps in Q3 2024 may affect trend analysis"],
)
```

**What AI receives in the context**:
- Statistical profiles (distributions, cardinality, null rates)
- Semantic annotations (roles, entity types, business terms)
- Topological structure (relationships, hierarchies, join paths)
- Temporal characteristics (granularity, gaps, trends)
- Quality assessment (scores, rules, anomalies)
- Domain context (ontology-mapped concepts, computable metrics)
- Suggested analyses (LLM-generated queries and insights)

### Delivery Interfaces

**FastAPI HTTP API**:

| Endpoint | Purpose |
|----------|---------|
| `POST /sources` | Register data source |
| `POST /profile` | Trigger profiling pipeline |
| `POST /context` | Get context document |
| `POST /query` | Execute read-only SQL |
| `GET  /metrics/{ontology}` | List available metrics |
| `PUT  /columns/{id}/annotation` | Human-in-loop updates |

**MCP Server** (AI tool interface):

| Tool | Purpose | Parameters |
|------|---------|------------|
| `get_context` | Retrieve context document | source, ontology |
| `query` | Execute SQL | sql, limit |
| `get_metrics` | List ontology metrics | ontology |
| `annotate` | Update semantic metadata | column_id, annotations |

**Design principle**: Minimal tool surface area. AI gets rich context upfront, not discovery tools.

### Orchestration with Hamilton

**Apache Hamilton** provides:
- Functional DAG (pure Python functions)
- Automatic lineage tracking
- Built-in validation (`@check_output`)
- Parallel execution
- Visual dataflow graphs

**Key Dataflows**:
- `ingest_dataflow` - Full pipeline: stage → profile → resolve → enrich → quality → context
- `profile_dataflow` - Re-profile after data changes
- `enrich_dataflow` - Re-enrich after manual type corrections

**Checkpoints** (PostgreSQL):
- Human-in-loop review points
- Workflow resume capability
- Decision audit trail

## Key Algorithms

### TDA for Relationship Detection

The topology prototype uses persistent homology to identify structural patterns 
in data distributions that suggest relationships:

1. Build point cloud from column value distributions
2. Compute persistence diagrams
3. Compare diagrams across tables for similarity
4. Combine with value overlap for FK confidence

### Type Inference Pipeline

```
Raw values
    ↓
Pattern matching (regex) → semantic hints
    ↓
Unit detection (Pint) → numeric + unit type
    ↓
Cast testing → success rates per type
    ↓
Confidence scoring → ranked candidates
    ↓
Human review (if ambiguous) → final decision
```

### Quality Rule Generation

```
Metadata signals
    ↓
Statistical baselines (mean, stddev, ranges)
    ↓
Cardinality analysis (unique, low-cardinality)
    ↓
Relationship constraints (FK integrity)
    ↓
Ontology rules (domain-specific)
    ↓
Rule deduplication and prioritization
```

## Deployment & Scaling

### Architecture Separation

| Component | Technology | Characteristics |
|-----------|------------|-----------------|
| **Data Compute** | DuckDB | Ephemeral, in-memory or file-based |
| **Metadata Store** | SQLite / PostgreSQL | Persistent, SQLite for dev, PostgreSQL for prod |
| **Data Files** | Parquet | Best DuckDB performance |

### Scalability Characteristics

| Operation | Parallelization | Notes |
|-----------|-----------------|-------|
| **Profiling** | Embarrassingly parallel | Per-column, scales linearly |
| **TDA** | Expensive, sample | Consider sampling for large tables |
| **Quality** | Parallel-friendly | Per-rule evaluation |
| **Context Assembly** | Fast | Metadata queries only |
| **Concurrent Access** | PostgreSQL | Switch from SQLite for production |

### Caching Strategy

- **Profile results**: Versioned, invalidate on data change
- **Context documents**: TTL-based cache
- **Configuration**: In-memory (ontologies, patterns, rules)

## LLM Integration

The system supports **optional** LLM integration. All LLM features have manual fallbacks.

### Modes of Operation

| Mode | Use Case | Configuration |
|------|----------|---------------|
| **LLM-powered** | Automatic semantic analysis, intelligent rules | `config/llm.yaml` + API keys |
| **Manual** | Privacy-sensitive, small datasets, cost control | YAML config files |

### Supported LLM Providers

| Provider | Config Key | Best For |
|----------|------------|----------|
| Anthropic Claude | `anthropic` | General use (recommended) |
| OpenAI GPT-4 | `openai` | Alternative provider |
| Local/Custom | `local` | Any OpenAI-compatible endpoint |

### LLM-Enhanced Features

| Feature | LLM Mode | Manual Mode | Impact if Skipped |
|---------|----------|-------------|-------------------|
| **Semantic Analysis** | Auto-infer roles, entities | `semantic_overrides.yaml` | High - required |
| **Quality Rules** | Domain-aware generation | `rules/*.yaml` | Medium |
| **Suggested Queries** | Context-aware SQL | - | Low - skip |
| **Context Summary** | Natural language | - | Low - skip |

### Customization & Privacy

**Prompt Customization** (`config/prompts/*.yaml`):
```yaml
prompt: |
  Your custom prompt here...
  {tables_json}
  {ontology_concepts}
```

**Privacy Protection** (`config/llm.yaml`):
```yaml
privacy:
  use_synthetic_samples: true  # Use SDV for synthetic data
  sensitive_patterns:
    - ".*email.*"
    - ".*ssn.*"
```

When enabled, sensitive columns are replaced with synthetic values before LLM calls.

## Extension Points

### Custom Ontologies

Add domain-specific ontologies to `config/ontologies/`:

```yaml
name: my_domain
concepts:
  - name: my_concept
    indicators: ["pattern1", "pattern2"]
metrics:
  - name: my_metric
    formula: "calculation"
quality_rules:
  - rule_definition
```

### Custom Patterns

Add pattern detectors to `config/patterns/`:

```yaml
my_patterns:
  - name: pattern_name
    pattern: regex_pattern
    inferred_type: TYPE
    examples: [...]
```
