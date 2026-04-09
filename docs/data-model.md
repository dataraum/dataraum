# Metadata Data Model

## Overview

DataRaum stores metadata in SQLite (`metadata.db`) and data in DuckDB (`data.duckdb`). The metadata database contains 33 tables across 10 domains. You can query it directly with any SQLite client.

## Model Organization

SQLAlchemy models are co-located with their business logic in `db_models.py` files:

| Domain | Location | Tables |
|--------|----------|--------|
| Core | `storage/models.py` | `sources`, `tables`, `columns` |
| Typing | `analysis/typing/db_models.py` | `type_candidates`, `type_decisions` |
| Statistics | `analysis/statistics/db_models.py` | `statistical_profiles` |
| Statistical Quality | `analysis/statistics/quality_db_models.py` | `statistical_quality_metrics` |
| Eligibility | `analysis/eligibility/db_models.py` | `column_eligibility` |
| Semantic | `analysis/semantic/db_models.py` | `semantic_annotations`, `table_entities` |
| Relationships | `analysis/relationships/db_models.py` | `relationships` |
| Temporal | `analysis/temporal/db_models.py` | `temporal_column_profiles` |
| Correlation | `analysis/correlation/db_models.py` | `derived_columns` |
| Views | `analysis/views/db_models.py` | `enriched_views`, `slicing_views` |
| Slicing | `analysis/slicing/db_models.py` | `slice_definitions`, `column_slice_profiles` |
| Temporal Slicing | `analysis/temporal_slicing/db_models.py` | `column_drift_summaries`, `temporal_slice_analyses` |
| Business Cycles | `analysis/cycles/db_models.py` | `detected_business_cycles` |
| Validation | `analysis/validation/db_models.py` | `validation_results` |
| Entropy | `entropy/db_models.py` | `entropy_objects` |
| Pipeline | `pipeline/db_models.py` | `pipeline_runs`, `phase_logs` |
| Fixes | `pipeline/fixes/models.py` | `data_fixes` |
| Documentation | `documentation/db_models.py` | `fix_ledger` |
| Investigation | `investigation/db_models.py` | `investigation_sessions`, `investigation_steps` |
| Graphs | `graphs/db_models.py` | `graph_executions`, `step_results` |
| Query | `query/db_models.py` | `query_executions` |
| Snippets | `query/snippet_models.py` | `sql_snippets`, `snippet_usage` |

All models inherit from `storage/base.py:Base`.

## Schema Reference

### Core Tables

These three tables form the foundation. Everything else references them.

```sql
sources (
    source_id   VARCHAR PRIMARY KEY,
    name        VARCHAR NOT NULL,      -- e.g. "zone1", "multi_source"
    source_type VARCHAR NOT NULL,      -- "file", "multi_source"
    status      VARCHAR,               -- "registered", "analyzed"
    archived_at DATETIME               -- soft delete
);

tables (
    table_id    VARCHAR PRIMARY KEY,
    source_id   VARCHAR NOT NULL,      -- FK → sources
    table_name  VARCHAR NOT NULL,      -- e.g. "zone1__invoices"
    layer       VARCHAR NOT NULL,      -- "raw", "typed", "quarantine"
    row_count   INTEGER,
    duckdb_path VARCHAR                -- path in DuckDB
);

columns (
    column_id       VARCHAR PRIMARY KEY,
    table_id        VARCHAR NOT NULL,  -- FK → tables
    column_name     VARCHAR NOT NULL,
    original_name   VARCHAR,           -- before normalization
    column_position INTEGER NOT NULL,
    raw_type        VARCHAR,           -- initial VARCHAR type
    resolved_type   VARCHAR            -- after type inference
);
```

### Typing

Type inference results and decisions. Each column gets candidates (detected patterns) and a final decision.

```sql
type_candidates (
    candidate_id       VARCHAR PRIMARY KEY,
    column_id          VARCHAR NOT NULL,
    data_type          VARCHAR NOT NULL,      -- "INTEGER", "DOUBLE", "DATE", etc.
    confidence         FLOAT NOT NULL,
    parse_success_rate FLOAT,
    failed_examples    JSON,                  -- sample of unparseable values
    detected_pattern   VARCHAR,               -- "iso_date", "uuid", "email", etc.
    detected_unit      VARCHAR,               -- "kg", "USD", etc. (from Pint)
    quarantine_count   INTEGER,
    quarantine_rate    FLOAT
);

type_decisions (
    decision_id     VARCHAR PRIMARY KEY,
    column_id       VARCHAR NOT NULL,
    decided_type    VARCHAR NOT NULL,
    decision_source VARCHAR NOT NULL,  -- "auto", "manual", "rule"
    decision_reason VARCHAR
);
```

### Statistical Profiling

Column-level statistics and quality metrics.

```sql
statistical_profiles (
    profile_id      VARCHAR PRIMARY KEY,
    column_id       VARCHAR NOT NULL,
    layer           VARCHAR NOT NULL,      -- "raw" or "typed"
    total_count     INTEGER NOT NULL,
    null_count      INTEGER NOT NULL,
    distinct_count  INTEGER,
    null_ratio      FLOAT,
    cardinality_ratio FLOAT,
    is_unique       INTEGER,
    is_numeric      INTEGER,
    profile_data    JSON NOT NULL          -- distributions, percentiles, top values
);

statistical_quality_metrics (
    metric_id           VARCHAR PRIMARY KEY,
    column_id           VARCHAR NOT NULL,
    benford_compliant   INTEGER,
    has_outliers        INTEGER,
    iqr_outlier_ratio   FLOAT,
    zscore_outlier_ratio FLOAT,
    quality_data        JSON NOT NULL
);

column_eligibility (
    eligibility_id  VARCHAR PRIMARY KEY,
    column_id       VARCHAR NOT NULL,
    table_id        VARCHAR NOT NULL,
    status          VARCHAR NOT NULL,      -- "eligible", "excluded"
    triggered_rule  VARCHAR,               -- which rule decided
    reason          TEXT,
    metrics_snapshot JSON NOT NULL
);
```

### Semantic Analysis

LLM-generated business meaning and entity classification.

```sql
semantic_annotations (
    annotation_id        VARCHAR PRIMARY KEY,
    column_id            VARCHAR NOT NULL,
    semantic_role        VARCHAR,           -- "measure", "dimension", "key", "timestamp"
    entity_type          VARCHAR,           -- "customer", "transaction", "product"
    business_name        VARCHAR,           -- human-friendly name
    business_description TEXT,
    business_concept     VARCHAR,           -- ontology concept match
    temporal_behavior    VARCHAR,           -- "additive", "snapshot"
    unit_source_column   VARCHAR,           -- column that provides the unit
    annotation_source    VARCHAR,           -- "llm", "manual"
    confidence           FLOAT
);

table_entities (
    entity_id            VARCHAR PRIMARY KEY,
    table_id             VARCHAR NOT NULL,
    detected_entity_type VARCHAR NOT NULL,
    description          TEXT,
    is_fact_table        BOOLEAN,
    is_dimension_table   BOOLEAN,
    time_column          VARCHAR,
    grain_columns        JSON
);
```

### Relationships

Detected cross-table joins.

```sql
relationships (
    relationship_id   VARCHAR PRIMARY KEY,
    from_table_id     VARCHAR NOT NULL,
    from_column_id    VARCHAR NOT NULL,
    to_table_id       VARCHAR NOT NULL,
    to_column_id      VARCHAR NOT NULL,
    relationship_type VARCHAR NOT NULL,    -- "foreign_key", "hierarchy", "semantic"
    cardinality       VARCHAR,             -- "1:1", "1:n", "n:m"
    confidence        FLOAT NOT NULL,
    detection_method  VARCHAR,             -- "value_overlap", "name_similarity"
    is_confirmed      BOOLEAN NOT NULL,    -- LLM-confirmed or not
    evidence          JSON
);
```

### Temporal

Time series profiles and drift detection.

```sql
temporal_column_profiles (
    profile_id           VARCHAR PRIMARY KEY,
    column_id            VARCHAR NOT NULL,
    min_timestamp        DATETIME NOT NULL,
    max_timestamp        DATETIME NOT NULL,
    detected_granularity VARCHAR NOT NULL,   -- "day", "month", "quarter", etc.
    completeness_ratio   FLOAT,
    has_seasonality      BOOLEAN,
    has_trend            BOOLEAN,
    is_stale             BOOLEAN,
    profile_data         JSON NOT NULL
);

column_drift_summaries (
    id                  VARCHAR PRIMARY KEY,
    slice_table_name    VARCHAR NOT NULL,
    column_name         VARCHAR NOT NULL,
    time_column         VARCHAR NOT NULL,
    max_js_divergence   FLOAT NOT NULL,     -- max Jensen-Shannon divergence
    mean_js_divergence  FLOAT NOT NULL,
    periods_analyzed    INTEGER NOT NULL,
    periods_with_drift  INTEGER NOT NULL,
    drift_evidence_json JSON
);

temporal_slice_analyses (
    id                        VARCHAR PRIMARY KEY,
    slice_table_name          VARCHAR NOT NULL,
    time_column               VARCHAR NOT NULL,
    period_label              VARCHAR NOT NULL,
    row_count                 INTEGER,
    coverage_ratio            FLOAT,
    is_complete               INTEGER,
    z_score                   FLOAT,
    is_volume_anomaly         INTEGER,
    anomaly_type              VARCHAR,
    period_over_period_change FLOAT,
    issues_json               JSON
);
```

### Enrichment (Views, Slicing, Correlations)

Enriched views join fact and dimension tables. Slicing identifies meaningful data segments.

```sql
enriched_views (
    view_id            VARCHAR PRIMARY KEY,
    fact_table_id      VARCHAR NOT NULL,
    view_name          VARCHAR NOT NULL,
    view_sql           TEXT NOT NULL,
    dimension_table_ids JSON,
    dimension_columns  JSON,
    is_grain_verified  BOOLEAN NOT NULL
);

slice_definitions (
    slice_id        VARCHAR PRIMARY KEY,
    table_id        VARCHAR NOT NULL,
    column_id       VARCHAR NOT NULL,
    column_name     VARCHAR,
    slice_priority  INTEGER NOT NULL,
    slice_type      VARCHAR NOT NULL,
    distinct_values JSON,
    reasoning       TEXT,               -- LLM reasoning
    business_context TEXT,
    confidence      FLOAT
);

derived_columns (
    derived_id         VARCHAR PRIMARY KEY,
    table_id           VARCHAR NOT NULL,
    derived_column_id  VARCHAR NOT NULL,
    source_column_ids  JSON NOT NULL,
    derivation_type    VARCHAR NOT NULL,
    formula            VARCHAR NOT NULL,
    match_rate         FLOAT NOT NULL
);
```

### Domain Analysis

Business cycle detection and validation rules.

```sql
detected_business_cycles (
    cycle_id        VARCHAR PRIMARY KEY,
    source_id       VARCHAR NOT NULL,
    cycle_name      VARCHAR NOT NULL,
    cycle_type      VARCHAR NOT NULL,     -- "order_to_cash", "procure_to_pay", etc.
    tables_involved JSON NOT NULL,
    stages          JSON NOT NULL,
    completion_rate FLOAT,
    confidence      FLOAT NOT NULL,
    evidence        JSON NOT NULL
);

validation_results (
    result_id     VARCHAR PRIMARY KEY,
    validation_id VARCHAR NOT NULL,
    table_ids     JSON NOT NULL,
    status        VARCHAR NOT NULL,
    severity      VARCHAR NOT NULL,
    passed        BOOLEAN NOT NULL,
    message       TEXT,
    sql_used      TEXT,
    details       JSON
);
```

### Entropy

Detector scores per column per dimension. This is what the `measure` tool reads.

```sql
entropy_objects (
    object_id                VARCHAR PRIMARY KEY,
    layer                    VARCHAR NOT NULL,   -- "STRUCTURAL", "SEMANTIC", "VALUE", "COMPUTATIONAL"
    dimension                VARCHAR NOT NULL,   -- "TYPES", "BUSINESS_MEANING", "NULLS", etc.
    sub_dimension            VARCHAR NOT NULL,   -- "TYPE_FIDELITY", "NAMING_CLARITY", etc.
    target                   VARCHAR NOT NULL,   -- "column:table.col" or "table:table"
    source_id                VARCHAR,
    table_id                 VARCHAR,
    column_id                VARCHAR,
    score                    FLOAT NOT NULL,     -- 0.0 (certain) to 1.0 (uncertain)
    evidence                 JSON,               -- detector-specific evidence
    detector_id              VARCHAR NOT NULL,    -- "type_fidelity", "null_ratio", etc.
);
```

### Pipeline Execution

Run history and per-phase logs.

```sql
pipeline_runs (
    run_id       VARCHAR PRIMARY KEY,
    source_id    VARCHAR NOT NULL,
    target_phase VARCHAR,
    config       JSON NOT NULL,
    status       VARCHAR NOT NULL,     -- "running", "completed", "failed"
    started_at   DATETIME NOT NULL,
    completed_at DATETIME,
    error        VARCHAR
);

phase_logs (
    log_id           VARCHAR PRIMARY KEY,
    run_id           VARCHAR NOT NULL,
    source_id        VARCHAR NOT NULL,
    phase_name       VARCHAR NOT NULL,
    status           VARCHAR NOT NULL,
    started_at       DATETIME NOT NULL,
    completed_at     DATETIME NOT NULL,
    duration_seconds FLOAT NOT NULL,
    error            VARCHAR,
    outputs          JSON
);
```

### Investigation Sessions (MCP Audit Trail)

Every MCP tool call is recorded as a step within a session.

```sql
investigation_sessions (
    session_id       VARCHAR PRIMARY KEY,
    source_id        VARCHAR NOT NULL,
    status           VARCHAR NOT NULL,     -- "active", "ended"
    started_at       DATETIME NOT NULL,
    ended_at         DATETIME,
    intent           VARCHAR NOT NULL,
    contract         VARCHAR,              -- e.g. "exploratory_analysis"
    outcome_summary  VARCHAR,
    step_count       INTEGER NOT NULL
);

investigation_steps (
    step_id          VARCHAR PRIMARY KEY,
    session_id       VARCHAR NOT NULL,
    ordinal          INTEGER NOT NULL,
    tool_name        VARCHAR NOT NULL,     -- "look", "measure", "query", etc.
    arguments        JSON NOT NULL,
    status           VARCHAR NOT NULL,
    result_summary   VARCHAR,
    error            VARCHAR,
    started_at       DATETIME NOT NULL,
    duration_seconds FLOAT NOT NULL
);
```

### Query & Graphs

Query execution history, SQL snippet cache, and metric computation records.

```sql
query_executions (
    execution_id     VARCHAR PRIMARY KEY,
    source_id        VARCHAR NOT NULL,
    question         TEXT NOT NULL,
    sql_executed     TEXT NOT NULL,
    success          BOOLEAN NOT NULL,
    row_count        INTEGER,
    confidence_level VARCHAR NOT NULL,
    contract_name    VARCHAR
);

graph_executions (
    execution_id    VARCHAR PRIMARY KEY,
    graph_id        VARCHAR NOT NULL,      -- e.g. "dso", "current_ratio"
    graph_type      VARCHAR NOT NULL,
    output_value    JSON,
    output_interpretation VARCHAR,
    period          VARCHAR
);
```

## DuckDB Tables

The `data.duckdb` database stores the actual data (not metadata):

| Pattern | Example | Description |
|---------|---------|-------------|
| `{source}__{table}` | `zone1__invoices` | Raw data (VARCHAR columns) |
| `typed_{source}__{table}` | `typed_zone1__invoices` | Type-resolved data |
| `quarantine_{source}__{table}` | `quarantine_zone1__invoices` | Rows that failed type casting |
| `enriched_{view}` | `enriched_zone1__invoices` | Fact + dimension joined views |
| `slice_{table}_{col}` | Various | Slice analysis tables |

Query DuckDB data via the `run_sql` MCP tool or `ctx.query()` in Python.
