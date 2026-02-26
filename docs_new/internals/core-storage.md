# Core & Storage

## Reasoning & Summary

`core/` and `storage/` are the foundation of the entire engine. Every other module depends on them.

**core/** provides three concerns:
- **Configuration** (`config.py`) — Central YAML/env-var resolution. All modules load config through `get_config_file()`, `get_config_dir()`, or `load_yaml_config()`. The only place that does path-relative-to-file resolution.
- **Connections** (`connections.py`) — Thread-safe connection pool for SQLAlchemy (SQLite/PostgreSQL), DuckDB (data compute), and optional DuckDB vector embeddings (query similarity). Manages lifecycle, write serialization, and WAL mode for concurrent access.
- **Logging** (`logging.py`) — Structured logging via structlog with ContextVar-based metrics collection. Tracks per-phase and per-pipeline metrics (duration, rows, LLM calls, DB ops).

**storage/** provides the entity foundation:
- **Base** (`base.py`) — SQLAlchemy declarative base, naming conventions, `init_database()` / `reset_database()`.
- **Core models** (`models.py`) — Source, Table, Column. The root of the entire data model hierarchy.

**core/models/** provides shared types:
- `Result[T]` — Generic success/failure type used across all modules.
- Enums: `DataType`, `SemanticRole`, `RelationshipType`, `Cardinality`, `QualitySeverity`, `DecisionSource`.
- Value objects: `ColumnRef`, `TableRef`, `SourceConfig`.

## Data Model

```
Source (sources)
├── source_id: UUID (PK, auto-generated)
├── name: String (UNIQUE)
├── source_type: String ('csv', 'parquet', 'postgres')
├── connection_config: JSON (optional)
├── created_at, updated_at: DateTime (UTC)
│
└── 1:N → Table (tables) [cascade delete-orphan]
    ├── table_id: UUID (PK, auto-generated)
    ├── source_id: FK → sources
    ├── table_name: String
    ├── layer: String ('raw', 'typed', 'quarantine')
    ├── duckdb_path: String
    ├── row_count: Integer
    ├── created_at, last_profiled_at: DateTime
    ├── UNIQUE(source_id, table_name, layer)
    │
    └── 1:N → Column (columns) [cascade delete-orphan]
        ├── column_id: UUID (PK, auto-generated)
        ├── table_id: FK → tables
        ├── column_name: String
        ├── column_position: Integer
        ├── raw_type: String (original type, usually VARCHAR)
        ├── resolved_type: String (final type after resolution)
        └── UNIQUE(table_id, column_name)
```

All analysis models (TypeCandidate, StatisticalProfile, SemanticAnnotation, etc.) hang off Column or Table via foreign keys with cascade delete-orphan. See individual module specs for their models.

## Configuration

### Central Config API (`core/config.py`)

```python
from dataraum.core.config import get_config_file, get_config_dir, load_yaml_config

# Resolve a config file (raises FileNotFoundError if missing)
path = get_config_file("system/llm.yaml")

# Resolve a config directory
dir = get_config_dir("system/prompts")

# Load + parse YAML in one call
data = load_yaml_config("system/entropy/thresholds.yaml")

# Application settings (env vars + .env file)
from dataraum.core.config import get_settings
settings = get_settings()
```

Config root is resolved once via `_find_config_dir()` (walks up from package location). Can be overridden via `DATARAUM_CONFIG_PATH` env var.

### Config Directory Layout

```
config/
├── system/           # Engine behavior (same across verticals)
│   ├── llm.yaml
│   ├── pipeline.yaml
│   ├── null_values.yaml
│   ├── column_eligibility.yaml
│   ├── typing.yaml
│   ├── entropy/thresholds.yaml
│   ├── entropy/contracts.yaml
│   └── prompts/*.yaml
│
├── verticals/        # Business-specific (per domain)
│   └── finance/
│       ├── ontology.yaml
│       ├── cycles.yaml
│       ├── validations/*.yaml
│       ├── metrics/**/*.yaml
│       └── filters/*.yaml
│
└── ontologies/       # Legacy location (compat shim, will be removed)
```

### Environment Variables (via Settings)

All prefixed with `DATARAUM_`. Key settings:

| Variable | Default | Purpose |
|----------|---------|---------|
| `DATARAUM_DATABASE_URL` | `sqlite:///...` | SQLAlchemy connection string |
| `DATARAUM_DUCKDB_PATH` | `:memory:` | DuckDB database file |
| `DATARAUM_CONFIG_PATH` | auto-detected | Config root directory |
| `DATARAUM_LOG_LEVEL` | `INFO` | Log level |
| `DATARAUM_LOG_FORMAT` | `json` | `json` or `console` |

## Connection Management

### Thread Safety Model

| Resource | Read | Write | Mechanism |
|----------|------|-------|-----------|
| SQLAlchemy | Thread-safe (QueuePool) | Thread-safe (QueuePool) | Per-thread sessions |
| SQLite | Concurrent reads (WAL) | Serialized at DB level | PRAGMA journal_mode=WAL |
| DuckDB data | Thread-safe (cursor per call) | Serialized | Python mutex (`_write_lock`) |
| DuckDB vectors | Thread-safe (cursor per call) | Serialized | Python mutex (`_vectors_lock`) |

### Usage Patterns

```python
manager = get_connection_manager(output_dir)

# SQLAlchemy session (auto-commit on success, rollback on error)
with manager.session_scope() as session:
    source = session.get(Source, source_id)

# DuckDB read cursor
with manager.duckdb_cursor() as cursor:
    result = cursor.execute("SELECT * FROM typed_sales").fetchall()

# DuckDB write (serialized)
with manager.duckdb_write() as conn:
    conn.execute("INSERT INTO ...")
```

## Logging & Metrics

```python
from dataraum.core.logging import get_logger, start_phase_metrics, end_phase_metrics

logger = get_logger(__name__)
logger.info("processing_table", table_name="sales", row_count=1000)

# Phase metrics (auto-collected via ContextVars)
start_phase_metrics("typing")
# ... do work ...
metrics = end_phase_metrics()  # duration, tables, rows, LLM calls, etc.
```

## Roadmap / Planned Features

- **Config path from CLI** — Pass `--config` flag to override auto-detection. Currently only env var override.
- **PostgreSQL support** — Settings support it, ConnectionManager needs testing with PostgreSQL + asyncpg.
- **Ontology directory rethink** — Current `config/ontologies/` compat shim should be replaced by proper vertical-aware ontology loading during semantic module cleanup.
- **Metrics export** — Phase/pipeline metrics are collected but only logged. Future: export to JSON/Prometheus for monitoring.
- **Missing foreign keys** — `validation_runs.table_ids` → `tables.table_id` (normalize from JSON), `temporal_slice_analyses.run_id` → `temporal_slice_runs.run_id`, `temporal_drift_analyses.run_id` → `temporal_slice_runs.run_id`, `slice_time_matrix_entries.run_id` → `temporal_slice_runs.run_id`.
- **Missing indexes** — Add indexes on: `semantic_annotations.column_id`, `table_entities.table_id`, `column_quality_reports.source_column_id`, `column_quality_reports.slice_column_id`, `quality_summary_runs.source_table_id`, `quality_summary_runs.slice_column_id`, `detected_business_cycles.analysis_id`, `slice_definitions.table_id`, `slice_definitions.column_id`.
