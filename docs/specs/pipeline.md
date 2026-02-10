# Pipeline Orchestrator

## Reasoning & Summary

The pipeline module answers: **"How do we execute 18 analysis phases in the right order with parallelism and fault tolerance?"**

It replaces ad-hoc scripts with a testable DAG orchestrator that resolves dependencies, runs independent phases in parallel via `ThreadPoolExecutor` (with true parallelism on free-threaded Python 3.14t), and tracks progress via checkpoint-based resume. Each phase gets its own SQLAlchemy session and DuckDB cursor for thread safety.

The module has three layers:
1. **Base** (`base.py`): DAG definition, Phase protocol, PhaseContext/PhaseResult types
2. **Orchestrator** (`orchestrator.py`): Dependency resolution, parallel execution, checkpoint management
3. **Runner** (`runner.py`): CLI entry point, YAML config loading, phase registration

## Architecture

```
pipeline/
├── __init__.py          # Public API: Phase, PhaseContext, PhaseResult, PhaseStatus, Pipeline, run_pipeline
├── base.py              # PIPELINE_DAG, Phase protocol, PhaseContext, PhaseResult, PhaseStatus
├── orchestrator.py      # Pipeline class, PipelineConfig, ThreadPoolExecutor execution
├── runner.py            # RunConfig, create_pipeline(), run(), CLI main()
├── status.py            # get_pipeline_status() queries
├── db_models.py         # PipelineRun, PhaseCheckpoint
└── phases/
    ├── __init__.py      # Phase class re-exports (used by runner + tests)
    ├── base.py          # BasePhase with default should_skip()
    └── *_phase.py       # 18 phase implementations
```

### Data Flow

```
runner.run(RunConfig)
  │
  ├── Load pipeline YAML config
  ├── ConnectionManager.initialize()
  ├── Check/create Source record
  │
  ├── create_pipeline(config, pipeline_yaml)
  │     ├── PipelineConfig from YAML (max_parallel, fail_fast, skip_completed)
  │     └── Register 16 active phases (CrossTableQuality + GraphExecution de-configured)
  │
  └── Pipeline.run(manager, source_id, target_phase, run_config)
        ├── Create PipelineRun record
        ├── Load completed checkpoints (if skip_completed)
        ├── Resolve dependency order (PIPELINE_DAG)
        │
        ├── ThreadPoolExecutor loop:
        │     ├── Find ready phases (all deps completed/skipped)
        │     ├── Submit to pool → _run_phase()
        │     │     ├── Retry on SQLite contention (max_retries from YAML)
        │     │     └── _execute_phase() with own session + cursor
        │     │           ├── phase.should_skip(ctx) → skip if applicable
        │     │           ├── phase.run(ctx) → PhaseResult
        │     │           └── Save PhaseCheckpoint with metrics
        │     └── Process completed futures, handle failures
        │
        └── Update PipelineRun with aggregate metrics
```

### Phase Protocol

```python
class Phase(Protocol):
    name: str
    description: str
    dependencies: list[str]
    outputs: list[str]

    def run(self, ctx: PhaseContext) -> PhaseResult: ...
    def should_skip(self, ctx: PhaseContext) -> str | None: ...
```

Phases declare their dependencies as strings matching other phase names. The DAG in `base.py` defines the canonical phase order and valid dependency graph.

## Data Model

### PipelineRun (`pipeline_runs`)

| Field | Type | Purpose |
|-------|------|---------|
| `run_id` | PK | UUID |
| `source_id` | FK (indexed) | Which data source |
| `target_phase` | String | Optional target (None = run all) |
| `config` | JSON | Runtime config snapshot |
| `status` | String | running/completed/failed |
| `started_at`, `completed_at` | DateTime | Timing |
| `phases_completed/failed/skipped` | Integer | Counts |
| `total_duration_seconds` | Float | Wall time |
| `total_llm_calls/input_tokens/output_tokens` | Integer | Aggregate LLM usage |
| `total_tables/rows_processed` | Integer | Aggregate data volume |
| `error` | String | Error message if failed |

### PhaseCheckpoint (`phase_checkpoints`)

| Field | Type | Purpose |
|-------|------|---------|
| `checkpoint_id` | PK | UUID |
| `run_id` | FK → PipelineRun | Which run |
| `source_id` | String (indexed) | For resume queries |
| `phase_name` | String (indexed) | Phase identifier |
| `status` | String | completed/failed/skipped |
| `started_at`, `completed_at` | DateTime | Phase timing |
| `duration_seconds` | Float | Phase wall time |
| `outputs` | JSON | Outputs for dependent phases |
| `tables/columns/rows_processed` | Integer | Data metrics |
| `llm_calls/input_tokens/output_tokens` | Integer | LLM metrics |
| `db_queries/db_writes` | Integer | DB metrics |
| `timings` | JSON | Sub-operation timing breakdown |
| `error`, `warnings` | String/JSON | Error info |

Composite indexes: `(run_id, phase_name)`, `(source_id, phase_name)`.

## Configuration

### `config/system/pipeline.yaml`

```yaml
pipeline:
  max_parallel: 4        # ThreadPoolExecutor workers
  fail_fast: true         # Stop on first failure
  skip_completed: true    # Resume from checkpoints
  retry:
    max_retries: 2        # Retries on SQLite contention
    backoff_base: 2.0     # Exponential: base * 2^attempt

import:
  junk_columns: [...]     # CSV artifacts to drop

semantic:
  ontology: financial_reporting

temporal:
  time_column: "Transaction date"
  time_grain: monthly

slicing:
  max_cardinality: 20
  min_slice_rows: 10

quality_summary:
  variance_filter: {...}  # See quality_summary spec
```

Config is loaded via `load_pipeline_config()`, flattened via `flatten_pipeline_config()`, and passed as `run_config` dict to all phases via `PhaseContext.config`.

## Consumers

| Consumer | What It Uses |
|----------|--------------|
| `cli/commands/run.py` | `RunConfig`, `run()` |
| `cli/commands/phases.py` | `PIPELINE_DAG` |
| `cli/commands/status.py` | `PhaseCheckpoint` |
| `cli/tui/screens/home.py` | `PhaseCheckpoint` |
| `api/routers/pipeline.py` | `PIPELINE_DAG`, `PipelineConfig`, `run_pipeline()`, `PhaseCheckpoint`, `PipelineRun` |

## Cleanup History (This Refactor)

| Change | Rationale |
|--------|-----------|
| Removed `DEFAULT_JUNK_COLUMNS` from `runner.py` | Duplicate of `pipeline.yaml` `import.junk_columns` |
| Removed `PIPELINE_CONFIG_REL` constant | Inlined as string literal |
| Removed `junk_columns` from `RunConfig` | Comes from YAML, not Python defaults |
| Removed silent fallback in `load_pipeline_config()` | Fail fast if config missing |
| Moved `max_parallel`, `fail_fast`, `skip_completed` to YAML | `pipeline` section in pipeline.yaml |
| Moved `max_retries`, `backoff_base` to YAML | `pipeline.retry` section |
| Moved inline imports to module level | `get_config_file`, `PIPELINE_DAG`, `Source`, `update` |
| Replaced `structlog` in `entropy_phase.py` | Use `dataraum.core.logging.get_logger()` |

## Roadmap

- **Phase invalidation**: Detect when upstream data changed and re-run dependent phases
- **Progress callbacks**: Real-time progress for TUI/API consumers
- **Graph/query re-introduction**: Re-enable `GraphExecutionPhase` after agent cleanup
- **Cross-table quality**: Evaluate if `CrossTableQualityPhase` feeds into entropy
