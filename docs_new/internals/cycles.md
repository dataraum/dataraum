# Business Cycle Detection Module

## Reasoning & Summary

The cycles module answers: **"What business cycles exist in this data?"**

Business cycles are recurring processes: Order-to-Cash, Procure-to-Pay, Accounts Receivable, etc. Detecting them requires understanding entity flows (dimension -> fact relationships), status/state columns (cycle completion indicators), and transaction type columns (cycle stages).

Unlike other LLM-powered phases, this module uses a **multi-turn agent loop** with exploration tools, not a single-shot tool-use call. The LLM forms hypotheses from pre-loaded context, validates them by querying the data, then submits structured findings via a terminal `submit_analysis` tool.

The module runs as pipeline phase `business_cycles` (after `semantic`).

## Architecture

```
cycles/
├── __init__.py     # Public API exports
├── models.py       # Pydantic models (8 models: 4 internal + 4 LLM output)
├── db_models.py    # SQLAlchemy persistence (2 models)
├── config.py       # YAML vocabulary loader (finance domain cycles)
├── agent.py        # BusinessCycleAgent: multi-turn tool loop
├── context.py      # Context builder: semantic + relationships + entities + topology
└── tools.py        # CycleDetectionTools: 4 exploration tools + submit_analysis
```

**~1,500 LOC** across 7 files.

### Data Flow

```
BusinessCycleAgent.analyze(session, duckdb_conn, table_ids)
  │
  ├── build_cycle_detection_context()       → rich context dict
  │     ├── Tables + columns from SA
  │     ├── SemanticAnnotations (roles, entity types)
  │     ├── TableEntity classifications (fact/dimension)
  │     ├── Relationships (FK, join candidates)
  │     ├── Graph topology (hub/leaf/bridge)
  │     ├── Status columns (auto-detected from entity_type)
  │     └── Domain vocabulary (from cycles.yaml)
  │
  ├── format_context_for_prompt()           → readable string
  │
  ├── Agent loop (max 15 iterations):
  │     ├── LLM receives context + tool definitions
  │     ├── LLM calls exploration tools:
  │     │   ├── get_column_value_distribution
  │     │   ├── get_cycle_completion_metrics
  │     │   ├── get_entity_transaction_flow
  │     │   └── get_functional_dependencies
  │     └── LLM calls submit_analysis (terminal) → structured output
  │
  ├── _parse_tool_output()                  → BusinessCycleAnalysis
  │     └── map_to_canonical_type()         → vocabulary mapping
  │
  └── _persist_results()                    → BusinessCycleAnalysisRun + DetectedBusinessCycle
```

### LLM Integration

| Aspect | Detail |
|--------|--------|
| Agent type | Multi-turn tool loop (NOT single-shot LLMFeature) |
| System prompt | Hardcoded in `agent.py` (SYSTEM_PROMPT) |
| User prompt | Template with `{context}` placeholder |
| Tools | 4 exploration + 1 terminal (submit_analysis) |
| Max tokens | 4096 |
| Terminal tool | `submit_analysis` with `BusinessCycleAnalysisOutput` schema |
| Fallback | If LLM doesn't use tool, tries JSON parse of text response |

## Data Model

### Pydantic Models (models.py)

**Internal models:**

| Model | Purpose |
|-------|---------|
| `CycleStage` | Stage within a cycle: name, order, indicator column/values, metrics |
| `EntityFlow` | Entity flowing through cycle: type, column, table, fact connection |
| `DetectedCycle` | Full cycle: type, stages, entities, status tracking, completion metrics, confidence |
| `BusinessCycleAnalysis` | Complete analysis: cycles, summary, processes, quality observations, recommendations |

**LLM tool output models:**

| Model | Purpose |
|-------|---------|
| `CycleStageOutput` | LLM output for a stage |
| `EntityFlowOutput` | LLM output for an entity flow |
| `DetectedCycleOutput` | LLM output for a detected cycle |
| `BusinessCycleAnalysisOutput` | Terminal tool schema: cycles + summary fields |

### SQLAlchemy Models (db_models.py)

**BusinessCycleAnalysisRun** (`business_cycle_analysis_runs`):
- PK: `analysis_id`
- Fields: `table_ids` (JSON), timing, cycle counts, `overall_cycle_health`, `llm_model`, `tool_calls_count`, `business_summary`, `detected_processes` (JSON), `data_quality_observations` (JSON), `recommendations` (JSON), `context_summary` (JSON)
- Relationship: `detected_cycles` -> DetectedBusinessCycle (cascade delete)

**DetectedBusinessCycle** (`detected_business_cycles`):
- PK: `cycle_id`, FK: `analysis_id`
- Classification: `cycle_name`, `cycle_type` (raw LLM), `canonical_type` (vocabulary mapped), `is_known_type`, `business_value`, `confidence`
- Structure: `tables_involved` (JSON), `stages` (JSON), `entity_flows` (JSON)
- Status: `status_table`, `status_column`, `completion_value`
- Metrics: `total_records`, `completed_cycles`, `completion_rate`
- `evidence` (JSON), `detected_at`

## Configuration

### Cycle Vocabulary (`config/verticals/finance/cycles.yaml`)

Defines known cycle types with aliases, typical stages, completion indicators, and domain-specific cycles. Used to:
1. Provide domain knowledge to the LLM (via `format_cycle_vocabulary_for_context()`)
2. Map LLM output to canonical types (via `map_to_canonical_type()`)

### Vocabulary Mapping

The LLM returns free-form `cycle_type` strings. `map_to_canonical_type()` maps these to vocabulary keys via:
1. Direct match (case-insensitive)
2. Alias matching (e.g., "ar_cycle" -> "accounts_receivable")

## Consumers

| Consumer | What It Uses |
|----------|--------------|
| `business_cycles_phase` | `BusinessCycleAgent.analyze()` |
| Entropy | Cycle health feeds into entropy scoring |
| Context assembly | Detected cycles included in GraphExecutionContext |

## Cleanup History (This Refactor)

| Change | Rationale |
|--------|-----------|
| Removed `get_completion_indicators()` | Never called |
| Removed `get_entity_roles()` | Never called |
| Removed `get_analysis_hints()` | Never called (config accessed directly via `.get()`) |
| Removed `clear_config_cache()` | Never called from tests or production |
| Removed `get_correlation_between_columns()` from tools | Not in tool definitions, not dispatched in `_execute_tool` |
| Removed empty `if TYPE_CHECKING: pass` block | Dead code |
| Moved inline imports to module level | `get_config_file`, `Relationship`, `SemanticAnnotation`, `TableEntity`, `Column`, `Table`, `FunctionalDependency`, `BusinessCycleAnalysisOutput`, `datetime`/`UTC` |
| Added logger to context.py | Silent `except Exception` blocks now log at debug level |

## Roadmap

- **Anomaly detection post-processing**: Detect anomalies after cycle detection — missing expected cycles (domain-based), low completion rates (<50%), unclassified cycles (don't match vocabulary), excessive cycle count (>15). Requires `CycleAnomaly` model, `detect_cycle_anomalies()` function, and integration into `BusinessCycleAgent.analyze()` after `_parse_tool_output()`.
- **Anomaly detection configuration**: Add `anomaly_detection` section to `config/verticals/finance/cycles.yaml` with thresholds (low_completion_threshold, excessive_cycles_threshold), severity mappings, and domain-specific anomaly rules. Dependency for anomaly detection implementation.
- **Prompt template externalization**: System/user prompts are hardcoded in `agent.py` (lines 54-130); move text content to `config/system/prompts/cycles_detection.yaml`. Tool logic stays in Python. Multi-turn agent pattern differs from other agents' single-shot templates.
- **LLMFeature alignment**: Agent doesn't extend `LLMFeature`; could align for consistent config handling
- **Cross-run comparison**: Track cycle detection changes across pipeline runs
