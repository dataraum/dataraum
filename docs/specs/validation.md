# Validation Module

## Reasoning & Summary

The validation module answers: **"Does this data satisfy domain-specific business rules?"**

Unlike statistical quality checks (completeness, outliers), validation checks encode domain knowledge — e.g., "debits must equal credits" or "assets = liabilities + equity". These rules are defined declaratively in YAML and executed via LLM-generated SQL against the actual data.

The module runs as pipeline phase `validation` (after `semantic`) and generates SQL by passing multi-table schemas with semantic annotations to the LLM.

## Architecture

```
validation/
├── __init__.py     # Public API exports
├── models.py       # Pydantic models (7 models, 2 enums)
├── db_models.py    # SQLAlchemy persistence (2 models)
├── config.py       # YAML spec loader with LRU caching
├── agent.py        # LLM-powered SQL generation and execution
└── resolver.py     # Schema resolution and prompt formatting
```

**~1,340 LOC** across 6 files.

### Data Flow

```
ValidationAgent.run_validations(session, duckdb_conn, table_ids)
  │
  ├── get_multi_table_schema_for_llm()     → schema dict (tables + relationships)
  ├── _validate_context()                   → check tables/columns exist
  ├── _get_applicable_specs()               → load from YAML, filter by category/ID
  │
  ├── For each ValidationSpec:
  │     ├── _generate_sql(spec, schema)     → LLM call with tool use
  │     │     ├── Render prompt template (validation_sql)
  │     │     ├── Call LLM with ValidationSQLOutput tool schema
  │     │     └── Parse tool response → GeneratedSQL
  │     ├── Execute SQL via DuckDB
  │     └── _evaluate_result()              → (passed, message, details)
  │
  ├── Summarize (pass/fail/skip/error counts)
  └── _persist_results()                    → ValidationRunRecord + ValidationResultRecord
```

### LLM Integration

The agent extends `LLMFeature` and uses tool-based structured output:

| Aspect | Detail |
|--------|--------|
| Prompt template | `config/system/prompts/validation_sql.yaml` |
| Tool schema | `ValidationSQLOutput.model_json_schema()` |
| Temperature | 0.0 (deterministic SQL generation) |
| Model tier | From `config.features.validation.model_tier` |
| Fallback | If LLM doesn't use tool, tries JSON parse of text response |

The LLM receives the full multi-table schema in XML format with semantic annotations, relationship information, and SQL usage hints (quoting rules, DuckDB syntax).

## Data Model

### Pydantic Models (models.py)

| Model | Purpose |
|-------|---------|
| `ValidationSeverity` | Enum: INFO, WARNING, ERROR, CRITICAL |
| `ValidationStatus` | Enum: PASSED, FAILED, SKIPPED, ERROR |
| `ValidationSpec` | Declarative rule: ID, name, category, severity, check_type, parameters, sql_hints |
| `ValidationSQLOutput` | LLM tool response: sql, explanation, columns_used, can_validate, skip_reason |
| `GeneratedSQL` | Parsed LLM output: sql_query, is_valid, model_used, generated_at |
| `ValidationResult` | Per-check result: status, passed, message, details, sql_used, result_rows |
| `ValidationRunResult` | Run summary: results list, pass/fail/skip/error counts, has_critical_failures |

### SQLAlchemy Models (db_models.py)

**ValidationRunRecord** (`validation_runs`):
- PK: `run_id`
- `table_ids` (JSON), `table_name`, timing, summary counters
- `results` (JSON): full list of ValidationResult dicts
- Relationship: `check_results` → ValidationResultRecord (cascade delete)

**ValidationResultRecord** (`validation_results`):
- PK: `result_id`
- FK: `run_id` → ValidationRunRecord
- `validation_id`, `status`, `severity`, `passed`, `message`
- `sql_used` (Text), `details` (JSON)

**Note:** Validation data is currently write-only — persisted but not yet surfaced in TUI, CLI, API, or MCP. The only read is the duplicate-prevention check in the pipeline phase.

## Check Types

The `_evaluate_result()` method interprets SQL results based on `check_type`:

| Check Type | Pass Condition | Example |
|-----------|---------------|---------|
| `balance` | Two totals differ by ≤ tolerance | Debits = Credits |
| `comparison` | Equation holds or difference ≤ tolerance | Assets = Liabilities + Equity |
| `constraint` | Zero violating rows returned | All amounts > 0 |
| `aggregate` | Any results returned (informational) | Sum by category |
| `custom` | row_count > 0 | Arbitrary SQL check |

## Configuration

### Validation Specs

Located in `config/verticals/finance/validations/`:

| File | Check Type | Description |
|------|-----------|-------------|
| `double_entry.yaml` | balance | Debits = Credits |
| `trial_balance.yaml` | comparison | Assets = Liabilities + Equity |
| `sign_conventions.yaml` | constraint | Account sign validation |
| `fiscal_period.yaml` | — | Fiscal period checks |

Each YAML spec provides:
- `validation_id`, `name`, `description`
- `category` (for filtering), `severity`, `check_type`
- `parameters` (e.g., `tolerance: 0.01`)
- `sql_hints` (guidance for the LLM)
- `expected_outcome` (what a passing result looks like)
- `tags`, `version`

### Config Loading

`config.py` uses `core.config.get_config_dir()` for path resolution and `ValidationSpec.model_validate()` for parsing:
- `load_all_validation_specs()` — recursive YAML load, Pydantic validation, no caching
- Filtering by category, tags, or specific ID

## Consumers

| Consumer | What It Uses |
|----------|--------------|
| `validation_phase` | `ValidationAgent.run_validations()` — writes results |
| `validation_phase` | `ValidationRunRecord` — reads for duplicate prevention |
| TUI/CLI/API/MCP | Not yet connected (write-only for now) |

## Cleanup History (This Refactor)

| Change | Rationale |
|--------|-----------|
| Fixed bare except in `agent.py` JSON fallback | Now logs parse failure with validation_id |
| Added warning log when LLM skips tool use | Visibility into LLM behavior |
| Extracted magic numbers to class constants | `MAX_TOKENS`, `MAX_STORED_ROWS`, `DEFAULT_TOLERANCE` |
| Moved inline import in `resolver.py` to module level | No circular import issue; cleaner code |
| Replaced bespoke config loader with `core.config` + `model_validate()` | Consistent config pattern, fail-fast on malformed specs |
| Removed LRU cache + `clear_cache()` | Single call per pipeline run, cache added complexity for no benefit |

## Roadmap

- **Surface validation results**: Add TUI screen, CLI command, API endpoint
- **Include in context documents**: Validation pass/fail status as part of data quality context
- **Cross-vertical specs**: Add validation specs for other domains beyond finance
- **Retry logic**: Handle transient LLM failures gracefully
- **SQL timeout**: Add execution timeout for LLM-generated queries
