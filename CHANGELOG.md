# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- **Workspace layout (DAT-192)**: the MCP server now isolates investigations by source set. The flat `~/.dataraum/workspace/{metadata.db, data.duckdb}` layout is gone. New layout:
  - `~/.dataraum/workspace.db` — source registry + active-session pointer (SQLite-only, no DuckDB)
  - `~/.dataraum/sessions/{fingerprint}/{metadata.db, data.duckdb}` — per-source-set analysis data; `begin_session` with the same sources reuses the same fingerprint directory and skips re-running the pipeline
  - `~/.dataraum/archive/{session_id}/` — ended sessions (existing flow, now archives a session directory rather than the workspace)
- **Migration: wipe-and-restart.** No automatic migration. Users upgrading from v0.2.1 must `rm -rf ~/.dataraum/` (or move it elsewhere) before the first v0.2.2 invocation. Source registrations and prior session data do not transfer.

### Added
- `ConnectionConfig.for_workspace(root)` factory — SQLite-only configuration for the workspace registry
- `ActiveSession` pointer table in `workspace.db` — single-row pointer that resolves "which session DB is active" before any session manager opens
- DAT-192 isolation invariants test suite (`tests/integration/mcp/test_session_isolation.py`) — verifies workspace-only writes for `add_source`, fingerprint-keyed reuse, cross-source isolation, two-DB write atomicity, and orphan-session cleanup on retry

### Removed
- **Graph-module dead code (DAT-266)**: ~865 lines deleted in a clean-cut sweep. Filter type scaffolding (`GraphType.FILTER`, `Classification`, `AppliesTo`, `ClassificationSummary`, `StepType.PREDICATE`), execution-persistence layer (`graphs/persistence.py`, `GraphExecutionRecord`/`StepResultRecord`, `graph_executions`/`step_results` tables), react-flow export pipeline (`graphs/export.py` — was exported but never called), the `graph_type` discriminator (single-valued enum after filter removal), and ~25 dead fields across `GraphStep`, `StepResult`, `GraphExecution`, `OutputDef`, `GraphMetadata`, `QueryAssumption`, `ExecutionContext`. LLM prompt sweep removed filter-graph guidance from `graph_sql_generation.yaml`.

## [0.2.1] - 2026-04-17

### Added
- **New MCP tools**: `teach` (9 teach types: concept, concept_property, validation, cycle, metric, type_pattern, null_value, relationship, explanation), `why` (evidence synthesis agent — explains elevated entropy and proposes teaches), `search_snippets` (discover reusable SQL patterns with provenance)
- **World model**: teach-driven vertical YAML overlay at `DATARAUM_HOME/workspace/<session>/vertical/`. Config teaches trigger a targeted phase re-run via `measure(target_phase=...)`; metadata teaches apply immediately
- **Cold-start bootstrap**: induces ontology, business cycles, validations, and metrics from the data when no matching vertical exists. New prompts: `ontology_induction`, `cycle_induction`, `validation_induction`, `metric_induction`
- **Graph execution phase**: computes business metrics via the graph agent. Each metric is cached as an authoritative `graph:{graph_id}` SQL snippet with provenance (field resolution, column mappings, LLM reasoning, repair status)
- **SQL snippet knowledge base**: `sql_snippets` + `snippet_usage` tables with `provenance` JSON. `search_snippets` exposes provenance so consumers know trust level. Snippet sources form a hierarchy: `graph:` (authoritative) > `query:` (exploratory) > `mcp:` (ad-hoc)
- **Snippet promotion path**: `teach(type="metric", inspiration_snippet_id=...)` promotes an ad-hoc `run_sql` or `query` snippet into an authoritative graph snippet on next `measure(target_phase="graph_execution")`
- **Query agent assumption tracking**: `query` responses include assumptions with dimension, target, basis (system_default/inferred/user_specified), and confidence
- **run_sql auto-repair visibility**: responses surface `repair_attempts` and `original_sql` when Haiku corrected broken SQL. Snippet provenance includes `was_repaired` flag
- **Server instructions**: MCP server emits session instructions on connect, guiding hosts when to use each tool
- **Pipeline guard**: `query` and `run_sql` block while the pipeline is running and return a clear busy state
- **File logging**: MCP server crashes are now recoverable. Structured logs + stdlib output write to `$DATARAUM_HOME/logs/mcp-server.log`
- **MCP registry publish**: release workflow publishes to the Model Context Protocol registry (OIDC auth). Server manifest at `server.json` (schema `2025-12-11`)

### Changed
- Pipeline grew from 17 to **18 phases** with the revived `graph_execution` phase
- 6 of 18 phases now use an LLM (graph_execution added to the set). Interactive agents (query, why) use LLM via MCP, not as a pipeline phase
- Snippet vocabulary restricted to `graph:` sources to keep ad-hoc noise out of discovery
- `measure(target_phase=...)` reruns a specific phase and cascades to its dependents — the normal path after a config teach

### Removed
- **Fix system retired**: `fixes.yaml`, `fix_schemas.py`, fix API/bridge, pattern filter, acceptance infrastructure. Resolution options renamed to teach types. `apply_fix` and related MCP tools superseded by `teach`
- `ResolutionOption` system removed from detectors (replaced by `teach` evidence)
- Dead filter infrastructure removed from graph module
- `GraphExecution.max_entropy_score` and `GraphExecution.entropy_warnings` (dead fields from the retired entropy interpretation agent)

### Fixed
- Graph execution now produces non-empty snippets (`standard_field` vocabulary aligned with ontology concepts)
- Config teach re-run cascade: affected phase and its dependents rerun correctly; `should_skip` logic respects `RunConfig`
- `search_snippets` resolves the full calculation chain and surfaces assumptions in provenance
- `teach(type="metric")` YAML now passes the graph loader (new `GraphSource.TEACH` value)

## [0.2.0] - 2026-03-28

### Breaking Changes
- Pipeline reduced from 23 to 17 phases — gate phases, quality_summary, entropy_interpretation, graph_execution, quality_review, analysis_review removed
- `column_quality` detector retired (circular with BBN readiness)
- CLI commands removed: `tui`, `query`, `sources`, `dev inspect`, `dev reset`, `fix`. Only `run` and `dev {phases, context}` remain
- MCP tools redesigned: old tools (`analyze`, `get_context`, `get_quality`, `apply_fix`, `continue_pipeline`, `discover_sources`, `export`) replaced by 7 practitioner tools
- Table names now prefixed with `{source_name}__` (e.g., `medium__invoices`)
- Default output directory changed from `./pipeline_output` to `~/.dataraum/workspace/`
- `textual` dependency removed
- Python minimum version lowered from 3.14 to 3.12

### Added
- **MCP practitioner tools**: `look`, `measure`, `begin_session`, `end_session`, `query`, `run_sql`, `add_source`
- **Session lifecycle**: `begin_session` creates investigation session with contract selection, `end_session` archives workspace
- **Session resumption**: `begin_session` resumes existing session after server restart
- **Investigation trace**: `InvestigationSession` + `InvestigationStep` models for MCP audit trail
- **JSON/JSONL source support**: loaded as VARCHAR via DuckDB `read_json`
- **Directory source support**: `add_source` accepts directories, scans for supported files
- **Format rejection**: unsupported file formats rejected with clear error at `add_source`
- **Export from MCP tools**: `run_sql` and `query` support `export_format` (CSV, Parquet) via DuckDB COPY
- **Export sidecar**: metadata file alongside each export with SQL, confidence, assumptions
- **Display limit push-down**: `run_sql` and `query` push LIMIT to DuckDB, no unbounded fetchall
- **Truncation signaling**: responses include `row_count`, `rows_returned`, `truncated`, `hint`
- **Prerequisite check**: `begin_session` validates API key availability
- **Source sealing**: `add_source` blocked during active session
- **Root dir architecture**: `DATARAUM_HOME` env var, with `workspace/`, `archive/`, `exports/` co-located
- **Readiness at 3 levels**: column, table, and dataset readiness via BBN
- **Target filtering**: `measure` and `look` accept target parameter with short-name resolution
- **Python 3.12/3.13 support**: lowered from 3.14-only; free-threading is optional performance enhancement
- **CI matrix**: tests run on Python 3.12, 3.13, and 3.14
- **PyPI release automation**: GitHub Release triggers publish via OIDC trusted publisher
- **UTF-8 encoding detection**: clear error message for non-UTF-8 CSV files

### Changed
- Pipeline measures entropy but does not interpret — interpretation is interactive via MCP tools
- BBN readiness replaces LLM quality grades
- `measure_entropy()` replaces gate infrastructure for on-demand scoring
- Agents use tool-use pattern (LLM generates structured output via Pydantic tool schemas)
- `execute_sql_steps()` shared by graph and query agents
- Export rewritten: single `export_sql()` using DuckDB COPY (no Python materialization)
- Session state DB-derived (not closure variables)
- Import path unified: `_load_from_path` delegates to `_load_file_source`
- Path escaping fixed across all loaders (CSV, Parquet, JSON, directory discovery)
- `pgmpy` bumped to >=1.0.0, `torch` to >=2.8.0

### Removed
- Gate phases and gate infrastructure (`persist_gate_result`, `EXIT_CHECK` events, `Resolution`/`ResolutionAction`)
- `PhaseLog.entropy_scores` field
- `is_quality_gate` from Phase protocol
- `apply_fix` MCP tool
- CLI TUI, query, sources, dev inspect, dev reset commands
- `textual` dependency
- `export_query_result()`, `export_data()`, `_export_tool_result()` — replaced by `export_sql()`
- Dead LLM prompts: `entropy_interpretation.yaml`, `quality_summary_batch.yaml`

## [0.1.0] - 2025-12-01

### Added
- 18-phase analysis pipeline (staging, profiling, enrichment, quality, context)
- MCP server with 5 tools: `analyze`, `get_context`, `get_entropy`, `evaluate_contract`, `query`
- Entropy system with uncertainty quantification across 8 dimensions
- Data readiness contracts (`aggregation_safe`, `executive_dashboard`, etc.)
- CLI (`dataraum`) with run, status, entropy, and contracts commands
- TUI for interactive pipeline monitoring
- Semantic analysis via LLM (Claude, OpenAI) or manual overrides
- Domain ontologies for financial, marketing, and custom verticals
- DuckDB compute engine with SQLite metadata storage
- Temporal analysis (granularity, gaps, seasonality, trends)
- Topological relationship detection and join path inference
- Statistical profiling (distributions, cardinality, null rates, patterns)
- Quality rule generation and scoring
- Privacy support via synthetic data generation (SDV)
- PostgreSQL backend option
