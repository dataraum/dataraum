# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.2] - 2026-05-14

### Added
- **HTTP transport for the MCP server (DAT-291)**. `dataraum-mcp --transport http --host <h> --port <p>` runs the server as a streamable-HTTP endpoint at `POST /mcp`, alongside an unauthenticated `GET /health`. Bearer-token auth is required and non-bypassable: the server refuses to start (`SystemExit(2)`) if `DATARAUM_MCP_TOKEN` is unset, and every `/mcp` request must carry `Authorization: Bearer <token>` (comparison via `hmac.compare_digest`). The default stdio transport is unchanged. See `docs/mcp-setup.md` HTTP section for the `claude mcp add` recipe. TLS is out-of-process — terminate at a reverse proxy for anything beyond `127.0.0.1`.
- **`resume_session` MCP tool (DAT-209)** — restore a finalized session and make it active. Pipeline data, snippets, teach overlays, and the original contract/vertical are preserved without re-running the pipeline. Call without arguments to list available archives. `archived_sessions` index table in `workspace.db` records each finalized session at `end_session` time so resume is fast and the agent can browse intent/contract/sources before picking one.
- **Database sources via yaml recipes (DAT-286)** — first non-file source kind. Microsoft SQL Server supported today; postgres/mysql/sqlite follow. Practitioners write a secret-free yaml recipe (`backend:` + named SELECT queries), commit it to git, and put the connection URL in `.env` as `DATARAUM_{NAME}_URL`. `add_source(path="erp.yaml", name="erp")` dispatches by extension. At pipeline time, `extract_backend` does `INSTALL FROM community` (for mssql) → `ATTACH READ_ONLY` → `USE catalog.{default_schema}` → `CREATE TABLE raw_{name} AS <your sql>` → `DETACH`. Every step fails loud (DAT-274 pattern). `READ_ONLY` is enforced at the extension level — best practice is still a read-only DB user (instructions in `docs/db-sources.md`). The MCP `add_source` tool dropped its old `backend`/`tables`/`credential_ref` params; surface is now just `(name, path)`.
- **`has_trend` surfaced in graph-agent context (DAT-284)**. The temporal-profile `has_trend` boolean now reaches the rendered metadata document fed to the graph-SQL-generation prompt — emitted as `"Trending over time."` in the per-column Notes column for time-axis columns. Helps the agent reason about period-comparison patterns (growth rates, YoY).
- **`<temporal_signals>` guidance in the graph SQL prompt (DAT-284)**. New section in `config/llm/prompts/graph_sql_generation.yaml` bridges the existing `temporal_behavior` column annotation (`point_in_time` vs `additive`) to the existing aggregation patterns (`end_of_period` vs `sum`). Includes a conflict-resolution rule: trust the column annotation over a misaligned step `aggregation`.
- `ConnectionConfig.for_workspace(root)` factory — SQLite-only configuration for the workspace registry.
- `ActiveSession` pointer table in `workspace.db` — single-row pointer that resolves "which session DB is active" before any session manager opens.
- DAT-192 isolation invariants test suite (`tests/integration/mcp/test_session_isolation.py`) — verifies workspace-only writes for `add_source`, fingerprint-keyed reuse, cross-source isolation, two-DB write atomicity, and orphan-session cleanup on retry.

### Changed
- **LLM models bumped to Sonnet 4.6 (DAT-284)**. `default_model` and the `balanced` tier in `config/llm/config.yaml` move from `claude-sonnet-4-5` → `claude-sonnet-4-6`. The `fast` tier (`claude-haiku-4-5`) was already current.
- **Graph execution: concurrent per-metric LLM dispatch (DAT-299 + perf tuning)**. The per-metric `agent.execute()` loop in `graph_execution_phase` now dispatches concurrently via `asyncio.to_thread + asyncio.gather` (cap of 10 in-flight LLM calls). Each parallel worker gets its own SQLAlchemy session (auto-commit via `manager.session_scope()`) and DuckDB cursor; the main session is untouched during parallel execution; snippet promotion runs sequentially post-gather. **Wall-clock impact on the finance fixture: `graph_execution` 283s → 77s (−73%).** No change to LLM call count, snippet content, or response shape. Falls back to a serial loop in unit tests where the connection manager isn't wired.
- **Analysis ThreadPoolExecutor default `max_workers` 4 → 8** across temporal, relationships, statistics, and correlation modules. Helps only under `PYTHON_GIL=0` (the 3.14t production target); under GIL the threads serialize on CPU anyway, so no regression risk on standard CPython.
- **Source model: single source per session (DAT-290)**. The `multi_source` synthetic pattern is gone. Each investigation session is bound to exactly one named source from the workspace registry, picked at session start.
  - **MCP API**: `begin_session(source="name", intent, ...)` — the `source` parameter is now required. `add_source` errors when the name is already registered (the registry is append-only). New `list_sources` MCP tool surfaces what's registered without revealing credentials.
  - **Response shape**: `begin_session` / `resume_session` return `source: "name"` (scalar). The previous `sources: [list]` field is gone — every session has exactly one source by construction. `resume_session()` archive listings show `source: "name"` per entry.
  - **`add_source` next_steps hint** now points at `begin_session` instead of `measure`.
  - **What this fixes**: DAT-288 (the import phase no longer short-circuits when an old session's raw tables exist — `should_skip` queries by the real source_id), DAT-289 (per-source failures surface verbatim instead of being swallowed into a generic warning), DAT-285 in spirit (workspace cleanliness — no synthetic rows to drift).
  - **What's deferred**: cross-source analysis in a single session. If you need to combine MSSQL with Postgres in one investigation, the answer is to extend the recipe yaml to declare multiple connections (v0.4+); reintroducing multi-source semantics is out of scope.
- **Workspace.db schema (DAT-290)**: `archived_sessions.source_names` (JSON list) → `archived_sessions.source_name` (scalar string). Existing workspaces with the old column require `rm -rf ~/.dataraum/` — consistent with DAT-192/DAT-209/DAT-286 precedent.
- **Workspace layout (DAT-192)**: the MCP server now isolates investigations by source set. The flat `~/.dataraum/workspace/{metadata.db, data.duckdb}` layout is gone. New layout:
  - `~/.dataraum/workspace.db` — source registry + active-session pointer (SQLite-only, no DuckDB)
  - `~/.dataraum/sessions/{fingerprint}/{metadata.db, data.duckdb}` — per-source-set analysis data; `begin_session` with the same sources reuses the same fingerprint directory and skips re-running the pipeline
  - `~/.dataraum/archive/{session_id}/` — ended sessions (existing flow, now archives a session directory rather than the workspace)
- **Migration: wipe-and-restart.** No automatic migration. Users upgrading from v0.2.1 must `rm -rf ~/.dataraum/` (or move it elsewhere) before the first v0.2.2 invocation. Source registrations and prior session data do not transfer.

### Fixed
- **LLM resilience: hard-fail on systemic LLM failures (DAT-274)**. Previously, several pipeline phases swallowed LLM `Result.fail` and continued with empty output: cold-start ontology induction, tier-1 column annotation, validation induction, cycle induction, metric induction, and enrichment recommendations. The pipeline ran to "completion" with degraded data, `measure()` reported `status: complete`, and the agent had no signal that anything went wrong. Now these phases hard-fail when their LLM call fails or returns empty results, the scheduler's natural cascade halts dependent phases, and `measure()` surfaces the failure detail.
- **`AnthropicProvider` error classification**: `Result.fail` messages now distinguish `permanent` (auth, bad request, payload too large — user must fix) from `transient` (5xx, rate-limit, timeout — retry may help). Agents and surface tools can act on the kind without parsing exception types.
- **`measure()` response surfaces failure**: new `pipeline_status` field (`complete` / `running` / `failed` / `not_started`), plus `phases_failed: [{phase, error}]` when the latest pipeline run failed. The previous shape returned `status: complete` with a shortened `phases_completed` list, hiding the partial failure.
- **`_run_pipeline` returns per-phase breakdown**: instead of `{"error": "Pipeline failed: <single error>"}`, returns `phases_completed`, `phases_failed`, `phases_blocked` so the caller can see which phase stopped the pipeline and what blocked downstream.
- **`runner.RunResult.phases` includes blocked phases**: previously dropped on the way from `PipelineResult` to `RunResult`. Synthesized `status="blocked"` entries now carry the cascade information through to the MCP response.

### Removed
- **Graph-module dead code (DAT-266)**: ~865 lines deleted in a clean-cut sweep. Filter type scaffolding (`GraphType.FILTER`, `Classification`, `AppliesTo`, `ClassificationSummary`, `StepType.PREDICATE`), execution-persistence layer (`graphs/persistence.py`, `GraphExecutionRecord`/`StepResultRecord`, `graph_executions`/`step_results` tables), react-flow export pipeline (`graphs/export.py` — was exported but never called), the `graph_type` discriminator (single-valued enum after filter removal), and ~25 dead fields across `GraphStep`, `StepResult`, `GraphExecution`, `OutputDef`, `GraphMetadata`, `QueryAssumption`, `ExecutionContext`. LLM prompt sweep removed filter-graph guidance from `graph_sql_generation.yaml`.
- **Unused validate-only DB-source flow (DAT-286)**: replaced by recipe-driven extraction. Deleted: `SourceManager.add_database_source` + `_tables_to_schema_dict`, `CredentialChain.instructions_for` + `BACKEND_URL_TEMPLATES`, `validate_backend` + `BackendValidationResult` + `TablePreview` + `discover_tables`, `Source.credential_ref` + `Source.last_validated` SQLAlchemy fields, `SourceConfig.credential_ref`, MCP `add_source` parameters `backend` / `tables` / `credential_ref`, list_sources serialization of those fields. Clean cut, no shims. Wipe-and-restart applies: any prior workspace.db rows referencing `credential_ref` / `last_validated` will fail to load after upgrade.

### Known issues
- **PyPI wheel ships without `config/` (DAT-292)**. The v0.2.x wheel doesn't bundle the `config/` directory, so a plain `pip install dataraum && dataraum-mcp` cannot start a session. Workaround: after installing, `export DATARAUM_CONFIG_PATH=/path/to/dataraum-checkout/config`, or run from a source checkout (`git clone … && uv sync && uv run dataraum-mcp`). Tracked in [DAT-292](https://real-dataraum.atlassian.net/browse/DAT-292); a packaging fix is deferred and the workaround is the supported path for the v0.2.x line.

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
