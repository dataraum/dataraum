# Calibration Handoff

Changes in dataraum that need attention in other repos.

Updated by `/implement` in this repo. Read by `/accept` in dataraum-eval.

## 2026-05-19: DAT-323 — L4 DuckLake substrate (per-session DuckDB files → DuckLake)

### dataraum-eval
- **Changed**: `src/dataraum/server/storage.py` (new — process-wide DuckLake anchor on a named in-memory DuckDB; `bootstrap_lake` / `get_anchor` / `connect_session` / `teardown_lake` / `health_probe`), `src/dataraum/server/app.py` (FastAPI lifespan calls bootstrap + /health probes postgres + ducklake), `src/dataraum/core/connections.py` (`_init_duckdb` swap; new `_LakeScopedConnection` wrapper that intercepts `.cursor()` and `__enter__/__exit__` so cursors and cursor-of-cursors stay scoped to `lake.session_<id>`; new `bind_session_id()` method; `ConnectionConfig.duckdb_path` dropped), `src/dataraum/mcp/server.py` (three sites use `bind_session_id`), `src/dataraum/sources/{csv,json}/loader.py` (inline comment on the ephemeral `:memory:` schema-sniff carve-out), `src/dataraum/analysis/{statistics/profiler.py,statistics/quality.py,temporal/processor.py,correlation/within_table/derived_columns.py,relationships/joins.py,relationships/evaluator.py}` (8 `.cursor()` call sites converted from `cursor = X.cursor(); try: ...; finally: cursor.close()` to `with X.cursor() as cursor:` so they actually receive USE-scoped cursors via the recursive wrapper).
- **Affects**: the runtime substrate for **all** per-session pipeline data. v0.2.x's `~/.dataraum/sessions/{fp}/data.duckdb` files are gone — every per-session DuckDB connection is now opened against the named in-memory DB `:memory:dataraum_lake`, with the DuckLake catalog ATTACHed as `lake` and a per-session schema `lake.session_<id_clean>`. Pipeline writes (`raw_*`, `typed_*`, `quarantine_*`) and all analysis cursors resolve unqualified table refs against the session schema. No MCP tool surface change, no detector logic change, no response-shape change.
- **Eval setup that must change**: `tests/integration` and any calibration harness that constructs a `ConnectionManager` (directly or via `create_server`) now requires the DuckLake anchor to be bootstrapped first. Mirror the pattern in `tests/conftest.py` (worktree at `tests/conftest.py`): session-scoped `lake_catalog_url` + `lake_data_path` + `lake_anchor` fixtures, and an autouse `lake_clean` between tests to drop per-session schemas (CASCADE). MCP-flow tests need an autouse `lake_anchor` + `lake_clean` (see `tests/{unit,integration}/mcp/conftest.py` for the shape).
- **Calibrate**: no detector regressions expected (no detector code changed). Re-run cold-start `clean_eval` end-to-end to confirm the full pipeline runs against DuckLake: import → typing → semantic → relationships → correlations → temporal → graph_execution → entropy. Watch for: (a) any DDL pattern the lane smoke didn't cover (`TEMP TABLE` semantics, schema-qualified DROPs); (b) `CHECKPOINT` requirements — DuckLake buffers writes in memory until `CHECKPOINT`, so parquet files only appear under DATA_PATH after explicit flush; (c) pool ceiling under heavy parallel-phase load (`DUCKLAKE_PG_POOL_MAX` env, default 64).
- **Env vars introduced**: `DUCKLAKE_CATALOG_URL` (required, e.g. `postgresql://user:pw@host:5432/dataraum_lake_catalog`), `DUCKLAKE_DATA_PATH` (required, filesystem dir for parquet output), `DUCKLAKE_PG_POOL_MAX` (optional, default 64), `DUCKLAKE_SKIP_INSTALL` (optional — set to skip the cold-start `INSTALL ducklake` network round trip; container images should pre-install at build time).
- **Notes**:
  - **Archive design (Option A)**: DuckDB does not support `ALTER SCHEMA RENAME` (probed; "Altering schemas is not yet supported"). `end_session` no longer touches the lake schema — active vs archived is a workspace-DB flag (`ArchivedSession` row); `resume_session` rebinds via `bind_session_id(sid)`, USEing the existing `lake.session_<id>`. Schemas accumulate; lake-side GC deferred post-spine.
  - **Coverage gap (acknowledged, deferred)**: pipeline-phase integration tests under `tests/integration/{pipeline,analysis,...}` use the harness fixture `integration_duckdb` which is plain `duckdb.connect(':memory:')`. They validate phase logic in isolation from substrate, **not** against DuckLake. Substrate validation lives in `tests/platform/smoke_dat323.py` (12 lane-smoke tests) + MCP unit+integration tests. Per the user, deferred until after platform stabilization.
  - **Postgres pool config**: `SET GLOBAL pg_pool_max_connections` MUST run before the `ATTACH` (not via `postgres_configure_pool` post-attach, which doesn't propagate to DuckLake's catalog pool). `SET` without `GLOBAL` only affects the local connection.
- **Status**: pending

## 2026-05-14: DAT-299 — Concurrent per-metric LLM dispatch in graph_execution

### dataraum-eval
- **Changed**: `src/dataraum/pipeline/phases/graph_execution_phase.py` (per-metric loop refactored: prep → execute (parallel/serial) → post), `src/dataraum/graphs/agent.py` (lock around `_code_cache`), `src/dataraum/core/connections.py` (docstring tightening only), `tests/unit/pipeline/test_graph_execution_dispatch.py` (new, 9 tests).
- **Affects**: `measure` / `_run_pipeline` wall clock during cold-start runs. Per-metric `agent.execute()` calls now dispatch concurrently via `asyncio.to_thread` + `asyncio.gather` with a semaphore cap of 5. **No MCP response shape or schema changes.** Per-metric results (snippets written, snippet promotion via inspiration_snippet_id delete) are functionally unchanged.
- **Calibrate**: graph-agent metric set wall-clock check on cold-start `clean_eval`. Expected: `graph_execution` phase drops from ~4-5 min sequential to ~60-90s on the same metric count. Snippets produced and metric correctness should be identical to pre-DAT-299 (the LLM is called the same number of times, just concurrently).
- **Notes**:
  - **Per-call resource isolation**: each parallel `agent.execute()` opens its own `manager.session_scope()` (auto-commit) and its own `manager.duckdb_cursor()`. The main `ctx.session` is untouched during parallel execution.
  - **Snippet promotion** (deleting the inspiration snippet after metric success) stays sequential on the main session, post-gather.
  - **Concurrency cap = 5** (hardcoded `_MAX_CONCURRENT_METRICS`). Sonnet 4.6 tier-3+ workspaces handle this easily; bump in the constant if profiling shows underutilization.
  - **Free-threading note**: `GraphAgent._code_cache` is now guarded by a `threading.Lock` because the same agent instance is shared across N concurrent workers; under PYTHON_GIL=0 the check-then-set was a race.
  - **Exception handling**: unexpected exceptions inside the parallel path (e.g. `session_scope` failing) are captured per-worker as `Result.fail(...)` — they no longer abort sibling workers via `asyncio.gather` propagation. The phase's failure semantics (`metrics_executed` / `metrics_failed` in `PhaseResult.outputs`, hard-fail when all failed) are unchanged.
  - **Serial fallback**: when `ctx.manager is None` (unit tests with no real connection manager), the phase falls back to the previous sequential loop with shared session/cursor. No behavior change for that path.
  - **Out of scope (deferred)**: cold-start induction parallelism across phases, AsyncAnthropic provider rewrite, configurable concurrency cap.
- **Status**: pending

## 2026-05-13: DAT-273 — Post-DAT-266 audit (dead symbols + db column + re-exports)

### dataraum-eval
- **Changed**: `src/dataraum/graphs/{models.py, __init__.py, induction.py, agent.py}`, `src/dataraum/entropy/db_models.py`, `src/dataraum/query/__init__.py`, `tests/integration/graphs/test_agent.py`
- **Affects**: nothing the eval harness consumes — pure code hygiene. No MCP tool, detector, pipeline phase, response shape, or behavior changes.
- **Calibrate**: nothing.
- **Notes**:
  - `entropy_objects.expires_at` column deleted. SQLAlchemy `create_all` is idempotent; existing workspaces keep the orphan column harmlessly. No wipe needed.
  - Deleted symbols (any eval-side reference would already be broken — none expected): `dataraum.graphs.StepValidation`, `dataraum.graphs.MetricScope`, `TransformationGraph.{scope, slice_dimension}`, `GeneratedCode.{graph_version, schema_mapping_id}`.
  - `dataraum.query.QueryAgent` no longer re-exported at package level — import via `dataraum.query.agent.QueryAgent`. Same for `QueryAnalysisOutput`, `QueryExecutionRecord`, `SQLSnippetRecord`, `SnippetGraph`, `SnippetLibrary`, `SnippetMatch`, `SnippetUsageRecord` — use the deeper `dataraum.query.{models, db_models, snippet_library, snippet_models}` paths. `QueryResult` + `answer_question` remain available from `dataraum.query`.
  - `induction.py` LLM tool schema no longer asks the model for a `validation` array — only affects metric induction prompt output.
- **Status**: pending

## 2026-05-13: DAT-284 — Quick wins (Sonnet 4.6 + graph prompt enrichment + has_trend)

### dataraum-eval
- **Changed**: `config/llm/config.yaml` (Sonnet 4.5 → 4.6 on `default_model` + `balanced`), `src/dataraum/graphs/context.py` (`ColumnContext.has_trend` field + populate + emit), `config/llm/prompts/graph_sql_generation.yaml` (new `<temporal_signals>` section).
- **Affects**: every LLM call routed through the `balanced` or `default` tier (semantic / column / validation / cycle / metric induction, graph SQL generation, enrichment, `why`). Graph SQL generation prompt now includes explicit `temporal_behavior` → aggregation guidance.
- **Calibrate**: graph-agent metric set smoke. Key scenarios:
  1. Existing finance metrics (DSO, gross_profit, current_ratio, etc.) still compute against `clean_eval` — no regression from added prompt context.
  2. Metrics on tables with `temporal_behavior: point_in_time` annotated columns (e.g. balance-sheet items) should pick the `end_of_period` aggregation pattern more reliably.
  3. Metric YAMLs whose declared `aggregation` conflicts with the column's `temporal_behavior` annotation — the LLM now explicitly trusts the column annotation and notes the override in assumptions.
- **Notes**:
  - **Model swap**: `claude-sonnet-4-5` → `claude-sonnet-4-6`. Sonnet 4.6 is the current generation; the short-form ID is canonical (no date suffix, matches existing Haiku pattern). Output format unchanged; structured-output prompts should remain stable but eval should validate.
  - **`has_trend` surface**: added as `bool | None` on `ColumnContext`, populated from `TemporalColumnProfile.has_trend` (only set for DATE/TIMESTAMP/TIMESTAMPTZ columns by construction). Emitted in the metadata-document's per-column Notes column as `"Trending over time."` when truthy. No DB schema change — `has_trend` was already persisted.
  - **`<temporal_signals>` prompt section**: bridges existing `temporal_behavior` semantic annotation to existing `<aggregation_types>` block. Includes conflict-resolution rule (trust the column annotation over a misaligned step aggregation). Explicitly notes that the `Trending over time.` note appears on the time-axis column and should be paired with the measure column's `temporal_behavior`.
  - **`detected_granularity` (AC7 second half)**: already emitted at `src/dataraum/graphs/context.py:1008-1009` for `table.time_column`. No code change in this PR.
  - **DAT-284 descope**: cold-start baseline + parallelism investigation (originally ACs 1, 3, 4, 5) split to **DAT-299** in v0.2.3. This PR is the quick-wins half (ACs 2, 6, 7, 8).
- **Status**: pending

## 2026-05-12: DAT-290 — Single source per session, multi_source pattern retired

### dataraum-eval
- **Changed**: `src/dataraum/mcp/server.py` (begin_session signature; new list_sources tool; multi_source filters purged; _orient_to_active_session shape fix), `src/dataraum/mcp/db_models.py` (ArchivedSession schema), `src/dataraum/pipeline/setup.py` (single-source resolution; fingerprint-of-set deleted), `src/dataraum/pipeline/phases/import_phase.py` (single-source dispatch; _load_registered_sources gone)
- **Affects**: every MCP call that goes through `begin_session`. The session-bound source must be selected explicitly. `_run_pipeline` semantics unchanged — still runs the pipeline against the active session's source.
- **Calibrate**: re-run MCP smoke / harness tests. Key adaptations the eval harness must make:
  1. `begin_session(source="<name>", intent="...", contract=...)` — `source` is required. Calling without it returns a schema-level error (`isError=True`). Calling with an unknown name returns a tool-level error that includes the list of available source names.
  2. `add_source(name="X", ...)` — calling twice with the same name now errors (`"Source 'X' already exists."`). The registry is append-only via `add_source`; use `SourceManager.remove_source` for archival (no MCP surface yet).
  3. New `list_sources` MCP tool — returns `{"sources": [{name, type, status, path, backend, recipe_tables}], "count": int}`. No URLs, no credentials. Use to discover what's registered before `begin_session`.
  4. Response shape change: `begin_session` and `resume_session` now return `source: "name"` (scalar). The previous `sources: [list]` field is gone — every session has exactly one source by construction. `resume_session()` archive listings have `source: "name"` per entry (was `sources: [list]`).
  5. `_orient_to_active_session` (idempotent-resume path) returns `source: "name"` to match.
  6. `multi_source` synthetic Source row no longer exists in session.db. Any eval code that filtered it out (`name != "multi_source"`) can be deleted.
- **Notes**:
  - **Workspace.db schema change**: `archived_sessions.source_names` (JSON list) → `archived_sessions.source_name` (scalar string). Existing workspaces with the old column require `rm -rf ~/.dataraum/` (consistent with DAT-192 / DAT-209 / DAT-286 precedent — v0.2.2 CHANGELOG documents this).
  - **What's deleted from the import phase**: `_load_registered_sources`, `_load_from_path`, `_detect_source_type`, `_get_or_create_source`, the `multi_source` row creation block, the silent per-source error swallowing that hid DAT-289's root causes.
  - **`setup_pipeline` runtime_config** changed shape — now carries `source_id`, `source_name`, `source_type`, `source_connection_config`, `source_backend`, `source_fingerprint` (single source). No `registered_sources` list, no `source_set_fingerprint`.
  - DAT-288 + DAT-289 close as superseded by this rework (no individual patches landed for them).
  - Cross-source analysis in a single session is **explicitly out of scope**. v0.4+ direction if it ever comes up: extend the recipe yaml to declare multiple connections (the recipe is already a multi-table aggregate), not reintroduce multi_source.
- **Status**: pending

## 2026-03-26: DAT-195 — server-level ConnectionManager, pipeline source_id fix

### dataraum-eval
- **Changed**: `src/dataraum/mcp/server.py`, `src/dataraum/core/connections.py` (1 line: investigation model import)
- **Affects**: all MCP tools (look, measure, begin_session, query, run_sql, add_source)
- **Calibrate**: re-run MCP smoke tests. The eval harness calls `_measure` — if it patched `get_manager_for_directory`, that patch path no longer exists. Harness needs to call `_measure(session, ...)` directly or go through `call_tool`.
- **Notes**:
  - `_run_pipeline` always uses multi-source mode now (`source_path=None`). The eval harness `_load_gate_scores` migration (mentioned in memory) needs to account for this: pipeline runs create a "multi_source" Source, not a source named after the file path.
  - `_resolve_source_path` and `_get_cached_contract` deleted — if eval patches these, remove the patches.
  - Handler signatures changed: `_measure(session, target)`, `_look(session, target, sample, *, cursor)`, etc.
  - `measure` response now shows `status: "running"` with `phases_completed` during pipeline runs (previously returned `pipeline_triggered` repeatedly).
- **Status**: verified (2026-04-10, /accept handoff)

## 2026-03-26: DAT-197 — measure/look target filter fixes

### dataraum-eval
- **Changed**: `src/dataraum/mcp/server.py` — `_resolve_table_name` helper, `_look` and `_measure` target resolution rewritten
- **Affects**: measure and look tools when called with target parameter
- **Calibrate**: re-run any smoke tests that use short table names or filter by target
- **Notes**:
  - Short table names now resolve via suffix match: `"invoices"` → `"zone1__invoices"`. Ambiguous names (matching multiple tables) return error.
  - `measure(target=...)` now returns error for nonexistent tables/columns (previously returned empty results silently).
  - Readiness filter fixed: keys have `"column:"` prefix, filter now accounts for it. Readiness populates correctly when target is specified.
  - Scores are now recomputed from filtered points when target is specified (previously returned dataset-wide averages regardless of target).
- **Status**: verified (2026-04-12, /accept handoff). Short name resolution, score recomputation, readiness filter all working via MCP.

### dataraum-eval (calibration concerns)
- **Observation**: outlier_rate detector scores 1.0 on 5 columns (invoices.amount, payments.amount, journal_lines.credit, fx_rates.rate, trial_balance.debit_balance). Score 1.0 means maximum entropy — likely a detector threshold issue, not actual data quality.
- **Observation**: temporal_drift scores 1.0 on bank_transactions.amount. Same concern.
- **Action**: calibration tests should verify these detectors against ground truth in entropy_map.yaml. If no injection exists for these columns, the detector is producing false positives.
- **Status**: verified (2026-04-10, /accept handoff). Target filter, readiness, and score recomputation all working.

### Known issues (not in this handoff)
- DAT-196: session model redesign (workspace vs. session isolation). Design doc published, blocked by DAT-197.

## 2026-03-28: Package A — CLI slimdown (DAT-227)

### dataraum-eval
- **Changed**: `src/dataraum/cli/` — removed tui, query, sources commands, dev inspect/reset. Only `run` and `dev {phases, context}` remain.
- **Affects**: any eval harness code that calls CLI commands (e.g. `dataraum sources add`, `dataraum query`). Use MCP tools instead.
- **Notes**: `textual` dependency removed from pyproject.toml.
- **Status**: verified (2026-04-10, /accept handoff). No eval code depends on removed CLI commands.

## 2026-03-28: Package B — JSON/JSONL loader, format rejection, directory support (DAT-197, DAT-198, DAT-199)

### dataraum-eval
- **Changed**: `src/dataraum/sources/json/` (new), `src/dataraum/sources/manager.py`, `src/dataraum/pipeline/phases/import_phase.py`
- **Affects**: `add_source` MCP tool — three behavior changes:
  1. JSON/JSONL files now accepted and loaded as VARCHAR (like CSV)
  2. Unsupported file formats (e.g. .xlsx) now rejected with clear error instead of silent acceptance
  3. Directories now accepted — returns file count, format breakdown, preview from first file
- **Calibrate**: run format matrix suite (DAT-216) once testdata has JSON fixtures (DAT-219). Smoke-test `add_source` with .json, .jsonl, directory, and unsupported format.
- **Notes**:
  - Nested JSON objects/arrays serialized via `to_json()` → VARCHAR (not `CAST`). Values stored as JSON strings like `{"city":"Berlin"}`.
  - Path escaping fixed across all loaders (CSV, Parquet, JSON, discovery) — single quotes in filenames no longer break SQL.
- **Status**: verified (2026-04-12, /accept handoff). Format matrix tests (5/5) pass: CSV, JSON, JSONL, Parquet, mixed directory.

### dataraum-testdata (hints)
- **Suggestion**: Add JSON and JSONL fixtures alongside existing CSV testdata. Same data, different format — enables format matrix testing.
- **Rationale**: DAT-216 (format matrix suite) needs multi-format fixtures to verify pipeline completion per source format.

## 2026-03-28: Package C — Session lifecycle + prerequisites (DAT-205, DAT-206, DAT-207, DAT-210, DAT-211, DAT-233)

### dataraum-eval
- **Changed**: `src/dataraum/mcp/server.py` — new `end_session` tool, idempotent `begin_session` (resume), DB-derived session state, root dir refactor, API key prereq check
- **Affects**: all MCP tools (session state is now DB-derived, not closure vars), new `end_session` tool, `begin_session` now checks API key
- **Calibrate**: session lifecycle suite (DAT-208). Key flows:
  1. `begin_session → look → measure → end_session(delivered)` → workspace archived
  2. Server restart → `begin_session` resumes existing session (`resumed: true`)
  3. `add_source` during session → error mentions "sealed"
  4. `end_session` → `add_source` → `begin_session` → fresh workspace
- **Notes**:
  - Default output dir changed from `./pipeline_output` to `~/.dataraum/workspace/`. Override via `DATARAUM_OUTPUT_DIR` env var.
  - `.mcp.json` no longer sets `DATARAUM_OUTPUT_DIR`.
  - `end_session` archives workspace to `~/.dataraum/archive/{session_id}/`. Archive failure is non-fatal (warning in response).
  - `begin_session` response has new field `resumed: true` and `step_count` when resuming.
  - `recorder.end_session()` bug fixed: naive/aware datetime mismatch on SQLite round-trip.
  - `begin_session` now checks `ANTHROPIC_API_KEY` (or configured provider's env var) and returns actionable error if missing.
  - `add_source` during active session blocked with "sources are sealed" error (not a soft hint — intentional design decision).
  - Root dir configurable via `DATARAUM_HOME` env var. `DATARAUM_OUTPUT_DIR` accepted as legacy fallback.
- **Status**: verified (2026-04-12, /accept handoff). Session lifecycle tests (14/14) pass: begin/end, resume, source sealing, DB-derived state, outcomes.

## 2026-03-28: Package D — Export + query UX (DAT-213, DAT-224)

### dataraum-eval
- **Changed**: `src/dataraum/export.py` (rewrite — single `export_sql` with DuckDB COPY), `src/dataraum/mcp/server.py`, `src/dataraum/mcp/formatters.py`, `src/dataraum/mcp/sql_executor.py`, `src/dataraum/query/core.py`, `src/dataraum/query/agent.py`, `src/dataraum/query/execution.py`
- **Affects**: `run_sql` and `query` tools — export, display limits, truncation signaling
- **Calibrate**: export suite. Key flows:
  1. `run_sql(sql="...", export_format="csv", export_name="test")` → CSV + sidecar at `{root}/exports/`
  2. `query(question="...", export_format="parquet")` → Parquet + rich sidecar (confidence, assumptions, SQL)
  3. Truncation: `run_sql` with 200+ rows → `truncated: true`, `row_count` shows total, `rows_returned` shows display
  4. No export when `export_format` omitted (backward compatible)
- **Notes**:
  - Export is DuckDB COPY only — no Python materialization. CSV and Parquet formats. JSON dropped.
  - `display_limit` pushed to DuckDB via `execute_sql_steps` — no unbounded `fetchall()` anywhere.
  - Temp views NOT dropped after execution — they survive on the cursor for export reuse.
  - `run_sql` response now includes `row_count` (total), `rows_returned` (display), `truncated`, `hint` when capped.
  - `query` response `data` block now includes `rows_returned`, `truncated`, `hint` when capped.
  - Sidecar = MCP result minus rows/data. Caller builds it, export just writes to disk.
  - Export path sanitized: regex strips special chars, resolve() containment check.
  - `run_sql` tool description updated with snippet/step/column-mapping guidance (DAT-224).
  - `export_query_result()`, `export_data()`, `_export_tool_result()` all deleted. Net -300 lines.
- **Status**: partially_verified (2026-04-12, /accept handoff). Truncation fields, snippet reuse, snippet_summary verified via MCP. Export (csv/parquet) still not tested.

## 2026-03-28: Import path unification + source hardening

### dataraum-eval
- **Changed**: `src/dataraum/pipeline/phases/import_phase.py` — `_load_from_path` now delegates to `_load_file_source`. Dead methods deleted (-255 lines). Max 20 files per source. Mixed-format directories load all formats. UTF-8 encoding error surfaced clearly.
- **Affects**: **BREAKING** — `RunConfig(source_path="/path/to/medium/")` now prefixes table names with `{source_name}__`. Tables become `typed_medium__invoices` instead of `typed_invoices`. Eval tests that hardcode unprefixed table names (e.g. `test_tool_chain.py:202`) need updating.
- **Action**: Update all SQL in eval that references `typed_invoices`, `typed_journal_lines`, etc. to use the prefixed form. The `source_name` is `path.stem.lower()` — for testdata at `output/medium/`, prefix is `medium__`.
- **Status**: verified (2026-04-10, /accept handoff). conftest._strip_source_prefix handles it correctly.

## 2026-04-06: DAT-254 — Snippet Search + Look Enrichment + run_sql Repair

### dataraum-eval
- **Changed**: `src/dataraum/mcp/server.py`, `src/dataraum/mcp/sql_executor.py`
- **Affects**: `look`, `run_sql`, new `search_snippets` tool
- **Calibrate**: MCP smoke tests. Key changes:
  1. **New tool `search_snippets`**: returns snippet vocabulary (standard_fields, statements, aggregations, graph_ids) or matching snippet graphs with SQL. Needs basic smoke test.
  2. **`look` (dataset-level)**: new `snippet_vocabulary` key when snippets exist (same shape as search_snippets vocabulary)
  3. **`look` (column-level)**: two new keys:
     - `detector_evidence`: list of `{detector, dimension, observations}` — detector observations, NOT scores. Dimension is `layer.dimension.sub_dimension` path.
     - `relevant_snippets`: list of `{sql, description, source, standard_field}` — matched via `SemanticAnnotation.business_concept`. Only present when column has a business concept.
  4. **`run_sql` LLM repair**: syntax errors now trigger LLM-based repair (up to 2 attempts). Repair only available when pipeline has run (table schema needed for prompt). When LLM unavailable, original error returned unchanged.
- **Notes**:
  - `search_snippets` requires active session (same flow enforcement as look/measure)
  - `look` boundary clarified: detector evidence = context/observations, entropy scores = measure only
  - `run_sql` repair is lazy-init — no LLM cost unless SQL actually fails
  - Table layer validation (raw_ table blocking) was deferred — not implemented
- **Status**: verified (2026-04-12, /accept handoff). search_snippets vocabulary + concept search working. look detector_evidence + relevant_snippets working. SQL repair working (test_invalid_sql needs update). Snippet saving from run_sql working.

## 2026-04-08: DAT-250 — Cold Start Vertical Bootstrap + Induction Agents

### dataraum-eval
- **Changed**: `src/dataraum/mcp/server.py` (begin_session vertical param, pipeline threading), `src/dataraum/pipeline/setup.py` (_adhoc scaffold, runtime_config vertical), `src/dataraum/pipeline/phases/semantic_phase.py` (ontology induction), `src/dataraum/pipeline/phases/business_cycles_phase.py` (cycle induction), `src/dataraum/pipeline/phases/validation_phase.py` (validation induction)
- **New files**: `src/dataraum/analysis/semantic/induction.py`, `src/dataraum/analysis/cycles/induction.py`, `src/dataraum/analysis/validation/induction.py`, 3 prompt YAMLs in `config/llm/prompts/`
- **Affects**: `begin_session` (new `vertical` param), `measure` (pipeline now threads vertical), all LLM-powered phases on cold start
- **Calibrate**: Cold-start scenario (no vertical selected). Key behaviors:
  1. `begin_session()` without `vertical` → `_adhoc` scaffold created, pipeline auto-generates ontology + cycles + validations via LLM
  2. `begin_session(vertical="finance")` → identical to pre-change behavior
  3. Three new LLM calls per cold-start run: ontology induction (semantic phase), cycle induction (business_cycles phase), validation induction (validation phase)
  4. Induced config written to `{output_dir}/config/verticals/_adhoc/` — ontology.yaml, cycles.yaml, validations/*.yaml
  5. `vertical: finance` removed from phase YAML defaults — vertical now comes from runtime_config
- **Notes**:
  - Cold start requires ANTHROPIC_API_KEY (3 extra LLM calls for induction)
  - Existing workspace DBs missing `investigation_sessions.vertical` column will error — delete workspace and restart
  - `_adhoc` vertical scaffold always created at pipeline setup (idempotent)
  - Induction only fires when config is empty — re-runs with populated config skip induction
  - Relationship filter in induction context: `detection_method != "candidate"` (LLM-confirmed only)
- **Status**: verified (2026-04-10, /accept handoff). Cold start with _adhoc vertical passes full calibration.

## 2026-04-09: DAT-256 — Fix System Retirement

### dataraum-eval
- **Changed**: entropy detectors, measurement.py, pipeline/fixes/
- **Affects**: `measure` tool response (no more `accepted_targets` or `filter_applied` fields in MeasurementResult), `check_contracts` simplified (no acceptance exclusion parameter)
- **Resolution option action names renamed**: `document_type_pattern` → `type_pattern`, `document_business_name` → `concept_property`, `document_unit`/`document_unit_source` → `concept_property`, `document_timestamp_role` → `concept_property`, `document_relationship` → `relationship`, `confirm_expected_pattern` → `explanation`. All `document_accepted_*` and `transform_*` options deleted.
- **Deleted**: `fix_schemas.py`, `pattern_filter.py`, `fixes/api.py`, `fixes/bridge.py`, `FixSchema`, `FixSchemaField`, `FixInput`
- **Kept (for teach DAT-251)**: `ConfigInterpreter`, `MetadataInterpreter`, `DataFix`, `FixDocument`, `DataFixesPhase`, `apply_config_yaml`
- **EntropyObjectRecord schema change**: `filter_confidence`, `expected_business_pattern`, `business_rule` columns removed. Existing workspace DBs need recreation.
- **Calibrate**: If eval reads `accepted_targets` or `filter_applied` from MeasurementResult, those fields are gone. If eval checks resolution option action names, update to new names.
- **Notes**: `interpreters.py` now sets `annotation_source="teach"` and `confirmed_by="teach"` (was `"fix_system"`). `_get_preferred_joins` in relations detector queries `action == "relationship"` (was `"document_join_path"`).
- **Status**: verified (2026-04-10, /accept handoff). Dead code removed from eval runner. No eval code references deleted APIs.

## 2026-04-09: DAT-258 — Retire ResolutionOption

### dataraum-eval
- **Changed**: entropy models, db_models, engine, measurement, network_context, contracts, all 15 detectors, graphs/context, mcp/sections, context.py
- **Deleted**: `src/dataraum/entropy/actions.py` (merge_actions, load_actions), `ActionsResultWrapper`
- **Affects**: `measure` tool (MeasurementResult no longer has `resolution_actions` field), `look` quality section (no more `resolution_actions` per column), network context (no `resolution_options` on nodes, no `suggested_fix` on at-risk columns, no `best_action` on top_fix)
- **Calibrate**: If eval reads `resolution_actions`, `resolution_options`, `suggested_fix`, or `best_action` from any MCP response — those fields are gone. Score and evidence fields unchanged.
- **Notes**:
  - `EntropyObjectRecord` schema changed: `resolution_options` column removed. Existing workspace DBs need recreation.
  - `ContractViolation` class and `check_contracts()` deleted (had zero callers).
  - `ContractEvaluation.recommendations` field removed (was never populated).
  - Python SDK `DataRaumContext.actions()` method deleted.
  - All detector scoring and evidence logic untouched — only resolution_options production removed.
  - Replacement: teach system (DAT-251/DAT-257) will provide teachable inventory in `look`.
- **Status**: verified (2026-04-10, /accept handoff). No eval code references resolution_actions or resolution_options.

## 2026-04-10: DAT-251 — teach (World Model Write Tool)

### dataraum-eval
- **Changed**: `src/dataraum/mcp/teach.py` (NEW), `src/dataraum/mcp/server.py` (teach tool + measure target_phase)
- **Affects**: New `teach` tool (8 types), `measure` tool (new `target_phase` param)
- **Calibrate**: Smoke test the teach → measure flow. Key scenarios:
  1. `teach(type="concept", params={name: "revenue", indicators: ["revenue"]})` → check ontology.yaml updated
  2. `teach(type="concept_property", target="orders.amount", params={field_updates: {semantic_role: "measure"}})` → verify annotation patched immediately
  3. `measure(target_phase="semantic")` → verify selective rerun works (only semantic + deps)
  4. `teach(type="null_value", params={value: "TBD"})` → check null_values.yaml updated
- **Notes**:
  - 8 teach types: concept, validation, cycle, type_pattern, null_value (config), concept_property, relationship, explanation (metadata)
  - Config teaches write to workspace config (`output_dir/config/`), NOT global package config
  - Config teaches return `measurement_hint` telling agent which phase to rerun
  - `measure(target_phase=...)` triggers selective rerun with `force_phase=True`
  - `type_override` removed — type overrides lead to quarantine, pattern learning (type_pattern) is the right approach
  - `forced_types` dead code removed from typing pipeline
  - Known limitation: relationship resolver uses `scalar_one_or_none()` — fails on columns with multiple relationships
- **Status**: verified (2026-04-12, /accept handoff). concept_property teach applies immediately. Target resolution correct. teach -> look -> relevant_snippets roundtrip verified. column_quality from column_mappings non-functional (always null).

## 2026-04-12: Bugs found by /accept — config teach re-run path broken

### dataraum-context (blocking for _adhoc UX)
- **Bug 1: `_run_pipeline(target_phase="import")` fails in multi-source mode**
  - `source_path=None` means multi-source mode, but the import phase can't find registered sources during a selective re-run. Import exits with `status: failed, duration: 0.00`.
  - **Repro**: `_run_pipeline(output_dir, target_phase="import", vertical="finance")`
  - **Affects**: all config teaches that hint re-running import (null_value, type_pattern)
  - **Test**: `calibration/tools/test_adhoc_teach_loop.py::TestConfigTeachWithRerun::test_null_value_teach_reruns_import` (xfail)

- **Bug 2: cascade cleanup deletes all validation results before re-run**
  - When `target_phase="validation"` triggers a selective re-run, the cascade cleanup deletes ALL existing validation results (9 → 0). Then import fails (Bug 1), so validation never actually re-runs. Net result: teach a validation rule, lose all previous validation results.
  - **Repro**: `_run_pipeline(output_dir, target_phase="validation", vertical="finance")`
  - **Affects**: validation teach type — the teach → measure loop for adding custom validation rules
  - **Test**: `calibration/tools/test_adhoc_teach_loop.py::TestConfigTeachWithRerun::test_validation_teach_reruns_validation` (xfail)

- **Impact**: The entire config teach → re-measure cycle is broken. `teach` returns a `measurement_hint` telling the agent to call `measure(target_phase=...)`, but that re-run fails. Metadata teaches (concept_property, relationship, explanation) work because they patch the DB directly. Config teaches (concept, validation, cycle, type_pattern, null_value) are write-only — they write YAML but the pipeline can't re-read it.
- **Status**: pending

### dataraum-eval (also fixed in this session)
- **Fixed**: `calibration/runner.py` now passes `vertical="finance"` to RunConfig (was missing, defaulted to `_adhoc`)
- **Fixed**: `test_error_ux.py::test_invalid_sql` updated for LLM SQL repair (DAT-254)
- **Fixed**: `sql_executor.py::_build_column_quality` — short table names in column_mappings now resolve via suffix matching (was returning null for all mapped columns)
- **New**: `calibration/tools/test_adhoc_teach_loop.py` — 7 tests for teach → measure loop (5 pass, 2 xfail documenting bugs above)

## 2026-04-12: DAT-252 — why (Evidence Synthesis Agent)

### dataraum-eval
- **Changed**: `src/dataraum/mcp/why.py` (NEW), `src/dataraum/mcp/server.py` (why tool), `config/llm/prompts/why_analysis.yaml` (NEW)
- **Affects**: New `why` MCP tool
- **Calibrate**: Smoke test why at all three levels. Key scenarios:
  1. `why(target="orders.amount")` → column-level analysis with evidence + teach suggestions
  2. `why(target="orders")` → table-level aggregation across columns
  3. `why()` → dataset-level summary with top entropy drivers
  4. `why(target="orders.amount", dimension="semantic")` → filtered to semantic layer
  5. `why → teach → measure` flow: take first resolution_option, pass to teach, rerun measure, verify improvement (AC#13 from Jira)
- **Notes**:
  - Response fields: `target`, `readiness`, `analysis`, `evidence[]`, `resolution_options[]`, `intents`
  - Each `resolution_option` has `teach_type`, `target`, `params`, `description`, `expected_impact`, `valid`, `validation_warning`
  - `valid=false` + `validation_warning` when LLM-generated params don't match teach schema (included, not dropped)
  - Feature toggle: `config.features.why_analysis.enabled`. When disabled, returns raw evidence without LLM synthesis.
  - Model tier: balanced (Sonnet). LLM call can take 5-30s.
  - `PARAM_MODELS` renamed from `_PARAM_MODELS` in teach.py (was private, now public — used by why for schema extraction)
- **Status**: pending

## 2026-04-12: DAT-253 — Graph Execution Revival

### dataraum-eval
- **Changed**: `src/dataraum/pipeline/phases/graph_execution_phase.py` (NEW), `src/dataraum/graphs/induction.py` (NEW), `config/llm/prompts/metric_induction.yaml` (NEW), `config/pipeline.yaml`, `src/dataraum/mcp/teach.py`
- **Affects**: New `graph_execution` pipeline phase (18th), `teach` tool (new `metric` type, now 9 types), cold-start metric induction
- **Calibrate**: Key scenarios:
  1. Pipeline run with finance vertical → graph_execution phase computes applicable metrics, snippets stored
  2. Cold start (`_adhoc` vertical, no metrics) → metric induction LLM call → metrics generated → executed
  3. `teach(type="metric", params={graph_id, name, description, dependencies})` → metric YAML written
  4. `measure(target_phase="graph_execution")` → selective rerun, taught metric computed → snippet stored
  5. `search_snippets()` → graph-sourced snippets discoverable
  6. `look(target="table.column")` → relevant snippets from graph execution shown when business_concept matches
- **Notes**:
  - Phase runs after `validation` (dependency in pipeline.yaml)
  - Metrics with unresolvable direct field mappings are still attempted — graph agent LLM infers from enriched views (see DAT-262)
  - `MetricInductionAgent` fires 1 extra LLM call on cold start (balanced tier)
  - `schema_mapping_id` for graph snippets: `f"{source_id}:semantic"`
  - `PARAM_MODELS` in teach.py now has 9 entries (was 8)
  - `_RERUN_PHASES["metric"] = "graph_execution"`
  - Graph agent (`agent.py`), loader, field_mapping, snippet_library unchanged
- **Status**: pending

## 2026-04-12: DAT-260 — Config teach re-run cascade fix

### dataraum-eval
- **Changed**: `src/dataraum/pipeline/setup.py` (phase filtering + cascade cleanup), `src/dataraum/mcp/teach.py` (measurement_hint)
- **Affects**: `measure(target_phase=...)` behavior after any config teach
- **Calibrate**: The xfail tests in `test_adhoc_teach_loop.py` should now pass. Key flow:
  1. `teach(type="concept", params={...})` → writes ontology YAML
  2. `measure(target_phase="semantic")` → re-runs semantic + all downstream (enriched_views, validation, business_cycles, graph_execution, etc.)
  3. Upstream deps (import, typing, statistics) skip via `should_skip` — no re-import
  4. `measure()` after re-run → scores reflect the taught concept
- **Notes**:
  - `cleanup_phase_cascade` now used instead of `cleanup_phase` — cleans target + all downstream
  - Phase set includes `deps | {target_phase} | downstream` (was `deps | {target_phase}`)
  - `measurement_hint` now says "downstream phases" and warns about expensive re-runs for type_pattern/null_value
  - Metadata teaches (concept_property, relationship, explanation) unaffected — they were already working
- **Status**: verified (2026-04-12, /accept handoff). Config teach → measure(target_phase) works correctly through the MCP call_tool dispatch. Earlier failure reports were test artifacts from calling _run_pipeline directly (wrong preconditions). In-memory MCP client tests confirm the full loop: teach(validation) → measure(target_phase="validation") → pipeline re-runs → scores update.

## 2026-04-13: DAT-255 — Discovery Polish (Server Instructions + Tool Descriptions + Pipeline Guard)

### dataraum-eval
- **Changed**: `src/dataraum/mcp/server.py` (server instructions, all 10 tool descriptions, pipeline-in-progress guard)
- **Affects**: All MCP tools — descriptions changed (text only, no behavioral changes). New: `query` and `run_sql` blocked during pipeline execution.
- **Calibrate**: `/smoke` after restart. Key behaviors:
  1. Server instructions visible in MCP handshake (three scenarios: first run, returning, teach+re-measure)
  2. `query` during pipeline → `{"error": "Pipeline is currently running..."}` (not session error)
  3. `run_sql` during pipeline → same error
  4. `look`, `teach`, `why`, `search_snippets` still work during pipeline
  5. Pipeline failure → guard clears → query/run_sql unblocked
- **Notes**:
  - `_pipeline_idle` flag (`threading.Event`) cleared before pipeline task/background launch, restored in `finally` blocks of both task and fire-and-forget paths
  - Server `instructions=` param added to `Server()` constructor — calling agents now get workflow guidance
  - Tool descriptions are text-only changes — no parameter schemas, return shapes, or dispatch logic changed
  - teach description now includes per-type re-run phase mapping and cascade cost warnings
  - measure description includes "bundle teaches before measuring" guidance
- **Status**: pending

## 2026-04-13: DAT-262 — Graph execution zero snippets (field mapping gate fix)

### dataraum-eval
- **Changed**: `src/dataraum/pipeline/phases/graph_execution_phase.py` (gate removed), `src/dataraum/graphs/models.py` (dead SchemaMapping models deleted), `src/dataraum/graphs/agent.py` (dead field removed), `src/dataraum/graphs/__init__.py` (exports cleaned)
- **Affects**: `graph_execution` pipeline phase — metrics that were previously skipped (missing direct field mappings) are now attempted via LLM inference
- **Calibrate**: `/smoke` with finance vertical on detection-v1. Key verification:
  1. `measure` → pipeline completes → `graph_execution` phase shows `metrics_executed > 0` (was `metrics_skipped: 12`)
  2. `search_snippets()` → graph-sourced snippets present (source prefix `graph:`)
  3. Finance metrics (gross_profit, DSO, etc.) produce reasonable values from GL data via account-type filtering
- **Notes**:
  - Root cause: DAT-253 rewrite added hard gate on `can_execute_metric()` that skipped metrics when `standard_field` had no matching `business_concept`. The pre-removal code (before DAT-183) treated this as advisory.
  - On GL data, semantic phase correctly assigns GL concepts (`transaction_amount`, `debit`, `credit`), not P&L concepts (`revenue`, `cost_of_goods_sold`). The graph agent LLM infers P&L from enriched views (e.g., `WHERE account_type ILIKE 'revenue'`).
  - Dead `SchemaMapping`, `DatasetSchemaMapping`, `ColumnMapping`, `AggregationDefinition` removed — these modeled a deterministic mapping approach that SQL snippets replace.
  - AC3 (teach metric warning) deliberately dropped — calling agent can't act on it, graph agent handles inference.
- **Status**: pending

## 2026-04-16: DAT-263 — Snippet Provenance + Assumption Tracking + Snippet Harmonization

### dataraum-eval
- **Changed**: `src/dataraum/graphs/models.py`, `src/dataraum/graphs/agent.py`, `src/dataraum/graphs/loader.py`, `config/llm/prompts/graph_sql_generation.yaml`, `src/dataraum/query/snippet_models.py`, `src/dataraum/query/snippet_library.py`, `src/dataraum/query/execution.py`, `src/dataraum/query/agent.py`, `src/dataraum/mcp/server.py`, `src/dataraum/mcp/teach.py`, `src/dataraum/mcp/sql_executor.py`, `src/dataraum/mcp/formatters.py`, `src/dataraum/pipeline/phases/graph_execution_phase.py`, `src/dataraum/core/logging.py`
- **Affects**: `search_snippets`, `run_sql`, `teach` (metric type), `measure` (graph_execution phase), graph agent, snippet library
- **Calibrate**: `/smoke` after restart. Key behaviors:
  1. `search_snippets()` vocabulary excludes `mcp:session_*` sources (run_sql noise filtered)
  2. `search_snippets(graph_ids=["dso"])` returns full calculation chain (shared snippets from other graphs included)
  3. `search_snippets` results include `field_resolution` (direct/inferred) and `was_repaired` per snippet
  4. `run_sql` with broken SQL → repair → response includes `repair_attempts` and `original_sql` in `steps_executed`
  5. `run_sql` with valid SQL → no repair fields in response
  6. `teach(type="metric", params={..., inspiration_snippet_id: "..."})` → metric YAML includes `inspiration_snippet_id`
  7. `measure(target_phase="graph_execution")` → taught metric executes, ad-hoc snippet deleted (`snippet_promoted` in logs)
  8. Graph agent assumptions stored in snippet provenance dict → surfaced in `search_snippets` results
- **Notes**:
  - `SQLSnippetRecord` has new `provenance` JSON column (nullable). Existing workspaces without this column need `end_session` + `begin_session` to get a fresh DB.
  - `GraphExecution.max_entropy_score` and `entropy_warnings` removed (dead fields from retired entropy interpretation agent)
  - `GraphSource.TEACH` added to enum — `teach(type="metric")` was silently rejected by graph loader without it
  - `_save_snippets` in graph agent moved from before to after execution — only saves SQL that actually ran. Repair info available in provenance.
  - `enable_file_logging()` added to `core/logging.py`. MCP server logs to `{DATARAUM_HOME}/logs/mcp-server.log`.
  - Server instructions updated with snippet promotion flow.
  - DAT-265 created under DAT-196 for incremental phase execution (currently re-runs all metrics, not just the new one)
- **Status**: verified (2026-04-17, /accept handoff). Provenance fields (field_resolution, assumptions, was_repaired) working in search_snippets. run_sql repair visibility working (repair_attempts + original_sql). mcp:session_* vocabulary filtering working. Degraded: query:* snippets leak into vocabulary (step IDs in standard_fields, UUIDs in graph_ids) — extend filter to exclude query:* sources.

## 2026-04-17: /accept findings — vocabulary pollution + query agent grounding

### dataraum-context (vocabulary pollution)
- **Bug**: `query:*` snippets leak into `search_snippets` vocabulary. After running `query` tool:
  1. Step IDs appear in `standard_fields` (e.g. `revenue_march_2025` — a one-off step, not a reusable concept)
  2. Execution UUIDs appear in `graph_ids` (e.g. `df2b8659-7d7d-47f5-969f-076c3912503c`)
- **Root cause**: Vocabulary filter excludes `mcp:%` sources but `query:%` sources pass through. Graph_id extractor parses `query:{uuid}` the same as `graph:{name}`.
- **Fix**: Extend vocabulary filter to `NOT LIKE 'mcp:%' AND NOT LIKE 'query:%'`, or only extract graph_ids from `graph:*` prefix.
- **Affects**: `search_snippets` vocabulary cleanliness
- **Status**: pending

## 2026-05-07: DAT-209 — resume_session MCP tool

### dataraum-eval
- **Changed**: `src/dataraum/mcp/server.py` (new `_restore_archived_session`, `_list_archived_sessions`, `_read_archive_summary`, `_close_session_manager`; `_archive_and_clear_active` now writes `ArchivedSession` rows; rename `_resume_session` → `_orient_to_active_session`), `src/dataraum/mcp/db_models.py` (new `ArchivedSession` model)
- **Affects**: New MCP tool `resume_session`. Workspace.db gains a new table `archived_sessions`. Behavioral pairing: `end_session` writes a row; `resume_session` consumes it.
- **Calibrate**: `/smoke` flow after restart. Key scenarios:
  1. `add_source → begin_session → end_session(delivered)` → archive dir created at `archive/{session_id}/` AND `archived_sessions` table has one row in workspace.db
  2. `resume_session()` (no args) → returns `{"archived_sessions": [...]}` ordered newest first; each entry has session_id, fingerprint, intent, contract, vertical, outcome, summary, sources, started_at, ended_at, step_count
  3. `resume_session(session_id=X)` → archive dir moves back to `sessions/{fingerprint}/`, ActiveSession pointer set, ArchivedSession row consumed. Workspace Sources replaced from archive's sources. New `InvestigationSession` created with carried contract+vertical and intent prefixed with "Resumed:" (or override via `intent` arg).
  4. After resume, `look`/`measure`/`query` return existing pipeline data without re-running the pipeline (`has_pipeline_data: true`)
  5. `resume_session(session_id=X)` while a session is active → error "A session is already active. Call end_session before resuming"
  6. `resume_session()` (no args) while a session is active → returns the listing, NOT an error (so the agent can browse before deciding to end the current session)
  7. `resume_session(session_id="bad")` → error includes `available` list of archives
  8. Stale archive (index entry exists but archive dir was manually deleted) → error includes `available`, index entry consumed
  9. Failure mid-restore (unlikely outside tests) → archive moved back; user can retry
- **Notes**:
  - **Schema change**: `workspace.db` gets an `archived_sessions` table. Existing workspaces without it need a wipe (`rm -rf ~/.dataraum/`) — same wipe instruction as DAT-192. CHANGELOG already states this for the v0.2.2 unreleased section.
  - Response fields for restore: `resumed_from`, `sources`, `contract`, `has_pipeline_data`, `vertical`, `prior_step_count`, `hint`. Internal `_session_id` and `_fingerprint` stripped before reaching the agent (same convention as begin_session).
  - Resume creates a NEW `InvestigationSession` (new session_id). The original is preserved in the session DB as a historical record with its terminal status. Audit trail: a session DB after resume+end has multiple InvestigationSession rows.
  - Tool description in `resume_session` schema lists `session_id` (optional) and `intent` (optional). Two-step UX: list first, then restore by id.
  - Atomicity: restore is wrapped in try/except that rolls back the directory move if any post-move step fails (manager open, source read, InvestigationSession create, workspace replace). Rollback closes the cached session manager before moving — Windows-safe.
  - `_orient_to_active_session` rename: any eval code that imports `_resume_session` directly from `dataraum.mcp.server` must update to the new name. Public response shape and MCP behavior unchanged.
  - Server instructions updated to mention resume_session in the lifecycle paragraph and the tool list.
- **Status**: pending

### dataraum-context (query agent grounding — critical)
- **Bug**: Query agent and graph agent misinterpret trial balance structure. They treat each period's row as a balance snapshot, but the trial balance has **periodic (monthly) figures** — debit_balance and credit_balance are monthly activity, not cumulative balances.
- **Evidence**: CoA has correct account names ("Accounts Receivable", "Cost of Goods Sold"). The pattern matching works. But:
  - DSO = 0.0 because `MAX(period)` is 2026-02 where AR activity = 0
  - Ending AR = 1,368,258 (December monthly change) vs expected 13,070,114 (cumulative through December)
  - Cumulative SUM of all monthly AR figures through December = 13,070,114.83 (matches ground truth exactly)
  - Gross profit wrong because COGS is under-counted (same periodic vs cumulative issue)
- **Fix needed**: Query/graph agents must understand that `SUM(debit_balance - credit_balance)` across all periods through the target date gives the cumulative balance, not a single period's debit_balance - credit_balance.
- **Affects**: `query` tool accuracy for balance-based metrics (DSO, AR, AP, any balance sheet item), graph snippets for DPO/current_ratio/etc.
- **Status**: pending

## 2026-05-12: DAT-286 — DB sources via yaml recipes (MSSQL Phase A)

### dataraum-eval
- **Changed**: `src/dataraum/sources/db_recipe/` (new), `src/dataraum/sources/backends.py` (rewrite — `extract_backend` replaces `validate_backend`), `src/dataraum/sources/manager.py` (new `add_recipe_source` + `resolve_source_path`; `add_database_source` deleted), `src/dataraum/mcp/server.py` (`add_source` tool surface), `src/dataraum/pipeline/phases/import_phase.py` (`_load_database_source` rewritten to use recipe queries), `src/dataraum/core/credentials.py` (dead code removed), `src/dataraum/storage/models.py` (fields removed), `docs/db-sources.md` (new)
- **Affects**: `add_source` MCP tool, `list_sources` response shape, pipeline import phase for any source with `source_type='db_recipe'`
- **Calibrate**: `/smoke` once a customer has MS SQL data. Key changes the harness should be aware of:
  1. **MCP `add_source` parameters changed**: `backend`, `tables`, `credential_ref` are gone. Surface is now `(name, path)` only. The agent dispatches based on file extension; `.yaml`/`.yml` → recipe loader, files → file loader. Bare names (no extension or `.yaml`/`.yml`) resolve against `{DATARAUM_HOME}/recipes/` as a fallback.
  2. **`list_sources` response no longer includes** `credential_ref` or `last_validated` fields (those columns were deleted from `Source`). Source dicts now include `recipe_tables: [...]` for `db_recipe` sources.
  3. **New `source_type='db_recipe'`** registered in workspace.db. Its `connection_config` contains `{recipe_path, backend, recipe_hash, tables: [{name, sql}, ...]}` — credentials never appear here.
  4. **New env-var contract**: pipeline import resolves `DATARAUM_{NAME}_URL` from `.env` (or `~/.dataraum/credentials.yaml`) at pipeline-import time. Missing creds → loud failure with the specific env-var name. Recipes never contain credentials — yaml top-level keys `connection`/`credentials`/`auth`/`password`/`secret`/`secrets` are rejected at parse time.
  5. **Recipe table name pattern**: must match `[a-z][a-z0-9_]*` — rejected at parse time. Names get interpolated into `CREATE TABLE` so this is correctness + injection defense.
  6. **MSSQL is the only end-to-end-supported backend in Phase A.** `postgres`/`mysql`/`sqlite` are present in `extract_backend` for internal sqlite-based unit tests but rejected by the user-facing recipe parser. They re-enable in Phase C.
  7. **Pipeline import for db_recipe sources**: ATTACH READ_ONLY → `USE catalog.schema` (per-backend default; mssql=`dbo`) → `CREATE TABLE raw_{name} AS {user_sql}` per recipe entry → DETACH. Type fidelity preserved end-to-end (DECIMAL, VARCHAR, BOOLEAN, INTEGER). Every step fails loud (DAT-274 pattern).
  8. **Spike-pinned behaviors**: `(READ_ONLY)` at ATTACH is enforced by the extension — even sa-level connections cannot CREATE inside an attached read-only DB (test `test_read_only_blocks_writes`). MSSQL requires `?TrustServerCertificate=yes` in the URL for typical self-signed certs (documented, not auto-appended).
- **Notes**:
  - **Workspace.db schema change**: `Source.credential_ref` and `Source.last_validated` columns deleted. Existing workspaces with the old columns: same wipe instruction as DAT-192/DAT-209 (`rm -rf ~/.dataraum/`). CHANGELOG states this for v0.2.2 unreleased.
  - Live MSSQL integration test (`tests/integration/sources/test_db_recipe_mssql.py`) is **skipped in CI** — gated on `DATARAUM_MSSQL_TEST_URL` being set. CI service container deferred until a customer requests MS SQL. Eval harness can opt into running it locally by exporting the env var (see test module docstring for the Smoke schema setup).
  - **DAT-287 follow-up filed**: replace the hand-rolled Smoke schema in the integration test with a realistic Microsoft sample DB (AdventureWorks LT or WideWorldImporters). The current 3-row schema is enough to pin the contract but doesn't exercise realistic FK chains or BI use cases.
  - **DBMetadataHints (PK/FK/index harvest) was YAGNI'd out of Phase A** (commit `ad554ad5`). The spike confirmed `information_schema` isn't easily visible through the attached MSSQL catalog. Harvest deferred to Phase B alongside its consumer.
- **Status**: pending

## 2026-05-12: DAT-287 — Integration test fixture: AdventureWorksLT replaces Smoke

### dataraum-eval
- **Changed**: `tests/integration/sources/mssql_setup.sh` (new — idempotent restore + reader login), `tests/integration/sources/test_db_recipe_mssql.py` (rewrite — points at SalesLT/dbo instead of hand-rolled Smoke), `docs/db-sources.md` (canonical setup path)
- **Affects**: nothing the eval harness consumes — this is local-test infrastructure only. No behavior changes in any MCP tool, pipeline phase, or response shape.
- **Calibrate**: nothing. Eval doesn't need to do anything.
- **Notes**:
  - The integration test is still skipped unless `DATARAUM_MSSQL_TEST_URL` is set. CI service container still deferred until a customer requests MS SQL.
  - If anyone in eval-land wants to run the MSSQL integration test against a local container, the setup is now one-liner: `tests/integration/sources/mssql_setup.sh` against a running `sql2025` container (~4 s cold).
  - Test coverage expanded from 3 cases against a 3-row Smoke table to 8 cases against AdventureWorksLT's ~290-product / ~850-customer schema, including cross-schema, recipe-side JOIN denormalization, DATETIME / MONEY round-trip.
- **Status**: pending

## 2026-05-07: DAT-274 — LLM resilience: hard-fail on systemic failures

### dataraum-eval
- **Changed**: `src/dataraum/llm/providers/anthropic.py` (error classification), `src/dataraum/pipeline/phases/{semantic,validation,business_cycles,graph_execution,enriched_views}_phase.py` (swallow → hard-fail), `src/dataraum/pipeline/runner.py` (include blocked phases in PhaseRunResult), `src/dataraum/mcp/server.py` (`_run_pipeline` rich return + `_measure` failure response)
- **Affects**: `measure` MCP tool response shape; pipeline halt behavior on cold-start when an LLM call fails
- **Calibrate**: `/smoke` after restart. Key changes the harness needs to know:
  1. **`measure()` response has new `pipeline_status` field**: `complete` / `running` / `failed` / `not_started`. Existing `status` field kept for backward compat (matches `pipeline_status` on the success/running/no_data paths).
  2. **`measure()` returns `pipeline_status: "failed"` with `phases_failed: [{phase, error}]` when the most recent pipeline run failed.** Eval harness assertions that expected `status: complete` may now turn red on cold-start runs that previously passed silently with degraded data — this is intentional.
  3. **`_run_pipeline` return shape changed**: was `{"status": "complete", "phases_completed": int}` or `{"error": "Pipeline failed: ..."}`. Now `{"pipeline_status": "complete"|"failed", "phases_completed": [names], "phases_failed": [{phase, error}], "phases_blocked": [names]}`. Note `phases_completed` is now a **list of names**, not an int count.
  4. **Cold-start pipeline now hard-fails when LLM is down or induction returns empty.** Previously the pipeline ran to "complete" with empty ontology/cycles/validations/metrics. Eval runs that simulated LLM failure to test fallback behavior will turn red — those tests should now assert `pipeline_status: failed`.
  5. **`Result.fail` from `AnthropicProvider`** now contains `transient` / `permanent` in the error string. Eval-side assertions on exact error text may need updating.
- **Notes**:
  - Hard-fail policy: cold-start induction (ontology, column annotation, validation, cycle, metric) and enrichment recommendations now propagate LLM failures to `PhaseResult.failed`. The scheduler's natural cascade halts dependent phases. Independent phases (e.g., correlations) still run.
  - `graph_execution` per-metric loop unchanged for partial-failure case — keeps warning-on-failure when ≥1 metric succeeds (legitimate per-dataset variance). Hard-fails only when 0 succeed and ≥1 failed (systemic).
  - `enriched_views` distinguishes "LLM intentionally unavailable" (config-disabled, provider-not-configured) from "LLM call attempted and failed" — only the latter is now a hard fail.
  - No SDK retry/timeout overrides — Anthropic SDK defaults retain. The user's call: APIs always time out eventually.
- **Status**: pending

<!--
## YYYY-MM-DD: brief description

### dataraum-eval
- **Changed**: files, modules, behaviors
- **Affects**: which MCP tools, detectors, or pipeline phases
- **Calibrate**: which eval tests, skills, or strategies to run
- **Notes**: context the eval session needs (e.g., new response fields, changed thresholds)
- **Status**: pending | verified | failed

### dataraum-testdata (hints)
- **Suggestion**: directional hints for new injections, ground truth values, or scenarios
- **Rationale**: why this would improve test coverage
(Keep these directional — testdata has its own design concerns)
-->
