# Calibration Handoff

Changes in dataraum that need attention in other repos.

Updated by `/implement` in this repo. Read by `/accept` in dataraum-eval.

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
- **Status**: pending

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
- **Status**: pending

### dataraum-eval (calibration concerns)
- **Observation**: outlier_rate detector scores 1.0 on 5 columns (invoices.amount, payments.amount, journal_lines.credit, fx_rates.rate, trial_balance.debit_balance). Score 1.0 means maximum entropy — likely a detector threshold issue, not actual data quality.
- **Observation**: temporal_drift scores 1.0 on bank_transactions.amount. Same concern.
- **Action**: calibration tests should verify these detectors against ground truth in entropy_map.yaml. If no injection exists for these columns, the detector is producing false positives.

### Known issues (not in this handoff)
- DAT-196: session model redesign (workspace vs. session isolation). Design doc published, blocked by DAT-197.

## 2026-03-28: Package A — CLI slimdown (DAT-227)

### dataraum-eval
- **Changed**: `src/dataraum/cli/` — removed tui, query, sources commands, dev inspect/reset. Only `run` and `dev {phases, context}` remain.
- **Affects**: any eval harness code that calls CLI commands (e.g. `dataraum sources add`, `dataraum query`). Use MCP tools instead.
- **Notes**: `textual` dependency removed from pyproject.toml.
- **Status**: pending

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
- **Status**: pending

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
- **Status**: pending

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
- **Status**: pending

## 2026-03-28: Import path unification + source hardening

### dataraum-eval
- **Changed**: `src/dataraum/pipeline/phases/import_phase.py` — `_load_from_path` now delegates to `_load_file_source`. Dead methods deleted (-255 lines). Max 20 files per source. Mixed-format directories load all formats. UTF-8 encoding error surfaced clearly.
- **Affects**: **BREAKING** — `RunConfig(source_path="/path/to/medium/")` now prefixes table names with `{source_name}__`. Tables become `typed_medium__invoices` instead of `typed_invoices`. Eval tests that hardcode unprefixed table names (e.g. `test_tool_chain.py:202`) need updating.
- **Action**: Update all SQL in eval that references `typed_invoices`, `typed_journal_lines`, etc. to use the prefixed form. The `source_name` is `path.stem.lower()` — for testdata at `output/medium/`, prefix is `medium__`.
- **Status**: pending

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
- **Status**: pending

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
- **Status**: pending

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
- **Status**: pending

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
