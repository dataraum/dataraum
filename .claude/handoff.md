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
- **Status**: smoke-tested (2026-04-16). All features verified against live data.

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
