# Calibration Handoff

Changes in dataraum that need attention in other repos.

Updated by `/implement` in this repo. Read by `/accept` in dataraum-eval.

## 2026-06-01: DAT-364 (tail) — temporal `analyze_update_frequency` NaN guard

Bug fix found while building the DAT-364 isolation test (the workflow-ID change itself is
**not** calibration-relevant — workflow-ID naming + workspace guard only, no detector/schema/phase
change).

What changed in the engine (calibration-relevant):
- **`analyze_update_frequency` now coerces a NaN `interval_std` to `0.0`** (`analysis/temporal/patterns.py`).
  A date column with exactly one interval (a 2-row table) has no sample std → pandas returns NaN →
  the JSON `profile_data` insert crashed (`invalid input syntax for type json … Token "NaN"`). A lone
  interval is trivially regular, so `0.0` is the correct reading. `interval_cv` (derived) is now
  finite too.

### dataraum-eval

- **Eval action: re-verify only if testdata has single-interval (2-row) date columns.** Previously
  such a column crashed the `temporal` phase; now it profiles cleanly with `interval_std=0.0`. No
  change to multi-interval columns or to detector recall/precision on healthy data — the coercion
  only fires on the degenerate single-interval case.

## 2026-05-31: DAT-378 — file source = explicit `file_uris` list (multi-file ingest, atomic)

Makes the engine import contract correct end-to-end for the cockpit `connect → select →
add_source` journey, and unifies the file-source connection contract.

What changed in the engine (calibration-relevant):
- **File-source contract unified on `connection_config['file_uris']` (a list).** A single-file
  source (`add_file_source`, one uploaded object) stores a one-element list; a multi-file source
  (the cockpit `select` stage enumerating a bucket prefix) stores many. The scalar
  `connection_config['path']` key and the dead CLI `source_path` fallback are **retired** — the
  worker path carried neither. `import` reads `file_uris` only; `db_recipe` sources still use the
  DISTINCT `connection_config['tables']` recipe-query key (unchanged). `ImportPhase._resolve_file_uris`,
  `SourceManager.add_file_source`, `SourceManager.list_sources`, and the cockpit seed
  (`drive-add-source.ts`) all moved to `file_uris`.
- **One raw table per URI.** `import` validates EVERY URI (`validate_source_uri` — the engine never
  globs) then loads each in turn, so one import activity yields N raw tables; `addSourceWorkflow`
  fans out one `processTableWorkflow` per raw table (DAT-370) — **no Temporal-contract change**.
- **Multi-URI import is now atomic.** Raw loaders (CSV/JSON/Parquet) use `CREATE OR REPLACE TABLE`,
  and a per-URI failure mid-list drops this run's DuckDB tables + rolls back the session, so a failed
  import commits nothing. Previously a partial failure committed the earlier URIs (failure is a
  RETURN, so `session_scope` committed on clean exit) and the next run's `should_skip` silently
  dropped the rest — a data-corruption wedge, now fixed.
- **Extension routing reconciled with the cockpit.** Engine suffix→loader now matches `connect.ts`
  FILE_READERS / `upload/policy.ts` ALLOWED_EXTENSIONS exactly: **csv/tsv/txt → CSV, parquet/pq →
  Parquet, json/jsonl/ndjson → JSON**. `.ndjson` previously fell through to the CSV loader (misparse);
  `.txt`/`.pq` were accepted by the cockpit but rejected at engine registration. Both fixed.

### dataraum-eval

- **Eval action: behavior-preserving for single-file sources — re-verify, don't expect a shift.** A
  single CSV/Parquet/JSON source produces the same raw table as before; only the connection key
  (`path` → one-element `file_uris`) and the raw `CREATE` (now `OR REPLACE`) changed. The multi-file
  path is **new capability** (a source can now be several files → several raw tables), exercised the
  same way (`addSourceWorkflow`); detector logic is untouched.
- **How to drive a run / seed a source**: a file source's `connection_config` is now
  `{"file_uris": ["s3://<lake-bucket>/<key>", ...]}` (NOT `{"path": ...}`). Any eval/harness fixture
  or seed that wrote `{"path": ...}` for a file source must switch to `{"file_uris": [...]}`. db_recipe
  sources are unchanged (`{"tables": [{name, sql}], "backend": ...}`).
- **`.ndjson` now lands in the JSON loader** — a fixture that relied on the old (wrong) CSV routing
  for an `.ndjson` file would change shape; none expected.

### dataraum-testdata (hints)

- No new injection types. A multi-file fixture (a bucket prefix with ≥2 loadable files that should
  ingest as ≥2 raw tables) would exercise the new fan-out + the atomic-failure path directly. Optional.

## 2026-05-31: DAT-382 — ontology induction LEAVES the engine for the cockpit agent tier

Lands the ADR-0004 cut: `_adhoc` ontology induction is no longer the engine's
job. The cockpit `frame` stage (TS, TanStack AI + `@tanstack/ai-anthropic`) now
induces concepts from the connect schema and writes them as `concept`
`config_overlay` rows; the engine grounds against those rows. Folds in DAT-377 as
the grounding-only frozen-artifact contract (ADR-0007).

What changed in the engine (calibration-relevant):
- **Deleted** `analysis/semantic/induction.py` (the `OntologyInductionAgent` +
  `induce_adhoc_concepts`), its `__init__` exports, and the
  `dataraum-config/llm/prompts/ontology_induction.yaml` prompt. The DAT-376 split
  stays — only induction's *home* moved (to the cockpit). The cycles / validation /
  graphs induction agents are **untouched**.
- **`semantic_per_column` is grounding-only.** The `if ontology == "_adhoc":
  induce_adhoc_concepts(...)` branch is replaced by a **fail-loud** guard: a cold-start
  `_adhoc` workspace with **zero** concept overlay rows now FAILS the phase with a clear
  error instead of grounding against an empty concept set. `ground_columns` is otherwise
  unchanged.
- The cold-start concept set is now produced by the **TS frame agent**, not the engine.
  Its prompt is the engine `ontology_induction.yaml` re-homed verbatim to
  `packages/cockpit/src/prompts/frame.ts`. Concept payload contract is unchanged
  (`OntologyConcept` field set; `core/overlay._apply_concept` consumes it as before).

Calibration impact: cold-start induction quality is now a cockpit (TS) concern. The
engine no longer makes the induction LLM call; evaluate induction against the TS frame
agent. Grounding (column→concept mapping) recall/precision is unaffected by this PR —
it still runs in the engine against the same concept rows. Recall coverage for the
relocated induction is handed to DAT-379/383.

## 2026-05-29: DAT-373 — stable typed Column ids + owner-scoped per-phase replay_cleanup (Option A)

Fixes the cross-stage data-loss hazard DAT-343 flagged: a type-teach replay used
to (a) drop the typed `Table` and cascade-wipe **every** per-Column row of **every**
stage, and (b) re-mint fresh `uuid4` typed Column ids on each re-type (orphaning
any other stage's per-Column rows even if cleanup were scoped). Both are fixed so
a future `begin_session` (DAT-356) / frame-ground (DAT-377) per-Column finding
survives an `add_source` teach. **No schema migration** (the `owner_stage`
discriminator, Option B, is a deferred fast-follow — not done here).

What changed in the engine:
- **Stable typed identity.** `resolve_types` + `TypingPhase._promote_strongly_typed`
  now RECONCILE the typed/quarantine `Table` + `Column` rows by
  `(source_id, table_name, layer)` / `(table_id, column_name)` — reuse + UPDATE in
  place, delete columns no longer present, insert genuinely new ones — instead of
  drop+recreate. Typed Table id AND typed Column ids are **unchanged across a
  re-type**. New shared helpers `reconcile_typed_table` / `reconcile_typed_columns`
  in `analysis/typing/resolution.py`.
- **`typing.replay_cleanup` is now in-place + owner-scoped.** It KEEPS the typed
  `Table`/`Column` rows; clears only typing-owned `TypeCandidate`/`TypeDecision`
  (raw + typed copies) and drops the DuckDB typed/quarantine tables (rebuilt by
  `_run`'s `CREATE OR REPLACE`). It NO LONGER deletes the typed `Table`, so it no
  longer cascade-wipes `StatisticalProfile` / `SemanticAnnotation` / temporal /
  quality / eligibility rows.
- **Per-phase owner-scoped `replay_cleanup`** added to `statistics`,
  `column_eligibility`, `statistical_quality`, `temporal` — each deletes only its
  OWN per-Column rows scoped to the replay's typed `table_ids`. The workflow now
  invokes `replay_cleanup_for_phase` for **every** phase that re-runs under a
  replay (`_maybe_replay_cleanup` gated by the new `_phase_reruns_on_replay`), not
  just `from_phase`; the source-level reduce always self-cleans.
- **`typing.should_skip`** now treats a typed table as "done" only if its columns
  still carry a `TypeDecision` (the row cleanup clears) — the surviving typed
  `Table` row alone is no longer the signal.
- **`BasePhase.replay_cleanup` docstring** now states the ownership contract:
  delete ONLY your own rows scoped to `table_ids`; NEVER delete a parent `Table`
  you don't exclusively own; the Table-delete cascade is reserved for
  `import`/source teardown.

### dataraum-eval

- **Eval action: NO recalibration needed.** No detector, prompt, threshold, or
  annotation-content change. Recall is unaffected: the re-type produces the same
  typed data + the same `TypeDecision`/`TypeCandidate` content as before; only the
  row identity (reuse vs. fresh uuid4) and the cleanup scope changed.
- **Eval-fixture flag:** any fixture or assertion that relied on a re-type
  **minting new typed `column_id`s** (or a new typed `table_id`) is now WRONG —
  ids are stable across replays. The cockpit integration smoke
  (`packages/cockpit/src/temporal/drive-add-source.ts`) asserted "every
  typed_table_id CHANGED" as proof `replay_cleanup` fired; that assertion must
  flip to assert ids are STABLE and that a seeded foreign per-Column row survives.
  Not changed in this lane (cross-PACKAGE, TS, not run here).

### Tests

- RED→GREEN hazard test + in-place semantics in
  `tests/unit/pipeline/test_phase_replay_cleanup.py`.
- Stable-id + downstream-skip update in `tests/unit/pipeline/test_typing_phase.py`.
- `_phase_reruns_on_replay` predicate in `tests/unit/worker/test_replay_scope.py`.
- New `tests/integration/pipeline/test_replay_cross_stage.py`: re-type keeps typed
  ids stable AND a simulated begin_session `SemanticAnnotation` on a typed column
  survives a re-type + statistics rebuild (real DuckLake substrate).

## 2026-05-29: DAT-376 — split induction ↔ grounding in `semantic_per_column` (structure-only)

Detached the two LLM steps inside `semantic_per_column` into independently
callable module-level functions, **in place** — no new pipeline stage, and
the `add_source` surface (workflow names, activity names, phase order,
`pipeline.yaml`, `contracts.py`) is byte-for-byte unchanged. This is a pure
extract-then-rewire; the phase `_run` is now a thin composer over the two
functions.

### dataraum-eval

- **Eval action: NO recalibration needed.** Recall is safe by construction —
  nothing that produces detector/annotation content changed:
  - The **ontology induction agent**, its prompt, and its tool schema are
    untouched (the extracted `induce_adhoc_concepts` wraps the *same*
    `OntologyInductionAgent.induce` call and the *same* per-concept
    `ConfigOverlay(type="concept", payload={"vertical":"_adhoc", ...})`
    insert + `session.commit()` as DAT-371's `_ensure_adhoc_ontology`).
  - The **`ColumnAnnotationAgent`** (the grounding step's worker), its prompt,
    its tool schema, and the `required_standard_fields` it receives from
    `GraphLoader(vertical=ontology).get_all_abstract_fields()` are unchanged.
  - **`persist_column_annotations`** row shapes are unchanged (reused verbatim).
  - All five `semantic_per_column` detectors are unchanged.
- **`replay_cleanup` is unchanged** — still drops `SemanticAnnotation` only and
  NEVER the induced `concept` `ConfigOverlay` rows. A new regression test pins
  this (`test_semantic_split_phases.py::TestPerColumnReplayCleanup`).

### The new seam (for DAT-377 / DAT-378)

`semantic_per_column` now composes two functions, both in
`dataraum.analysis.semantic` (and re-exported from its `__init__`):

- `induction.induce_adhoc_concepts(*, session, config, provider, renderer, table_ids) -> Result[int]`
  — cold-start `_adhoc` ontology induction. Short-circuits (returns `Result.ok(0)`)
  when concepts already exist; otherwise induces and inserts one `concept`
  overlay row per concept, then commits. The `if ontology == "_adhoc":` gate
  stays at the call site.
- `processor.ground_columns(*, session, config, provider, renderer, table_ids, ontology, session_id) -> Result[int]`
  — per-column annotation + `persist_column_annotations`, returns the row count.

This is the seam DAT-377/378 act on: the **connect/frame relocation moves the
induction CALL upstream** (induction belongs in `frame`, where the user declares
concepts before data — see the `project_frame_stage_ontology` memory), leaving
`add_source` / `semantic_per_column` calling **only** `ground_columns`. No
content change to either step is implied by that move — purely *where* the
induction call lives.

- **Status**: pending

## 2026-05-28: DAT-371 — `_adhoc` ontology induction moves to `concept` overlay rows

Follow-up to DAT-343 that unblocks DAT-339 user testing. The baked-in
config root is bind-mounted `:ro`, so `semantic_per_column`'s cold-start
`_adhoc` path (which used to `OntologyLoader.save()` back to
`verticals/_adhoc/ontology.yaml`) crashed with `OSError: Read-only file
system`. Induced concepts now persist as `config_overlay` rows.

### dataraum-eval

- **Eval action: no recalibration needed.** No detector logic changed;
  concept-content is still produced by the same LLM induction agent
  with the same prompt and tool schema. What changed is *where* the
  induced concepts live (Postgres overlay rows, not a YAML file) and
  the layered-read path that materializes them.
- **New `concept` overlay applier** in `dataraum.core.overlay`:
  ``verticals/<v>/ontology.yaml`` reads now merge concept rows
  (upsert-replace by `name`) **before** `concept_property` patches. If
  any eval fixture inserts both for the same vertical, the order matters
  — a `concept` row replaces a concept wholesale; subsequent
  `concept_property` rows for that concept patch on top.
- **`OntologyLoader.load` now routes through `load_yaml_config`** so the
  overlay applies. The in-loader cache is removed (live reads must
  reflect freshly-inserted rows). Eval fixtures that pass
  `verticals_dir=...` still bypass the overlay and are deterministic.
- **`OntologyLoader.save` is deleted.** Any eval helper that wrote a
  vertical YAML via the loader must switch to inserting `ConfigOverlay`
  rows (one per concept; `type='concept'`; payload includes `vertical`).
- **New `_adhoc` baseline ships at
  `packages/dataraum-config/verticals/_adhoc/ontology.yaml`** with
  `concepts: []`. The induction-on-cold-start path inserts overlay rows
  on top of this empty baseline.
- **Cockpit `concept` payload is now typed** (`ConceptPayload` in
  `teach.validation.ts`) mirroring `OntologyConcept` — required:
  `vertical` + `name`; everything else optional with passthrough.

### dataraum-testdata

- No testdata change required. Adhoc induction still happens on the
  same data shape; the only difference is the persistence substrate.

## 2026-05-28: DAT-343 — teach via Postgres `config_overlay` + remove-and-replay (E3)

DAT-343 retires the DAT-358 filesystem teach overlay and replaces it with a
per-workspace `ws_<id>.config_overlay` Postgres table. Teach edits flow
through that single seam; layered reads merge active rows over the
baked-in YAML via per-type appliers in `dataraum.core.overlay`. The
`addSourceWorkflow` grows an optional `replay: ReplayScope` input so the
cockpit can re-run the affected portion of the chain after a teach.

### dataraum-eval

- **Eval action: re-baseline.** This PR doesn't change detector logic, but it
  changes the substrate detectors observe AND the trigger surface that
  invalidates their inputs.
- **`relationship` detector now reads `ConfigOverlay`, not `DataFix`.**
  `entropy/detectors/structural/relations.py:_get_preferred_joins` queries
  rows of `type='relationship'` with `superseded_at IS NULL`. Payload shape
  changed from nested `{parameters: {table, target_table, ...}}` to flat
  `{source_id, table, target_table, ...}`. Any eval fixture writing the
  legacy shape needs updating. The detector lives in `semantic_per_table`
  which isn't in the slice-1 chain — no calibration impact in slice 1; flag
  for slice 2 when that phase joins.
- **`Relationship.is_confirmed` no longer gets stamped by user teaches.**
  `MetadataInterpreter._create_relationship` was the only writer; deleted in
  P3. `relationship_entropy` still reads `is_confirmed` and gives confirmed
  joins a lower entropy. Same slice-2+ latency — neither detector runs
  today, but when they do, user teaches will affect `join_path_determinism`
  scoring (cuts ambiguity) but NOT `relationship_entropy` scoring (the
  "confirmed" branch). Tracked as **DAT-372** (`Relationship.is_confirmed
  signal lost from relationship_entropy post-DAT-343`).
- **Per-Column cleanup is FK-cascade-driven, not per-phase-owned.** Critical
  for slice 2: `typing.replay_cleanup` deletes the typed `Table` row,
  SQLAlchemy cascade wipes its `Column` rows, and every per-Column row
  cascades from there. Works in slice 1 because `add_source` is the only
  stage writing per-Column. The moment `begin_session` lands and attaches
  findings to those same Columns, an `add_source` teach replay silently
  wipes them. Tracked as **DAT-373** (`Per-phase replay_cleanup ownership
  — required before begin_session writes per-Column data`); marked
  `Blocks DAT-356` (slice 2). Re-design needed: per-stage tables, or
  per-stage column identity, or scoped cascade declarations.
- **Replay paths re-run detectors.** A teach + `replay(from_phase="typing",
  raw_table_ids=[t])` re-runs typing + analytics + `detect_table` for that
  table → `type_fidelity`, `null_ratio` regenerate. A teach +
  `replay(from_phase="import", raw_table_ids=None)` re-runs the full source
  → all per-table detectors + `detect_source` (`business_meaning`,
  `unit_entropy`, `temporal_entropy`, `outlier_rate`, `benford`). On any
  replay the source-level reduce (`semantic_per_column` + `detect_source`)
  always re-runs — eval should expect detector outputs to refresh on every
  replay invocation, not just on initial `add_source` runs.
- **How to drive a teach round-trip**:
  1. `teach({type, payload})` → inserts a row in `ws_<id>.config_overlay`;
     returns `{overlay_id, type}`.
  2. (optional) batch more teaches.
  3. `replay({source_id, scope: ReplayScope, vertical?})` → starts
     `addSourceWorkflow` with `ReplayScope` carrying the from_phase + the
     raw_table_ids to narrow the fan-out. `workflow_id` is reused as
     `addsource-<source_id>` with `ALLOW_DUPLICATE` policy — Temporal UI
     shows iterations grouped per source. Returns the run_id; await via
     `client.workflow.getHandle(...).result()`.
  4. (undo) `undoTeach(overlay_id)` → sets `superseded_at = now()`. The
     row is still readable by audit queries but no longer participates in
     layered reads. Idempotent.
- **Cold-start regression — DAT-371 follow-up:**
  `semantic_per_column._ensure_adhoc_ontology` still writes
  `verticals/_adhoc/ontology.yaml` to the bind-mounted (read-only)
  baked-in config dir — OSError on every initial `add_source` run with the
  default `_adhoc` vertical. Workaround for now: pass an explicit
  `vertical` (e.g. `"finance"`) in the `SourceIdentity`. **DAT-371 blocks
  DAT-339 user testing**; the fix moves induced concepts to `concept`
  overlay rows via a new per-type applier.
- **Container-restart persistence is architecturally guaranteed**
  (Postgres-backed; survives engine + cockpit restarts). Spec asked for
  explicit verification — not added as a test. If you want it, a single
  `docker compose restart engine-worker` between a teach and a
  `getPendingOverlays` assertion is the minimum.
- **`DATARAUM_HOME` env + `dataraum_workspace` Docker volume retired.**
  Local dev setups holding stale data in that volume should
  `docker compose down -v` once before next bring-up.

### dataraum-testdata (hints)

- No new injection types needed — the substrate change doesn't introduce
  new detection surface.
- A teach-aware fixture set would be useful for slice-2 calibration: data
  with known mis-typing that a `type_pattern` teach should fix on replay.
  Not a slice-1 ask.

## 2026-05-27: DAT-370 follow-up — restore the source-level detectors (eval-caught regression)

Eval found that DAT-370 orphaned `semantic_per_column`'s detectors. When detectors
moved off the per-phase path, only `detect_table` (the table-local phases) was
wired; `semantic_per_column` runs as the source-level reduce but nothing ran its
declared detectors — `business_meaning`, `unit_entropy`, `temporal_entropy`,
`outlier_rate`, `benford` — so they were dead from DAT-370 until now.

Fix: added a source-level `detect_source` activity that runs after the reduce in
`addSourceWorkflow`, executing the `_SOURCE_LEVEL_PHASES` (= `semantic_per_column`)
detectors **source-wide** (`run_detector_post_step(table_ids=None)`; single
sequential step in the parent, no concurrency). Mirrors `detect_table`. A unit
guard (`test_no_chain_phase_detector_is_orphaned`) now fails if any chain phase
declares a detector that no detect step runs.

### dataraum-eval

- **Action: re-run the semantic detectors — they now produce scores.**
  `business_meaning`, `unit_entropy`, `temporal_entropy`, `outlier_rate`, `benford`
  execute once after the reduce, source-wide (same scope as the pre-DAT-370 coarse
  run). No detector logic changed — purely the missing execution path restored.
- Drive a run the same way (`addSourceWorkflow`); the new `detect_source` step is
  internal to the workflow.
- `relationship_entropy` / `join_path_determinism` (semantic_per_table) and the
  other Zone-2/3 detectors remain unwired — their phases aren't in the chain yet.

### dataraum-testdata (hints)

- None.

## 2026-05-27: DAT-370 — per-table fan-out for add_source (E4b-2)

The table is now the unit of work. `addSourceWorkflow` imports the source,
**fans out one `processTableWorkflow` child per raw table** (`asyncio.gather`),
then runs `semantic_per_column` once as the source-level reduce. Each child runs
the table-local chain scoped to its one table: `typing` (mints a typed id) →
`statistics` → `column_eligibility` → `statistical_quality` → `temporal` →
`detect_table`. This replaces DAT-368's coarse single pass over the whole source.

Two structural changes ride along:
- **Detectors moved off the per-phase path to a stage-level step.** They no
  longer run as a post-step after each phase; instead one `detect_table` step at
  the tail of each child runs the table-local detectors (`type_fidelity`,
  `null_ratio`) **scoped to that child's typed table**. `run_detector_post_step`
  gained a `table_ids` scope (delete-before-insert + scan restricted to the
  table) so parallel children never clobber each other's
  `(source_id, detector_id)` rows.
- **Message contract redesigned per-boundary.** The uniform
  `PhaseActivityInput`/`PhaseActivityResult` envelope is gone; activities take
  typed inputs (a `SourceIdentity` header + their real args) and the workflow
  returns `AddSourceResult { raw_table_ids, tables:[{raw_table_id, typed_table_id}] }`.

### dataraum-eval

- **Eval action: behavior-preserving — re-verify, don't expect a shift.** Same
  detectors, same per-table/per-column analysis; only granularity (per-table) and
  detector *timing* (once per table at stage end vs. once per source-wide phase)
  changed. The union of per-table detector records equals the old single
  source-wide run. **This is the per-table execution the eval gate was waiting
  on** — calibration can now run against the stabilized pattern.
- **How to drive a run**: start `addSourceWorkflow` (task queue
  `dataraum-pipeline`) with `AddSourceInput` = `{ identity: { workspace_id,
  source_id, session_id, vertical? } }`. It fans out per table and stops after
  `semantic_per_column`; `relationships` + `semantic_per_table` (slice-2) and
  teach (DAT-343) are still not in the chain.
- **If recall moves**: suspect the per-table detector scoping (`table_ids` in
  `run_detector_post_step`) or the per-table `should_skip` rewrites in the four
  analytics phases — those are the only behavioral touches.
- **Status**: per-table execution stabilized; eval unblocked to run in parallel.

### dataraum-testdata (hints)

- None. No detector or fixture surface changed; output is preserved.

## 2026-05-27: DAT-369 — de-monolith (retire the hand-rolled scheduler + monitoring)

Pure-cleanup follow-up to DAT-368. Now that the engine is a Temporal activity
worker, the hand-rolled orchestration is dead and gone: deleted the
scheduler/runner/setup/event-system, the `PipelineRun`/`PhaseLog` monitoring
tables + `pipeline/status.py`, the YAML dependency-DAG machinery (per-phase
`dependencies`/`produces`, `YAMLAwarePhase`, the transitive-dep helpers), the
MCP-only `investigation/recorder.py`, and `ConnectionManager.bind_session_id`.
The dead MCP surface moved out of the package to `reference/mcp/`. `TEMPORAL_*`
settings are now required/fail-loud.

### dataraum-eval

- **Eval action: none.** No detector, pipeline-phase behavior, response-shape,
  or Temporal-contract change. `pipeline.yaml` kept every phase's `description`
  + `detectors` (the worker still runs detectors as post-steps via
  `PhaseDeclaration.detectors`); only the unused DAG metadata was removed. The
  one behavioral touch — `enriched_views` `should_skip` now checks for an
  `EnrichedView` row instead of a `PhaseLog` "completed" row — is on a slice-2
  phase that calibration doesn't exercise yet.
- **Status**: no calibration impact; informational only.

### dataraum-testdata (hints)

- None. No detector or fixture surface changed.

## 2026-05-27: DAT-368 — slice-1 run surface lands (addSourceWorkflow)

The engine run surface that DAT-362 + DAT-341 calibration were **blocked on**
now exists. The engine is a Temporal worker; all seven slice-1 table-local
phases are registered as activities (`import`, `typing`, `statistics`,
`column_eligibility`, `statistical_quality`, `temporal`, `semantic_per_column`)
and the `addSourceWorkflow` workflow drives them in dependency order over a
source, then completes.

### dataraum-eval

- **What changed**: no detector or response-shape change — this is purely the
  *execution surface*. Phases now run through `dataraum.worker.run_phase_activity`
  (scoped Postgres session + a per-activity DuckDB cursor) and are orchestrated
  by `addSourceWorkflow`, instead of the in-process scheduler / `PipelineTestHarness`.
- **How to drive a run**: trigger `addSourceWorkflow` via the Temporal Client
  (task queue `dataraum-pipeline`) with `{workspace_id, source_id, session_id,
  vertical?, table_ids?}`. It runs **once over all the source's tables** (coarse;
  per-table fan-out + column batching is E4b-2 / DAT-370). It stops at
  `semantic_per_column` — `relationships` + `semantic_per_table` (slice-2) and
  teach (DAT-343) are **not** in the chain yet.
- **Calibrate**: the DAT-362 semantic-split calibration (business_meaning /
  unit_entropy recall vs. the pre-split baseline) can now actually run end-to-end
  through this surface. `semantic_per_table` detectors (`join_path_determinism`,
  `relationship_entropy`) remain un-runnable here until slice-2.
- **Status**: run surface ready; DAT-362 calibration unblocked.

### dataraum-testdata (hints)

- None. Same fixtures; this is an orchestration change, not a detector change.

## 2026-05-26: DAT-362 — semantic phase split (per-column + per-table)

The monolithic `semantic` phase is split into two pipeline phases (Option B):
`semantic_per_column` (annotates + **persists** columns on the balanced model)
and `semantic_per_table` (classifies tables + confirms relationships, reasoning
over the persisted annotations). The old single `analyze_schema` LLM call is gone.

### dataraum-eval

- **What changed**: the semantic detectors' *inputs* are produced differently,
  even though the detectors themselves are untouched:
  - **Column annotations now come from a column-only LLM call** that runs
    **before** relationships (table-local), instead of the old capable-model
    pass that saw relationship context. The deliberate trade (DAT-362 Option B):
    the LLM cross-table column-upgrade pass is **dropped**; human/agent teach
    between the phases is meant to replace it. This is the change most likely
    to move `business_meaning` recall.
  - **Unit detection moved**: the table-level `unit_relationships` backfill is
    removed. `unit_source_column` is now set **directly per column** by the
    per-column model (prompt `<unit_detection>`). Watch `unit_entropy`.
  - The per-column model tier changed `fast → balanced` (was a throwaway
    pre-pass; now authoritative). Net annotation quality should hold or improve.
  - `temporal_entropy`, `outlier_rate`, `benford` read the same persisted
    annotations — should be unaffected. `join_path_determinism`,
    `relationship_entropy` read relationships from `semantic_per_table` —
    same data, later phase.
- **Affected phases/detectors**: `semantic_per_column` produces `[semantic]`
  + detectors `business_meaning, unit_entropy, temporal_entropy, outlier_rate,
  benford`; `semantic_per_table` runs `join_path_determinism,
  relationship_entropy`. Downstream (`enriched_views`, `business_cycles`,
  `validation`, `data_fixes`) now depend on `semantic_per_table`.
- **Expected calibration outcome**: recall on `business_meaning` / `unit_entropy`
  is the open question — this is the first run of the next-gen split, and the
  user accepted that quality is validated here, in eval, not in-repo. If recall
  regresses, fix the per-column prompt (`column_annotation.yaml`) /
  `semantic_per_table.yaml`, not the detectors.
- **Calibrate**: full suite once the engine run surface lands (blocked on
  DAT-344 / E4, same as DAT-341). Compare `business_meaning` + `unit_entropy`
  recall against the pre-split baseline specifically.
- **Status**: pending (blocked on DAT-344)

### dataraum-testdata (hints)

- No new injection types required. If `unit_entropy` regresses, a targeted
  fixture with cross-column unit dimensions (e.g. a `currency_code` column
  defining units for several measures in one table) would exercise the new
  per-column unit-detection path directly.

## 2026-05-21: DAT-341 — workspace-typed substrate (slice 1 E1)

Substrate change: typed tables move from `lake.session_<id>` (per-session,
ephemeral) to `lake.{raw,typed,quarantine}.<source>__<table>`
(workspace-stable). `Table.workspace_id` and `EntropyObjectRecord.workspace_id`
FKs added (NOT NULL). `EntropyObjectRecord.session_id` stays NOT NULL but
is no longer the load-bearing scope.

### dataraum-eval

- **What changed (and what didn't)**: substrate-only refactor. Detector
  logic is unchanged; data reaching detectors is identical. The schema
  rename (`lake.session_<id>.typed_<x>` → `lake.typed."<x>"`) is the only
  surface-level shift, and it shows up in detector evidence strings as
  `<name>` instead of `typed_<name>` — cosmetic, not score-affecting.
- **Expected calibration outcome**: identical recall to pre-DAT-341.
  Eval's known-injection tests are deterministic; any drop in recall
  is a **bug** (a missed read site where some detector or analysis
  module still does `FROM "typed_<name>"` and now resolves to an empty
  schema slot), not "drift" or "expected variation". Investigate the
  failing detector's SQL — grep for hardcoded `typed_*` / `raw_*`
  prefixes that the substrate migration missed.
- **Calibrate**: run the full calibration suite as soon as the API
  surface lands (`dataraum-eval` calls into the engine via REST —
  blocked on DAT-344 / E4). Per the CLAUDE.md "calibration is the
  definition of done" rule, recall must not regress.
- **Notes**: workspace.db schema gained a `workspace_id` FK on `tables`
  and `entropy_objects`. Existing eval state on disk needs
  `rm -rf ${DATARAUM_HOME}` before the first calibration run.
- **Status**: pending (blocked on DAT-344)

### dataraum-testdata (hints)

- No new injection types required for this migration. The substrate change
  is structural and detector-agnostic.
- One directional hint: now that raw/typed/quarantine share a bare table
  name across layers, an injection that produces noisy raw data + clean
  typed data (e.g. "values DO TRY_CAST to numeric but the original
  strings have suspicious whitespace patterns") becomes easier to test —
  raw and typed are siblings in the catalog rather than schema-mates.
  Optional, not blocking.

## 2026-05-19: Open vendor bugs surfaced by eval tools-test port (NOT in PR #118)

While porting `calibration/tools/test_tool_chain.py` and friends to drive the
control plane over HTTP MCP, three real upstream bugs in `begin_session` /
`resume_session` / `look` / `run_sql` came out. These are **not fixed in
PR #118** — they need their own ticket(s) and an architectural call.

### Root cause: per-session lake schema + workspace-scoped entropy + resume that doesn't resume

Post-DAT-323 each `begin_session` creates a brand-new
`lake.session_<id>` schema. Pipeline writes (raw/typed/quarantine tables)
go to that schema. But entropy scores live in workspace Postgres
(`EntropyObjectRecord` keyed by `source_id`), so `_measure` sees scores
from the FIRST session that ran the pipeline and reports `status:complete`
regardless of which session is currently active.

Net effect when a user begins a second session on the same source:
- `measure()` returns the existing (workspace) scores — no pipeline trigger
- `look()` and `look(target=tbl)` work because they go through SQLAlchemy
  against workspace tables
- **`look(target=tbl, sample=N)` fails** — it executes
  `SELECT * FROM "typed_<src>__<tbl>" LIMIT N` on the per-session DuckDB
  cursor, which USEs an empty `lake.session_<new id>` schema
- **`run_sql` fails for raw-SQL paths that reference typed tables** — same
  reason; LLM repair masks this nondeterministically (sometimes patches
  the SQL with the schema prefix, sometimes doesn't, so the same test
  flips between PASS and XPASS)

DuckDB's error message even hints at the right schema:

```
Catalog Error: Table with name typed_detection_v1__invoices does not exist!
Did you mean "session_d71492d0_8e89_481d_8e4d_bfa49a284be1.typed_detection_v1__invoices"?
```

### The intended escape hatch (`resume_session`) is broken

`_restore_archived_session` in `src/dataraum/mcp/server.py:1481-1641` is
documented (and intended) to rebind the manager to the *existing*
`lake.session_<archived id>` schema — that's where the populated tables
live. The implementation instead calls `begin_session(...)` to mint a
**new** `InvestigationSession` id and binds the manager to that:

```python
# server.py:1619-1631
inv = begin_session(
    session,
    anchor_source_id,
    resume_intent,
    contract=archived_contract,
    vertical=archived_vertical,
)
new_session_id = inv.session_id
session_mgr.bind_session_id(new_session_id)   # ← wrong id; should be the archived session_id
```

So restoring an archive lands you in *another* empty lake schema. The
"data reused as-is" promise in the docstring (`# Pipeline data, snippets,
and teach overlays are reused as-is`) is false post-DAT-323 because the
schema isn't reused.

### Reproduction

```python
# Two fresh begin_sessions against the same source on a populated workspace
async with mcp_session(handle) as s:
    await call_tool(s, "add_source", {"name": "detection_v1", "path": "/var/lib/dataraum/sources/detection-v1"})
    await call_tool(s, "begin_session", {"source": "detection_v1", "intent": "first"})
    await call_tool(s, "measure", {})                      # triggers pipeline → populates lake.session_<id_A>
    await call_tool(s, "end_session", {"outcome": "delivered"})
    # Resume the archived session — supposedly attaches to id_A's schema
    archives = await call_tool(s, "resume_session", {})
    target = next(a["session_id"] for a in archives["archived_sessions"] if a["source"] == "detection_v1")
    await call_tool(s, "resume_session", {"session_id": target, "intent": "second"})
    # Should see typed data via raw SQL — fails because manager is bound to a NEW empty schema
    r = await call_tool(s, "run_sql", {"sql": "SELECT COUNT(*) FROM typed_detection_v1__invoices"})
    print(r)  # → "Catalog Error: Table ... does not exist! Did you mean session_<id_A>.typed_..."
```

### Design question (not just a one-line fix)

The architectural tension is: per-session lake schemas (DAT-323) make
session isolation clean, but the "resume" UX needs the resumed session
to see the prior session's data. Three plausible directions:

1. **Make `_restore_archived_session` pass the archived session_id to
   `bind_session_id` instead of a new one.** Loses the audit-trail
   benefit of a new `InvestigationSession` record per resume, but the
   schema reuse works. Probably 5-line patch.
2. **Pipeline data lives in a per-source schema (not per-session)** —
   `lake.source_<id>` instead of `lake.session_<id>`. Session schemas
   become a layer of overlays (teach, snippets, …) on top of shared
   pipeline data. Bigger refactor, cleaner UX.
3. **Resume copies the prior schema to the new session's schema.**
   Duplicates data on every resume; probably worst option.

### Where the bug bites in eval

Two ported tests live as `xfail(strict=True)` in
`calibration/tools/test_tool_chain.py` linked to this writeup:
`TestLookSample.test_sample_rows` and `TestRunSql.test_columns_metadata`.
Remove the `xfail` markers once the vendor fix lands.

### Status

- **PR #118** ships the seven other bugs we found end-to-end. This one is
  **not in it** — a fix would either be a 5-line patch with stronger
  semantic claims to make (option 1), or a real architectural change
  (option 2).
- **No urgency for the detector-recall eval** — that flow only uses
  `look` (short-name target) and `measure`, both of which work today.
- **Blocks the practitioner tools-test surface** — `look(sample)` and
  `run_sql` against typed tables can't be exercised reliably until this
  is fixed.

## 2026-05-19: DAT-325 — L6 Cutover (HTTP MCP is the only entrypoint; CLI + stdio + rich gone)

### dataraum-eval
- **Changed**: `pyproject.toml` (dropped `dataraum-mcp` script entry, dropped `typer` + `rich` deps), `src/dataraum/server/app.py` (mounts `/mcp/` Starlette sub-app behind bearer middleware; chained lifespans; `DATARAUM_MCP_TOKEN` refuse-to-start), `src/dataraum/mcp/server.py` (deleted `main()`, `run_server()`, `run_http_server()`, `_build_http_app()`, `_health()`, `_StreamableHTTPASGIApp`, `BearerAuthMiddleware`, `_TOKEN_ENV_VAR`, plus `hmac`/`stdio_server`/`StreamableHTTPSessionManager`/`sys` imports), `src/dataraum/mcp/__init__.py` (`run_server` re-export dropped), `src/dataraum/cli/` (entire tree deleted), `tests/unit/cli/` (deleted), `docs/cli.md` (deleted), `src/dataraum/core/logging.py` (Rich rendering path stripped — `LogBuffer`, `activate_console`/`deactivate_console`, `_build_text`, `_active_console`/`_active_log_buffer` globals gone; `_ProxyLogger.msg` always routes through stderr).
- **Affects**: **the calibration harness in dataraum-eval that currently shells out to `dataraum-mcp` over stdio is broken.** The script entry no longer exists; stdio is unreachable; the only transport is HTTP at `POST /mcp/` behind `Authorization: Bearer $DATARAUM_MCP_TOKEN`. **Per user (2026-05-19): do not block on this — eval gets adapted after L7.**
- **Adaptation path (post-L7)**:
  - **Option A (preferred):** spin up the control plane via `docker compose up -d --wait` (or `uvicorn dataraum.server.app:app` in-process for hermetic runs); set `DATARAUM_MCP_TOKEN` in the harness's env; talk to it over HTTP MCP (`mcp.client.streamable_http.streamablehttp_client(url, headers={"Authorization": f"Bearer {token}"})`). Most realistic — matches what shipping clients (Claude Code via `claude mcp add --transport http`) do.
  - **Option B (in-process, no transport):** import `from dataraum.mcp.server import create_server` and drive the MCP `Server` instance directly. Bypasses HTTP entirely; useful for unit-style calibration that doesn't need transport in the loop.
  - **Do NOT** try to reanimate stdio. The runner functions are gone; the import paths the eval harness used (`dataraum.mcp.run_server`, `dataraum.mcp.server.main`) raise `ImportError`.
- **No detector change. No tool surface change. No response shape change.** Same 12 MCP tools, same arguments, same outputs — only the transport that delivers them changed.
- **Env vars affecting eval**: `DATARAUM_MCP_TOKEN` (required) is the only addition. The DAT-323 set (`DUCKLAKE_CATALOG_URL`, `DUCKLAKE_DATA_PATH`, `DATABASE_URL`, `DUCKLAKE_PG_POOL_MAX`, `DUCKLAKE_SKIP_INSTALL`) still applies — see the DAT-323 handoff entry below.
- **Status**: pending — gated on L7 (DAT-326) merging first so eval has a stable integration smoke story to anchor against.

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

---

*Older handoffs (2026-03 and earlier, v0.2.x packages — resolved) are archived in [archive/handoff-2026-03-and-earlier.md](./archive/handoff-2026-03-and-earlier.md).*
