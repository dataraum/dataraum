# DAT-339 Slice 1 — feature implementation plan

Companion to [`dat339-pivot-status.md`](./dat339-pivot-status.md) (locked decisions + phase state) and the [DAT-339 epic](https://real-dataraum.atlassian.net/browse/DAT-339) (authoritative ticket structure). The Confluence canonical spec is [DD/23363586](https://real-dataraum.atlassian.net/wiki/spaces/DD/pages/23363586) — kept in lockstep with this doc.

**Slice 1 is `add_source` ONLY.** End-to-end vertical: register a workspace (auto-bootstrapped), connect a source (file upload or MSSQL recipe), extract + type every table, iterate per-table through `look → why → teach → measure → re-look` until data quality is good. `begin_session`, `end_session`, `resume_session`, and downstream stages are slice 2 ([DAT-356](https://real-dataraum.atlassian.net/browse/DAT-356)).

This doc supersedes the previous "Phase 1 = read surfaces, Phase 2 = add_source, Phase 4 = teach" framing, which was misaligned with the epic.

---

## Canonical add_source definition

From `packages/engine/docs/architecture-future.md` (the v1 vision doc):

> "Turns a raw file into a typed, profiled, annotated source. Loop: import, type, profile, deduplicate, annotate semantically. Where entropy is elevated, surface `why`. The why may indicate an unparseable date format the agent should declare as `type_pattern`; a token like `TBD` that means missing and should be declared as `null_value`; a column the semantic agent could not annotate confidently and that needs a `concept_property` patch. Apply the teach, rerun, re-measure."
>
> "The semantic agent runs *inside* add_source. Types are only fully resolvable with semantic context. **add_source is not done until typing and meaning agree.**"
>
> Operations: `type_pattern.declare`, `null_value.declare`, `concept.bind`, `concept_property.declare`, `explanation.declare`.

The semantic agent's reasoning is what makes add_source iterative. Without teach + why + re-measure, types stay wrong and add_source never converges.

---

## Substrate decisions (locked 2026-05-22)

### Engine kernel — 3 verbs
- `measure` (SSE — pipeline runner, target_phase + table_filter; reconnect replays current state)
- `run_sql` (Arrow IPC — DuckDB SQL over the lake; renamed from `query` 2026-05-22 to disambiguate from the legacy NL-to-SQL MCP tool)
- `probe` (read-only SQL against external sources — pre-import sniff)

FastAPI + OpenAPI + `packages/api/openapi.yaml` + `export_openapi.py` + `packages/cockpit/src/api/types.ts` + `pnpm codegen` deleted in Phase 0c.

### Persistence — per-workspace Postgres `config_overlay`
- Lives in engine `ws_<id>` schema (per-workspace by virtue of being in a per-workspace schema)
- Columns: `workspace_id, session_id NULLABLE, type, target, payload, created_at, superseded_at`
- **Workspace-scoped rows** (`session_id NULL`): `type_pattern`, `null_value`, `concept_property` — survive across sessions
- **Session-scoped rows** (`session_id NOT NULL`): `metric`, `validation`, `cycle` — die with the session
- Engine SQLAlchemy creates + migrates
- Cockpit pulls schema via existing `drizzle-kit pull` (rides the ws_<id> filter; no new client)
- Cockpit writes data via Drizzle metadata client (small policy break: metadata client is otherwise read-only, but `config_overlay` writes are allowed)
- **DAT-358's filesystem overlay was a stepping-stone**, not the destination. Retires when DAT-343 lands the Postgres path. See [[recency-not-value]] memory.

### Cockpit — hand-written TS tools
- `packages/cockpit/src/tools/` (no openapi-fetch, no codegen, no generated REST client)
- 7 slice-1 tools call Drizzle (metadata + cockpit_db) + kernel verbs (or the chosen orchestration framework, pending spike)
  1. `list_sources` — Drizzle against `sources` table
  2. `list_tables` — Drizzle against `tables` table
  3. `add_source_file` — TS server function (multipart upload to mounted lake) + kicks off measure(import + typing + statistics + semantic_*)
  4. `add_source_recipe` — TS server function (recipe YAML write) + same pipeline kickoff
  5. `look_table` — Drizzle metadata + `/run_sql` for sample rows + statistical profile aggregation
  6. `why_column` — **ported from `mcp/why.py` to TS**: Drizzle evidence aggregation (entropy, slice profiles, snippets, validations) + Anthropic synthesis in chat handler
  7. `teach` — writes `config_overlay` row via Drizzle metadata client + triggers replay-typing for the affected table(s)
- Each tool owns its widget response shape (TableProfile JSON, WhyPanel hybrid markdown+suggestions, traffic-light bands derived from `exploratory_analysis` contract)
- **The current cockpit routes** (`chat.tsx`, `sources.tsx`, `index.tsx`) are placeholders — the real UI is the three-region layout + Stage Navigator built in C1 (DAT-347)

### Pipeline phases — semantic phase MUST split
- `analysis/semantic/` decomposes into two phases: per-column (LLM-driven column roles, business terms) and per-table (entity types, table-level synthesis)
- Per-column annotations are the surface in-loop teach acts on; per-table synthesis runs over post-teach per-column annotations
- New sub-ticket under DAT-339 (no current ticket assignment — file as part of this rewrite)
- See `[[semantic-phase-split]]` memory

### Config package extraction
- `packages/engine/config/` extracts to standalone `packages/dataraum-config/`
- Contains: `config/entropy/contracts.yaml`, `config/llm/`, `config/verticals/`, etc.
- Engine consumes via env-var / mount path (`DATARAUM_CONFIG_PATH` already wired in compose)
- Cockpit can also consume vertical YAMLs for "which concept did the agent bind to?" UX
- Docker mount: shared `dataraum_config` volume

### MCP folder fate
- `packages/engine/src/dataraum/mcp/` stays through slice 1 as **dead code** (no engine consumer once teach moves to TS)
- Whole folder deletes in slice 2 alongside session-lifecycle reimplementation (`begin_session` / `end_session` / `resume_session`)
- Per `[[mcp-dead-reference-only]]` memory

### Orchestration framework — OPEN (spike pending)
- Three-way spike: **DBOS vs Temporal vs Restate**
- Tight scope: convert typing phase to each, write minimal `import → typing` workflow in each
- Compare: lines deleted from current orchestrator, retry/resume behavior, polyglot fit (TS-drives-Python), op cost, framework feature usage
- Output: framework decision OR "keep monolith, port current Python orchestrator to TS"
- E4 (DAT-344) shape is pending spike
- Memory `[[durable-execution-lean]]` (which leaned DBOS pre-pivot, dismissed Temporal as too heavy) is being re-evaluated — its rationale was single-language. Post-pivot is TS-drives-Python, which changes the calculus.

### Hard rules (no-corner-cutting)
- No backwards-compat shims
- No legacy Python wrapped in TS — TS owns teach writes directly via Drizzle
- Recency does not imply value (`[[recency-not-value]]` memory)
- Don't frame LLM phases as latency costs — this is an agentic system (`[[semantic-phase-split]]` memory)

---

## Ticket structure (per DAT-339 epic)

### Done

| ID | Ticket | Status |
|---|---|---|
| EW | [DAT-358](https://real-dataraum.atlassian.net/browse/DAT-358) | Workspace foundation — filesystem overlay (stepping-stone; superseded by Postgres `config_overlay` in DAT-343) |
| E0 | [DAT-340](https://real-dataraum.atlassian.net/browse/DAT-340) | MCP-surface test retire — shipped PR #129; Jira lags (transition To Do → Done) |
| E1 | [DAT-341](https://real-dataraum.atlassian.net/browse/DAT-341) | Workspace-typed schema substrate |

(Slice-1 substrate phases 0a–0f also merged via PR #132; they're sub-tasks of E0/E1 not separate tickets.)

### To Do — Engine

| ID | Ticket | Title | Rewrite status |
|---|---|---|---|
| E2 | [DAT-342](https://real-dataraum.atlassian.net/browse/DAT-342) | Per-table extract + type at add_source | Mostly aligned |
| **E2b** | **NEW** | Semantic phase split (per-column + per-table) | File pending |
| E3 | [DAT-343](https://real-dataraum.atlassian.net/browse/DAT-343) | Teach via Postgres `config_overlay` + remove-and-replay undo | **Rewrite pending** (filesystem → Postgres) |
| E4 | [DAT-344](https://real-dataraum.atlassian.net/browse/DAT-344) | Kernel / activity implementation | **Rewrite pending spike outcome** |
| ~~E5~~ | ~~DAT-345~~ | ~~SSE /api/jobs/{job_id}~~ | **Fold into E4** — `measure` IS the SSE verb; reconnect replays current state |

### To Do — Cockpit

| ID | Ticket | Title | Rewrite status |
|---|---|---|---|
| C1 | [DAT-347](https://real-dataraum.atlassian.net/browse/DAT-347) | Three-region layout + Stage Navigator (the REAL UI) | Verify against post-pivot |
| C2 | [DAT-348](https://real-dataraum.atlassian.net/browse/DAT-348) | AddSourceWizard | Verify (no REST refs) |
| C3 | [DAT-349](https://real-dataraum.atlassian.net/browse/DAT-349) | WorkspaceInventory + SourceCard | Verify |
| C4 | [DAT-350](https://real-dataraum.atlassian.net/browse/DAT-350) | TableProfile | Verify |
| C5 | [DAT-351](https://real-dataraum.atlassian.net/browse/DAT-351) | WhyPanel + TeachProposal + **why agent ported to TS** | **Rewrite pending** |
| C6 | [DAT-352](https://real-dataraum.atlassian.net/browse/DAT-352) | MeasureProgress + chat-as-audit-trail rehydration | Verify (re-shape post-framework spike) |

### To Do — Chat

| ID | Ticket | Title | Rewrite status |
|---|---|---|---|
| CH1 | [DAT-353](https://real-dataraum.atlassian.net/browse/DAT-353) | Tool registry + intent → canvas dispatch | **Rewrite pending** (drop openapi-fetch; absorb widget response shapes) |
| CH2 | [DAT-354](https://real-dataraum.atlassian.net/browse/DAT-354) | Tool-result chip rendering | Verify |

### Cross-cutting — NEW

| ID | Title | Notes |
|---|---|---|
| SPIKE | DBOS vs Temporal vs Restate (tight scope) | Blocks E4 + downstream |
| CFG | Config package extraction (`engine/config/` → `dataraum-config/`) | Independent; ship first |

---

## Dependency graph

```
EW (DAT-358) ──► E0 (DAT-340) ──► C1 (DAT-347)
                  │
                  └──► E1 (DAT-341)
                        │
                        ├──► E2 (DAT-342) ──► E2b (semantic split) ──► E3 (DAT-343)
                        │
                        └──► E4 (DAT-344) ──► CH1 (DAT-353) ──► C2 → C3 → C4 → C5 → C6
                                          └──► CH2 (DAT-354)

Cross-cutting:
  SPIKE (DBOS/Temporal/Restate) ──blocks──► E4
  CFG (config package)          ──independent──► ship first
```

True parallel lanes once each prerequisite lands:
- (E2 chain) ⫦ (CFG) ⫦ (SPIKE) — all on Done substrate
- After E4: (CH1) ⫦ (CH2)
- After CH1: cockpit chain (C2 → C3 → C4 → C5 → C6) is sequential by design (each widget builds on the previous tool's data)

---

## Per-ticket implementation notes

### E2 (DAT-342) — Per-table extract + type at add_source

Refactor the import + typing phases to operate on a single table at a time. Today they batch over all tables in the run. Slice-1 needs `TypingPhase(table_filter=[table_id])` so the per-table iterate loop can replay typing for just one table after a teach.

- Engine: `TypingPhase.table_filter` + `ImportPhase.table_filter` (where needed)
- Pipeline runner: accept per-table scope on a measure invocation
- No API/contract change yet — that's E4's scope

### E2b (NEW) — Semantic phase split

`analysis/semantic/` contains both per-column annotation logic and per-table entity-type synthesis. They must run as two separate phases so the in-loop teach (between per-column results and per-table synthesis) is possible.

- Engine: refactor `analysis/semantic/` into `semantic_per_column/` + `semantic_per_table/` (or rename in place)
- Pipeline registry: register both as distinct phases with `semantic_per_table` depending on `semantic_per_column`
- Tests: each phase has its own unit + integration tests
- This MUST land before C4 (TableProfile widget) needs to render per-column semantic results

### E3 (DAT-343) — Teach via Postgres `config_overlay`

Major rewrite from the original "filesystem overlay" framing:

- Engine: `config_overlay` SQLAlchemy table in `ws_<id>` schema with the column shape above
- Engine config loader: layered read (base config from `dataraum-config/` + workspace + session overlay rows). Resolves type+target conflicts via `created_at` ordering (later supersedes earlier; soft-delete via `superseded_at`)
- Engine `_get_config_root()` rewires: no more `${DUCKLAKE_DATA_PATH}/workspaces/<id>/config/` — config comes from Postgres
- TS teach tool writes `config_overlay` rows directly via Drizzle metadata client
- TS teach tool triggers replay-typing via the orchestrator (kernel `/measure(target_phase=typing, table_filter=[id])` OR Temporal signal OR DBOS workflow event — pending spike)
- Undo: write `superseded_at = now()` on the teach record + trigger same replay path

3 teach types fully round-trip in slice 1: `type_pattern`, `null_value`, `concept_property`. Other 6 currently-shipped types continue via existing engine paths (still write to `config_overlay`, but their phase replay is not yet wired in slice 1).

### E4 (DAT-344) — Kernel / activity implementation (PENDING SPIKE)

Two possible shapes:

**If spike chooses "keep monolith" or DBOS:**
- 3 kernel verbs as Starlette routes: `/measure` (SSE), `/run_sql` (Arrow IPC streaming), `/probe` (read-only SQL)
- `/measure` implements SSE with replay-current-state on reconnect (the "fold E5 in" decision)
- `/run_sql` uses the pyarrow batch streaming pattern (see implementation reference below)
- `/probe` runs against external sources via DuckDB ATTACH READ_ONLY

**If spike chooses Temporal or Restate:**
- 2 kernel verbs: `/run_sql` + `/probe` (still Starlette)
- Pipeline phases become standalone activity functions registered with the framework's worker
- Cockpit starts workflows directly via the framework's TS SDK
- Measure progress comes from workflow events, not SSE

**`/run_sql` pyarrow streaming pattern** (user-provided reference, 2026-05-22):

```python
async def run_sql(request):
    data = await request.json()
    query = data.get("query"); schema = data.get("schema")
    cursor = db.cursor()
    try:
        if not query.strip().endswith(";"): query += ";"
        query = f"USE {catalogue_name}.{schema}; {query}"
        results = cursor.sql(query).fetch_arrow_reader(batch_size=1_000_000)
        def yield_batches():
            for batch in results:
                buf = BytesIO()
                with pa.ipc.new_stream(buf, batch.schema) as writer:
                    writer.write_batch(batch)
                buf.seek(0); yield buf.getbuffer()
        return StreamingResponse(yield_batches(), media_type="application/octet-stream",
                                 headers={"Content-Disposition": 'attachment; filename="results.arrow"'})
    finally:
        cursor.close()
```

TODOs the reference flags: query validation, batch ordering on reconnect.

### C1 (DAT-347) — Three-region layout + Stage Navigator

The real UI. Replaces the placeholder routes. Layout:
- **Stage Navigator** (left): frame → add_source → begin_session → operating_model → answer/simulate (slice 1 only highlights add_source as active)
- **Focus Canvas** (center): widgets dispatched by chat tool calls land here (WorkspaceInventory, SourceCard, TableProfile, WhyPanel, AddSourceWizard, TeachProposal)
- **Chat surface** (right): the agent UI; tool calls dispatch into Focus Canvas

Build the layout shell + Stage Navigator scaffolding. Widgets land in C2-C6.

### C5 (DAT-351) — WhyPanel + TeachProposal + why agent port

`mcp/why.py` ports to `packages/cockpit/src/tools/why_column.ts`:
- Drizzle aggregation of evidence: entropy_objects + slice_profiles + sql_snippets + validation_results for the target column
- Anthropic synthesis call (same SDK already in chat.ts) with a strict prompt + response schema
- Output: `{ narrative: markdown, suggestions: [{type, payload, summary}] }`
- Render: WhyPanel shows narrative + suggestion list; clicking a suggestion opens TeachProposal pre-filled

Engine no longer hosts agent logic post-port. Engine becomes data + kernel verbs only.

### CH1 (DAT-353) — Tool registry + intent → canvas dispatch

Major rewrite from "openapi-fetch + REST routes":

- `packages/cockpit/src/tools/registry.ts` — explicit `Tool[]` + handler map imported by `chat.tsx`
- Each tool is a TS function: read inputs (Anthropic Tool schema) → Drizzle metadata client + orchestrator calls → typed response
- Each tool owns its widget response shape:
  - `list_sources` → `Source[]` (drives WorkspaceInventory's source list)
  - `list_tables` → `Table[]` with `source_id` (drives WorkspaceInventory's table list)
  - `look_table(table_id)` → `{ schema: ColumnDef[], sample: Row[], stats: StatisticalProfile[], traffic_light: TrafficLightBands }` (drives TableProfile)
  - `why_column(table_id, column)` → `{ narrative: markdown, suggestions: TeachSuggestion[] }` (drives WhyPanel)
  - `teach(table_id, type, payload)` → `{ status: "done" | "running", config_overlay_id, replay_job_id? }` (triggers TeachProposal confirm/edit + MeasureProgress)
  - `add_source_file(name, file)` → `{ source_id, table_ids }` (drives AddSourceWizard)
  - `add_source_recipe(name, recipe_yaml)` → same shape (drives AddSourceWizard recipe path)
- Traffic-light derivation (band thresholds from `exploratory_analysis` contract) ports from the original DAT-344 Python design to TS
- Network requests: Drizzle to Postgres; orchestrator calls to kernel verbs or framework SDK

### CFG (NEW) — Config package extraction

- New top-level package: `packages/dataraum-config/`
- Move: `packages/engine/config/{entropy,llm,verticals,...}` → `packages/dataraum-config/{entropy,llm,verticals,...}`
- Engine: replace hardcoded paths with `os.environ["DATARAUM_CONFIG_PATH"]` lookup
- Cockpit: optional Drizzle/json reads of `dataraum-config/verticals/*.yaml` for UX hints
- Docker: shared `dataraum_config` volume bind-mounted into both engine + cockpit containers
- `pyproject.toml` / `package.json` references: no — the config is data, not a code dependency
- Update compose env vars + `.env.example`

### SPIKE — DBOS vs Temporal vs Restate

- Tight scope: typing phase as activity in each + `import → typing` workflow in each
- Time-box: 2 days max
- Compare:
  - Lines of orchestrator code deleted from `packages/engine/src/dataraum/pipeline/`
  - Retry / resume behavior under simulated crash (kill the worker mid-typing)
  - Polyglot ergonomics (TS workflow ↔ Python activity)
  - Operational footprint (extra Postgres? extra cluster? extra worker process?)
  - Determinism rules / footguns when writing workflows
- Output: decision memo + recommendation in this file's "Substrate decisions" section
- If "keep monolith" wins: open ticket for "port Python orchestrator to TS" as the lighter alternative

---

## Out of scope (slice 2 or later — [DAT-356](https://real-dataraum.atlassian.net/browse/DAT-356))

- `begin_session` / `end_session` / `resume_session` lifecycle
- `list_archived_sessions`
- Cross-table teach propagation; workspace inventory refresh on a single teach
- Concurrent-session / multi-tab isolation
- Goodhart firewall types (`concept.bind`, `validation_exception`) — [DAT-355](https://real-dataraum.atlassian.net/browse/DAT-355)
- Multi-workspace UX, workspace switching, file→DB config migration — [DAT-357](https://real-dataraum.atlassian.net/browse/DAT-357)
- Recipe-template HTTP route — AddSourceWizard CodeMirror prefills from inline placeholder
- `mcp/` directory deletion (slice 2 cleanup)
- Dedupe phase (architecture-future mentions it; not in current engine pipeline — file as a follow-up if needed)

---

## Open questions / pending decisions

1. **Orchestration framework** — pending spike
2. **Dedupe phase placement** — architecture-future.md says add_source loop includes deduplicate. Does this phase exist? If not, scope for slice 1 or defer to slice 2?
3. **`/probe` semantics for DB recipes** — does `/probe` SQL go against the source via DuckDB ATTACH? Or do recipes generate `/probe` SQL stubs? Settle during E4 implementation
4. **C5's TeachProposal widget UX for the 6 non-round-tripped teach types** — slice 1 round-trips 3 types fully. The other 6 (`concept`, `validation`, `cycle`, `metric`, `relationship`, `explanation`) write to `config_overlay` but no replay path. UX implications for TeachProposal: disable or document?

---

## Resume protocol

1. Read this file's "Ticket structure" — find next To Do tickets
2. Read locked decisions in [`dat339-pivot-status.md`](./dat339-pivot-status.md)
3. Skim memory entries that are load-bearing:
   - `[[no-corner-cutting-via-deferral]]`
   - `[[recency-not-value]]`
   - `[[teach-writes-measure-runs]]` (note: being updated for Postgres overlay)
   - `[[semantic-phase-split]]`
   - `[[durable-execution-lean]]` (under re-evaluation via spike)
   - `[[mcp-dead-reference-only]]`
4. Confluence [DD/23363586](https://real-dataraum.atlassian.net/wiki/spaces/DD/pages/23363586) — kept in lockstep with this doc
5. `/refine` only if reality conflicts with this doc; otherwise `/implement`

---

## Decisions made 2026-05-22 (this session)

Captured here for cross-session continuity, since this entire doc is a rewrite triggered by today's findings:

1. The previous `dat339-slice1-features-plan.md` invented Phase 1-5 that didn't match the DAT-339 epic. Deleted.
2. Confirmed canonical `add_source` definition from architecture-future.md.
3. `config_overlay` placement: engine `ws_<id>` schema, per-workspace (not global, not filesystem).
4. Semantic phase split into per-column + per-table (`[[semantic-phase-split]]` memory).
5. Why agent moves from `mcp/why.py` to `packages/cockpit/src/tools/why_column.ts`.
6. Config package extraction: `engine/config/` → `dataraum-config/`.
7. `/query` → `/run_sql` rename.
8. DAT-345 folds into DAT-344.
9. mcp/ folder stays as dead code through slice 1; whole-folder delete in slice 2.
10. Recency does not imply value — DAT-358 retires despite being 2 days old (`[[recency-not-value]]` memory).
11. Spike DBOS vs Temporal vs Restate, tight scope (typing phase + import→typing workflow), 2 days max. `[[durable-execution-lean]]` memory under re-evaluation.
12. The current cockpit UI (`chat.tsx`, `sources.tsx`, `index.tsx`) is throwaway — DAT-347 (C1) builds the real UI.
