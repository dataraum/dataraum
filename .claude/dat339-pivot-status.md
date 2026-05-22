# DAT-339 pivot — multi-session status

Companion to `.claude/platform-status.md`. The pivot is multi-session work; this file persists locked decisions + the phase chain across compactions.

Integration branch: `feat/dat-339-pivot`
Active phase branch: `feat/dat-339-pivot-p0-substrate`

## Decisions locked by /refine (2026-05-22)

These are the binding architectural decisions. Do NOT renegotiate without going back through /refine.

- **Engine kernel = 3 verbs.** `measure` (SSE), `query` (Arrow), `probe` (read-only SQL against sources). FastAPI + OpenAPI + `packages/api/openapi.yaml` + `packages/engine/scripts/export_openapi.py` + `packages/cockpit/src/api/types.ts` + `pnpm codegen` all delete.
- **Schema-per-workspace.** Each workspace = its own Postgres schema (`ws_<uuid_with_underscores>`) in the engine metadata DB. No RLS, no GUC. Physical isolation. Per-workspace DuckLake catalog (user, 2026-05-22).
- **Cockpit_db is the control plane.** Holds `workspaces` registry table + (future) conversations / ui_state. Engine reads active workspace from `DATARAUM_WORKSPACE_ID` env var (slice 1 simple); cockpit_db lookup is multi-workspace future.
- **`drizzle-kit pull`** (NOT `introspect` — EOL in drizzle 1.0). Two configs: `drizzle.config.cockpit.ts` (push/generate cockpit_db) + `drizzle.config.metadata.ts` (pull-only against engine workspace schema).
- **Tools are hand-written TS in cockpit.** No openapi-fetch underneath. Cockpit data hooks = chat agent tools (DAT-353 was already N:M; pivot just changes the runtime from REST → drizzle/kernel).
- **Teach goes proper-from-day-one in Phase 4.** TS owns overlay writes against a Postgres `config_overlay(workspace_id, session_id NULLABLE, type, target, payload)` table. Filesystem overlay (DAT-358) deletes in Phase 4. `mcp/teach.py` deletes in Phase 4. No legacy-Python teach wrapped in TS — see `[[no-corner-cutting-via-deferral]]` memory.
- **MCP server (`packages/engine/src/dataraum/mcp/`) stays untouched in slice 1.** Code-on-disk reference for slice 2's session-lifecycle reimplementation. Slice 2 deletes the directory after porting `begin_session` / `end_session` / `resume_session` to TS + kernel.

## Phase chain

Each phase is roughly one session. Tick when committed AND tests green.

- [x] **Phase 0a — Schema-per-workspace substrate**
  - [x] A1: Drop `workspace_id` columns from `Table` + `EntropyObjectRecord`, drop `idx_entropy_workspace`, remove 11 row-stamping call sites, drop `workspace_id` param from `entropy/engine._make_record`. Commit `ffb9b345`. 1350/1350 unit tests green.
  - [ ] A2: Delete `Workspace` SQLA model + storage exports + `workspace_models.py`. `apply_and_persist` drops `workspace_id` param, uses `_get_config_root()`. `mcp/teach.py` drops the kwarg from its `apply_and_persist` call (1-line touch — necessary to keep engine building). Refactor `server/workspace.py` → env-var `DATARAUM_WORKSPACE_ID`; `get_active_workspace_id` returns module pointer. `server/app.py` lifespan adjusts. conftest cleanup (drop Workspace seeding + autofill hook's workspace_id branch). Test fixes (`tests/unit/server/test_workspace.py`, `tests/platform/smoke_dat_358.py`).
  - [ ] Commit B: SQLAlchemy `event.listens_for(engine, "connect")` setting `search_path` to `ws_<id>`. `bootstrap_workspace` issues `CREATE SCHEMA IF NOT EXISTS` before `init_database` runs.
- [ ] **Phase 0b — Drizzle two-config setup.** `drizzle.config.cockpit.ts` + `drizzle.config.metadata.ts`. Run `drizzle-kit pull` against the active workspace schema. Commit generated `metadata-schema.ts`. Cockpit `db/client.ts` splits into cockpit-client + metadata-client (latter sets schema via postgres-js `search_path` option).
- [ ] **Phase 0c — Starlette + delete FastAPI.** Replace FastAPI app, port `/health`, add 3 `501 Not Implemented` stubs for measure/query/probe. Delete `packages/engine/src/dataraum/api/` entirely. Delete `packages/api/openapi.yaml` + `packages/engine/scripts/export_openapi.py`. NOTE: `Workspace` SQLA model can now also delete (last consumer `api/services.py` is gone in this phase). Drop fastapi from `packages/engine/pyproject.toml`.
- [ ] **Phase 0d — Cockpit cleanup.** Delete `packages/cockpit/src/api/`. Remove `pnpm codegen` + `openapi-fetch` + `openapi-typescript` deps. Drop `list_sources` test tool from `chat.ts`. Clear test UI in routes/ to minimal placeholders.
- [ ] **Phase 0e+0f — Tool registry scaffold + infra mount + CI swap.** Bundled. `src/tools/` directory with README documenting hand-written N:M policy. Mount `dataraum_lake` (writable) into cockpit service in `packages/infra/docker-compose.yml`. CI: drizzle-kit pull check post-migration.
- [ ] **Phase 1 — Read surfaces.** TS Drizzle tools: list_sources, list_tables, look_table, search_snippets. Engine `query` Arrow verb. Widgets WorkspaceInventory + TableProfile (DAT-349 + DAT-350).
- [ ] **Phase 2 — add_source.** TS upload to mounted lake volume, recipe authoring in TS, engine `probe` verb, engine `measure` SSE verb. `TypingPhase.table_filter` (DAT-342 logic). Widget AddSourceWizard (DAT-348).
- [ ] **Phase 3 — why.** TS Drizzle + LLM synthesis in chat. Widget WhyPanel (DAT-351 without TeachProposal).
- [ ] **Phase 4 — teach (proper).** Postgres `config_overlay` table. Engine config loader reads Postgres overlay. Delete filesystem overlay logic (DAT-358 retire). Delete `mcp/teach.py`. TS owns teach writes. Widget TeachProposal + MeasureProgress (DAT-351 completion + DAT-352).
- [ ] **Phase 5 — Cleanup.** Verify `api/` empty + deleted (happened in 0c). `mcp/` directory **untouched** (carries to slice 2 for session-lifecycle reimplementation). Engine = pipeline + storage + analysis + kernel only.

## Resume protocol

1. `git status` on `feat/dat-339-pivot-p0-substrate` (or the active phase branch — check this file's header).
2. Read this file's "Phase chain" — find the next unchecked checkbox.
3. Read locked decisions above; do NOT renegotiate.
4. Skim relevant memory entries: `[[mcp-dead-reference-only]]`, `[[teach-writes-measure-runs]]`, `[[drizzle-kit-pull-not-introspect]]`, `[[platform-pivot]]`, `[[no-corner-cutting-via-deferral]]`.
5. Confluence spec DD/23363586 only if the pivot decisions above feel underspecified — they shouldn't be.
6. Continue from the next unchecked checkbox.

## Ticket-rewrite todo (deferred from /refine, low urgency)

The slice-1 phase tickets still reflect the pre-pivot REST-routes shape. Rewrite when each ticket actually comes up:

- **DAT-340 (E0)** — most MCP-surface tests now obsolete because the surface is going away. Scope shrinks.
- **DAT-343 (E3)** — drop the "teach handler invokes typing with table_filter" framing (hallucinated per user). Rewrite as "teach writes Postgres overlay row; cockpit calls `measure(target_phase='typing', table_filter=[id])` after."
- **DAT-344 (E4)** — replace "9 FastAPI routes" with "3-verb kernel surface" (measure + query + probe).
- **DAT-353 (CH1)** — tools call drizzle / kernel underneath, not openapi-fetch.

Doing this in-flight (when each phase actually starts) keeps the rewrites grounded in what we just built, not theoretical.
