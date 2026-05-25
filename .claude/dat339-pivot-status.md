# DAT-339 pivot — multi-session status

Locked decisions + phase state for the cockpit v1 slice 1 (`add_source`) pivot. Companion to:

- [`dat339-slice1-features-plan.md`](./dat339-slice1-features-plan.md) — per-ticket implementation plan
- [`platform-status.md`](./platform-status.md) — at-a-glance lane board
- [DAT-339 epic](https://real-dataraum.atlassian.net/browse/DAT-339) — authoritative ticket structure
- Confluence [DD/23363586](https://real-dataraum.atlassian.net/wiki/spaces/DD/pages/23363586) — canonical spec

Substrate (0a–0f) merged via PR #132. Slice-1 feature work continues per ticket structure under the epic; no integration branch — each ticket lands its own PR off `main`.

---

## Decisions locked (2026-05-22, refined throughout the day)

Binding. Do NOT renegotiate without `/refine`.

### Engine kernel (post-spike 2026-05-25)
- **2 verbs + Temporal worker**: `/run_sql` (Arrow IPC, renamed from `query` 2026-05-22) + `/probe` (read-only SQL against external sources) + `/health`. The engine container hosts a Starlette HTTP shell for these alongside a long-running Temporal activity worker process.
- **`/measure` retired** — replaced by Temporal workflow start from cockpit (`client.workflow.start(addSourceWorkflow, ...)`).  DAT-345 (separate SSE job_id surface) closed; folded into DAT-344.
- `/run_sql` + `/probe` stay Python per DAT-360 spike P5 (Bun [#13910](https://github.com/oven-sh/bun/issues/13910) is real production risk on `@duckdb/node-api`; one-owner-per-substrate principle).
- FastAPI + OpenAPI + `packages/api/openapi.yaml` + `export_openapi.py` + `packages/cockpit/src/api/types.ts` + `pnpm codegen` all deleted in Phase 0c.

### Persistence
- **Schema-per-workspace**: each workspace = own Postgres schema (`ws_<uuid_with_underscores>`) in engine metadata DB. No RLS, no GUC. Per-workspace DuckLake catalog.
- **`config_overlay` table** lives in engine `ws_<id>` schema (per-workspace by virtue of being in a per-workspace schema):
  - Cols: `workspace_id, session_id NULLABLE, type, target, payload, created_at, superseded_at`
  - Workspace-scoped rows (`session_id NULL`): `type_pattern`, `null_value`, `concept_property`
  - Session-scoped rows (`session_id NOT NULL`): `metric`, `validation`, `cycle`
  - Engine SQLAlchemy creates + migrates; cockpit pulls via existing `drizzle-kit pull`; cockpit writes via Drizzle metadata client (policy: metadata client otherwise read-only, `config_overlay` writes allowed)
- **DAT-358 filesystem overlay = stepping-stone**, not destination. Retires when DAT-343 lands Postgres `config_overlay`. See `[[recency-not-value]]` memory.
- **Cockpit_db** is the control plane: `workspaces` registry + (future) conversations / ui_state. Engine reads active workspace from `DATARAUM_WORKSPACE_ID` env var.
- **`drizzle-kit pull`** (NOT `introspect`).

### Cockpit
- Tools are hand-written TS in `packages/cockpit/src/tools/`. No openapi-fetch, no codegen.
- 7 slice-1 tools: `list_sources`, `list_tables`, `add_source_file`, `add_source_recipe`, `look_table`, `why_column`, `teach`.
- **Why agent moves from Python (`mcp/why.py`) to TS** (`tools/why_column.ts`): Drizzle evidence aggregation + Anthropic synthesis in chat handler. Engine no longer hosts agent logic.
- Each tool owns its widget response shape (TableProfile JSON, WhyPanel hybrid markdown+suggestions, traffic-light bands).
- Current cockpit routes (`chat.tsx`, `sources.tsx`, `index.tsx`) are **placeholders**. The real UI is the three-region layout + Stage Navigator from DAT-347 (C1).

### Pipeline phases
- **Semantic phase MUST split** into per-column (LLM-driven column roles, business terms) and per-table (entity types, table-level synthesis). Tracked as [DAT-362](https://real-dataraum.atlassian.net/browse/DAT-362) (E2b). See `[[semantic-phase-split]]` memory.
- Per-column annotations are the surface in-loop teach acts on; per-table synthesis runs over post-teach annotations.
- Don't frame LLM phases as "latency cost" — this is an agentic system.

### Config package extraction
- `packages/engine/config/` extracts to standalone `packages/dataraum-config/`. Engine consumes via env-var / mount path. Cockpit can also consume vertical YAMLs for UX. Independent of all other slice-1 work; ships first.

### MCP folder
- `packages/engine/src/dataraum/mcp/` stays through slice 1 as dead code (teach moves to TS, why moves to TS, server.py loses its only HTTP mount). Whole-folder delete in slice 2 alongside session-lifecycle reimplementation.

### Orchestration framework: Temporal (locked 2026-05-25 via [DAT-360](https://real-dataraum.atlassian.net/browse/DAT-360))
- **Temporal adopted.** Engine becomes a Python Temporal activity worker; cockpit (TS) is the workflow author + Temporal client. Workflows live in new `packages/dataraum-workflows/` package; activity-name catalog mirrored in TS + Python with CI drift check.
- **DBOS disqualified** on a silent-stranding bug: `client.enqueue()` (default cross-lang trigger) writes a workflow the Python worker never picks up, with zero error on either side. CLAUDE.md's "correctness over speed" forbids silent failure modes.
- **Restate dropped** at P1 closure: RPC-style design center; not optimized for multi-minute / multi-hour pipeline phases.
- **Keep-monolith disqualified**: existing `pipeline/scheduler.py` + `runner.py` have three structural absences (no durable execution, no TS workflow author, no retry primitives).
- **3 dev containers added**: Postgres (Temporal-dedicated), Temporal server, Temporal UI.
- **Bun ≥ 1.3.14** enforced (Temporal TS worker segfault on 1.3.0 in shutdown).
- **Existing scheduler retires** in DAT-344 (no parallel-run period, per `[[no-corner-cutting-via-deferral]]`). Estimated ~900 lines deleted → ~150 lines wrappers.
- **Deferred validations** (DAT-344 first commits): workflow-worker crash replay, real `TypingPhase` as activity, multi-workspace isolation strategy.
- Memory `[[durable-execution-lean]]` rewritten to lock Temporal. Pre-pivot DBOS lean explicitly superseded.
- Spike artifact: `spike/dat-360-orchestration/README.md`.

### Hard rules
- No backwards-compat shims.
- No legacy Python wrapped in TS — TS owns teach writes directly via Drizzle (`[[no-corner-cutting-via-deferral]]`).
- Recency does not imply value (`[[recency-not-value]]`).

---

## Phase chain (substrate — Done, historical)

Substrate phases 0a–0f shipped via PR #132. Detail kept for historical reference; do not modify.

- [x] **Phase 0a — Schema-per-workspace substrate**
  - [x] A1: Drop `workspace_id` columns from `Table` + `EntropyObjectRecord`, drop `idx_entropy_workspace`, remove 11 row-stamping call sites, drop `workspace_id` param from `entropy/engine._make_record`. Commit `ffb9b345`. 1350/1350 unit tests green.
  - [x] A2: Delete `Workspace` SQLA model + `workspace_models.py` + storage export. `apply_and_persist` drops `workspace_id` param + Workspace lookup, resolves `config_root` via `_get_config_root()`. `mcp/teach.py` call site updated. `server/workspace.py` refactored to env-var bootstrap (`DATARAUM_HOME` + `DATARAUM_WORKSPACE_ID`); `get_active_workspace_id` is arg-less and returns a module pointer; added `BootstrappedWorkspace` dataclass + `reset_active_workspace_id_for_tests`. `server/app.py` lifespan calls `bootstrap_workspace()` arg-less (dropped `_get_workspace_manager` import). **Note**: this commit left `packages/infra/docker-compose.yml` without `DATARAUM_WORKSPACE_ID` in the control-plane env block — container could not start until that gap was patched as part of Phase 0b. Scope creep approved with user: also deleted the `/api/workspace` route + `get_workspace_service` + `Workspace` Pydantic schema + `tests/unit/api/test_workspace_route.py` (the api/ surface retires in 0c anyway). conftest cleanup: dropped `_TEST_WORKSPACE_ID`, `baseline_workspace_id`, Workspace seeding (unit + integration), and the autofill hook's workspace_id branch. Test rewrites: `tests/unit/server/test_workspace.py` (10 tests, no SQLAlchemy), `tests/platform/smoke_dat_358.py` (filesystem + pointer probes, no /api/workspace). 1348/1348 unit tests + 236/236 integration tests + 9/9 platform smokes green. mypy clean.
  - [x] Commit B: `schema_name_for(workspace_id)` helper in `server/workspace.py` (ws_<uuid-with-underscores>; validates length + identifier rules). `ConnectionManager._init_sqlalchemy` issues `CREATE SCHEMA IF NOT EXISTS "ws_<id>"` then registers an `event.listens_for(engine, "connect")` listener that does `SET search_path TO "ws_<id>", public` per dbapi connect. Dialect-gated — SQLite engines (unit-test fallback) bypass entirely. Pool-checkout reuse inherits search_path from the connection's initial state. Test infrastructure: `tests/conftest.py` sets `DATARAUM_WORKSPACE_ID=test` + module-pointer at import time (mirrors what `bootstrap_workspace()` does at lifespan); `tests/integration/conftest.py` mirrors the listener + schema-create pattern; `pg_url_clean` TRUNCATE qualifies by schema. New tests: `TestSchemaNameFor` (4 tests in test_workspace.py) + `TestSchemaPerWorkspace` (4 tests in test_connections.py: schema exists, search_path set, tables in workspace schema, CREATE idempotent). 1356/1356 unit + 236/236 integration + 9/9 platform smokes green. mypy clean.
- [x] **Phase 0b — Drizzle two-config setup.**
  - `drizzle.config.cockpit.ts` (push/generate, `schema: src/db/cockpit/schema.ts`, `out: drizzle/cockpit`) + `drizzle.config.metadata.ts` (pull-only, `schemaFilter: [ws_<workspace>]`, URL augmented with `?options=-c search_path=<schema>` though it didn't end up affecting drizzle 1.0's identifier-mangling behavior).
  - **Drizzle 1.0 mangles pull output**: emits 33 tables as `xInWs00000000000000000000000000000001` exports + a `ws00000000000000000000000000000001` schema const. No drizzle config flag flattens this. Wrote `scripts/normalize-metadata-pull.mjs` (post-process) to strip the `InWs<id>` suffix from identifiers and rename the schema const to `metadataSchema` — `pgSchema("ws_<id>")` argument stays so emitted SQL still qualifies by the real schema. Bundled as `pnpm db:pull:metadata` (pull + normalize chained). Also deletes the timestamped `<ts>_<slug>/` migration dir drizzle pull writes alongside (pull-only config never pushes, so the SQL + snapshot are dead-on-arrival noise that would re-accumulate).
  - **search_path moot for metadata-client**: drizzle's `pgSchema().table()` emits fully qualified `"ws_<id>"."table"` SQL. No postgres-js `search_path` option needed (deviated from the originally specified plan). `metadata-client.ts` is a plain `postgres-js + drizzle({ schema, relations })`.
  - **Cockpit db layout split**: `src/db/{schema,client}.ts` → `src/db/cockpit/{schema,client}.ts` (hand-written, zero consumers so move was safe) + `src/db/metadata/{schema,relations,client}.ts` (schema + relations generated). Single old `drizzle.config.ts` deleted. `cockpitDb` and `metadataDb` exported separately.
  - **`drizzle/` un-gitignored**: was a scaffold default with no real justification; cockpit_db migrations need history once `workspaces` registry lands in Phase 1+. Surfaced + agreed with user.
  - **Phase 0a aftermath fix**: docker-compose.yml control-plane block was missing `DATARAUM_WORKSPACE_ID` (A2 commit moved bootstrap to env-var, never updated compose). Added `DATARAUM_WORKSPACE_ID: ${DATARAUM_WORKSPACE_ID:-00000000-0000-0000-0000-000000000001}` + documented in `.env.example`. Container couldn't start without this.
  - **Substrate boot was stale**: postgres named volume had pre-pivot lake catalog path + 34 `public`-schema tables. `docker compose down -v` + fresh up. Triggered substrate init manually inside container (`ConnectionConfig.for_workspace()` → `ConnectionManager.initialize()`) — produced 33 SQLAlchemy tables in `ws_00000000_0000_0000_0000_000000000001`.
  - **Substrate-init lifespan gap surfaced (not fixed in 0b)**: `server/app.py:lifespan` calls `bootstrap_lake` + `bootstrap_workspace` but never initializes `ConnectionManager` — workspace schema/tables only materialize when something hits the substrate. Slice-1 unblocks via /api routes in 0c+, but worth treating as a real startup gap. Flagged for ticket / Phase 0c.
  - **Senior-code-review delta**: caught `drizzle(client, { schema })` two-arg call pattern that drizzle 1.0 rejects (`TS2345`, also a silent runtime footgun since `Sql` would destructure as a config). Fixed both clients to `drizzle({ client, ... })`. Also fixed `drizzle.config.{cockpit,metadata}.ts` to throw on missing URL env (was `?? ''` silent), added no-match guard to `normalize-metadata-pull.mjs` (exits 1 + warns if `InWs<id>` remains after normalize — catches "pulled with different workspace" footgun), and added a docs-only `environment:` comment block to the cockpit service so Phase 1+ knows which env vars to wire.
  - Verified: `pnpm exec tsc --noEmit` clean, `pnpm build` clean, `pnpm check` clean on new files (24 pre-existing biome errors in `src/api/`, `src/routes/`, `biome.json` schema version drift untouched).
- [x] **Phase 0c — Starlette + delete FastAPI.**
  - `packages/engine/src/dataraum/server/app.py` rewritten from FastAPI to Starlette. Routes: `GET /health` (unchanged shape), `POST /measure`, `POST /query`, `POST /probe` (501 stubs with `{"detail": "<verb> is not implemented yet (DAT-339 pivot Phase 0c stub)."}` body). CORS middleware preserved (localhost:3000 + 5173).
  - **Substrate eager-init landed** (resolves 0b follow-up): lifespan now calls `bootstrap_lake` → `bootstrap_workspace` → `ConnectionManager(ConnectionConfig.for_workspace()).initialize()` → `app.state.workspace_manager = manager`. Manager closed in `finally:` before `teardown_lake()`. Acceptance verified end-to-end: fresh `docker compose down -v` + `up -d --build --wait` produces `ws_00000000_0000_0000_0000_000000000001` schema with 33 tables; logs show `ducklake_bootstrapped` → `workspace_bootstrapped` → first `GET /health` (substrate ready before first request).
  - **Deletions**: `packages/engine/src/dataraum/api/` (5 files: `__init__.py`, `deps.py`, `routes.py`, `schemas.py`, `services.py`); `packages/engine/scripts/export_openapi.py`; `packages/engine/tests/unit/api/` (test_sources.py + __init__.py); `packages/api/` (whole directory — `openapi.yaml` + `README.md`). The `Workspace` SQLA model note in the original 0c spec was stale (deleted in 0a A2); confirmed no Workspace model exists in the tree.
  - **Dependency churn**: `fastapi==0.136.1` dropped; `starlette>=0.47.0` added explicit; `httpx>=0.27.0` added to dev group (starlette.testclient post-0.27 requires it). `uv lock` resolves 124 packages cleanly.
  - **Test rewrite**: `tests/unit/server/test_app.py` ported to Starlette `TestClient` + new `TestLifespanEagerInit` class (spies `ConnectionManager` to assert `.initialize()` called exactly once at startup + `.close()` exactly once at teardown + `app.state.workspace_manager` is populated). New `TestKernelStubs` parametrized over `/measure`, `/query`, `/probe`. All 10 test_app tests green; 1067 unit tests pass total (40 pre-existing testcontainers/docker-socket errors unrelated to this phase).
  - **Doc sweep**: root `CLAUDE.md` + `README.md` updated (3 packages not 4, FastAPI→Starlette, no OpenAPI/codegen, drizzle-kit pull as the metadata path); `Makefile` `codegen` target deleted; `packages/engine/CLAUDE.md` module-structure box updated + transport language refreshed; `packages/cockpit/CLAUDE.md` flags `src/api/` and `pnpm codegen` as legacy (Phase 0d cleanup).
- [x] **Phase 0d — Cockpit cleanup.**
  - **Deletions**: `packages/cockpit/src/api/{client,types}.ts` (whole directory). `openapi-fetch` + `openapi-typescript` deps removed from `package.json`. `codegen` script removed.  pnpm-lock.yaml regenerated (2 deps gone, 119 packages now).
  - **`src/routes/sources.tsx`**: rewritten as a placeholder Mantine card ("Coming soon — Phase 1 wires this via Drizzle metadata client"). Keeps the `/sources` route alive so `__root.tsx` nav link still resolves.
  - **`src/routes/api/chat.ts`**: dropped `list_sources` tool definition + `runTool` function. The agentic outer loop was kept-then-collapsed because with `tools: []` every body path broke, making `round++` unreachable (biome `noUnreachable` caught it). Replaced with a single-pass text streamer; SSE event shape (`text`, `done`, `error`) is unchanged so the cockpit chat UI doesn't churn. Phase 1+ reintroduces the agentic loop + tool_call_start/tool_result events when real TS tools land.
  - **`src/routes/index.tsx`**: "step 4" stale copy refreshed to describe the Phase-1 read surfaces.
  - **`packages/cockpit/CLAUDE.md`**: dropped the `src/api/` block from the layout diagram; removed the "LEGACY — retires in Phase 0d" annotations now that 0d has happened; cleaned the commands table.
  - Verified: `pnpm exec tsc --noEmit` clean, `pnpm build` clean (output 654 kB router + 377 kB Anthropic SDK, identical to pre-0d), `biome check` clean on changed files (3 of 3 auto-fixed; chat.ts cleanup also resolved a `noUnreachable` finding).
- [x] **Phase 0e+0f — Tool registry scaffold + infra mount + CI swap.**
  - **`packages/cockpit/src/tools/README.md`**: scaffold doc explaining the hand-written N:M policy (one tool wraps N engine ops; N tools share M backends), Anthropic Tool schema convention, no auto-discovery (explicit registry file), `tools/registry.ts` lands in Phase 1 alongside the first batch (`list_sources`, `list_tables`, `look_table`, `search_snippets`). Empty dir otherwise — no real tools yet.
  - **Docker-compose mounts**: cockpit service block gets `dataraum_lake:/var/lib/dataraum/lake` (same path as control-plane, for symmetry) + five env vars: `COCKPIT_DATABASE_URL` (cockpit_db), `METADATA_DATABASE_URL` (engine substrate, read-only via Drizzle), `DATARAUM_WORKSPACE_ID` (propagated from the control-plane), `DATARAUM_LAKE_PATH` (so TS upload code knows the directory), `ANTHROPIC_API_KEY` (forward-compat for Phase 1+ chat tooling, no-op when unset). Phase 2's `add_source` wizard writes user files into this volume from TS; engine reads them via DuckLake at the same path.
  - **`cockpit_db` Postgres database created at first-boot**: `packages/engine/docker/postgres-init/init-databases.sh` now creates both `$DUCKLAKE_CATALOG_DB` and `$COCKPIT_DB`. Postgres env block + `.env.example` + `.env` document `COCKPIT_DB=cockpit_db`. Keeps the cockpit's env vars honest (`COCKPIT_DATABASE_URL` points at something real) even though slice 1 doesn't yet import the cockpit client (no tables until Phase 1+ adds the workspaces registry).
  - **Cockpit Dockerfile fix**: `corepack` was unbundled from Node 25+, so `node:26-alpine` (set by user pre-0c) doesn't ship it. Swapped both stages' `corepack enable && corepack prepare pnpm@latest --activate` → `npm install -g pnpm@latest`. Surfaced when `--build` ran for the first time on this branch.
  - **`pnpm db:pull:metadata` chain extended**: drizzle-kit pull emits non-biome-formatted output (single-line imports including unused symbols). Without biome in the chain, every CI re-pull would show "drift" purely from formatting churn. Added `biome check --write --unsafe src/db/metadata/` as the final step. Re-pull on an unchanged substrate now produces zero diff.
  - **CI extension (`.github/workflows/compose-smoke.yml`)**:
    - "Verify both Postgres databases exist" → "Verify all three" (adds `$COCKPIT_DB`).
    - "Verify dataraum_lake mounted writable" now tests both containers individually + the cross-container handoff (cockpit writes a sentinel file, control-plane reads it).
    - New step: install node 26 + pnpm, `pnpm install --frozen-lockfile`, run `pnpm db:pull:metadata` against the running engine, `git diff --exit-code -- src/db/metadata/`. Drift = engine SQLAlchemy changed without the cockpit refreshing → CI fails with `::error::Drift detected...` message pointing at the fix.
  - Verified end-to-end locally: `docker compose down -v` + `up -d --build --wait` brings all three services healthy; all 3 databases present; lake mount writable + handoff works; `pnpm db:pull:metadata` against the live stack produces zero diff against the committed schema.

---

## Slice-1 feature tickets (live state)

Per the DAT-339 epic decomposition. See [`dat339-slice1-features-plan.md`](./dat339-slice1-features-plan.md) for per-ticket implementation detail.

### Engine

| ID | Ticket | Status | Notes |
|---|---|---|---|
| EW | [DAT-358](https://real-dataraum.atlassian.net/browse/DAT-358) | Done | Filesystem overlay stepping-stone; superseded by Postgres `config_overlay` in DAT-343 |
| E0 | [DAT-340](https://real-dataraum.atlassian.net/browse/DAT-340) | Shipped PR #129; Jira transition pending | |
| E1 | [DAT-341](https://real-dataraum.atlassian.net/browse/DAT-341) | Done | |
| E2 | [DAT-342](https://real-dataraum.atlassian.net/browse/DAT-342) | To Do | Mostly aligned; minor touchup |
| E2b | [DAT-362](https://real-dataraum.atlassian.net/browse/DAT-362) | To Do | Semantic phase split per-column / per-table |
| E3 | [DAT-343](https://real-dataraum.atlassian.net/browse/DAT-343) | To Do — REWRITE PENDING | Filesystem → Postgres `config_overlay` |
| E4 | [DAT-344](https://real-dataraum.atlassian.net/browse/DAT-344) | To Do — rewritten 2026-05-25 post-spike | Temporal worker + activity wrappers + workflow scaffolding + `/run_sql` + `/probe` kernel verbs |
| ~~E5~~ | ~~DAT-345~~ | Folded into E4 | `/measure` IS the SSE verb; reconnect replays current state |

### Cockpit

| ID | Ticket | Status | Notes |
|---|---|---|---|
| C1 | [DAT-347](https://real-dataraum.atlassian.net/browse/DAT-347) | To Do | Three-region layout + Stage Navigator (the real UI; current routes are placeholders) |
| C2 | [DAT-348](https://real-dataraum.atlassian.net/browse/DAT-348) | To Do | AddSourceWizard |
| C3 | [DAT-349](https://real-dataraum.atlassian.net/browse/DAT-349) | To Do | WorkspaceInventory + SourceCard |
| C4 | [DAT-350](https://real-dataraum.atlassian.net/browse/DAT-350) | To Do | TableProfile |
| C5 | [DAT-351](https://real-dataraum.atlassian.net/browse/DAT-351) | To Do — REWRITE PENDING | WhyPanel + TeachProposal + why agent port from Python to TS |
| C6 | [DAT-352](https://real-dataraum.atlassian.net/browse/DAT-352) | To Do | MeasureProgress + chat-as-audit-trail rehydration |

### Chat

| ID | Ticket | Status | Notes |
|---|---|---|---|
| CH1 | [DAT-353](https://real-dataraum.atlassian.net/browse/DAT-353) | To Do — rewritten 2026-05-22 | Drop openapi-fetch; tools call Drizzle + Temporal client + kernel verbs; absorb widget response shapes from DAT-344 |
| CH2 | [DAT-354](https://real-dataraum.atlassian.net/browse/DAT-354) | To Do | Tool-result chip rendering |

### Cross-cutting

| ID | Ticket | Status | Notes |
|---|---|---|---|
| SPIKE | [DAT-360](https://real-dataraum.atlassian.net/browse/DAT-360) | **Done 2026-05-25** | Temporal selected. Spike artifact in `spike/dat-360-orchestration/README.md`. |
| CFG | [DAT-361](https://real-dataraum.atlassian.net/browse/DAT-361) | To Do | Config package extraction (`engine/config/` → `dataraum-config/`); independent of all other slice-1 work, ships first |

---

## Active state

No active phase branch. Each slice-1 ticket lands its own PR off `main`.

Doc rewrite in flight on `chore/dat-339-doc-rewrite`. Ships with: this file, [`dat339-slice1-features-plan.md`](./dat339-slice1-features-plan.md), [`platform-status.md`](./platform-status.md).

---

## Resume protocol

1. Check `git branch --show-current`; if on a feature branch, check the ticket's status row above.
2. Read locked decisions above. Do NOT renegotiate.
3. Read [`dat339-slice1-features-plan.md`](./dat339-slice1-features-plan.md) for per-ticket implementation detail.
4. Skim memory entries: `[[no-corner-cutting-via-deferral]]`, `[[recency-not-value]]`, `[[teach-writes-measure-runs]]`, `[[semantic-phase-split]]`, `[[durable-execution-lean]]` (Temporal locked 2026-05-25), `[[mcp-dead-reference-only]]`.
5. Confluence [DD/23363586](https://real-dataraum.atlassian.net/wiki/spaces/DD/pages/23363586) — kept in lockstep with this doc.
6. `/refine` if reality conflicts with locked decisions; otherwise `/implement` on the next To Do ticket.
