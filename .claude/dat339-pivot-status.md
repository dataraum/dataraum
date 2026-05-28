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

### Engine kernel (revised 2026-05-26 — `run_sql` + `probe` move to cockpit)

> **Supersedes the 2026-05-25 "stay Python" lock.** `/run_sql` + `/probe` move from the Python engine to the cockpit (TS/Bun + `@duckdb/node-api`). Rationale + the accepted Bun #13910 risk are in the **2026-05-26 decision entry** below.

- **Engine = pure Temporal activity worker — no HTTP server.** With `run_sql`/`probe` in the cockpit and `measure` retired, the engine exposes **no HTTP kernel at all** (the Starlette shell goes away). Health is the [Temporal worker health](https://docs.temporal.io/cloud/worker-health) surface, not an HTTP `/health`. Two consequences for DAT-344: (a) substrate bootstrap moves from the (removed) Starlette lifespan into worker startup; (b) the engine container's compose healthcheck changes — no more `curl :8000/health`.
- **Dual DuckDB.** The cockpit owns a DuckDB connection (via `@duckdb/node-api`) for the interactive read verbs (`run_sql`, `probe`); the engine keeps DuckDB for the Temporal pipeline activities (import / typing / statistics / semantic_*) that operate on the lake. Two owners of the same lake + DuckLake catalog — **cross-process read consistency needs verification** (open question).
- **`/probe` credential resolution moves to TS.** Per-source DB URLs (`DATARAUM_<NAME>_URL`, today resolved by the engine's `CredentialChain`) now resolve in the cockpit, since `/probe` runs there. This reshapes DAT-363's cockpit scope.
- **`/measure` retired** — replaced by Temporal workflow start from cockpit (`client.workflow.start(addSourceWorkflow, ...)`).  DAT-345 (separate SSE job_id surface) closed; folded into DAT-344.
- **Known accepted risk — Bun [#13910](https://github.com/oven-sh/bun/issues/13910)** (open, ~30% segfault rate for `@duckdb/node-api` under Bun). The DAT-360 P5 probe passed 30/30 + 10/10 — but only on **macOS arm64, NOT the Linux x64 production platform**. A Linux-x64 reproduction probe **gates the migration before ship** (new task — see 2026-05-26 entry).
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
- Spike artifact: `README.md` + `duckdb-bun-probe/` live on branch `spike/dat-360-orchestration-spike` (committed there; not on `main`). The `spike/` dir in working trees holds only untracked build artifacts.

### Decision revised 2026-05-26 — `run_sql` + `probe` move to the cockpit

**Reverses the 2026-05-25 "stay Python / one-owner-per-substrate" lock** (the prior rationale is kept above for history; it no longer governs).

- **What:** `/run_sql` + `/probe` are implemented in the **cockpit** (TS/Bun, `@duckdb/node-api`), not the Python engine. The engine becomes a **pure Temporal activity worker with no HTTP server** (no `/health` either — health via [Temporal worker health](https://docs.temporal.io/cloud/worker-health)). Substrate bootstrap moves into worker startup; the engine container's compose healthcheck changes.
- **DuckDB ownership = dual.** Cockpit DuckDB for interactive read verbs; engine DuckDB for Temporal pipeline activities. Same lake + DuckLake catalog, two processes — cross-process read consistency is an **open question** to validate.
- **Credential resolution for `/probe` moves to TS.** The per-source `DATARAUM_<NAME>_URL` lookup (engine `CredentialChain`) is re-homed in the cockpit. **This expands DAT-363's cockpit config scope** — the Zod config / a TS credential resolver must handle the dynamic per-source URLs (no longer "engine-only, descope" as the DAT-363 refine assumed).
- **Accepted risk — Bun [#13910](https://github.com/oven-sh/bun/issues/13910)** (open; ~30% segfault rate for `@duckdb/node-api` under Bun). DAT-360 P5 probe: 30/30 + 10/10 PASS, but **macOS arm64 only** — production is **Linux x64**, untested. Risk accepted *conditional on* a Linux-x64 reproduction probe (new task) that **gates the migration before ship**.
- **Follow-on tickets to file:** (1) `run_sql` + `probe` as cockpit-owned TS DuckDB + TS probe-credential resolution; (2) Linux-x64 #13910 validation probe (blocks #1).
- **DAT-344 re-scoped:** Temporal worker + activity wrappers + workflow scaffolding; engine = pure worker, no HTTP (health via Temporal worker health; substrate bootstrap into worker startup). `run_sql`/`probe` removed from its scope.

### Hard rules
- No backwards-compat shims.
- No legacy Python wrapped in TS — TS owns teach writes directly via Drizzle (`[[no-corner-cutting-via-deferral]]`).
- Recency does not imply value (`[[recency-not-value]]`).

---

## Phase chain (substrate) — Done

Archived in [archive/dat339-substrate-phasechain.md](./archive/dat339-substrate-phasechain.md).

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
| E3 | [DAT-343](https://real-dataraum.atlassian.net/browse/DAT-343) | In Review | Filesystem → Postgres `config_overlay` + ReplayScope workflow surgery + per-phase replay_cleanup + cockpit teach/replay tools. **One follow-up gates user testing:** `semantic_per_column`'s `_adhoc` ontology induction still writes to the bind-mounted (read-only) baked-in config; tracked as **E3b** below. |
| **E3b** | [DAT-371](https://real-dataraum.atlassian.net/browse/DAT-371) | To Do — **blocks DAT-339 user testing** | Move `_adhoc` ontology induction off filesystem writes — induced concepts become `concept` overlay rows via a new per-type applier in `dataraum.core.overlay`. Matches `project_frame_stage_ontology` direction; ~150 LOC + 1 applier + integration smoke unblock. Use the smoke in `packages/cockpit/src/temporal/drive-add-source.ts` (currently uses `vertical: "finance"` to bypass) as the regression gate — flip it back to `_adhoc` when E3b lands. |
| E4 | [DAT-344](https://real-dataraum.atlassian.net/browse/DAT-344) | To Do — re-scoped 2026-05-26 | Temporal worker + activity wrappers + workflow scaffolding. Engine = **pure worker, no HTTP** (health via Temporal worker health; substrate bootstrap moves into worker startup). **`/run_sql` + `/probe` removed from E4** — they move to the cockpit (TS/Bun DuckDB). See 2026-05-26 decision. |
| RS/PR | NEW (file) | To Do | `run_sql` + `probe` as cockpit-owned TS DuckDB (`@duckdb/node-api`) + TS credential resolution for probe. Gated by the #13910 Linux-x64 validation task. |
| 13910 | NEW (file) | To Do | Linux-x64 reproduction probe of Bun [#13910](https://github.com/oven-sh/bun/issues/13910) — **blocks** the run_sql/probe-to-cockpit migration. |
| ~~E5~~ | ~~DAT-345~~ | Folded into E4 | `/measure` IS the SSE verb; reconnect replays current state |

### Cockpit

| ID | Ticket | Status | Notes |
|---|---|---|---|
| C1 | [DAT-347](https://real-dataraum.atlassian.net/browse/DAT-347) | To Do | Three-region layout + Stage Navigator (the real UI; current routes are placeholders) |
| C2 | [DAT-348](https://real-dataraum.atlassian.net/browse/DAT-348) | To Do | AddSourceWizard |
| C3 | [DAT-349](https://real-dataraum.atlassian.net/browse/DAT-349) | To Do | WorkspaceInventory + SourceCard |
| C4 | [DAT-350](https://real-dataraum.atlassian.net/browse/DAT-350) | To Do | TableProfile — **surface pending teach overlays in the response** (DAT-343 left `src/db/metadata/pending-overlays.ts`'s `getPendingOverlays(workspaceId)` ready; the chip should read "N teaches pending — replay before trusting") |
| C5 | [DAT-351](https://real-dataraum.atlassian.net/browse/DAT-351) | To Do — REWRITE PENDING | WhyPanel + TeachProposal + why agent port from Python to TS — **same pending-teach hint as C4** (helper ready; the why-panel UX is the cleanest place to nudge the agent toward `replay`) |
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
| CFG | [DAT-361](https://real-dataraum.atlassian.net/browse/DAT-361) | To Do | Config package extraction (`engine/config/` → `dataraum-config/`); independent, ships first |
| CONF | [DAT-363](https://real-dataraum.atlassian.net/browse/DAT-363) | To Do | Typed config modules (Pydantic Settings + Zod) + Temporal env additions; ships before DAT-344's first commit |
| ISO | [DAT-364](https://real-dataraum.atlassian.net/browse/DAT-364) | To Do | Isolation cornerstones (workflow ID convention, activity workspace_id, non-default-UUID test); inside DAT-344 review gate |
| ACT | [DAT-365](https://real-dataraum.atlassian.net/browse/DAT-365) | To Do | `actor_id` seam for slice 2+ identity; folds into DAT-344 / DAT-343 / DAT-353 PRs |

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
