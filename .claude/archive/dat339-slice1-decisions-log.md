# DAT-339 Slice 1 — cross-session decisions log (archived)

Continuity log of decisions made across /refine + spike sessions, split out of `dat339-slice1-features-plan.md` (2026-05-26). The plan itself stays live; this is the why-history.

---

## Decisions made 2026-05-22 + 2026-05-25 (cross-session continuity)

### 2026-05-22 (`/refine` session)

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
11. Spike DBOS vs Temporal vs Restate, tight scope (typing phase + import→typing workflow), 2 days max.
12. The current cockpit UI (`chat.tsx`, `sources.tsx`, `index.tsx`) is throwaway — DAT-347 (C1) builds the real UI.

### 2026-05-25 (DAT-360 spike closure)

13. **Temporal adopted** as the orchestration framework. DBOS disqualified on cross-lang silent-stranding (`client.enqueue()` writes a workflow the Python worker never picks up; zero error on either side). Restate dropped at P1 closure (RPC-style design not suited for multi-minute pipeline phases). Keep-monolith disqualified on three structural absences (no durable execution, no TS workflow author, no retry primitives). `[[durable-execution-lean]]` rewritten to lock Temporal.
14. **Engine becomes a Python Temporal activity worker.** Cockpit (TS) is the workflow author + Temporal client. Existing `pipeline/scheduler.py` + `runner.py` retire (per `[[no-corner-cutting-via-deferral]]`; no parallel-run period).
15. ~~**`/run_sql` + `/probe` stay Python.**~~ **SUPERSEDED 2026-05-26 (see entry 19).** (Original: P5 — `@duckdb/node-api` works in Bun on macOS but #13910 is a production risk; one-owner-per-substrate kept DuckDB in Python.)
16. **`/measure` retired.** No HTTP verb for orchestration; cockpit calls `client.workflow.start(addSourceWorkflow, ...)` instead.
17. **3 dev containers added**: Postgres (Temporal-dedicated), Temporal server, Temporal UI. Bun ≥ 1.3.14 enforced (Temporal TS worker segfault on 1.3.0 in shutdown).
18. **Deferred validations** become DAT-344 first commits: workflow-worker crash replay, real `TypingPhase` as activity, multi-workspace isolation strategy.

### 2026-05-26 (`run_sql` + `probe` → cockpit)

19. **`/run_sql` + `/probe` move from the Python engine to the cockpit** (TS/Bun + `@duckdb/node-api`). Reverses entry 15. **Dual DuckDB**: engine keeps DuckDB for Temporal pipeline activities; cockpit owns DuckDB for the interactive read verbs. **Engine becomes a pure Temporal worker with no HTTP server** — no `/health` route; health via [Temporal worker health](https://docs.temporal.io/cloud/worker-health); substrate bootstrap moves into worker startup; the engine's compose healthcheck changes.
20. **`/probe` credential resolution moves to TS.** Per-source `DATARAUM_<NAME>_URL` lookup re-homes from engine `CredentialChain` to the cockpit. **Expands DAT-363's cockpit scope** — the TS config must handle dynamic per-source URLs (the DAT-363 refine's "descope database_urls, engine-only" assumption no longer holds).
21. **Bun #13910 accepted as a gated risk.** P5 passed on macOS arm64 only; a **Linux-x64 reproduction probe gates the migration before ship**. If it reproduces, the cockpit-owned-DuckDB decision still holds — Bun is one host. Mitigation options are open: Node instead of Bun; await the upstream Bun fix ([#6139](https://github.com/oven-sh/bun/issues/6139)); a Rust-tokio/Go DuckDB service exposing run_sql/probe (Arrow streaming); or Python-engine fallback. Not a single prescribed revert.
22. **DAT-344 re-scoped** to Temporal worker + activities + workflow scaffolding; engine = pure worker, no HTTP server (health via Temporal worker health; substrate bootstrap into worker startup). `run_sql`/`probe` removed. Two new tickets to file: cockpit run_sql/probe + TS probe-credentials; Linux-x64 #13910 validation probe (blocks the first).
