# Cockpit UI brilliance: user-facing SQL + data-grid surfaces (Connect → Analyse)

> Ideated 2026-06-18. Companion to closed **DAT-385** (streaming result grid — shipped P1–P3; this picks up the type-formatting remainder it split out). Neighbours, not overlaps: DAT-489 (stage-gate `answer`), DAT-490 (uncapped answer grid), DAT-494 (lift exact_reuse SQL to the prompt/consumer). Distinct from DAT-281 (engine-side date *type* detection — same words, different layer).

## Problem

The cockpit never shows the user the SQL or the data layer it runs on. Today:

- **The SQL is invisible.** Every grid the user sees was produced by a query — `run_sql`, the composed `answer` CTE, or a `probe` against an external DB — and none of those queries are ever surfaced. A practitioner who wants to know "what did you actually run?" has no answer. The data is *right there* in the client (`result-grid` canvas state carries `sql`/`params`; `answer-result` carries the composed `grid.sql`; the `answer` tool computes per-concept `components[]` with reuse tags) — it's simply not rendered.
- **There is no user-facing way to query a source.** `probe` runs read-only SQL against an external DB (MS SQL included — wired today: ATTACH `MSSQL`, community extension pre-baked) but is **agent-only**. A user cannot point at a configured database, write SQL, and see results. The "write SQL (or have the agent generate it) for source-connect" capability does not exist.
- **Grids render raw values.** `formatCell` (`result-grid.tsx:50`) is three lines — `null → —`, `object → JSON`, else `String(value)`. Dates show as raw ISO, numbers as unformatted left-aligned strings, no thousands separators, no numeric right-align. The already-plumbed `ColumnMeta.duckdbType` (`result-grid.tsx:41`) is unused. This is the #1 "looks unfinished" tell on the most-used data surface.

These are not one feature. They are **three distinct SQL surfaces** plus a cross-cutting grid-quality thread.

## Design

### The three surfaces

| # | Surface | Phase | What the user does | Representation |
|---|---------|-------|--------------------|----------------|
| 1 | **Probe** | Connect | Picks a configured DB source, writes/edits/generates read-only SQL, runs it before ingest | Editable SQL editor → result grid |
| 2 | **run_sql grid SQL** | Analyse | Sees the literal SQL behind a grid | Read-only SQL disclosure on the grid |
| 3 | **Answer provenance** | Analyse | Sees how a composed answer was built from concepts | DAG or stacked view (TBD) |

Cross-cutting: **grid type-formatting** (every grid) and a **per-surface polish pass** (the Playwright-loop punch-list).

The literal-SQL viewer (surface 1's editor in read-only mode, surface 2's disclosure) is **one shared component**. The answer-provenance viewer (surface 3) is **a different component** — it renders structure (concepts → final_sql), not a single statement. Conflating them was the original mistake.

---

### Surface 1 — Probe (Connect). The keystone; where we start.

**Locked decisions:** env-configured sources (no in-UI credential entry in this phase); SQL **editable from the start**; capped (non-streaming) result for v1.

**What exists already**
- `probe()` (`duckdb/probe.ts`) — READ_ONLY ATTACH, `LIMIT`-wrapped, throwaway in-memory DuckDB connection, MS SQL supported (`INSTALL mssql FROM community`, image pre-baked). Fails loud on bad backend / missing credential / failed ATTACH.
- `resolveCredential()` — `DATARAUM_<NAME>_URL` env, never serialized. Single-provider chain (a secrets-manager provider can slot in later untouched).
- `list_sources` — already enumerates configured DB sources as `(name, backend)` rows (URL never exposed). The UI can list the MS SQL source today.
- `connect()` / `schema-preview` widget — schema + sample peek, so the user can see table/column names to write SQL against.

**What's new**
1. **Source picker** — a Connect-surface widget listing configured DB sources from `list_sources` (kind=database); selecting one shows its schema (reuse `connect`/schema-preview).
2. **SQL editor** — CodeMirror (new dep) with a SQL language mode. Client-only — **must be SSR-guarded** (TanStack Start renders on the server; cf. the DOMPurify SSR gotcha). Read-only *safety* is already enforced one layer down: the ATTACH is `READ_ONLY` and the result is `LIMIT`-wrapped, so even arbitrary user SQL is contained. Seeded by the agent or hand-written.
3. **`/api/probe-sql`** — a server route (or server function) taking `{ source_name, backend, sql }`, calling `probe()` directly, returning the capped grid result. Reuses the existing isolation + credential path. (Streaming `/api/probe-sql` mirroring `/api/run-sql` is a documented follow-on — probe is sniff/sample, capped is right for v1.)
4. **Result grid** — reuse `ResultGridView` (the pure render half of `result-grid.tsx`) fed by the capped probe result, with the new type-formatting applied.
5. **Agent-generate affordance** — "ask the agent to write this" drops the agent's `probe` SQL into the editor for the user to edit + run. (The agent already calls `probe`; this routes its SQL to the editor instead of only the chat.)

**Concrete flow:** user opens Connect → sees `northwind_mssql` (MS SQL) in the source list → clicks it → schema renders → types `SELECT TOP 100 * FROM Orders WHERE OrderDate > '2024-01-01'` (or asks the agent) → Run → formatted grid. No data ingested; pure read.

---

### Surface 2 — run_sql grid SQL disclosure (Analyse)

A read-only "SQL" disclosure on the result-grid header, rendering `state.sql` (+ `params`). Precedent component: `metric-why.tsx:62` already renders SQL via Mantine `<Code block>` — promote that to a shared, optionally syntax-highlighted viewer reused by surfaces 1 (read-only mode) and 2. Because `answer-result` composes `ResultGridWidget`, the disclosure covers `run_sql` *and* `answer` grids' final SQL in one change.

### Surface 3 — Answer provenance (Analyse). Representation TBD.

The `answer` tool computes `components[]` — each `{ concept, sql, snippet_id, usage: exact_reuse | adapted | fresh }` — forming a DAG (concept CTEs → composed `final_sql`). Today only the *counts* reach the canvas (`AnswerConfidence.reuse`); the per-concept SQL does not. This surface:
1. **Carries `components[]` onto the `answer-result` canvas state** (data plumbing — required either way).
2. **Renders provenance.** Representation decided when the loop reaches this surface (see Open Questions): an **xyflow DAG** (concepts → final_sql, click a node for its SQL + reuse tag — richest, adds the app's first graph dep) vs a **stacked code view** (components listed with reuse badges, final_sql at the bottom — no new dep, ships fast, graph as an upgrade).

---

### Cross-cutting A — Grid type-formatting (foundation)

Make `formatCell` type-driven off the already-plumbed `duckdbType` (read at render via `cell.column.columnDef.meta.duckdbType`):
- **Numbers** — right-aligned, locale thousands separators, sane decimal handling.
- **Dates / timestamps** — readable locale format instead of raw ISO.
- **Booleans / null** — consistent styling (null already `—`).
- **Good locale defaults**, not user-configurable (explicitly out of scope — avoids gold-plating before we know what's wanted).

Foundational: lands first/with probe so probe, `run_sql`, and answer grids all benefit immediately.

### Cross-cutting B — Per-surface polish checklist (Playwright-loop punch-list)

Seeded from the widget audit; extended live as we drive each surface. Order: **Connect → Stage → Analyse** (per the user — start at probe).

- **Connect** — `source-list`, `schema-preview` (no loading/error state today), `upload-area`, `workspace-inventory` (capped at 100, not virtualized). Probe surface lands here.
- **Stage** — `measure/session/operating-model-progress` (shared `workflow-progress` core; already mature — verify phase-label consistency + error affordances).
- **Analyse** — `chat-rail`, `focus-canvas`, `result-grid` (surfaces 2 + formatting), `answer-result` (surface 3), the readiness/why family (most mature — hold the line).

### What explicitly does NOT change

- The engine. All three surfaces are cockpit-only (read-only SQL, existing tools/endpoints).
- The agent `run_sql`/`probe` tool contracts (in-context sample shape) — untouched; the human surfaces sit beside them.
- Credentials stay env-resolved + never serialized; no in-UI connection entry this epic.
- No editable SQL in Analyse (surfaces 2/3 read-only); editability is a probe-only capability for now.

## Open Questions

- **Answer-provenance representation (surface 3)** — xyflow DAG vs stacked code view. *Matters:* xyflow is a new dependency and the edge-derivation (parse CTE refs vs star fan-in from concepts → final_sql) is non-trivial. *Resolved by:* deciding when the hands-on loop reaches Analyse, with `components[]` already on the canvas to prototype against.
- **CodeMirror under TanStack Start SSR** — client-only module; needs the `typeof window` guard pattern and a real-browser smoke (the happy-dom/jsdom traps bit DOMPurify). *Resolved by:* a thin spike when building the probe editor.
- **MS SQL connectivity in dev/smoke** — the probe path needs a reachable MS SQL DB + `DATARAUM_<NAME>_URL`. *Resolved by:* the user's test DB; confirm network reachability from the cockpit/dev process and that the community `mssql` extension installs (host-dev installs on demand into `~/.duckdb`).
- **Probe result scale** — capped v1 assumed sufficient. *Resolved by:* the loop — if browsing large external results matters, promote to a streaming `/api/probe-sql`.

## Alternatives Considered

- **One shared "SQL viewer" for all three surfaces** — rejected. Probe (editable, against an external ATTACH) and answer-provenance (a DAG of components) are different interactions; only the literal-SQL *display* (surfaces 1-read-only / 2) is genuinely shared.
- **Server-side pre-formatting of cells** — rejected. The server already coerces to JSON-safe (bigint→string, dates→ISO); the *display* formatting needs the column type, which lives client-side in `duckdbType`. Client-side type-driven formatting is the right seam.
- **In-UI connection entry now** — deferred. Storing customer DB credentials is security-sensitive and deserves its own design; env-config unblocks the whole probe surface today.
- **Minimal final_sql-only viewer as v1** — rejected by the PO ("why do a throwaway version?"). The read-only viewer ships with real provenance (surface 3 carries `components[]`); only *editing in Analyse* is deferred, as a genuinely additive future capability.

## Suggested phasing (for `/decompose`)

1. **P1 — Grid type-formatting** (foundation, small): `duckdbType`-driven `formatCell`, locale dates/numbers, numeric right-align. Every grid benefits.
2. **P2 — Probe surface** (keystone, start here): source picker + schema + CodeMirror editor (SSR-guarded, read-only-safe) + `/api/probe-sql` (capped) + formatted grid + agent-generate. Hands-on loop begins with an MS SQL source.
3. **P3 — run_sql grid SQL disclosure**: shared literal-SQL viewer on the result-grid header (covers run_sql + answer final SQL).
4. **P4 — Answer provenance**: carry `components[]` onto canvas + the provenance viewer (representation chosen at this surface).
5. **P5 — Per-surface polish pass**: the punch-list, driven continuously by the Playwright loop.
6. **Deferred follow-ons**: in-UI connection entry; streaming `/api/probe-sql`; editable Analyse SQL; xyflow upgrade if not chosen in P4.
