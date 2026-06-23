# DAT-594 — Frame from the staging set: a unified Connect surface (files + SQL) → frame → import

> Ideated 2026-06-20. Child of epic **DAT-574** (Cockpit UI brilliance). Fast-follow to **DAT-592** (import a probed query as a source) and its follow-up **PR #342** (credential_source decoupling + the frame-before-import gate). Cockpit-only — **no engine change**. Couples with **DAT-596** (re-import-with-replace) on the re-frame path. Explicitly *not* multi-vertical (that is a separate, bigger ideate — see Alternatives).

## Problem

The DAT-592 follow-up made the import set work, but left two seams the user has to cross by hand:

- **The frame↔import chicken-egg is gated, not closed.** `semantic_per_column` fails loud ("Run frame before add_source") if the active vertical has no concepts, so PR #342 *disables* "Add to import set" until the workspace is framed (`getActiveVerticalStatus().framed`). But the user still has to **leave the probe surface** and frame (or `use_vertical`) over in the connect chat, then come back. The schema needed to frame is *already in hand* — the read-only ATTACH and the file readers both yield a schema before any import — but nothing wires that productive path.

- **Probe and upload are two disconnected surfaces.** A user picks a configured DB and writes SQL in the probe widget (`widgets/probe.tsx`); to add a *file* they go to a separate upload widget (`widgets/upload-area.tsx`) driven by the agent `upload` tool. There is no single place to assemble "the set of things I'm importing" when that set mixes files and SQL queries.

The result: assembling a real import (a couple of CSVs + a probed JOIN, under a business model) means bouncing across three surfaces and the chat, in a specific order dictated by an engine gate.

## Design

Evolve the probe widget into a **staging hub**: one surface where the user assembles a heterogeneous import set (uploaded files **and** SQL queries), declares the business model (frame a new vertical **or** adopt a builtin), and starts the engine workflow — with the gate moved from "can't *add*" to "can't *start* until ready".

### The surface: a small toolbar + a primary action

```
┌─ Staging ─────────────────────────────────────────────┐
│  [📤 Upload]   [🎯 Frame / Vertical]      [🛒 Set · N]  │   ← toolbar
│                                                         │
│  source picker ▸ DATARAUM_WWI_URL (mssql)               │
│  ┌─ SQL editor ───────────────────────────────────┐    │
│  │ SELECT … FROM Sales.Orders WHERE …              │    │
│  └─────────────────────────────────────────────────┘   │
│  [ Run ]   [ Add to set ]                               │
│  ── result grid ──                                      │
│                                                         │
│                      [ ▶ Start import (N sources) ]     │   ← gated
└─────────────────────────────────────────────────────────┘
```

| Affordance | Reuses | Behaviour |
|---|---|---|
| **📤 Upload** | `upload-dropzone.tsx` directly (not the agent `upload` tool) | Opens a modal with the existing dropzone → `POST /api/upload` → `s3://` handle. Sniff schema via DuckDB. Adds a **file spec** to the set. |
| **🎯 Frame / Vertical** | `frame` + `use_vertical` tools | Opens a modal: **(a)** adopt a builtin vertical (`use_vertical`), or **(b)** frame a new one — induced from the **assembled set's schemas**, curated in the ModelFrame widget. |
| **🛒 Set · N** | existing import-set modal (DAT-592) | Lists staged **files + SQL queries**, each removable. The cart/count symbol from #342. |
| **▶ Start import** | `importSources` → batched `addSourceWorkflow` | Persists the whole set and triggers **one** run. **Gated** (see below). |

### The set becomes heterogeneous

Today the set is `ImportSpec[]` — DB recipes only (`{source_name, credential_source, backend, sql}`). It generalises to a tagged union:

```ts
type StagedItem =
  | { kind: "query"; source_name: string; credential_source: string; backend: string; sql: string }
  | { kind: "file";  file_uri: string; filename: string; source_type: "csv" | "parquet" | "json" }
```

- **Query items** are unchanged from DAT-592 (one query = one `db_recipe` source).
- **File items** are the content-keyed upload path (DAT-422): `src_<digest>` sources, `connection_config.file_uris`. Naming stays digest-based (existing behaviour) — the set lists them by filename.

### Frame seeds from the set's *output shape*

Frame's input is the **union of the staged items' schemas** — the assembled set's shape, not the raw source tables. This makes "a subset / a denormalised JOIN is a different business model" literal: you frame against what you're actually importing.

| Item kind | Schema extraction (DuckDB, in cockpit) |
|---|---|
| **query** | `DESCRIBE (<sql>)` over the probe ATTACH → result columns + types |
| **file** | `DESCRIBE SELECT * FROM read_csv / read_parquet / read_json_auto('s3://…')` — one idiom across all three formats |

Both feed one synthetic `ConnectSchema` → the **existing** `frame` tool. `frame` accepts **any** `ConnectSchema`-shaped object (`frame.ts:387` — no hidden tie to the `connect` tool result), so an assembled schema is fine. **Include sample values** (`sampleValues` per column): we already ran the query (probe) / can `SELECT * LIMIT n` the file, so populate samples — empty is structurally valid but degrades induction quality.

**Verified seams (refine):**
- **`DESCRIBE` needs a new helper.** `probe()` wraps SQL as `SELECT * FROM (<sql>) AS _probe LIMIT n` — you cannot `DESCRIBE` that. But `openProbeConnection()` (`duckdb/probe.ts`) exposes a raw conn and `DESCRIBE SELECT * FROM (<sql>)` runs on it directly — the **file path already does exactly this** (`duckdb/connect.ts:411`). Add a small `probeDescribe()` (+ extract the file-sniff for standalone server-side use).
- **`frame` writes immediately — there is no propose/hold/commit beat.** `frame` is an *acting* tool: it persists overlay rows the moment it's called (`frame-family.ts:100` via `teach()`); ModelFrame is a read-only review of *already-written* results; "edit" = re-invoke `frame` = re-write (append-only/versioned). **This is acceptable for 594:** frame commits the *ontology* immediately while Start gates the *import*; re-framing before Start cheaply overwrites overlay rows, and a framed-but-not-imported workspace is legitimate. A true propose→review→commit frame is filed as **DAT-598** (out of scope here).
- **`frame` and `use_vertical` are agent-only.** Both are `toolDefinition().server()` (agent loop); the underlying `frame(input)` / `useVertical(name)` functions are exported. The modal needs **thin server-fn wrappers** to call them from the UI (no agent round-trip).

Schema extraction needs **no persistence** — the query already ran in the probe; the file is already staged in S3 — so framing stays a pre-Start step. If the set is empty when the frame modal opens, it offers **`use_vertical` only** (nothing to frame against yet).

### The gate moves: Add is free, Start is gated

PR #342 gated *Add-to-set* on `framed`. This **replaces** that: staging is always free; the gate moves to **Start**.

```ts
const canStart = stagedItems.length > 0 && activeVerticalStatus.framed;
//                └─ (a) have sources        └─ (b) a vertical WITH CONCEPTS
```

Three precise points:
- **(b) is `framed`, not "a vertical name is set".** `_adhoc` has a name and zero concepts. Reuse `getActiveVerticalStatus().framed` (the concept-count check, the *same* count the engine checks, `server/active-vertical.ts`) — so both `use_vertical` (builtin) and `frame` (new) clear it; `_adhoc`/nothing does not.
- Disabled Start shows *why* ("add a source" / "frame or pick a vertical first").
- **Stale-cache gotcha (verified).** `getActiveVerticalStatus` is cached 5 min and there is **zero `invalidateQueries` anywhere in the cockpit today** — no precedent to copy. After framing in the modal, the Start gate would read stale `framed=false` and stay locked. The new modal path is a *direct UI action*, so we control the call site: **invalidate `["active-vertical-status"]` on the frame/use_vertical modal's success callback** so the gate flips immediately.

### Start: fan out by kind, then one workflow

The one real integration seam — **verified** (refine, 2026-06-20). The engine workflow loop is already **source-type-agnostic**: it iterates `payload.sources` (just IDs) and the `import` activity dispatches per-row off `Source.source_type` (`worker/workflows.py:285`, `pipeline/phases/import_phase.py:165`). So a mixed set in one run Just Works at the engine — no engine change.

Start persists the heterogeneous set by **fanning out per kind**, collects all source-ids, then triggers a **single** batched run:

```
StagedItem[]
  ├─ query items → persistRecipeSources()    (select/recipe-source.ts)  → source_ids
  └─ file  items → persistFileSources()  [NEW] → source_ids
                                   │
                                   ▼
            triggerAddSource(union of source_ids)  → ONE addSourceWorkflow run
```

**The catch (verified):** file persistence is currently **trapped inside the agent `select` tool** (`tools/select.ts:207` — `upsertSource({ sourceType: "csv|parquet|json", connectionConfig: { file_uris: [uri] } })`), not in a UI-callable seam. `importSources` (`server/import-sources.ts`) today calls **only** `persistRecipeSources`. So the work is:

1. **Factor file persistence out of `select.ts`** into a shared `persistFileSources` (mirroring `persistRecipeSources`), reusing the same `upsertSource` (`select/source-write.ts`) — both already write to the same `ws_<id>.sources` table.
2. **Widen `importSources`** (or a new unified `startImport` server fn) to persist **both** kinds, union the source-ids, and `triggerAddSource(union)` — preserving the existing `runWithConversation(conversationId, …)` threading so the completion-watcher tracks the run.

The engine then sees an ordinary batched `add_source` run over a set of source rows — exactly what it handles now. **Mixing files + queries in one set/run is allowed** (no mode-toggle): a few CSVs + a probed JOIN under one business model is the use case.

### What explicitly does NOT change

- **Engine: nothing.** No new source_type, no import_phase branch, no contract/codegen change. We deliberately **skip SQL-recipes-over-files** (DuckDB *can* `SELECT … FROM read_csv(…)`, but the engine has no `file_recipe` path; adding one is an engine bet for another day — see Alternatives).
- **The `frame` and `use_vertical` tools** — used as-is; we only change *where* they're invoked from and *what schema* seeds frame.
- **`addSourceWorkflow`** — still one batched run over a set of source-ids.
- **File content-keying (DAT-422)** and **db_recipe `credential_source` (DAT-592)** — untouched.

## Decisions (resolved in ideate) & Open Questions

**Decided:**

- **Persistence atomicity — no machinery needed; rely on idempotence.** File upsert is idempotent on digest (DAT-422), recipe persist on `recipe_hash` (DAT-592). A partial persist + re-click Start **converges** — no duplicates, no transaction. Two error surfaces, both with an existing home: a **persist** failure (pre-workflow source-row write) shows **inline on the staging surface** (the #342 import-set already does this); a **workflow** per-source failure shows in the **run progress UI** (`MeasureProgressWidget`). No validate-all-upfront ceremony required.
- **Frame lives in the staging UI, not the chat.** The staging hub has the set's schemas in-memory, so the toolbar frame/`use_vertical` modal is the **primary** path. The connect-chat `frame`/`use_vertical` tools become redundant agent-*assist* (they write the same `workspaces.vertical` + overlay rows; last-write-wins is acceptable). The staging hub is built as a **complete direct-manipulation surface** — Start (server fn), probe (direct fetch), upload (direct), frame (modal) all work with **zero chat turns**; the only agent dependency anywhere is the optional "generate SQL for me." (This also de-risks a future chat-less Connect for free — no pre-abstraction.)
- **File naming — show the original filename, key on digest.** The upload route already stages `s3://…/<digest>/<filename>`, so the filename is in hand. The cart lists files by original name; the internal source stays content-keyed `src_<digest>`. Two same-name files → different digests → two sources, no collision.

**Open:**

- **Re-frame ⇒ full workspace re-run (current architecture).** Concepts feed per-source `semantic_per_column` grounding; re-framing makes every imported source's grounding stale, so re-grounding means a new `begin_session`/import run. The run_id-snapshot model makes that *clean* (runs coexist) but it is still a re-run, not an incremental patch. **Out of scope for 594** — keep the re-frame/replace blast radius in **DAT-596**.
- **Re-run is blocked on DAT-596.** "Re-run the whole set" (or just the failed ones) after a *partial* import hits DAT-596's replace guard — any source that imported-then-failed left raw tables and can't be re-imported under the same name. So **594 ships the clean first-import flow; the re-run story is gated on DAT-596** (critical-path *with* 594, not after).
- **Chat-less Connect — separate ideate.** "Should Connect be a conversational surface at all?" is a real strategic call (Connect is one of the three visual chats from the Cockpit Autonomy epic, DAT-526). Not resolved here; 594's standalone hub keeps the option open without betting on it.

## Alternatives Considered

- **SQL import recipes over files (`file_recipe`).** Powerful — filter/reshape a CSV before import, symmetric with `db_recipe` — and DuckDB supports it natively. **Rejected for 594:** the engine has only two import branches (`db_recipe` ATTACH+SELECT, and direct file `read_csv`); a recipe-over-files path is an *engine* change that breaks the epic's cockpit-only boundary, and the clean form ("attach uploaded files as a catalog, then SELECT uniformly") overlaps the parked **DAT-593** (view-import). Its own bet.
- **Coarse-then-refined two-pass framing.** Frame coarsely first (to guide future auto-SQL-generation), refine after the set is assembled. **Rejected:** `frame` already does induce-then-**edit** (LLM induction + user-curated sets accepted verbatim) — the "refine" pass already exists as curation, without a second LLM call and its overlay-reconciliation problem. And the named consumer (auto-SQL-generation) does not exist yet. Frame once, after set assembly. Revisit if/when auto-SQL lands.
- **Frame on the raw source/table schema (today's `ConnectSchema`).** Simpler — no `DESCRIBE (<sql>)` seam. **Rejected:** a probed JOIN/projection's *output* shape is the business model; framing on raw tables frames the wrong thing.
- **Multi-vertical per workspace.** The honest long-term model (one workspace spans sales + finance + …). **Deferred to its own ideate:** the one-vertical assumption is concentrated (cockpit `workspaces.vertical` single column + `begin_session`'s session ontology) — the engine's `add_source` already takes `vertical` *per-run* — but redesigning that seam is epic-sized. Per the no-shims rule, 594 builds cleanly on today's single-active-vertical and does **not** pre-abstract for a multi-vertical future that isn't designed yet.

## Value / Effort

**High value** (closes the chicken-egg *and* unifies the Connect entry surface into one assemble-frame-import flow) / **medium effort** — revised up from the ideate's "low–medium" after the refine verified the cockpit is less ready than the doc implied. Cockpit-only, no engine change, no contract change.

**Work inventory (verified, all cockpit):**
1. Heterogeneous import set — `probe.tsx` state (tagged union) + cart modal lists files + queries.
2. **Persist seam** — factor `persistFileSources` out of `tools/select.ts`; widen `importSources` to persist both kinds → union ids → `triggerAddSource`.
3. **Schema assembly** — `probeDescribe()` helper + standalone file-sniff; build synthetic `ConnectSchema` (with samples) across the set.
4. **Server-fn wrappers** for `frame` and `use_vertical` (UI-callable, no agent).
5. **Toolbar UI** — upload modal (reuse `upload-dropzone`, CLEAN), frame/vertical modal (Mantine `Modal`, existing pattern).
6. **Gate move** Add→Start + **`["active-vertical-status"]` invalidation** (net-new — no `invalidateQueries` precedent in the cockpit).

**Reuses (verified clean):** `upload-dropzone` (props `{onUploaded, disabled?}`, no canvas coupling), the cart `Modal` pattern, `upsertSource`/`triggerAddSource`/`addSourceWorkflow` (source-type-agnostic run), `frame`/`useVertical` underlying fns.

**Test strategy:** units on the pure bits (heterogeneous-set validation, synthetic-`ConnectSchema` assembly); real-browser Playwright smoke of stage→frame→Start against the WWI MS SQL source **+ a CSV**, on a **freshly-framed** workspace (first import → stays clear of the DAT-596 replace guard).

**Spun off during refine:** DAT-597 (chat-less Connect — evaluate), DAT-598 (propose/commit frame). Re-run story stays on DAT-596.

→ Ready for `/implement DAT-594` (M — the skill creates the phase plan).
