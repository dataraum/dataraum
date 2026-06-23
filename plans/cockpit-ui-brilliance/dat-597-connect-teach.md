# DAT-597 — Consolidate Connect to one-way-per-thing (ephemeral plan)

> Tech-debt under **DAT-574** (NOT an epic, no Confluence). Grew from "teach awareness" into a **Connect consolidation** during refine 2026-06-22: the Connect surface is in a transition state with TWO parallel stacks (a deterministic widget + an LLM chat that duplicates it). Philipp's directive: **one way of doing each thing; remove the rest.** No engine work — DAT-515 closed (resolved by DAT-506, verified live).

## The transition state (what we're ending)

The staging hub (`ui/cockpit/widgets/probe.tsx`, DAT-594) is a deterministic, server-fn-driven acquisition surface (assemble files+SQL → frame via `ModelModal` → Import via `importSources`), but it's **chat-summoned** into a message-derived canvas (`open_probe`/`upload`/`probe`/`connect` UI tools; canvas defaults to `{kind:"empty"}`). The connect chat toolstack **duplicates** the widget: `frame`/`useVertical`/`listVerticals` ≈ `ModelModal` (`frameStagingSet`/`adoptVerticalForStaging`), `select` ≈ the Import button (`importSources`). Progress is shown TWICE (widget `MeasureProgressWidget`+live events AND completion-watcher chat narration).

## The one way per thing

| Thing | The ONE way | Removed |
|---|---|---|
| Acquire (assemble/frame/import) | the **hub** as the Connect surface — **default canvas content, PINNED to the top** (persistent home base; the import set survives while teach inspections come & go) | `open_probe`, `probe`, `connect`, `upload`, `select`, `frame`, `useVertical`, `listVerticals` (connect-only → delete tool + canvas projection + tests) |
| Frame | `ModelModal` (induce new via `frameStagingSet` + adopt via `adoptVerticalForStaging`) — already does both | the `frame`/`useVertical`/`listVerticals` chat path |
| Import progress / parked gap | widget `MeasureProgressWidget` + live events + `NeedsYouPanel` | completion-watcher's **import** narration into Connect |
| Mechanical grounding | the autonomous loop (DAT-551) | (unchanged) |
| **Teach a judgement gap** | the **Connect chat** (its sole job) | — (this is what we ADD) |

## Connect chat = teach surface only

Toolstack (`tools/registry.ts` `connect`): `teach` (add_source-scoped) + `look_table` + `why_column` + `replay` + `listSources`/`listTables` (read context). All already exist; add a connect-scoped `teach` advertising the **add_source teach types** only.

**Teach boundary (non-overlapping, `tools/teach.validation.ts:335`):** Connect teaches `type_pattern`/`null_value`/`unit` (mechanical, loop auto-applies) + `concept`/`concept_property`/`rebind` (judgement, loop parks). Stage keeps `relationship`/`hierarchy`/`validation`/`cycle`/`metric`. → new `CONNECT_TEACH_TYPES = AGENT_TEACH_TYPES − {relationship, hierarchy}`.

**Narration, the RIGHT place:** kill the import-completion narration into Connect (acquisition progress = the widget). KEEP narration for the **teach→replay** outcome ("replayed — `amount` is now ready") — that's the teach surface verifying the teach helped, folds **DAT-569** (parked `awaiting_input` → originating chat). The grounding loop's parked note already preserves `conversationId` (`runs.ts`).

## Bug 1 — grain pin (cockpit, the only readiness fix)

DAT-515's "under-promotion" does NOT exist (DAT-506 fixed it; verified). The inspect tools (`why_column`/`look_table`) already pick correctly via `pickCurrentRow` (catalog supersedes add_source) — **do not touch them.** The ONE unpinned reader is `readGroundingReadiness` (`db/metadata/grounding-readiness.ts`), whose sole caller is the journey loop (runs during add_source, single grain) — pin `eq(viaTableHead, true)` so a replay-after-session can't feed it stale catalog rows. One function.

## Phases (each green before next; commit per phase)

0. **Bug 1** grain-pin in `grounding-readiness.ts` (+ test). Isolated, tiny.
1. **Hub as default+pinned canvas** for `kind:"connect"`: render `ProbeWidget` with `state={{kind:"probe"}}` (it mounts standalone — `source`/`sql` are optional seed) as the pinned-top default; verify import-set persists across teach-widget swaps.
2. **Add the teach stack** to the connect toolstack + `CONNECT_TEACH_TYPES` + connect-scoped `teach` tool.
3. **Delete** the 8 acquisition/opener tools + their canvas-state kinds / registry entries / tool-result-to-canvas cases / tests (clean cut — verify connect-only first; `listSources`/`listTables` stay, used by other kinds).
4. **Narration:** drop import-narration into Connect; keep/scope the teach→replay outcome narration.

## Scope fence
- **DO change:** `tools/registry.ts`, `tools/teach.ts`+`teach.validation.ts`, `db/metadata/grounding-readiness.ts`, the connect canvas default (`ui/cockpit/cockpit-state.ts` / `cockpit-view.tsx` / `canvas-registry.ts` / `canvas-state.ts`), `lib/completion-watcher.ts` (+ note builder), the 8 deleted tool files + their tests.
- **DO NOT change:** the engine; the inspect tools' grain pick (`pickCurrentRow`/`why_column`/`look_table`); stage/analyse toolstacks; the staging widget's internal acquisition logic (it already works).
- **Side-finding (separate ticket if it reproduces clean):** a table with entropy objects but no generation head → invisible (`wwi_recent_orders`, likely a DAT-596 re-import promote-gap).

## Open impl detail (resolve in Phase 1/4)
Pinned-hub vs teach-widget coexistence (single-member canvas today: `pinned ?? live ?? empty`) — the hub pins top; teach inspections render without evicting it (existing `probe-return-to-live` affordance is the precedent).
