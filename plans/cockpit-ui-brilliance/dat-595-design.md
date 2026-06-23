# DAT-595 — Stale add_source progress: give the run a real per-run id

> Refined 2026-06-20. Child of epic **DAT-574**. Bug surfaced by the DAT-592 follow-up smoke (PR #342); **pre-existing**, not introduced by 592. Cockpit-only (the add_source workflow is Python, but the run-identity lives in the cockpit + its TS journey worker — confirm the `signalRunAddSource` contract at implement). Sequenced **after DAT-594** (file overlap).

## Problem

Every per-workspace add_source run shares **one** id: `run_id == workflow_id == addsource-<ws>` (DAT-562 retired the per-import segment). Two failure modes follow:

1. **Stale prior-run state.** The progress widget keys its TanStack query on `workflow_id` **alone** (`progressQueryKey`, deliberately per DAT-530 — "workflow id is the stable identity; progress reflects the latest execution"). Consecutive imports reuse that workspace-stable id, so the cache collapses multiple runs into one entry: a second Start renders the prior run's terminal state (e.g. a stale failure alert) while the current run has actually advanced. The DAT-594 staging hub makes repeat imports a normal gesture, so this is now a first-encounter bug.
2. **Fast-failed run never narrated.** A run that goes terminal within the watcher's first-poll delay (~2.5s) never enters `listWatchableRuns` (which filters `status="running"`), so it's never tracked → never narrated; the widget freezes on the static pipeline with no error.

## Root cause

The trigger returns a **placeholder** `run_id == workflow_id` *before* the journey starts the real Temporal child (which mints a distinct `firstExecutionRunId`). The cockpit UI/watcher therefore never receive a per-run identity; everything keys on the workspace-stable workflow id, and `/api/workflow-progress` resolves the **latest** execution by workflow id (`getHandle(workflow_id)`, ignores `run_id`).

Key evidence (file:line):
- `temporal/trigger-add-source.ts:~102` — `run_id: workflowId` (placeholder).
- `temporal/workflow-id.ts:~29` — `addsource-${workspaceId}` (per-ws stable).
- `lib/workflow-progress-event.ts:~35` — `progressQueryKey(workflowId)` = `["workflow-progress", workflowId]` (no runId).
- `ui/cockpit/widgets/workflow-progress.tsx:~165` — query keyed on workflowId only.
- `routes/api/workflow-progress.ts:~206` — `getHandle(input.workflow_id)` (latest execution; `run_id` accepted but not pinned).
- `lib/completion-watcher.ts:~55,~147` — `POLL_MS = 2500`, sleeps before first poll; `listWatchableRuns` filters `status="running"` → fast-failed runs escape.
- Per-run id DOES exist: `worker/workflows/journey.ts:~180` captures `child.firstExecutionRunId` and `attachRunId(workflowId, runId)` records it — but asynchronously, after the trigger already returned.

## Decision — Axis (i): a real per-run id (confirmed 2026-06-20)

Mint a **per-run id at trigger time** (a cockpit correlation id, `randomUUID()`) and thread it as the run's identity everywhere the cockpit keys on a run; the journey adopts it (via the existing `attachRunId`) instead of the placeholder. This is the only axis that fixes **both** symptoms with a real identity rather than two partial patches.

**Touch list:**
- `temporal/trigger-add-source.ts` — mint `run_id = randomUUID()`, return it; pass it through `signalRunAddSource` to the journey.
- `worker/workflows/journey.ts` (+ the trigger signal contract) — adopt the supplied id as the run identity / `attachRunId` value, rather than minting/placeholdering.
- `lib/workflow-progress-event.ts` — `progressQueryKey(workflowId, runId)` → `["workflow-progress", workflowId, runId]`.
- `ui/cockpit/widgets/workflow-progress.tsx` + `measure-progress.tsx` — key the query on the per-run id; carry it on `add-source-progress` canvas state.
- `routes/api/workflow-progress.ts` — pin the run by id (resolve the specific execution) instead of `getHandle(workflow_id)` latest-only.
- `lib/completion-watcher.ts` — `runKey` on the real per-run id; **include terminal runs** in what's watchable (or poll once eagerly) so a fast-failed run is still narrated.
- `server/import-sources.ts` + `ui/cockpit/tool-result-to-canvas.ts` + `canvas-state.ts` — thread the per-run id through to the widget. **(Overlaps DAT-594 — sequence after it merges.)**

### Rejected axes
- **(ii) Invalidate `["workflow-progress", workflowId]` on a new run** — fixes stale-state only; leaves the silent fast-fail (the nastier failure). A band-aid around a missing identity.
- **(iii) Expand the watcher to terminal runs** — fixes fast-fail only, and incompletely (needs the run recorded in cockpit_db in time). Another partial patch.

## Open questions / risks

- **Is the trigger signal contract cross-package?** The add_source workflow is Python, but it's started as a child by the **TS** journey worker; the synthetic id is a cockpit correlation id threaded via the journey signal. Confirm `signalRunAddSource` / the journey contract is TS-only (cockpit) at implement — if a field crosses to the Python worker contract, it's a cross-package mirror change (see `feedback_cockpit_mirrors_worker_contracts`).
- **DAT-530's "key on workflow id" was deliberate** for journey-started stages (runId unknown at trigger). Axis (i) supersedes that rationale by making the runId known at trigger — verify no other journey-stage widget depends on the workflow-id-only key.

## Sequencing & test

- **After DAT-594 merges** (overlap on `import-sources.ts` + `tool-result-to-canvas.ts`). Implements on `main`.
- **Test:** unit the new `progressQueryKey(workflowId, runId)` + the watcher's terminal-run inclusion; smoke = **two consecutive imports** on one workspace show their OWN progress (no stale prior-run alert), and a deliberately fast-failed import surfaces an error (not a frozen widget).
