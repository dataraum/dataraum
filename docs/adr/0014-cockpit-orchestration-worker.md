# ADR-0014 — Cockpit orchestration worker: TS JourneyWorkflow owns stage execution

- **Status:** Accepted
- **Date:** 2026-06-17
- **Ticket:** DAT-529 (co-located worker), DAT-530 (journey owns stages + cascade + breaker)
- **Design doc:** Confluence DD/36175874 (Cockpit Autonomy epic, DAT-526)
- **Amends:** [ADR-0001](./0001-temporal-orchestration-python.md) (scoped — analysis stays Python)

## Context

The autonomy epic (DAT-526) needs a durable *orchestrator* above the engine's analysis
workflows: something that drives the connect → stage → analyse journey, advances a workspace
stage-by-stage when each completes, and stays alive across restarts. ADR-0001 made the cockpit
a Temporal **Client only**. But the orchestrator is control-plane logic that reuses the
cockpit's session model (cockpit_db), its run-recording + narration substrate (DAT-461/462/528),
and its tools — not analysis. Putting it on the Python engine worker would split control-plane
logic across two languages and drag cockpit concerns into the engine; standing up a third
runtime/deploy unit for it adds architecture the project doesn't need yet. The engine's analysis
workflows (begin_session, operating_model, add_source) remain the right home for analysis.

## Decision

The cockpit hosts its **own co-located Temporal worker** (`@temporalio/worker` + `@temporalio/workflow`)
for **orchestration** workflows, alongside its existing Client role for analysis. Scope: analysis
stays Python on the engine worker (ADR-0001 holds); only orchestration is TypeScript. The worker is
a **module-level singleton started at server boot** (Nitro plugin), polling the `cockpit-orchestration`
queue under **Bun ≥ 1.3.14** (Bun-worker support landed in SDK 1.15). Workflow code is pre-bundled at
build time (`workflowBundle`, never `workflowsPath` in prod) because the server is Nitro-bundled and
the worker pulls a native Rust core-bridge — so `@temporalio/*` is externalized and the workflow
sandbox imports only `@temporalio/workflow` + pure modules (`contracts.ts`, `breaker.ts`, the pure
`workflow-id` helpers).

The one orchestration workflow today is **`JourneyWorkflow`** — one long-lived execution **per
workspace** (`journey-<workspaceId>`), bounded by continue-as-new. It **owns stage execution**: on a
trigger signal it starts the matching **Python engine workflow as a cross-language CHILD** on the
workspace's `engine-<id>` queue (string type name + `ParentClosePolicy.ABANDON` so the journey's
continue-as-new never kills a running stage), records the run in cockpit_db around it, and awaits the
child. Awaiting a child is the "advance when the stage completes" primitive. A clean begin_session
**auto-cascades** into operating_model (the journey's next child) — gated by `patched()` and by a
**circuit breaker** (trips the cascade off after N consecutive stage failures; `pauseAutoMode`/
`resumeAutoMode` are the manual counterpart, pause-don't-kill). The cockpit's tools (begin_session,
operating_model) became **journey signallers**, not workflow starters — the journey is the single
owner of all stage execution.

## Consequences

- The cockpit's tools no longer start engine workflows directly — they `signalWithStart` the
  per-workspace journey (`signalRunBeginSession` / `signalRunOperatingModel`). A tool returns the
  deterministic workflow id; the journey owns the real Temporal execution id, so **progress is keyed
  on `workflowId` alone** (the latest execution), and the conversationId is captured at the tool
  boundary and threaded to the journey (it has no request ALS — narration routing, DAT-528, would
  silently break otherwise).
- **Two workers, two queues:** engine = Python on `engine-<id>` (analysis); cockpit = TS on
  `cockpit-orchestration` (orchestration). Changing the engine workflow signatures/return shapes is
  still cross-package (the cockpit hand-mirrors them in `types.ts`).
- Determinism is verified offline via `Worker.runReplayHistory` against a committed fixture; the
  control-flow change is `patched()`-gated. The breaker fold is a pure reducer, unit-tested without a
  Temporal server (the test-server stalls CI — ADR-0001).
- Cross-language child workflows (TS parent → Python child) are now a load-bearing pattern, not
  documented by the SDK skill → proven by spike + the replay fixture.
- One benign Bun caveat: `promiseHooks` is unavailable, so the built-in `__stack_trace` query is
  disabled.
- **Deliberately NOT built:** no workflow-level RetryPolicy on the engine children (retrying
  multi-minute LLM stages is a money-pit; the breaker is the protection and the cockpit_db activities
  already retry); no in-flight child cancellation on pause (the sequential drain re-checks auto-mode at
  the cascade decision point, so pause stops the *next* cascade without aborting a running ABANDON
  child mid-LLM). The "auto-re-run stale stages" cascade-remainder is deferred to a later phase.
- Open follow-ups: per-tenant queue fairness (each tenant gets a full deployment, so strict separation
  isn't needed yet — DAT-526 note); the agentic grounding-teach loop (DAT-551) rides on this substrate;
  the `verticalEstablished` entry gate is not yet wired into frame/use_vertical.
