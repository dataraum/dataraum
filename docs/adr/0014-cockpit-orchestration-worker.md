# ADR-0014 — Cockpit orchestration worker: TS workflows own stage execution

- **Status:** Accepted (the co-located TS orchestration worker), **amended by DAT-609**
- **Date:** 2026-06-17 (amended 2026-06-23)
- **Ticket:** DAT-529 (co-located worker), DAT-530 (journey owns stages + cascade + breaker), DAT-609 (collapse the singleton)
- **Design doc:** Confluence DD/36175874 (Cockpit Autonomy epic, DAT-526)
- **Amends:** [ADR-0001](./0001-temporal-orchestration-python.md) (scoped — analysis stays Python)

> **Amended by DAT-609 (2026-06-23).** The **co-located TS orchestration worker** (the
> core decision) stands. What changed: the long-lived **singleton `JourneyWorkflow`** is
> replaced by **two short-lived per-trigger workflows** (`groundingLoopWorkflow`,
> `sessionCascadeWorkflow`) plus **direct single-shot engine starts** for `replay` and
> `operating_model`. Consequently the **`patched()` discipline, continue-as-new-at-500,
> the circuit breaker / pause-resume, the `JourneyState` query, and the
> "deploy-on-next-signal" model documented below are OBSOLETE** — see
> "## DAT-609 — collapse to short-lived workflows" at the end. The original text is kept
> as the record of why the singleton existed and what replaced it.

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

## Deploying journey-code changes (DAT-567)

The journey is long-lived (one execution per workspace, bounded by continue-as-new every ~500
events), so "how does a running journey pick up new workflow code?" is a real operational question.
The answer, **verified empirically** (DAT-567, against a live 24-minute-old journey on the smoke
stack):

- **A running journey adopts new code on its NEXT signal after the *worker process* is running the
  new bundle — no continue-as-new and no workflow restart required.** When the redeployed worker
  picks up the next workflow task it replays the full history under the new bundle and then processes
  the new signal live. Confirmed: redeploying the cockpit container and signalling the *existing*
  journey ran the new code path on the same execution (`runId` unchanged) with no
  `DeterminismViolation`.
- The true gate is therefore **"is the worker process running the new bundle?"**, NOT continue-as-new.
  In prod that means **recreating the cockpit container so the new `workflowBundle` is loaded**:

  ```bash
  docker compose -f packages/infra/docker-compose.yml up -d --build --force-recreate cockpit
  ```

  The real failure mode is the **cached-image / no-recreate trap** — `up` without `--force-recreate`
  (or without rebuilding) leaves the old worker polling the old bundle, so the change silently never
  lands. (Same family as the cockpit-migrate cached-image gotcha.)
- **Already-executed and in-flight stages keep their original behavior by design.** `patched()`
  returns `false` while replaying the marker-less historical part of the journey, so a stage that
  already ran under the old code is *not* retroactively re-run under new logic — you cannot, e.g.,
  retro-apply the grounding loop to an import that already completed. A *new* stage after the deploy
  gets the new path. This is correct, not a bug (it's exactly what DAT-551 observed and what got
  re-filed as DAT-567).
- **Discipline for future edits — never reorder the marker sequence.** Inserting a new `patched()`
  gate (or a new activity call, timer, or child) *before* an existing committed marker in a path the
  live history already traversed reorders commands against history → `DeterminismViolation` on every
  running journey. Append-only changes are safe: a new `patched()` gate reached only on *new* work, a
  side-effect-free `log.*`, or a trailing branch. The offline `journey-replay.test.ts` guard catches
  reorders against its committed fixture — keep a rich, recent fixture (DAT-568) so it covers the
  cascade + grounding paths, not just begin_session-start.

In **dev** (`bun --bun run dev`) the worker is a `globalThis`-pinned singleton bundled once at
`Worker.create`; HMR re-imports the module but reuses the running worker, so **edits to
`src/worker/` are NOT hot-reloaded — restart the dev server to load them** (see `worker.ts` and the
cockpit CLAUDE.md dev loop).

## DAT-609 — collapse to short-lived workflows

The singleton was the wrong shape. The only thing it *had* to provide — per-workspace
cross-stage serialization — is already guaranteed by the versioned-metadata model (atomic
`promote_run`/`promote_session_run` + born-loud `resolve_operating_model_scope`), so a resident
coordinator bought nothing and taxed every change with `patched()` ordering, continue-as-new, and
replay fixtures. DAT-609 replaces it with:

- **Two short-lived per-trigger orchestration workflows** on the same `cockpit-orchestration`
  queue, started by a **deterministic per-workspace id** (no signals):
  - `groundingLoopWorkflow` (`grounding-<ws>`) — the onboarding import + the autonomous
    teach-and-replay loop (a bounded `while` loop, ≤ `numberOfAttempts`; no continue-as-new —
    three rounds have a trivial history). Started by the `select` import trigger only.
  - `sessionCascadeWorkflow` (`session-<ws>`) — begin_session → clean → operating_model.
  Both carry their state on the **start payload** (incl. `conversationId`), have **no `patched()`,
  no continue-as-new, no breaker, no cross-run state**, and bracket each engine stage with the same
  `recordRun` → `startChild` (`ABANDON`) → `attachRunId` → `markRunStatus` writes (shared
  `worker/workflows/run-stage.ts`).
- **Direct single-shot engine starts** for `replay` and `operating_model` (no orchestration
  workflow — there is no follow-on stage). The tool itself does `recordRun` → `client.workflow.start`
  → `attachRunId` (`temporal/orchestration-trigger.ts#startDirectRun`). A manual `replay` must NOT
  re-enter the autonomous grounding loop — the user is doing teach+replay by hand.
- **Single-flight is the workflow-id reuse policy** — `start` with `ALLOW_DUPLICATE`
  (re-start once the prior is CLOSED) + `workflowIdConflictPolicy: FAIL` (reject while one is
  RUNNING) → `WorkflowExecutionAlreadyStartedError`, translated to a `RunAlreadyRunningError`
  (an `AgentActionableError`). Because the orchestration workflows start the engine stage under the
  SAME engine-child id, single-flight holds across the orchestration/direct boundary too.
- **Narration is unchanged** — the server-side `lib/completion-watcher.ts` still narrates on the
  run's done edge (and skips the onboarding import per DAT-597). It keys on the engine-child run
  rows, which all four paths write identically, so it is decoupled from the workflow shape. The
  poll→push transport rework is split out as **DAT-615**; teach-aware narration (DAT-569) is a
  now-trivial follow-up on whatever transport DAT-615 picks.

Consequences for the rules above:
- **The "Deploying journey-code changes (DAT-567)" section is obsolete.** There is no long-lived
  execution to adopt code "on its next signal", and **no `patched()` discipline** — short-lived
  workflows simply run the new code on their next start. A worker bundle change still needs the
  container recreated (`up -d --build --force-recreate cockpit`) so the new `workflowBundle` loads
  (the cached-image / no-recreate trap is unchanged); but the marker-ordering rule no longer applies.
- **Determinism coverage:** the committed-history `Replayer` fixtures are retired with the journey.
  An offline `worker/workflow-bundle.test.ts` guards sandbox-safety (the bundle compiles with no
  disallowed imports); per-workflow `Replayer` determinism fixtures are captured from the **DAT-579**
  Connect→Stage→Analyse compose-smoke (the project bans the test-server). Until then determinism
  rests on the shrunk surface (no `patched`, short-lived) + the `decideGroundingStep` unit tests.
