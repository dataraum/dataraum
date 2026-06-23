# Cockpit orchestration is over-engineered — collapse to short-lived workflows + direct single-shots (DAT-609)

> One Jira task (DAT-609). Tech-debt, not a feature. Amends DAT-529 / DAT-530 / ADR-0014.
> **Design locked + refined 2026-06-23.** This supersedes the earlier "Option A" (engine
> cascade — rejected) AND the first Option B draft (which routed replay through the grounding
> loop and described workflow-side narration). The code on disk + this note win.

## Problem

DAT-529/530 shipped a long-lived **singleton** per-workspace `JourneyWorkflow`
(`worker/workflows/journey.ts`): signal-fed, holds circuit-breaker state, continues-as-new
every ~500 events, and gates every change behind `patched()` (ADR-0014). It bundles
serialization + breaker + cascade + the grounding loop into one resident execution — a
determinism tax on every evolution (patched ordering, replay fixtures, cross-run in-memory
state). The only thing that *needs* a resident actor is per-workspace cross-stage
serialization — and the versioned-metadata model already makes concurrent stages safe:

- **Atomic promote.** `promote_run` (add_source) upserts all per-table heads in a single
  `session_scope()` transaction; `promote_session_run` (begin_session) flips the one
  `(catalog,"catalog")` head in one transaction (`worker/activity.py`). A concurrent reader
  sees all-old or all-new — never a torn catalog.
- **Born-loud OM.** `resolve_operating_model_scope` raises non-retryable `PhaseFailed` if no
  begin_session catalog run is promoted. operating_model structurally cannot run over a
  partial workspace.

So cross-stage serialization is unnecessary, and the singleton's whole determinism tax buys
nothing.

## The serialization that *does* remain — id-reuse, not a resident actor

Two runs of the same engine stage for one workspace must not run at once (they reuse the
deterministic engine-child id `addsource-<ws>` / `beginsession-<ws>` / `operatingmodel-<ws>`).
That single-flight is provided by **Temporal's workflow-id reuse** — `start` with
`workflowIdReusePolicy: ALLOW_DUPLICATE` (re-start once the prior is closed) +
`workflowIdConflictPolicy: FAIL` (reject while one is running) — NOT by a resident
coordinator. A rejected start surfaces `WorkflowExecutionAlreadyStartedError`, which the
trigger turns into an agent-actionable "already running" message. This replaces the entire
signal-queue mechanism. Because the orchestration workflows start the engine stage under the
SAME engine-child id, single-flight holds across the orchestration/direct boundary too.

## Fix — two short-lived orchestration workflows + two direct single-shots

There are five cockpit→engine kick points across three engine workflows. Only the two that are
genuinely **multi-step** need a durable cockpit-side orchestrator; the single shots call the
engine directly.

| Cockpit caller | Engine workflow | Path |
|---|---|---|
| `select` → `triggerAddSource` | `addSourceWorkflow` | **grounding-loop workflow** (import + autonomous teach loop) |
| `replay` tool | `addSourceWorkflow` | **direct** (no loop — the user is doing teach+replay by hand) |
| `beginSession` tool | `beginSessionWorkflow` | **session-cascade workflow** (begin_session → OM) |
| `operating_model` tool | `operatingModelWorkflow` | **direct** (single shot) |
| *(autonomous)* clean begin_session | `operatingModelWorkflow` | the cascade's second child |

### Orchestration workflows (cockpit TS, on the `cockpit-orchestration` queue)

Both are **short-lived, per-trigger, stateless across runs**: state on the start payload,
`conversationId` first-class, no `patched()`, no continue-as-new-at-500, no breaker, no signal
queue. They run on the cockpit worker only for crash-durability + tab-independent autonomy.
They do **not** narrate — narration stays the completion-watcher (see below). Both bracket each
engine child with the same cockpit_db writes the journey used (`recordRun` authoritative before
start → `attachRunId` → `markRunStatus`), via a shared `runStage` helper.

1. **`groundingLoopWorkflow`** (id `grounding-<ws>`). add_source child (awaited — the loop needs
   the result to assess) → `assessAndGround` activity auto-applies the mechanical teaches a
   detector can verify → `decideGroundingStep`: `replay` (re-run add_source, bounded by the
   attempt budget), `surface` (`markRunAwaitingInput` → "Needs you" inbox), or `done`. A
   **bounded while-loop** (≤ `numberOfAttempts`, default 3) — NOT continue-as-new: three
   iterations have a trivial history, so CAN buys nothing and adds determinism edges. This is the
   journey's `runGroundingLoop` extracted verbatim. Import run recorded with the real
   `conversationId` (so the watcher tracks it for the progress widget) but `kind:"onboarding"`
   (so the watcher skips its chat narration, DAT-597); internal replays recorded with
   `conversationId:null` (no per-attempt narration). Trigger: `triggerAddSource` only.

2. **`sessionCascadeWorkflow`** (id `session-<ws>`). begin_session child → await → if clean →
   operating_model child (id computed via the pure `operatingModelWorkflowId` helper) → await →
   done. The journey's begin_session arm, minus breaker / signal-queue / patched. `conversationId`
   rides the payload to both children; the watcher narrates both completions into the originating
   chat. Trigger: `beginSession`.

### Direct single-shots (no orchestration workflow, ZERO determinism surface)

`replay` and `operating_model` are single engine stages with no follow-on. The tool (a server
function with cockpit_db access) does the bracket itself: `recordRun` (placeholder) →
`client.workflow.start(<engineWorkflow>)` → `attachRunId(realRunId)` → returns. Nothing
cockpit-side awaits; the completion-watcher narrates on the done edge exactly as for any other
run. This **restores the pre-DAT-551 direct-start pattern** for single shots — and is the
primitive the Connect/Stage teach interactions will reuse. A shared `startEngineWorkflow` client
helper carries the id-reuse/conflict policy + the "already running" translation.

- **Why `replay` must NOT route through the grounding loop:** the grounding loop *is* automated
  teach-and-replay. A user manual replay is the human doing that same loop deliberately —
  wrapping it in the autonomous loop would make the agent re-teach/re-replay on top of the
  user's action. Direct keeps the manual path manual.

### Decision: this over the engine cascade (Option A, rejected)

Moving the begin_session→OM cascade into the engine (Python begin_session fire-and-forgets OM)
was rejected: it **breaks the orchestration boundary** (DAT-529 / CLAUDE.md: *analysis stays
Python, orchestration is the cockpit TS worker*), and **`cockpit_db` ownership fights it** — the
engine can write neither the run record (with `conversationId`) nor narration, so an
engine-started OM would still need the cockpit to record + narrate it, splitting the cascade
decision from its bookkeeping. Keeping the cascade as a short-lived TS workflow leaves the engine
**untouched** and run-recording / `conversationId` where they already work.

## Narration stays the completion-watcher — unchanged in this task

`lib/completion-watcher.ts` (server-side, one per open `/api/chat-stream`) polls the
conversation's in-flight runs against Temporal and, on the not-done→done edge, pushes the live
progress snapshot to the widget AND (for non-onboarding stages) narrates via the chat-bus. It is
**fully decoupled from the orchestration workflow shape** — it keys on the engine-child run rows
(`addsource-<ws>` etc.), which all four paths write identically. So this collapse leaves it
untouched; replay/begin_session/operating_model still narrate, onboarding still doesn't (DAT-597).

The watcher's Temporal-polling (vs a push/listener) is real tech-debt but **orthogonal** to the
journey-vs-short-lived question, and not free (the watcher also feeds the live progress widget;
engine progress is a pull `get_progress` query; the two direct single-shots have nothing
cockpit-side to push from). Split out as **DAT-615**. DAT-569 (teach-aware post-import narration)
likewise stays a follow-up — now trivial, it rides whatever transport DAT-615 picks.

### Delete
`JourneyWorkflow` (`worker/workflows/journey.ts`), the signal-queue serialization, `breaker.ts` +
`breaker.test.ts` + `pauseAutoMode`/`resumeAutoMode`, continue-as-new-at-500, the journey
`patched()` machinery (`CASCADE_PATCH` / `GROUNDING_PATCH`), the cross-run state carry, the
`JourneyState` query, `journey-replay.test.ts` (replaced by per-workflow Replayer fixtures), and
the unwired `verticalEstablished` entry gate. Trim `worker/contracts.ts` (drop the `RUN_*_SIGNAL`
signals + pause/resume + `JourneyState` + `journeyWorkflowId` + `JOURNEY_WORKFLOW_TYPE` +
`VERTICAL_ESTABLISHED`).

### Keep / reuse
- `assessAndGround` (`worker/grounding-agent.ts`) — the LLM teach activity, unchanged.
- `decideGroundingStep` (`worker/workflows/grounding-step.ts`) — the pure decision reducer; its
  unit tests carry over verbatim.
- the cockpit_db run activities (`recordRun` / `attachRunId` / `markRunStatus` /
  `markRunAwaitingInput`) — reused by both the workflows (via `runStage`) and the direct tools.
- the entire run-row / progress / reconcile / **completion-watcher** substrate.

### Dropped (never wanted)
- **Pause-autonomy switch** — no persistent flag, no DB table. To change teachings the user
  teaches in the Connect chat + replays.
- **Auto-trip-after-N-failures breaker** — the attempt cap (on the payload) is the only runaway
  safety needed.

## Properties
- **conversationId** is first-class on each start payload: one execution = one trigger = one
  conversation. No per-signal threading through a shared singleton.
- **Multi-workspace safe — and cleaner than today.** Per-ws ids isolate single-flight per
  workspace; each workflow/tool starts the engine stage on the correct `engine-<ws>` queue; **zero
  resident cross-ws or cross-run state** (the singleton held per-ws breaker state across
  continue-as-new; this holds none).
- **Analysis stays Python.** The cockpit composes by starting engine workflows by name and
  awaiting them — orchestration flexibility without moving the analysis pipeline to TS.

## Size / risk / test
- **Size: L.**
- **Risks:** (1) **in-flight journeys at cutover** — terminate any running `journey-<ws>` on
  deploy (dev/smoke only, redeployable). (2) **run-recording relocation** — the direct single-shots
  move `recordRun`/`attachRunId` from the journey into the tool; must preserve "authoritative
  record before start" + `conversationId` or runs orphan / don't narrate. (3) **id-reuse policy** —
  `FAIL` on conflict surfaces an error the trigger must translate to an actionable sentence (the
  begin_session/operating_model tools also keep their existing `hasRunningRun` pre-checks).
- **Test strategy:** `decideGroundingStep` unit tests (kept); the tool unit tests updated to assert
  the new start-by-id / direct-start payloads; per-workflow `Replayer` determinism fixtures
  captured from the **DAT-579** Connect→Stage→Analyse compose-smoke (the project bans the
  test-server, so fixtures come from a real run, as the journey fixture did) — until then
  determinism rests on the shrunk surface (no `patched`, short-lived) + the unit tests. Documented
  honestly, not faked offline.
- **Cross-repo:** none (orchestration only; engine untouched; no analysis/detector change → no
  eval handoff). Update **ADR-0014** (the patched / CAN-500 / deploy-on-next-signal model is gone)
  and the cockpit↔worker contract mirror (the trimmed `contracts.ts`).

## Refs
DAT-529, DAT-530, ADR-0014. DAT-615 (poll→push narration transport, split out). DAT-569
(teach-aware narration, follow-up). Singleton = `worker/workflows/journey.ts`; trigger seam =
`temporal/journey-trigger.ts`; safety premise = the versioned-metadata head
(`promote_run` / `promote_session_run` + born-loud `resolve_operating_model_scope`).
