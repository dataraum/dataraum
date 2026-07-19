# ADR-0020 — All workflows on the engine worker; the cockpit worker is activity-only

- **Status:** Accepted
- **Date:** 2026-07-07
- **Ticket:** DAT-708 (Phase 0 of the observability epic DAT-704; research record in DAT-705)
- **Supersedes:** [ADR-0014](./0014-cockpit-orchestration-worker.md) (TS orchestration workflows); restores the full scope of [ADR-0001](./0001-temporal-orchestration-python.md)

## Context

ADR-0014 placed the two orchestration workflows (grounding loop, session cascade) on a
TypeScript worker co-located in the cockpit's Bun server process. Building OpenTelemetry
tracing (ADR-0019, DAT-705) exposed the foundation as unsupported: Temporal's TypeScript
SDK "strongly discourage[s] running Temporal Workers in anything except authentic
Node.js" — the worker depends on Node-API, `worker_threads`, `vm`, and
`AsyncLocalStorage`, exactly Bun's documented weak spots (oven-sh/bun#14407: ALS loses
context inside `vm`, open). DAT-705 measured the concrete cost: headers set by any
workflow interceptor — official, polyfilled, or hand-rolled — silently never reach
Temporal commands under the Bun threaded-vm worker. The failure mode is the worst kind:
no error, no log, just absent behavior. Activities run on the main isolate and were
proven working under Bun (functionally and for OTel context propagation) since DAT-529.

Requirement: orchestration must be durable, traceable end-to-end, and hosted on a
runtime its SDK supports. Options considered:

- **Move the TS worker to a Node sidecar** — keeps the workflows in TS but adds a third
  runtime and deploy unit whose only job is dodging Bun; rejected as another moving
  target.
- **Keep the TS workflows untraced** — permanently splits every cross-stage trace and
  leaves workflow code on a runtime the vendor discourages; rejected.
- **Port the workflows to Python on the engine worker** — chosen. The workflows are
  ~370 lines of deterministic control flow with no cockpit-runtime dependency; the
  cockpit-bound side effects were already isolated in four activities.

## Decision

All Temporal workflows — analysis and orchestration — are Python on the engine worker.
`groundingLoopWorkflow` and `sessionCascadeWorkflow` keep their registered type names,
per-workspace ids (`grounding-<ws>`, `session-<ws>`), single-flight start policy,
record-after-start run bracketing (DAT-595), `ParentClosePolicy.ABANDON` engine
children, and the bounded teach loop (`decide_grounding_step`, ported with its unit
tests). The teach loop's shape changed with the port: re-run, don't loop — each
execution runs one round (import → assess → decide) and a `replay` verdict tail-calls
the workflow via `continue_as_new`, carrying the decremented budget on the input
message. The engine carries the loop state durably; the replay bound is structural
(a round can only recur through the input contract), and each execution's history
stays one stage + one assess. The engine children became native same-queue Python
child workflows — the cross-language child hop is gone.

The cockpit remains the Client — it starts the orchestration workflows by type name on
the workspace's engine queue — and hosts an **activity-only** worker on its own
per-workspace `cockpit-<ws>` queue, derived from the boot workspace identity on both
sides (DAT-818; originally the singleton `cockpit-orchestration`): no workflow bundle, no vm sandbox, no
`@temporalio/workflow` import anywhere in the package. Its four activities stay in
TypeScript where their substrate lives (ADR-0003: the cockpit_db run writers
`recordRun` / `markRunStatus` / `markRunAwaitingInput`; ADR-0004: the grounding-teach
agent `assessAndGround`); the engine-hosted workflows schedule them cross-language by
name, with one `task_queue=` kwarg.

The contract seam reverses direction: the engine owns the orchestration start payloads
(`worker/contracts.py`, snake_case; the cockpit hand-mirrors them in
`src/temporal/types.ts`), while the activity IO shapes stay TS-owned and the engine
mirrors them as deliberately camelCase Pydantic models (wire fidelity over local
convention).

## Consequences

- One workflow home: workflow-logic changes deploy with the engine image; the cockpit
  image no longer bakes a workflow bundle (build pre-step, generated module, and
  offline bundle test deleted). The cockpit still ships `@temporalio/*` (native
  core-bridge) for the activity worker, so the bundle externalization and prod-deps
  Docker stage remain.
- The engine's workflow-registration guard covers only its own queue; cross-queue
  activity calls (explicit `task_queue=`) are exempt by construction, and the
  activity names + IO shapes are a cross-package contract enforced by lockstep edits,
  not codegen.
- OTel (the re-scoped DAT-705) now needs only supported paths: Python workflow/activity
  tracing on the engine worker plus TS client/activity tracing on Node-compatible main
  isolates. No Bun workflow-sandbox work remains anywhere.
- Activity workers are language-independent queue pollers — registering activities
  requires none of the workflow machinery. This unlocks polyglot topologies (e.g. a Go
  workflow runner with Python or TS activities) as a future option; recorded here as an
  unlocked consequence, not a plan.
- In-flight orchestration executions do not survive the cutover (the old worker's queue
  goes unpolled). Deploys are clean-sweep restarts on the dev stage — accepted.
