# ADR-0014 — Cockpit orchestration worker: short-lived TS workflows for the control plane

- **Status:** Superseded by [ADR-0020](./0020-workflows-python-cockpit-activity-only.md) (2026-07-07 — the orchestration workflows moved to Python on the engine worker; the cockpit worker is activity-only)
- **Date:** 2026-06-17 (revised 2026-06-23)
- **Ticket:** DAT-529 (co-located worker), DAT-530 (journey), DAT-609 (collapse to short-lived)
- **Design doc:** Confluence DD/36175874
- **Amends:** [ADR-0001](./0001-temporal-orchestration-python.md) (scoped — analysis stays Python)

## Context

The cockpit needs a durable orchestrator above the engine's analysis workflows: something
that drives the journey across stages, advances a workspace when a stage completes, and
survives restarts. ADR-0001 made the cockpit a Temporal Client only.

The orchestrator is control-plane logic: it reuses the cockpit's session model
(cockpit_db), its run-recording and narration substrate, and its tools — not analysis.
Three homes were considered:

- **On the Python engine worker** — splits control-plane logic across two languages and
  pulls cockpit concerns into the engine.
- **A third runtime and deploy unit** — adds architecture the project doesn't need.
- **A worker co-located in the cockpit process** — chosen.

## Decision

The cockpit hosts its own co-located Temporal worker (`@temporalio/worker`) for
orchestration workflows, alongside its Client role for analysis. Analysis stays Python on
the engine worker (ADR-0001 holds); only orchestration is TypeScript.

Orchestration workflows are **short-lived and per-trigger**, started under a
deterministic per-workspace workflow id (no signals):

- `groundingLoopWorkflow` (`grounding-<ws>`) — the onboarding import plus the bounded
  teach-and-replay loop. Started by the import trigger only.
- `sessionCascadeWorkflow` (`session-<ws>`) — begin_session → operating_model.
- `replay` and `operating_model` start as **direct single-shot engine workflows** — no
  orchestration workflow, since there is no follow-on stage. A manual replay does not
  re-enter the grounding loop; the user is doing teach-and-replay by hand.

Each workflow carries its state on the start payload, holds no cross-run state, and
brackets each engine stage with the same run-recording writes. Engine stages start as
**cross-language child workflows** (string type name, `ParentClosePolicy.ABANDON`) on the
workspace's engine queue.

**Single-flight** is the workflow-id reuse policy: `ALLOW_DUPLICATE` (restartable once
the prior run closed) plus conflict policy `FAIL` (rejected while one runs), surfaced to
the agent as an actionable "already running" error. Orchestration workflows start the
engine child under the same id a direct start would use, so single-flight holds across
both paths.

**Bundling:** the server is Nitro-bundled and the worker pulls a native core-bridge, so
workflow code is pre-bundled at build time (`workflowBundle`), `@temporalio/*` is
externalized, and the workflow sandbox imports only `@temporalio/workflow` plus pure
modules.

## Consequences

- Two workers, two queues: engine = Python on `engine-<id>` (analysis), cockpit = TS on
  `cockpit-orchestration` (orchestration). Engine workflow signatures are hand-mirrored
  in the cockpit, so changing them is a cross-package edit.
- Deploying a worker-code change requires recreating the cockpit container so the new
  bundle loads (`up -d --build --force-recreate cockpit`); `up` without rebuild and
  recreate leaves the old worker polling with the old bundle and the change never lands.
  In dev the worker is bundled once at startup; edits under `src/worker/` need a
  dev-server restart.
- Deliberately not built: workflow-level retry on the engine children (retrying
  multi-minute LLM stages is expensive; the run-recording activities already retry) and
  in-flight child cancellation.
- Determinism rests on the reduced surface (short-lived, no `patched()`) plus unit tests
  of the pure decision functions; an offline bundle test guards sandbox safety.

## History: the singleton journey (superseded within this record)

The first shape (DAT-529/530) was one long-lived `JourneyWorkflow` per workspace:
signal-driven, `patched()`-gated, bounded by continue-as-new, with a cascade circuit
breaker and pause/resume. DAT-609 replaced it. The one property the singleton had to
provide — per-workspace cross-stage serialization — was already guaranteed by the
versioned-metadata model (atomic promotes plus fail-loud scope resolution), so a resident
coordinator added nothing and taxed every change with `patched()` ordering,
continue-as-new, and committed replay fixtures. With the singleton went the `patched()`
discipline, the `JourneyState` query, the breaker, and the "running workflow adopts new
code on its next signal" deploy model (DAT-567): a short-lived workflow runs the new code
on its next start.
