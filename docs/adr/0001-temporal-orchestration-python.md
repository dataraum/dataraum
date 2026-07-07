# ADR-0001 — Temporal orchestration: Python workflows + activities on one worker

- **Status:** Accepted (amended by [ADR-0014](./0014-cockpit-orchestration-worker.md); scaled per-workspace by [ADR-0012](./0012-per-workspace-tenancy.md) — "one worker, one task queue" now holds *per workspace*, `engine-<workspace_id>`)
- **Date:** 2026-05-25
- **Ticket:** DAT-344 (built), DAT-360 (locked the direction)
- **Design doc:** Confluence DD space

## Context

Long-running engine work (ingest, the detection pipeline, per-table fan-out) needs durable
execution: retries, visibility, resumption across worker restarts. An earlier direction
(DAT-360) put **workflows in TypeScript** in the cockpit. That split the durable logic
across two languages and two deploy units, and pulled orchestration away from the code it
orchestrates (the Python pipeline).

## Decision

Use **Temporal** for orchestration. **Both workflows and activities are Python**, bundled
on **one engine worker** on a single task queue. The cockpit is a **Client only**: it
triggers workflows by name (`@temporalio/client`) and renders progress. The cockpit does
**not** author or host workflows. This reverses DAT-360.

## Consequences

- Durable logic lives next to the pipeline it drives; one language, one worker to deploy.
- The cockpit↔engine contract is "trigger by name + read progress", not shared workflow code — but the cockpit **hand-mirrors** the worker's Temporal contracts (`worker/contracts.py` → cockpit `types.ts`), so changing a workflow's signature/return shape is a cross-package change.
- Determinism is verified offline via Temporal's `Replayer`; tests use **testcontainers**, not the bundled `WorkflowEnvironment` test server (it stalls CI).
- Retires: TS-authored workflows; any second orchestration mechanism (the hand-rolled scheduler/monitoring was removed in DAT-369).
- **Amended by [ADR-0014](./0014-cockpit-orchestration-worker.md):** scoped — **analysis** workflows stay Python on the engine worker (this ADR holds for them). The cockpit now also authors and hosts short-lived **orchestration** workflows on its own co-located TS worker (`cockpit-orchestration` queue), which drive the engine analysis workflows as cross-language children. "The cockpit does not author workflows" is now true only of *analysis*.
