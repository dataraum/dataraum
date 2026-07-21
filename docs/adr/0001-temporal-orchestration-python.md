# ADR-0001 — Temporal orchestration: Python workflows + activities on one worker

- **Status:** Accepted (2026-07-07 — [ADR-0020](./0020-workflows-python-cockpit-activity-only.md) restored this ADR's original scope; the [ADR-0014](./0014-cockpit-orchestration-worker.md) amendment is void)
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
- **Amendment history:** [ADR-0014](./0014-cockpit-orchestration-worker.md) scoped this ADR to analysis and let the cockpit author orchestration workflows on a co-located TS worker. [ADR-0020](./0020-workflows-python-cockpit-activity-only.md) reversed that: all workflows are Python on the engine worker again, and the cockpit worker is activity-only. The Decision above holds unscoped — the cockpit authors no workflows of any kind.
