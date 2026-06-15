# ADR-0012 — Per-workspace tenancy: registry source-of-truth, one container/queue/catalog/prefix per workspace

- **Status:** Accepted
- **Date:** 2026-06-15
- **Ticket:** DAT-505 (epic DAT-501)
- **Design doc:** Confluence DD/34045953 §1

## Context

A workspace is the unit of isolation, provisioning, and deployment. Before this,
workspace_id was a decorative routing key threaded into every workflow ID and
re-asserted by 8 copy-pasted per-activity guards in `worker/activity.py`, while
all workspaces shared one Temporal task queue, one DuckLake catalog database, and
one `s3://<bucket>/lake` + `s3://<bucket>/uploads` prefix. The cockpit read the
active workspace from a bare `DATARAUM_WORKSPACE_ID` env var. None of that
actually isolated two workspaces running side by side.

## Decision

The cockpit_db `workspaces` registry is the source of truth for "which
workspace". `DATARAUM_WORKSPACE_ID` demotes to a per-CONTAINER boot identity. A
workspace = ONE engine container + ONE task queue + ONE catalog DB + ONE S3
prefix:

- **Queue:** `engine-<workspace_id>` (`server/workspace.py::task_queue_for`,
  mirrored cockpit-side as `engineTaskQueueFor`). The engine polls exactly its
  own queue; the 4 cockpit drivers route `workflow.start` to it from the registry
  row (`resolveActiveWorkspaceRow().taskQueue`). The 8 per-activity mismatch
  guards collapse to ONE boot-time assertion (env workspace ↔ queue name) in
  `bootstrap_workspace` — a payload for another workspace never reaches this
  worker, so the per-activity defence is redundant.
- **Lake:** per-workspace DuckLake catalog DATABASE + `s3://<bucket>/<ws>/lake`
  (never a shared catalog schema). The cockpit READ_ONLY-ATTACHes the active
  workspace's catalog.
- **Uploads:** staged under `s3://<bucket>/<ws>/uploads/<digest>/<file>`; the
  upload digest salt reads the registry workspace id.
- **Vertical:** `workspaces.vertical` (a WORKSPACE property) + a boot-read of it.
  DAT-505 adds the CAPABILITY only; retiring the per-add_source vertical channel
  (payload field + session-row read + cockpit `select` pick) is DAT-506.
- **Compose:** the engine-worker is parameterized per workspace via YAML anchors;
  a second workspace lives behind the `multi-workspace` profile.
- **Deletion sweep:** `packages/infra/scripts/delete-workspace.sh` — stop
  container → drop `ws_<id>` + `ws_<id>_read` schemas → drop catalog DB → delete
  the `s3://<bucket>/<ws>/` prefix → cockpit_db rows (or `--soft` → `archived_at`).

## Consequences

- Two workspaces run side by side with zero cross-talk: distinct queues, schemas,
  catalog DBs, and S3 prefixes. "Create a workspace" in dev = a registry insert +
  a compose service that derives the four routing knobs from its workspace id.
- The engine's workspace-isolation surface is one boot assertion, not 8 scattered
  guards. A misconfigured container (queue ↔ workspace mismatch) fails loud at
  boot, before it advertises itself as polling.
- READ-path workspace SCHEMA scoping in the cockpit (`write-surface.ts`,
  `query.ts`, `snippet-search.ts`) still resolves from the env-designated
  workspace — correct in single-active-workspace; the per-request switcher across
  workspaces is DAT-357.
- `TEMPORAL_TASK_QUEUE` is no longer a routing input (per-workspace queues replace
  it); the engine still validates it as a Settings field.
