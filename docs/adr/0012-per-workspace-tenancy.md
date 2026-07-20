# ADR-0012 — Per-workspace tenancy: registry source-of-truth, one container/queue/catalog/prefix per workspace

- **Status:** Accepted — lake clause amended 2026-07-19 by DAT-815 (epic DAT-813,
  design doc Confluence DD/51740673): the per-workspace catalog DATABASE became a
  per-workspace catalog SCHEMA (`METADATA_SCHEMA`) in ONE installation-wide
  catalog database. Isolation properties unchanged (independent snapshot chains
  per schema — spike DAT-814); provisioning/teardown became transactional SQL.
- **Date:** 2026-06-15
- **Ticket:** DAT-505 (epic DAT-501); lake clause revised by DAT-815 (epic DAT-813)
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
- **Lake** (as amended by DAT-815): per-workspace DuckLake catalog SCHEMA —
  `ws_<id>` inside the ONE installation-wide catalog database, selected via
  `METADATA_SCHEMA` on the ATTACH and created by the ATTACH itself — plus
  `s3://<bucket>/<ws>/lake`. The cockpit READ_ONLY-ATTACHes the active
  workspace's catalog schema. (DAT-505 originally allocated a catalog DATABASE
  per workspace; the DAT-814 spike verified schemas give the same isolation —
  independent snapshot chains — with transactional provisioning.)
- **Uploads:** staged under `s3://<bucket>/<ws>/uploads/<digest>/<file>`; the
  upload digest salt reads the registry workspace id.
- **Vertical:** `workspaces.vertical` (a WORKSPACE property) + a boot-read of it.
  DAT-505 adds the CAPABILITY only; retiring the per-add_source vertical channel
  (payload field + session-row read + cockpit `select` pick) is DAT-506.
- **Compose:** defines exactly one engine-worker + cockpit pair — the bootstrap
  workspace. Every further workspace is created by the provisioner, which clones
  that pair's container config (DAT-820).
- **Deletion sweep:** the provisioner's archive operation (DAT-820, cockpit
  `src/portal/lifecycle.ts`; it retired the `delete-workspace.sh` script) —
  stop/remove the pair → drop `ws_<id>` + `ws_<id>_read` schemas → drop the
  per-workspace roles → drop the `ws_<id>` catalog schema in the shared
  catalog DB (DAT-815) → delete the `s3://<bucket>/<ws>/` prefix → registry
  `state = 'archived'` (control-plane rows remain as the record).

## Consequences

- Two workspaces run side by side with zero cross-talk: distinct queues, schemas,
  catalog schemas, and S3 prefixes. "Create a workspace" in dev = a registry
  insert + a compose service that derives the three routing knobs (workspace id,
  queue, lake prefix) from its workspace id — the catalog URL is installation-wide
  common env since DAT-815.
- The engine's workspace-isolation surface is one boot assertion, not 8 scattered
  guards. A misconfigured container (queue ↔ workspace mismatch) fails loud at
  boot, before it advertises itself as polling.
- READ-path workspace SCHEMA scoping in the cockpit (`write-surface.ts`,
  `query.ts`, `snippet-search.ts`) still resolves from the env-designated
  workspace — correct in single-active-workspace; the per-request switcher across
  workspaces is DAT-357.
- `TEMPORAL_TASK_QUEUE` is no longer a routing input (per-workspace queues replace
  it); the engine still validates it as a Settings field.
