# Persistence

Where data lives and who may touch it.

## One Postgres, two owners

- One Postgres instance, separate schemas: the engine owns the per-workspace
  `ws_<id>` schema (SQLAlchemy), the cockpit owns `cockpit_db` (Drizzle). The
  schemas are never shared — no shared ORM models, no cross-schema writes.
- The cockpit's metadata connection is the `cockpit_reader` role: SELECT on the
  per-workspace read schema `ws_<id>_read` only. Raw run-stamped tables are not
  visible to it — the wrong query is unwritable, not merely discouraged.
- The one write carve-out is the control-plane surface
  (`storage/read_views.py::_CONTROL_WRITE_GRANTS`): `sources` (column-level
  UPDATE), `config_overlay`, `sql_snippets` — exactly those verbs on exactly
  those tables.

## Reads go through the promoted surface

- Metadata is run-versioned: phases append run-stamped rows; a terminal promote
  flips `metadata_snapshot_head` per (target, stage) atomically; failed runs
  never become visible.
- The read surface is generated, head-joined `current_<table>` views in
  `ws_<id>_read` (`storage/read_views.py`); the head join is written exactly
  once, in DDL. Every run-stamped table must appear on the surface — an
  unclassified one fails the build.
- Engine in-process readers resolve heads through the `head_run_id()`
  chokepoint (`storage/snapshot_head.py`); in-run readers (detectors, loaders)
  use this-run run ids plus a base-run map pinned once at run start. **Views
  are never read inside a run** — promote is terminal, so mid-run the head
  still names the prior run.
- No reader queries raw run-stamped tables for current state. There is no
  metadata API service — do not add one.

## Writers converge under redelivery

- Activities are at-least-once. Postgres owns within-attempt atomicity (a
  failed phase rolls back and persists nothing); writer idempotency owns
  success-redelivery.
- Two sanctioned writer forms: the default is a `(key, run_id)` UNIQUE +
  ON CONFLICT upsert (`storage/upsert.py`) with in-batch dedup; run-scoped
  delete-then-insert is the exception, reserved for producers whose row-set
  legitimately shrinks on redelivery.
- `storage/read_views.py::enforce_run_grain` polices this structurally, at
  workspace boot and in CI: every run-stamped table carries a run-including
  UNIQUE or appears on `_RUN_GRAIN_EXEMPT` with its reason. Never remove a
  delete or skip before its writer has a DB-enforced grain.
- Only metadata is versioned. The physical lake is latest-only
  (`CREATE OR REPLACE` / `DROP`).

## The schema seam is an offline dump

- The cockpit's Drizzle mirror of engine metadata is generated, never
  hand-maintained: SQLAlchemy models → offline DDL dump
  (`packages/engine/schema.sql` + `schema_read.sql`, via `storage/dump_ddl.py`)
  → ephemeral scratch Postgres → `bun run db:pull:metadata`. Never hand-edit
  `schema.sql` or the mirror — re-dump.
- The `schema-drift` CI job (`.github/workflows/ci.yml`) fails when models,
  dump, and mirror disagree.

## A workspace is the unit of isolation

- The cockpit_db `workspaces` registry is the source of truth for which
  workspaces exist; `DATARAUM_WORKSPACE_ID` is a per-container boot identity
  only.
- One workspace = one engine container + one task queue + one DuckLake catalog
  database + one S3 prefix. Two workspaces run side by side with zero
  cross-talk.
- Deleting a workspace is one sweep
  (`packages/infra/scripts/delete-workspace.sh`): container, `ws_<id>` +
  `ws_<id>_read` schemas, catalog DB, S3 prefix, registry rows.

## The lake is DuckLake on S3

- The analytical store is DuckLake: parquet under `s3://<bucket>/<ws>/lake`
  with a per-workspace Postgres catalog database; uploads stage under
  `s3://<bucket>/<ws>/uploads/`. Every stored ref is an `s3://` URI — never a
  local path.
- The object store is S3-compatible (SeaweedFS in compose); credentials are
  plain env (`.env`), not Docker secrets.
- Engine code reaches persistence only through the SQLAlchemy (metadata) and
  DuckDB (lake) seams — nothing addresses the backing store directly, so the
  store swaps under the abstraction.
- The cockpit is a lake **reader**: it ATTACHes the same catalog + data path
  READ_ONLY (`src/duckdb/lake.ts`) and never contends for writes; a read-only
  attach sees the last CHECKPOINTed snapshot.
