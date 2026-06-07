# ADR-0008 — Promoted reads are enforced by the database (head-joined views + grants), not by reader convention

- **Status:** Accepted
- **Date:** 2026-06-07
- **Ticket:** DAT-453 (epic DAT-442)
- **Design doc:** —

## Context

DAT-413/408 made metadata run-versioned: phases append run-stamped rows, a terminal
promote flips `metadata_snapshot_head` per `(target, stage)` atomically, failed runs
never become visible. Reading "current" therefore requires a nontrivial, per-stage
join through the head — today a **convention each reader re-implements**: 6+
hand-rolled resolution sites in the engine (`readiness_context`, `graphs/context`,
`cycles/context`, `validation/resolver`, `entropy/detectors/loaders`,
`typing/recipe`) plus every cockpit Drizzle query against the `ws_<id>` mirror.
Three independent convention misses occurred in one week (DAT-405): `temporal_drift`
read zero records (fail-closed), `slice_variance` lost its role gate (fail-open),
and stale `SliceDefinition`s leaked across runs. Both failure directions are silent
and corrupt measurements. Git avoids this class not by discipline but by chokepoint:
`checkout` materializes the snapshot, after which a wrong read is *inexpressible*.

## Decision

**The promoted-only read surface is a set of generated, head-joined SQL views,
enforced by grants.** Concretely:

1. **Views**: one `current_<table>` view per run-stamped metadata table, living in a
   per-workspace read schema (`ws_<id>_read`). Each view joins its base table to
   `metadata_snapshot_head` on the table's grain and stage (e.g.
   `h.target = 'table:' || c.table_id AND h.stage = 'semantic_per_column' AND
   h.run_id = a.run_id`; session grain via `'session:' || r.session_id`). No
   parameters needed; the hard join is written exactly once, in DDL.
2. **Generated, never hand-maintained**: view DDL is emitted from SQLAlchemy model
   metadata by the same mechanism as `schema.sql` (`dump_ddl`); the `schema-drift`
   CI job polices it.
3. **Enforced by grant**: the cockpit's DB role gets `SELECT` on the read schema
   **only** — raw run-stamped tables are not visible to it, so
   `bun run db:pull:metadata` introspects views only. The wrong query is not
   discouraged; it is unwritable.
4. **Two read modes** (the git frame): *current-state* readers (cockpit, engine
   readers wanting "the promoted state now") use the views — the tracking-branch
   mode. *In-run* readers (detectors, loaders, measurement) use this-run run_ids
   plus a **pinned base-run map resolved once at run start** — the detached-HEAD
   mode (DAT-448). Views are never used inside a run, for a reason stronger than
   torn reads: promote is the *terminal* step, so during `detect_table` /
   `detect_source` the head still names the **prior** run — a view read inside
   detect would return stale metadata on **every** run, not just under races.
5. **No metadata API container.** A second container restates the convention as
   "you must call the API" at the highest cost (HTTP surface back, contracts,
   deployment unit, availability coupling) and does not cover the observed bug
   class — all three misses were engine-internal Python readers. A thin typed TS
   layer over the introspected view schema is fine as ergonomics; it is not the
   enforcement mechanism.

## Consequences

- The head-join exists once in generated DDL; the hand-rolled resolution sites
  migrate to views (current-state readers) or pinned run_ids (in-run readers) and
  the per-site copies get deleted.
- Refines ADR-0002/0003 without reversing them: the integration surface stays
  Postgres + Temporal; the cockpit's Drizzle mirror narrows from raw `ws_<id>`
  tables to the read schema. ADR-0003's "future SQL-DDL artifact may replace
  live-introspection" is partially realized — the read surface becomes generated
  DDL.
- **Forbidden**: new readers querying raw run-stamped tables for current state;
  cockpit grants on the raw schema; views inside detect/measurement runs.
- View coverage tracks the version axis: tables without `run_id`
  (`SliceDefinition`, `ColumnDriftSummary`, `DerivedColumn`) cannot get a
  `current_*` view until DAT-448 stamps them — closing that gap is a prerequisite,
  not a parallel track.
- Follow-ups / risks: verify the cockpit's pinned `drizzle-kit` supports view
  introspection on `pull`; workspace bootstrap must create the read schema and
  grant the cockpit role per new `ws_<id>`; eval's score read moves to
  head-resolved runs (DAT-447 step 0).

## Deviations (2026-06-07, DAT-453 implementation)

- **Engine in-process readers stay on the `head_run_id()` helper seam** rather
  than querying the views. Grants only bind external roles — the engine connects
  as schema owner, so views would give it convention, not enforcement — and view
  SQL would cost typed ORM reads (or a parallel set of hand-maintained view
  models). Decision §4's "engine readers … use the views" is satisfied in
  spirit: one chokepoint per consumer class — `head_run_id()` engine-side,
  `current_*` views for the cockpit (the role grants make those the only
  expressible reads).
- **A minimal control-plane write surface exists alongside the read schema**:
  the cockpit legitimately WRITES three un-versioned control tables (`sources`,
  `investigation_sessions`, `config_overlay` — registering a source, opening a
  session, teaching). The reader role carries exactly those verbs on exactly
  those tables (column-level UPDATE on `sources` for the select upsert); all
  run-stamped tables remain unreachable raw.
- **Dual-grain artifacts carry discriminators**: `entropy_objects` /
  `entropy_readiness` are written by BOTH detect paths, so after
  add_source + begin_session a column has two legitimately-current rows;
  `current_*` exposes `via_table_head` / `via_session_head` for consumers to
  pin a grain.
