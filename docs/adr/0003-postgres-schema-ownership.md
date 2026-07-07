# ADR-0003 — Postgres schema ownership: engine `ws_<id>`, cockpit `cockpit_db`, never shared

- **Status:** Accepted (refined by [ADR-0008](./0008-promoted-read-views.md) — the "future SQL-DDL artifact" consequence is realized: the mirror regenerates from the offline `schema.sql`/`schema_read.sql` dump against a scratch Postgres (`packages/cockpit/scripts/pull-metadata.sh`), introspecting `ws_<id>_read`, not a live workspace DB)
- **Date:** 2026-05-25
- **Ticket:** DAT-321 (unified substrate)
- **Design doc:** Confluence DD space

## Context

Engine and cockpit both need persistence and both need to see *some* of the same data
(the cockpit renders engine metadata). The temptation is a shared schema or shared ORM
models. That couples two languages' migration cycles and makes either side's refactor a
breaking change for the other.

## Decision

**One Postgres instance, separate schemas.** The engine owns `ws_<id>` (per-workspace),
managed with **SQLAlchemy**. The cockpit owns `cockpit_db`, managed with **Drizzle**. The
schemas are **never shared** and neither side writes the other's tables. The cockpit reads
engine metadata by **live-introspecting** `ws_<id>` into a Drizzle mirror
(`db:pull:metadata`) — a read-only, regenerated view, not a hand-maintained duplicate.

## Consequences

- Each side migrates independently; ownership is unambiguous.
- The Drizzle mirror is an **intermediate** state: regen against a *fresh* DB (`create_all` is additive, so a reused volume captures stale tables). Dropping an engine table is a cross-package change (re-pull the mirror).
- A future SQL-DDL artifact may replace live-introspection; until then, the mirror is generated, never edited by hand.
- Retires: any shared-schema or shared-ORM-model approach.
