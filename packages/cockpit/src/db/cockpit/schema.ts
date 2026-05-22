// cockpit_db schema — placeholder.
//
// Owned by TanStack Start via Drizzle ORM. Lives in its own Postgres database
// inside the shared Postgres instance (separate from the engine's `dataraum`
// and `dataraum_lake_catalog` databases).
//
// Tables land here when they're actually needed:
//   - workspaces                              (DAT-339 pivot, slice 1)
//   - conversations / conversation_messages   (chat persistence)
//   - ui_state / ui_preferences               (future)
//   - admin_*                                 (future)
//
// Source of truth: this file. Migrations land in ../../../drizzle/cockpit/
// via `drizzle-kit generate --config drizzle.config.cockpit.ts`.

export {};
