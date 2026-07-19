// Engine metadata Drizzle config — pull-only. The engine owns the SQLAlchemy
// schema; the cockpit re-derives matching Drizzle types by introspection.
// We do NOT push from here — the engine is the source of truth.
//
// Usage — don't run drizzle-kit directly; the wrapper provisions everything
// (offline DDL dump from the SQLAlchemy models → scratch Postgres → pull;
// no running stack, no engine boot):
//   bun run db:pull:metadata        (scripts/pull-metadata.sh)
//
// Env (set by the wrapper):
//   METADATA_DATABASE_URL — postgres URL of the scratch DB holding the
//                           materialized schema.
//
// Workspace-neutral (DAT-816): the wrapper materializes the promoted-READ
// surface (ADR-0008/DAT-453) — the current_* head-joined views + pass-throughs,
// the cockpit's entire metadata surface — into the scratch `public` schema, so
// the pull emits plain unqualified pgView() exports. At runtime the reader
// ROLE's search_path resolves which ws_<id>_read schema those names hit; no
// workspace id exists anywhere in this chain.

import { defineConfig } from 'drizzle-kit'

const url = process.env.METADATA_DATABASE_URL
if (!url) {
  throw new Error(
    'METADATA_DATABASE_URL is not set. Run `bun run db:pull:metadata` — the ' +
      'wrapper provisions the scratch Postgres and sets the URL.',
  )
}

export default defineConfig({
  dialect: 'postgresql',
  schema: './src/db/metadata/schema.ts',
  out: './src/db/metadata',
  schemaFilter: ['public'],
  dbCredentials: {
    url,
  },
})
