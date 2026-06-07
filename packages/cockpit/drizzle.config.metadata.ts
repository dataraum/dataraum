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
//                           materialized schema. The URL is augmented with a
//                           ?options=-c%20search_path=<ws_schema> hint so
//                           drizzle-kit introspects the workspace schema as
//                           the default and generates plain pgTable() exports
//                           (no schema-prefixed identifier suffix).
//   DATARAUM_WORKSPACE_ID — the active workspace_id. Schema name is derived
//                           as ws_<id-with-dashes-as-underscores>, matching
//                           engine/server/workspace.py:schema_name_for.

import { defineConfig } from 'drizzle-kit'

const workspaceId = process.env.DATARAUM_WORKSPACE_ID
if (!workspaceId) {
  throw new Error(
    'DATARAUM_WORKSPACE_ID is not set. drizzle-kit pull needs to know which ' +
      'ws_<id> schema to introspect. Set it to the same value the engine ' +
      'control-plane was bootstrapped with.',
  )
}

// The cockpit introspects the promoted-READ schema (ADR-0008/DAT-453): the
// current_* head-joined views + pass-throughs are its entire metadata surface.
// The raw ws_<id> tables are not visible to the cockpit_reader role at runtime,
// so they must not be in the mirror either.
const schemaName = `ws_${workspaceId.replace(/-/g, '_')}_read`

const baseUrl = process.env.METADATA_DATABASE_URL
if (!baseUrl) {
  throw new Error(
    'METADATA_DATABASE_URL is not set. Point it at the engine metadata DB ' +
      '(e.g. the `dataraum` database in the shared Postgres instance) before ' +
      'running drizzle-kit pull.',
  )
}

const url = `${baseUrl}${baseUrl.includes('?') ? '&' : '?'}options=${encodeURIComponent(`-c search_path=${schemaName}`)}`

export default defineConfig({
  dialect: 'postgresql',
  schema: './src/db/metadata/schema.ts',
  out: './src/db/metadata',
  schemaFilter: [schemaName],
  dbCredentials: {
    url,
  },
})
