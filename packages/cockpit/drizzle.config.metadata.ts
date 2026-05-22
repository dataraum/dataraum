// Engine metadata Drizzle config — pull-only against the active workspace's
// Postgres schema in the engine substrate (DAT-339 pivot, slice 1).
//
// The engine owns the SQLAlchemy schema; the cockpit re-derives matching
// Drizzle types by introspection. We do NOT push from here — the engine is
// the source of truth for these tables.
//
// Usage:
//   pnpm exec drizzle-kit pull --config drizzle.config.metadata.ts
//
// Env:
//   METADATA_DATABASE_URL — postgres URL pointing at the engine's metadata DB
//                           (same instance as the engine; e.g. .../dataraum).
//                           The connection URL is augmented with a
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

const schemaName = `ws_${workspaceId.replace(/-/g, '_')}`

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
