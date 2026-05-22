// Cockpit_db Drizzle config — push/generate against the cockpit's own
// Postgres database. Hand-written schema is the source of truth here.
//
// Usage:
//   pnpm exec drizzle-kit push     --config drizzle.config.cockpit.ts
//   pnpm exec drizzle-kit generate --config drizzle.config.cockpit.ts
//
// Env: COCKPIT_DATABASE_URL must point at the cockpit_db database in the
// shared Postgres instance (separate from the engine's `dataraum` and
// `dataraum_lake_catalog` databases).

import { defineConfig } from 'drizzle-kit'

const url = process.env.COCKPIT_DATABASE_URL
if (!url) {
  throw new Error(
    'COCKPIT_DATABASE_URL is not set. Set it to the cockpit_db connection ' +
      'string before running drizzle-kit push/generate.',
  )
}

export default defineConfig({
  dialect: 'postgresql',
  schema: './src/db/cockpit/schema.ts',
  out: './drizzle/cockpit',
  dbCredentials: { url },
})
