// Cockpit_db Drizzle client — TanStack Start app's own Postgres database.
// Holds the workspaces registry (slice 1), conversations, and ui_state.
//
// Source of truth for the schema lives in ./schema.ts; the engine's metadata
// schema is consumed via ../metadata/client.ts instead.

import { drizzle } from "drizzle-orm/postgres-js";
import postgres from "postgres";

import { config } from "../../config";

const client = postgres(config.cockpitDatabaseUrl, { prepare: false });

// schema is intentionally not passed yet — ./schema.ts is a placeholder. Once
// real tables land (e.g. workspaces registry), pass `{ client, schema }`.
export const cockpitDb = drizzle({ client });
