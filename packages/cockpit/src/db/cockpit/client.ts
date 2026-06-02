// Cockpit_db Drizzle client — TanStack Start app's own Postgres database.
// Holds the workspaces registry (slice 1), conversations, and ui_state.
//
// Source of truth for the schema lives in ./schema.ts; the engine's metadata
// schema is consumed via ../metadata/client.ts instead.

import { SQL } from "bun";
import { drizzle } from "drizzle-orm/bun-sql";
import { config } from "../../config";

const client = new SQL(config.cockpitDatabaseUrl);

// schema is intentionally not passed yet — ./schema.ts is a placeholder. Once
// real tables land (e.g. workspaces registry), pass `{ client, schema }`.
export const cockpitDb = drizzle({ client });
