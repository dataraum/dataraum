// Cockpit_db Drizzle client — TanStack Start app's own Postgres database.
// Holds the workspaces registry (slice 1), conversations, and ui_state.
//
// Source of truth for the schema lives in ./schema.ts; the engine's metadata
// schema is consumed via ../metadata/client.ts instead.

import { drizzle } from "drizzle-orm/postgres-js";
import postgres from "postgres";

const connectionString = process.env.COCKPIT_DATABASE_URL;

if (!connectionString) {
	throw new Error(
		"COCKPIT_DATABASE_URL is not set. Point it at the cockpit_db database in the shared Postgres instance.",
	);
}

const client = postgres(connectionString, { prepare: false });

// schema is intentionally not passed yet — ./schema.ts is a placeholder. Once
// real tables land (e.g. workspaces registry), pass `{ client, schema }`.
export const cockpitDb = drizzle({ client });
