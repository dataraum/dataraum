// Cockpit_db Drizzle client — TanStack Start app's own Postgres database.
// Holds the workspaces registry (slice 1), conversations, and ui_state.
//
// Source of truth for the schema lives in ./schema.ts; the engine's metadata
// schema is consumed via ../metadata/client.ts instead.

import { SQL } from "bun";
import { drizzle } from "drizzle-orm/bun-sql";
// Base config, NOT the workspace config: cockpit_db is the shared control
// plane and the PORTAL role needs this client too (DAT-819) — the workspace
// config throws in portal mode.
import { baseConfig } from "../../config.base";

const client = new SQL(baseConfig.cockpitDatabaseUrl);

// The control-plane tables (DAT-461) live in ./schema.ts; callers import the
// table objects directly and use `cockpitDb.insert(...)` / `.select(...)`. We do
// NOT pass `schema` to drizzle() — that only enables the relational query API
// (`db.query.*`), which nothing here uses, and the drizzle 1.0 relations rewrite
// makes the bun-sql `{ client, schema }` overload awkward to type.
export const cockpitDb = drizzle({ client });
