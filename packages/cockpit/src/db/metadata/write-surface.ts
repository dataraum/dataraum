// Control-plane WRITE surface on the raw ws_<id> schema (ADR-0008 / DAT-453).
//
// The generated mirror (./schema.ts) introspects the promoted-READ schema —
// views only, by design: the cockpit_reader role cannot SELECT raw run-stamped
// tables. But three un-versioned CONTROL tables are deliberate cockpit writes:
//
//   sources                — register a data source before addSourceWorkflow
//   investigation_sessions — open the session a workflow runs under
//   config_overlay         — the teach vocabulary IS overlay rows
//
// The engine bootstrap grants the reader role exactly these verbs on exactly
// these tables (storage/read_views.py::_CONTROL_WRITE_GRANTS); everything else
// raw stays unreachable. Hand-declared here (pull can't see them anymore) with
// only the columns the cockpit writes/returns — the engine's SQLAlchemy models
// stay the source of truth for the full shapes.

import {
	integer,
	jsonb,
	pgSchema,
	timestamp,
	varchar,
} from "drizzle-orm/pg-core";

import { config } from "../../config";

// Unit tests stub `#/config` with an empty object (vitest.config.ts contract),
// so resolve the id defensively at module load; real boots always carry it
// (zod-required) and the fallback matches the bootstrap workspace id.
const workspaceId: string =
	config.dataraumWorkspaceId ?? "00000000-0000-0000-0000-000000000001";
const rawSchemaName = `ws_${workspaceId.replaceAll("-", "_")}`;
const rawSchema = pgSchema(rawSchemaName);

/** Raw `sources` — INSERT (+ RETURNING) when registering a source. */
export const sourcesWrite = rawSchema.table("sources", {
	sourceId: varchar("source_id").primaryKey(),
	name: varchar("name").notNull(),
	sourceType: varchar("source_type").notNull(),
	connectionConfig: jsonb("connection_config"),
	createdAt: timestamp("created_at", { mode: "date" }).notNull(),
	updatedAt: timestamp("updated_at", { mode: "date" }).notNull(),
	status: varchar("status"),
	stage: varchar("stage"),
	backend: varchar("backend"),
	discoveredSchema: jsonb("discovered_schema"),
	archivedAt: timestamp("archived_at", { mode: "date" }),
});

/** Raw `investigation_sessions` — INSERT when opening a session. */
export const investigationSessionsWrite = rawSchema.table(
	"investigation_sessions",
	{
		sessionId: varchar("session_id").primaryKey(),
		status: varchar("status").notNull(),
		startedAt: timestamp("started_at", { mode: "date" }).notNull(),
		endedAt: timestamp("ended_at", { mode: "date" }),
		intent: varchar("intent").notNull(),
		contract: varchar("contract"),
		vertical: varchar("vertical"),
		outcomeSummary: varchar("outcome_summary"),
		outcomePayload: jsonb("outcome_payload"),
		stepCount: integer("step_count").notNull(),
	},
);

/** Raw `config_overlay` — INSERT (teach) + UPDATE (supersede). */
export const configOverlayWrite = rawSchema.table("config_overlay", {
	overlayId: varchar("overlay_id").primaryKey(),
	sessionId: varchar("session_id"),
	type: varchar("type").notNull(),
	payload: jsonb("payload").notNull(),
	createdAt: timestamp("created_at", { mode: "date" }).notNull(),
	supersededAt: timestamp("superseded_at", { mode: "date" }),
});
