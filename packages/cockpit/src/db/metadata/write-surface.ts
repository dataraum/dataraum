// Control-plane WRITE surface on the raw ws_<id> schema (ADR-0008 / DAT-453).
//
// The generated mirror (./schema.ts) introspects the promoted-READ schema —
// views only, by design: the cockpit_reader role cannot SELECT raw run-stamped
// tables. But three un-versioned CONTROL tables are deliberate cockpit writes:
//
//   sources         — register a data source before addSourceWorkflow
//   config_overlay  — the teach vocabulary IS overlay rows
//   sql_snippets    — save-on-clean grows the snippet library (DAT-486)
//
// (DAT-506 removed `investigation_sessions` — sessions live in cockpit_db now,
// and the engine dropped the table + its write grant.)
//
// The engine bootstrap grants the reader role exactly these verbs on exactly
// these tables (storage/read_views.py::_CONTROL_WRITE_GRANTS); everything else
// raw stays unreachable. Hand-declared here (pull can't see them anymore) with
// only the columns the cockpit writes/returns — the engine's SQLAlchemy models
// stay the source of truth for the full shapes.

import {
	integer,
	json,
	jsonb,
	pgSchema,
	text,
	timestamp,
	varchar,
} from "drizzle-orm/pg-core";

import { config } from "../../config";

// Unit tests stub `#/config` with an empty object (vitest.config.ts contract),
// so resolve the id defensively at module load; real boots always carry it
// (zod-required) and the fallback matches the bootstrap workspace id.
//
// Read-path workspace scoping (DAT-505 boundary): WHICH ws_<id> schema to read is
// resolved from the env-designated workspace here, NOT the cockpit_db registry.
// In single-active-workspace the env id == the registry row id, so this is
// correct. It binds the `pgSchema()` table objects at MODULE LOAD, which cannot
// await a registry lookup — moving read-scoping to per-request registry
// resolution is the DAT-357 switcher. DAT-505 only routed the WRITE/ROUTING side
// (queue/container/S3 prefix) through the registry.
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

/** Raw `config_overlay` — INSERT (teach) + UPDATE (supersede). No `session_id`
 * post-DAT-506 (the overlay vocabulary is workspace-scoped via the ws_<id> schema). */
export const configOverlayWrite = rawSchema.table("config_overlay", {
	overlayId: varchar("overlay_id").primaryKey(),
	type: varchar("type").notNull(),
	payload: jsonb("payload").notNull(),
	createdAt: timestamp("created_at", { mode: "date" }).notNull(),
	supersededAt: timestamp("superseded_at", { mode: "date" }),
});

/**
 * Raw `sql_snippets` — SELECT (IS-NULL-aware dedup lookup) + INSERT a learned
 * `query:` snippet on a clean run (save-on-clean, DAT-486). Only the columns the
 * writer touches; the engine SQLAlchemy model owns the full shape. The NOT-NULL
 * columns with no DB default — `description`, `column_mappings`, `execution_count`,
 * `failure_count`, `created_at`, `updated_at` — must be set on every insert
 * (the model's defaults are ORM-side, not server defaults). `column_mappings`
 * is `json` (not `jsonb`) to match the column type. Identity/quality columns the
 * cockpit never writes (last_used_at, column_hash, provenance, input_fields,
 * normalized_expression) are omitted.
 */
export const sqlSnippetsWrite = rawSchema.table("sql_snippets", {
	snippetId: varchar("snippet_id").primaryKey(),
	workspaceId: varchar("workspace_id").notNull(),
	snippetType: varchar("snippet_type").notNull(),
	standardField: varchar("standard_field"),
	statement: varchar("statement"),
	aggregation: varchar("aggregation"),
	schemaMappingId: varchar("schema_mapping_id").notNull(),
	parameterValue: varchar("parameter_value"),
	sql: text("sql").notNull(),
	description: text("description").notNull(),
	columnMappings: json("column_mappings").notNull(),
	source: varchar("source").notNull(),
	llmModel: varchar("llm_model"),
	executionCount: integer("execution_count").notNull(),
	failureCount: integer("failure_count").notNull(),
	createdAt: timestamp("created_at", { mode: "date" }).notNull(),
	updatedAt: timestamp("updated_at", { mode: "date" }).notNull(),
});
