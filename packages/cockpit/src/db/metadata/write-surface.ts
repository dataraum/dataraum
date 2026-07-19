// Control-plane WRITE surface on the raw ws_<id> schema (ADR-0008 / DAT-453).
//
// The generated mirror (./schema.ts) introspects the promoted-READ schema —
// views only, by design: the reader role cannot SELECT raw run-stamped
// tables. But four un-versioned CONTROL tables are deliberate cockpit writes:
//
//   sources         — register a data source before addSourceWorkflow
//   config_overlay  — the teach vocabulary IS overlay rows
//   concepts        — the typed concept vocabulary `frame` declares/edits (DAT-728)
//   sql_snippets    — save-on-clean grows the snippet library (DAT-486)
//
// (DAT-506 removed `investigation_sessions` — the engine dropped the table + its
// write grant; run-grouping is the cockpit's concern, in cockpit_db.)
//
// These tables ride `metadataWriteDb` (./client.ts) — the per-workspace WRITER
// role, whose `search_path` the engine bootstrap pins to the raw ws_<id> schema
// (DAT-816). The role resolves the schema, so the declarations are UNQUALIFIED
// plain pgTable()s and no workspace literal exists here. The bootstrap grants
// the writer exactly these verbs on exactly these tables
// (storage/read_views.py::_CONTROL_WRITE_GRANTS); everything else raw stays
// unreachable. Hand-declared (pull can't see them) with only the columns the
// cockpit writes/returns — the engine's SQLAlchemy models stay the source of
// truth for the full shapes.

import {
	integer,
	json,
	jsonb,
	pgTable,
	text,
	timestamp,
	varchar,
} from "drizzle-orm/pg-core";

/** Raw `sources` — INSERT (+ RETURNING) when registering a source. */
export const sourcesWrite = pgTable("sources", {
	sourceId: varchar("source_id").primaryKey(),
	name: varchar("name").notNull(),
	sourceType: varchar("source_type").notNull(),
	connectionConfig: jsonb("connection_config"),
	createdAt: timestamp("created_at", { mode: "date" }).notNull(),
	updatedAt: timestamp("updated_at", { mode: "date" }).notNull(),
	stage: varchar("stage"),
	backend: varchar("backend"),
	discoveredSchema: jsonb("discovered_schema"),
	archivedAt: timestamp("archived_at", { mode: "date" }),
});

/** Raw `config_overlay` — INSERT (teach) + UPDATE (supersede). No `session_id`
 * post-DAT-506 (the overlay vocabulary is workspace-scoped via the ws_<id> schema). */
export const configOverlayWrite = pgTable("config_overlay", {
	overlayId: varchar("overlay_id").primaryKey(),
	type: varchar("type").notNull(),
	payload: jsonb("payload").notNull(),
	createdAt: timestamp("created_at", { mode: "date" }).notNull(),
	supersededAt: timestamp("superseded_at", { mode: "date" }),
});

/** Raw `concepts` — the typed concept vocabulary (DAT-728, config→DB). `frame`
 * declares/edits concepts as an edit = supersede active (UPDATE superseded_at) +
 * INSERT a new active row; the readiness count SELECTs active rows. Only the
 * columns the cockpit writes/reads — the engine SQLAlchemy `Concept` model owns
 * the full shape (identity `concept_id` minted here as a uuid; `source='frame'`).
 * The list columns are engine `JSON` (not JSONB). */
export const conceptsWrite = pgTable("concepts", {
	conceptId: varchar("concept_id").primaryKey(),
	vertical: varchar("vertical").notNull(),
	name: varchar("name").notNull(),
	kind: varchar("kind").notNull(),
	description: text("description"),
	indicators: json("indicators").$type<string[]>(),
	excludePatterns: json("exclude_patterns").$type<string[]>(),
	unitFromConcept: varchar("unit_from_concept"),
	source: varchar("source"),
	createdAt: timestamp("created_at", { mode: "date" }).notNull(),
	supersededAt: timestamp("superseded_at", { mode: "date" }),
});

/**
 * Raw `sql_snippets` — SELECT (IS-NULL-aware dedup lookup) + INSERT a learned
 * `query:` snippet on a clean run (save-on-clean, DAT-486). Only the columns the
 * writer touches; the engine SQLAlchemy model owns the full shape. The NOT-NULL
 * columns with no DB default — `description`, `execution_count`,
 * `failure_count`, `created_at`, `updated_at` — must be set on every insert
 * (the model's defaults are ORM-side, not server defaults). Identity/quality columns the
 * cockpit never writes (provenance, input_fields, normalized_expression) are
 * omitted.
 */
export const sqlSnippetsWrite = pgTable("sql_snippets", {
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
	source: varchar("source").notNull(),
	executionCount: integer("execution_count").notNull(),
	failureCount: integer("failure_count").notNull(),
	createdAt: timestamp("created_at", { mode: "date" }).notNull(),
	updatedAt: timestamp("updated_at", { mode: "date" }).notNull(),
});
