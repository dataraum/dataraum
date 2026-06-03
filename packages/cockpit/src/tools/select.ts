// select tool (DAT-398) — the agent-tier step that turns a connected source +
// the user's subset choice into a real `sources` row, advancing the onboarding
// cursor to `add_source`.
//
// This is the FIRST cockpit writer of the engine-owned `ws_<id>.sources` table.
// Nothing upstream creates the Source row: `connect` is read-only and `frame`
// writes concept overlays, not a Source. The engine import phase explicitly
// assumes "the workflow caller — the cockpit — wrote it before triggering
// addSourceWorkflow" (import_phase.py). So `select` OWNS the INSERT: it writes
// the Source at `stage='add_source'` (the cursor the journey readiness reads),
// via the SAME metadata-client cross-schema write seam `teach`/`frame` use — the
// documented policy break (the metadata client is otherwise read-only; the
// engine owns the schema, and these onboarding writes flow through this one
// seam).
//
// It does NOT trigger `addSourceWorkflow` — that is the future `add_source` tool
// (engine ingest, DAT-389). `select` only persists + advances the cursor.
//
// Dispatch is on `ConnectSchema.sourceKind`:
//   - file:     persist `connection_config.file_uris` (the single connect URI,
//               or a prefix enumerated to N concrete URIs via
//               `enumeratePrefixUris`) + a suffix-derived `source_type`.
//               Duplicate basenames are REJECTED before persisting (the engine
//               fails loud on colliding `<source>__<stem>` raw tables).
//   - database: persist `source_type='db_recipe'`, the `backend` COLUMN, and
//               `connection_config.tables` synthesized from the picked tables.
//
// `needsApproval: true` — it mutates workspace state (creates/updates a Source
// row), so the SDK pauses for the user exactly like `teach`/`frame`/`replay`.

import { randomUUID } from "node:crypto";
import { toolDefinition } from "@tanstack/ai";
import { z } from "zod";

import { config } from "../config";
import { metadataDb } from "../db/metadata/client";
import { sources } from "../db/metadata/schema";
import { ConnectSchema } from "../duckdb/connect";
import { SUPPORTED_BACKENDS } from "../duckdb/probe";
import { enumeratePrefixUris } from "../select/enumerate";
import {
	connectTablesToRecipeTables,
	duplicateBasenames,
	sourceTypeForUris,
} from "../select/mappers";

// The engine's source-name rule (`sources/manager.py` `_NAME_PATTERN`): lowercase,
// starts with a letter, 2–49 chars of `[a-z0-9_]`. The persisted name must match
// it (the engine credential lookup `DATARAUM_<NAME>_URL` and raw-table prefix
// `<name>__` both key off it) and is UNIQUE (`uq_sources_name`).
const SOURCE_NAME_PATTERN = /^[a-z][a-z0-9_]{1,48}$/;

// The onboarding stage `select` leaves the Source at. The cockpit drives a
// source `connect → frame → select → add_source` BEFORE the workflow triggers;
// `select` writes the row already at `add_source`, the next interactive stage.
const STAGE_AFTER_SELECT = "add_source";

// Initial source status. Mirrors the seed in the integration driver
// (`temporal/drive-add-source.ts`): a source the cockpit has registered but not
// yet imported reads `configured`.
const INITIAL_STATUS = "configured";

// The default vertical when none is chosen — the unnamed cold-start ontology.
const DEFAULT_VERTICAL = "_adhoc";
// A chosen vertical keys the engine's `verticals/<name>` config resolution, so a
// supplied name must be a safe segment + engine-valid (lowercase, starts with a
// letter) — or the `_adhoc` default (exempt: the built-in leading-underscore key).
const VERTICAL_NAME_PATTERN = /^[a-z][a-z0-9_]{1,48}$/;

/** The vertical add_source will ground against — a builtin the user adopted
 * (e.g. finance), a vertical just framed, or `_adhoc`. Echoed in the result so
 * the add_source trigger seeds the session with it (it isn't persisted on the
 * source row — the conversation carries it). */
function resolveVertical(name?: string | null): string {
	const trimmed = name?.trim();
	if (!trimmed || trimmed === DEFAULT_VERTICAL) return DEFAULT_VERTICAL;
	if (!VERTICAL_NAME_PATTERN.test(trimmed)) {
		throw new Error(
			`Invalid vertical '${trimmed}'. Must match ${VERTICAL_NAME_PATTERN.source} ` +
				"(lowercase, start with a letter, 2–49 chars of [a-z0-9_]) or be '_adhoc'.",
		);
	}
	return trimmed;
}

/** The persisted Source descriptor `select` returns (and the canvas renders). */
export const SelectResult = z.object({
	source_id: z.string(),
	name: z.string(),
	source_type: z.string(),
	backend: z.string().nullable(),
	stage: z.string(),
	// The vertical add_source will ground against (adopted builtin / framed /
	// `_adhoc`). The trigger reads it off the selection to seed the session.
	vertical: z.string(),
	// The concrete file URIs persisted (file source), else null (db source).
	file_uris: z.array(z.string()).nullable(),
	// The synthesized recipe tables persisted (db source), else null (file).
	recipe_tables: z
		.array(z.object({ name: z.string(), sql: z.string() }))
		.nullable(),
});
export type SelectResult = z.infer<typeof SelectResult>;

export interface SelectInput {
	source_name: string;
	schema: ConnectSchema;
	// File source: an explicit list of `s3://` URIs to register as ONE multi-file
	// source (DAT-391: several files uploaded together). Takes precedence over
	// `prefix` — the client already holds the staged URIs, so no S3 re-listing.
	// Ignored for a database source.
	file_uris?: string[] | null;
	// File source: optional `s3://<bucket>/<prefix>` to enumerate into a multi-URI
	// `file_uris` list. Omitted → the single connect URI (`schema.source`) is the
	// one file persisted. Ignored for a database source.
	prefix?: string | null;
	// Database source: the picked subset of `schema.tables[].name` (display
	// names). Omitted/empty → every table in the schema is selected.
	table_names?: string[] | null;
	// Database backend, persisted as the `backend` COLUMN (required for db sources;
	// the engine import fails loud without it). For a file source it is ignored.
	backend?: string | null;
	// The vertical add_source grounds against: a builtin the user adopted (e.g.
	// finance), a vertical just framed (the SAME `vertical_name` passed to frame),
	// or omitted → `_adhoc`. Echoed in the result for the trigger; not persisted.
	vertical?: string | null;
	session_id?: string | null;
}

/** Build the file-source `connection_config.file_uris` list for a connect
 * schema. Precedence: an explicit `fileUris` list (DAT-391 — files uploaded
 * together; the client already holds them) → a `prefix` enumerated to its
 * concrete URIs → the single connect URI. `enumerate` is injected so the unit
 * test exercises the prefix mapping without a live bucket. */
async function resolveFileUris(
	schema: ConnectSchema,
	opts: { fileUris?: string[] | null; prefix?: string | null },
	enumerate: typeof enumeratePrefixUris,
): Promise<string[]> {
	if (opts.fileUris && opts.fileUris.length > 0) {
		// Register the staged URIs directly — no S3 re-listing. Sorted for a
		// deterministic persisted artifact, matching the prefix-enumeration path.
		return [...opts.fileUris].sort();
	}
	if (opts.prefix) {
		return enumerate(config.s3Bucket, opts.prefix);
	}
	return [schema.source];
}

/**
 * Persist (UPSERT) a `sources` row for the selected subset and advance its
 * onboarding cursor to `add_source`. Returns the persisted descriptor.
 *
 * The write keys on the UNIQUE `name`: a fresh source INSERTs; re-selecting the
 * same name re-writes its `connection_config` / `source_type` / `backend` /
 * `stage` (an idempotent re-select, not a duplicate-name error). `enumerate` is
 * injected for testability; the default is the real `enumeratePrefixUris`.
 */
export async function select(
	input: SelectInput,
	enumerate: typeof enumeratePrefixUris = enumeratePrefixUris,
): Promise<SelectResult> {
	const name = input.source_name;
	if (!SOURCE_NAME_PATTERN.test(name)) {
		throw new Error(
			`Invalid source name '${name}'. Must match ${SOURCE_NAME_PATTERN.source} ` +
				"(lowercase, start with a letter, 2–49 chars of [a-z0-9_]).",
		);
	}
	const vertical = resolveVertical(input.vertical);
	const schema = ConnectSchema.parse(input.schema);

	let sourceType: string;
	let backend: string | null;
	let connectionConfig: Record<string, unknown>;
	let fileUris: string[] | null = null;
	let recipeTables: { name: string; sql: string }[] | null = null;

	if (schema.sourceKind === "file") {
		const uris = await resolveFileUris(
			schema,
			{ fileUris: input.file_uris, prefix: input.prefix },
			enumerate,
		);
		const dupes = duplicateBasenames(uris);
		if (dupes.length > 0) {
			throw new Error(
				`Selected files collide on the same raw table(s): ${dupes.join(", ")}. ` +
					"Each file must have a distinct basename — rename or drop the duplicates " +
					"before importing.",
			);
		}
		sourceType = sourceTypeForUris(uris);
		backend = null;
		fileUris = uris;
		// DISTINCT key from the db_recipe `tables` key — never folded together.
		connectionConfig = { file_uris: uris };
	} else {
		if (!input.backend || !SUPPORTED_BACKENDS.includes(input.backend)) {
			throw new Error(
				`Database select requires a supported backend (got '${input.backend ?? ""}'; ` +
					`supported: ${SUPPORTED_BACKENDS.join(", ")}). The engine import fails ` +
					"loud on a db_recipe source with no backend.",
			);
		}
		const picked =
			input.table_names && input.table_names.length > 0
				? schema.tables.filter((t) => input.table_names?.includes(t.name))
				: schema.tables;
		if (picked.length === 0) {
			throw new Error(
				`None of the requested tables (${(input.table_names ?? []).join(", ")}) ` +
					"are in the connected schema.",
			);
		}
		recipeTables = connectTablesToRecipeTables(picked);
		sourceType = "db_recipe";
		backend = input.backend;
		// DISTINCT key from the file `file_uris` key — never folded together.
		connectionConfig = { tables: recipeTables };
	}

	const now = new Date();
	const sourceId = randomUUID();

	// Policy break (documented, like teach/frame): the metadata client is
	// otherwise read-only; `sources` (here) + `config_overlay` (teach/frame) are
	// the onboarding tables the cockpit writes. Workspace scope is implicit in the
	// ws_<id> schema the client targets (no workspace_id column post-DAT-343).
	//
	// UPSERT on the UNIQUE name: a fresh select INSERTs a new source_id; a repeat
	// select of the same name re-points its config/type/backend/stage. `created_at`
	// is only set on insert; the update touches `updated_at`.
	const [row] = await metadataDb
		.insert(sources)
		.values({
			sourceId,
			name,
			sourceType,
			connectionConfig,
			status: INITIAL_STATUS,
			stage: STAGE_AFTER_SELECT,
			backend,
			createdAt: now,
			updatedAt: now,
		})
		.onConflictDoUpdate({
			target: sources.name,
			set: {
				sourceType,
				connectionConfig,
				status: INITIAL_STATUS,
				stage: STAGE_AFTER_SELECT,
				backend,
				updatedAt: now,
			},
		})
		.returning({
			sourceId: sources.sourceId,
			name: sources.name,
			sourceType: sources.sourceType,
			backend: sources.backend,
			stage: sources.stage,
		});

	return {
		source_id: row.sourceId,
		name: row.name,
		source_type: row.sourceType,
		backend: row.backend ?? null,
		stage: row.stage ?? STAGE_AFTER_SELECT,
		vertical,
		file_uris: fileUris,
		recipe_tables: recipeTables,
	};
}

/**
 * The `select` tool for the agent loop. `needsApproval: true` — it creates/
 * updates a Source row (workspace state), so the SDK pauses for user
 * confirmation before `.server` runs. Run it after `connect` (and `frame` on a
 * cold-start workspace) and before `add_source`.
 */
export const selectTool = toolDefinition({
	name: "select",
	description:
		"Register the data the user chose to import as a workspace source and advance " +
		"it to the add_source stage. Pass the `connect` result as `schema` plus a valid " +
		"`source_name` (lowercase, starts with a letter). For a file source: optionally " +
		"pass `prefix` (an s3:// folder) to import every loadable object under it, " +
		"otherwise the single connected file is used. For a database source: pass " +
		"`backend` and optionally `table_names` (a subset of the schema's tables; all " +
		"tables if omitted). Requires user approval — it writes to the workspace. Does " +
		"NOT start the import; that is the add_source step.",
	inputSchema: z.object({
		source_name: z
			.string()
			.describe(
				"Unique source name (lowercase, starts with a letter, [a-z0-9_], 2–49 chars).",
			),
		schema: ConnectSchema.describe("The `connect` tool result for the source."),
		file_uris: z
			.array(z.string())
			.nullish()
			.describe(
				"File source only: an explicit list of `s3://` URIs to register as ONE " +
					"multi-file source — e.g. several files uploaded together. Takes " +
					"precedence over `prefix`; omit for a single connected file or a prefix.",
			),
		prefix: z
			.string()
			.nullish()
			.describe(
				"File source only: an `s3://<bucket>/<prefix>` folder to enumerate into a " +
					"multi-file selection. Omit to register just the single connected file.",
			),
		table_names: z
			.array(z.string())
			.nullish()
			.describe(
				"Database source only: the subset of the schema's table display names to " +
					"import. Omit to select every table.",
			),
		backend: z
			.enum(SUPPORTED_BACKENDS as [string, ...string[]])
			.nullish()
			.describe(
				"Database source only: the backend (required for a db source).",
			),
		vertical: z
			.string()
			.nullish()
			.describe(
				"The vertical add_source grounds against. Pass a builtin from " +
					"`list_verticals` with a non-zero concept_count (e.g. finance) to " +
					"ADOPT it — no frame needed, it ships its concepts. Pass the SAME " +
					"`vertical_name` you gave `frame` for a newly framed vertical. Omit " +
					"only for an unnamed cold-start (_adhoc).",
			),
		session_id: z.string().nullish(),
	}),
	outputSchema: SelectResult,
	needsApproval: true,
}).server((input) => select(input));
