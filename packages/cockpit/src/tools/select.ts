// select tool (DAT-398, DAT-422; one-gate trigger DAT-436) — the agent-tier
// step that turns a connected source + the user's subset choice into real
// `sources` rows AND starts the import: approving `select` is the SINGLE
// approval that registers the source(s) and kicks off addSourceWorkflow,
// mirroring the one-gate pattern of `replay` and `begin_session` (seed +
// workflow.start atomically inside ONE needsApproval gate). There is no
// separate "Add source" button or `/api/add-source` route.
//
// This is the FIRST cockpit writer of the engine-owned `ws_<id>.sources` table.
// Nothing upstream creates the Source row: `connect` is read-only and `frame`
// writes concept overlays, not a Source. The engine import phase explicitly
// assumes "the workflow caller — the cockpit — wrote it before triggering
// addSourceWorkflow" (import_phase.py). So `select` OWNS the INSERT: it writes
// the source(s) at `stage='add_source'` (the cursor the journey readiness reads),
// via the SAME metadata-client cross-schema write seam `teach`/`frame` use — the
// documented policy break (the metadata client is otherwise read-only; the engine
// owns the schema, and these onboarding writes flow through this one seam).
//
// Order inside the gate: the vertical PRE-FLIGHT (NoConceptsError when it
// resolves to zero concepts) runs BEFORE any write, so a refused vertical
// leaves no half-state; then the source upsert(s); then the trigger (the
// investigation_sessions seed + the non-blocking workflow.start —
// temporal/trigger-add-source.ts). The result carries the workflow/run/session
// ids so the progress canvas member follows immediately.
//
// Dispatch is on `ConnectSchema.sourceKind`:
//   - file:     each uploaded file is its OWN content-keyed source (DAT-422 — the
//               model is one file = one content-keyed source). For every staged
//               upload URI (`uploads/<digest>/<file>`) `select` UPSERTs a source
//               named `src_<digest>` with `connection_config.file_uris=[that one
//               URI]` + a suffix-derived `source_type`. Identical bytes (same
//               digest) UPSERT one row (re-upload dedup); two distinct files never
//               collide on a raw table even with matching basenames (the digests
//               differ), so no basename rejection is needed. A run then ingests
//               the SET of these source ids (`source_ids`).
//   - database: ONE source — `source_type='db_recipe'`, the `backend` COLUMN, and
//               `connection_config.tables` synthesized from the picked tables,
//               plus `recipe_hash` (sha256 over the canonical {backend, tables}
//               JSON, DAT-430) so the engine can tell an idempotent re-select
//               from a re-pointed recipe — including the same table names
//               against a different backend; the engine-stamped
//               `imported_recipe_hash` witness on an existing row is carried
//               forward. The user-chosen `source_name` is required here (files
//               are content-keyed, so it is ignored for them).
//
// `needsApproval: true` — it mutates workspace state (creates/updates source
// rows) AND starts a durable engine run, so the SDK pauses for the user exactly
// like `teach`/`frame`/`replay`.

import { randomUUID } from "node:crypto";
import { toolDefinition } from "@tanstack/ai";
import { eq } from "drizzle-orm";
import { z } from "zod";

import { config } from "../config";
import { metadataDb } from "../db/metadata/client";
import { sources } from "../db/metadata/schema";
import { sourcesWrite } from "../db/metadata/write-surface";
import { ConnectSchema } from "../duckdb/connect";
import { SUPPORTED_BACKENDS } from "../duckdb/probe";
import { enumeratePrefixUris } from "../select/enumerate";
import {
	connectTablesToRecipeTables,
	contentKeyedSourceName,
	RESERVED_SOURCE_NAME_PREFIXES,
	recipeContentHash,
	reservedSourceNamePrefix,
	SOURCE_NAME_PATTERN,
	sourceTypeForUri,
} from "../select/mappers";
import {
	NoConceptsError,
	triggerAddSource,
} from "../temporal/trigger-add-source";
import { verticalConceptCount } from "./list-verticals";

// The onboarding stage `select` leaves the source(s) at. The cockpit drives a
// source `connect → frame → select → add_source` BEFORE the workflow triggers;
// `select` writes the row already at `add_source`, the next interactive stage.
const STAGE_AFTER_SELECT = "add_source";

// Initial source status. Mirrors the seed in the integration driver
// (`scripts/smoke-add-source.ts`): a source the cockpit has registered but not
// yet imported reads `configured`.
const INITIAL_STATUS = "configured";

// The default vertical when none is chosen — the unnamed cold-start ontology.
const DEFAULT_VERTICAL = "_adhoc";
// A chosen vertical keys the engine's `verticals/<name>` config resolution, so a
// supplied name must be a safe segment + engine-valid (lowercase, starts with a
// letter) — or the `_adhoc` default (exempt: the built-in leading-underscore key).
const VERTICAL_NAME_PATTERN = /^[a-z][a-z0-9_]{1,48}$/;

/** The vertical the run grounds against — a builtin the user adopted (e.g.
 * finance), a vertical just framed, or `_adhoc`. Seeded onto the run's session
 * by the trigger and echoed in the result (it isn't persisted on the source
 * row — the conversation carries it). */
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

/** The persisted Source descriptor + the started run's identity. The
 * workflow/run/session ids are what the progress canvas member keys its
 * `get_progress` poll on (tool-result-to-canvas.ts, replay precedent). */
export const SelectResult = z.object({
	// Every source `select` minted/UPSERTed for this selection — the SET the
	// add_source run ingests (DAT-422). N for a file selection (one content-keyed
	// source per uploaded file), 1 for a database selection; always ≥1 (a select
	// that registered nothing is a loud failure), matching the trigger contract.
	source_ids: z.array(z.string()).min(1),
	// A human display label for the selection — NOT a source name (file sources
	// are content-keyed, so there is no single user-chosen name): the database
	// source name, or the filename / "N files" for a file selection.
	name: z.string(),
	// `db_recipe` for a database source; the file kind for a file selection —
	// `csv`/`parquet`/`json`, or `csv+parquet` for a (future) mixed batch.
	source_type: z.string(),
	backend: z.string().nullable(),
	stage: z.string(),
	// The vertical the run grounds against (adopted builtin / framed / `_adhoc`),
	// pre-flighted against the effective concept count BEFORE any write.
	vertical: z.string(),
	// The concrete file URIs persisted (file source), else null (db source).
	file_uris: z.array(z.string()).nullable(),
	// The synthesized recipe tables persisted (db source), else null (file).
	recipe_tables: z
		.array(z.object({ name: z.string(), sql: z.string() }))
		.nullable(),
	// The started addSourceWorkflow run (DAT-436: approving select STARTS the
	// import). workflow_id + run_id pin the precise execution the progress
	// canvas polls; session_id is the run's seeded investigation session.
	workflow_id: z.string(),
	run_id: z.string(),
	session_id: z.string(),
});
export type SelectResult = z.infer<typeof SelectResult>;

/** The persisted selection BEFORE the trigger — what `persistSelection`
 * returns. `select` composes this with the trigger's run identity. The
 * integration smoke drives `persistSelection` directly (row-shape contract
 * against a real Postgres; starting a real workflow is the compose smoke's
 * job). */
export type PersistedSelection = Omit<
	SelectResult,
	"workflow_id" | "run_id" | "session_id"
>;

export interface SelectInput {
	// Database source only: the unique source name (lowercase, starts with a
	// letter). File sources are content-keyed (`src_<digest>`), so this is ignored
	// for them.
	source_name?: string | null;
	schema: ConnectSchema;
	// File source: an explicit list of `s3://` URIs to register — one content-keyed
	// source per file (DAT-422: several files uploaded together). Takes precedence
	// over `prefix` — the client already holds the staged URIs, so no S3 re-listing.
	// Ignored for a database source.
	file_uris?: string[] | null;
	// File source: optional `s3://<bucket>/<prefix>` to enumerate into concrete
	// URIs. Omitted → the single connect URI (`schema.source`) is the one file
	// registered. Ignored for a database source. (Each enumerated URI must be a
	// staged upload — a non-content-addressed bucket object is a loud failure.)
	prefix?: string | null;
	// Database source: the picked subset of `schema.tables[].name` (display
	// names). Omitted/empty → every table in the schema is selected.
	table_names?: string[] | null;
	// Database backend, persisted as the `backend` COLUMN (required for db sources;
	// the engine import fails loud without it). For a file source it is ignored.
	backend?: string | null;
	// The vertical the run grounds against: a builtin the user adopted (e.g.
	// finance), a vertical just framed (the SAME `vertical_name` passed to frame),
	// or omitted → `_adhoc`. Pre-flighted before any write; not persisted.
	vertical?: string | null;
	session_id?: string | null;
}

/** Build the file-source URI list for a connect schema. Precedence: an explicit
 * `fileUris` list (DAT-391 — files uploaded together; the client already holds
 * them) → a `prefix` enumerated to its concrete URIs → the single connect URI.
 * `enumerate` is injected so the unit test exercises the prefix mapping without a
 * live bucket. */
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
 * UPSERT one `sources` row (on the UNIQUE name) and return its source_id.
 *
 * A fresh name INSERTs a new source_id; re-selecting the same name re-points its
 * `connection_config` / `source_type` / `backend` / `stage` (an idempotent
 * re-select, not a duplicate-name error). `created_at` is only set on insert; the
 * update touches `updated_at`. Workspace scope is implicit in the ws_<id> schema
 * the client targets (no workspace_id column post-DAT-343).
 */
async function upsertSource(values: {
	name: string;
	sourceType: string;
	backend: string | null;
	connectionConfig: Record<string, unknown>;
	now: Date;
}): Promise<string> {
	const [row] = await metadataDb
		.insert(sourcesWrite)
		.values({
			sourceId: randomUUID(),
			name: values.name,
			sourceType: values.sourceType,
			connectionConfig: values.connectionConfig,
			status: INITIAL_STATUS,
			stage: STAGE_AFTER_SELECT,
			backend: values.backend,
			createdAt: values.now,
			updatedAt: values.now,
		})
		.onConflictDoUpdate({
			target: sourcesWrite.name,
			set: {
				sourceType: values.sourceType,
				connectionConfig: values.connectionConfig,
				status: INITIAL_STATUS,
				stage: STAGE_AFTER_SELECT,
				backend: values.backend,
				updatedAt: values.now,
			},
		})
		.returning({ sourceId: sourcesWrite.sourceId });
	return row.sourceId;
}

/** The display basename (filename leaf) of an `s3://` URI. */
function basename(uri: string): string {
	return uri.split("/").filter(Boolean).at(-1) ?? uri;
}

/**
 * The engine-stamped `imported_recipe_hash` witness on an existing source row,
 * if any (DAT-430).
 *
 * At import success the engine copies the recipe's `recipe_hash` into
 * `connection_config.imported_recipe_hash` — the record of WHICH recipe the
 * source's raw tables were materialized from. The db-source upsert below
 * REPLACES the whole `connection_config` JSON, so `select` must carry that
 * engine-owned key forward: preserving it is what lets the engine skip an
 * idempotent re-select (current hash == witness) and fail loud on a re-pointed
 * recipe (mismatch) instead of silently serving stale raw tables. A fresh name
 * (or a never-imported source) has no witness — returns null, and the key is
 * simply absent from the new config.
 */
async function importedRecipeHash(name: string): Promise<string | null> {
	const rows = await metadataDb
		.select({ connectionConfig: sources.connectionConfig })
		.from(sources)
		.where(eq(sources.name, name))
		.limit(1);
	const cc = rows[0]?.connectionConfig as Record<string, unknown> | null;
	const witness = cc?.imported_recipe_hash;
	return typeof witness === "string" && witness.length > 0 ? witness : null;
}

/**
 * Persist (UPSERT) the `sources` row(s) for the selected subset and advance the
 * onboarding cursor to `add_source`. Returns the selection descriptor (the SET
 * of source ids the trigger runs over) — persistence ONLY; the one-gate
 * composition (pre-flight → persist → trigger) lives in `select`.
 *
 * `enumerate` is injected for testability; the default is the real
 * `enumeratePrefixUris`.
 */
export async function persistSelection(
	input: SelectInput,
	enumerate: typeof enumeratePrefixUris = enumeratePrefixUris,
): Promise<PersistedSelection> {
	const vertical = resolveVertical(input.vertical);
	const schema = ConnectSchema.parse(input.schema);
	const now = new Date();

	if (schema.sourceKind === "file") {
		const uris = await resolveFileUris(
			schema,
			{ fileUris: input.file_uris, prefix: input.prefix },
			enumerate,
		);
		// One content-keyed source per file (DAT-422). Dedup by content key so a
		// repeated URI UPSERTs once; `contentKeyedSourceName` fails loud on a
		// non-upload URI (content identity requires the upload digest).
		const byName = new Map<string, { uri: string; sourceType: string }>();
		for (const uri of uris) {
			const name = contentKeyedSourceName(uri);
			if (!byName.has(name)) {
				byName.set(name, { uri, sourceType: sourceTypeForUri(uri) });
			}
		}

		const persisted = [...byName.entries()];
		const sourceIds: string[] = [];
		for (const [name, { uri, sourceType }] of persisted) {
			sourceIds.push(
				await upsertSource({
					name,
					sourceType,
					backend: null,
					// DISTINCT key from the db_recipe `tables` key — never folded together.
					connectionConfig: { file_uris: [uri] },
					now,
				}),
			);
		}

		const fileUris = persisted.map(([, p]) => p.uri);
		const distinctTypes = [
			...new Set(persisted.map(([, p]) => p.sourceType)),
		].sort();
		return {
			source_ids: sourceIds,
			name:
				fileUris.length === 1
					? basename(fileUris[0])
					: `${fileUris.length} files`,
			source_type: distinctTypes.join("+"),
			backend: null,
			stage: STAGE_AFTER_SELECT,
			vertical,
			file_uris: fileUris,
			recipe_tables: null,
		};
	}

	// database — ONE source, named by the user (files are content-keyed instead).
	const name = input.source_name;
	if (!name || !SOURCE_NAME_PATTERN.test(name)) {
		throw new Error(
			`Database select requires a valid source_name (got '${name ?? ""}'). ` +
				`Must match ${SOURCE_NAME_PATTERN.source} (lowercase, start with a ` +
				"letter, 2–49 chars of [a-z0-9_]).",
		);
	}
	// Family-prefix reservation (DAT-433): the display rules in
	// lib/display-names.ts are sound only if no source name starts with a
	// derived-table family prefix. Reject here — `select` is the only writer of
	// source rows, so this check IS the reservation.
	const reserved = reservedSourceNamePrefix(name);
	if (reserved !== null) {
		throw new Error(
			`Source name '${name}' starts with the reserved prefix '${reserved}' — ` +
				`${RESERVED_SOURCE_NAME_PREFIXES.join("/")} name the derived-table ` +
				"families (content-keyed uploads, enriched views, slice tables), and a " +
				"source name using one would make table display names ambiguous. " +
				"Pick a different source_name.",
		);
	}
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
	const recipeTables = connectTablesToRecipeTables(picked);
	// Content-hash the synthesized recipe (DAT-430): db sources are NAME-keyed,
	// so the engine's import skip can't rely on row presence — it compares this
	// hash against the `imported_recipe_hash` witness it stamped at import. The
	// backend is part of the hashed identity (same table names on a different
	// DBMS = a different recipe). The witness is read off the existing row (if
	// any) and carried forward, because this upsert replaces the whole
	// connection_config JSON.
	const witness = await importedRecipeHash(name);
	const connectionConfig: Record<string, unknown> = {
		// DISTINCT key from the file `file_uris` key — never folded together.
		tables: recipeTables,
		recipe_hash: recipeContentHash(input.backend, recipeTables),
		...(witness === null ? {} : { imported_recipe_hash: witness }),
	};
	const sourceId = await upsertSource({
		name,
		sourceType: "db_recipe",
		backend: input.backend,
		connectionConfig,
		now,
	});

	return {
		source_ids: [sourceId],
		name,
		source_type: "db_recipe",
		backend: input.backend,
		stage: STAGE_AFTER_SELECT,
		vertical,
		file_uris: null,
		recipe_tables: recipeTables,
	};
}

/**
 * The one-gate select (DAT-436): vertical pre-flight → source upsert(s) →
 * investigation_sessions seed + non-blocking workflow.start. Approving the
 * `select` tool is the single approval that starts the import — mirroring
 * `replay` / `begin_session`, which also seed + start inside their one gate.
 *
 * Ordering is load-bearing: the pre-flight runs BEFORE any write so a refused
 * vertical (NoConceptsError) leaves no half-state — no source row, no orphan
 * session, no doomed workflow. `trigger` is injected for testability; the
 * default is the real `triggerAddSource`.
 */
export async function select(
	input: SelectInput,
	enumerate: typeof enumeratePrefixUris = enumeratePrefixUris,
	trigger: typeof triggerAddSource = triggerAddSource,
): Promise<SelectResult> {
	// PRE-FLIGHT (Theme A guard, relocated from the retired trigger hop): refuse
	// early when the chosen vertical resolves to zero concepts — builtin
	// ontology.yaml concepts PLUS active overlay rows. The engine fails loud on
	// this deep in semantic_per_column, surfacing only as a dead Temporal run;
	// catching it here gives the user a readable message BEFORE anything is
	// written. An adopted builtin (finance) ships concepts → passes; an empty
	// _adhoc or an un-framed vertical → refused.
	const vertical = resolveVertical(input.vertical);
	if ((await verticalConceptCount(vertical)) === 0) {
		throw new NoConceptsError(vertical);
	}

	const selection = await persistSelection(input, enumerate);

	// Seed the run's session + start addSourceWorkflow over the persisted SET.
	// Non-blocking: the ids come back immediately and the progress canvas member
	// (keyed on workflow_id + run_id) follows the run.
	const run = await trigger({
		source_ids: selection.source_ids,
		vertical: selection.vertical,
	});

	return {
		...selection,
		workflow_id: run.workflow_id,
		run_id: run.run_id,
		session_id: run.session_id,
	};
}

/**
 * The `select` tool for the agent loop. `needsApproval: true` — it creates/
 * updates source rows (workspace state) AND starts a durable engine run, so the
 * SDK pauses for user confirmation before `.server` runs. Run it after
 * `connect` (and `frame` on a cold-start workspace); approving it IS the
 * add_source start.
 */
export const selectTool = toolDefinition({
	name: "select",
	description:
		"Register the data the user chose to import as workspace source(s) AND start " +
		"the import (add_source) in one step — approving this tool kicks off the " +
		"engine run. Pass the `connect` result as `schema`. For a FILE source: each " +
		"uploaded file becomes its own content-keyed source automatically — pass " +
		"`file_uris` (the staged s3:// upload URIs) or `prefix` (an s3:// folder), " +
		"or omit both for the single connected file; no `source_name` is needed. For a " +
		"DATABASE source: pass `source_name` (lowercase, starts with a letter), `backend`, " +
		"and optionally `table_names` (a subset of the schema's tables; all if omitted). " +
		"Requires user approval — it writes to the workspace and starts a durable run. " +
		"Returns the run's workflow_id + run_id; progress renders live in the canvas, " +
		"and workflow_status with those ids reports completion.",
	inputSchema: z.object({
		source_name: z
			.string()
			.nullish()
			.describe(
				"Database source only: a unique source name (lowercase, starts with a " +
					"letter, [a-z0-9_], 2–49 chars; must NOT start with the reserved " +
					"prefixes src_/enriched_/slice_ — they name derived-table families). " +
					"Ignored for file sources (content-keyed).",
			),
		schema: ConnectSchema.describe("The `connect` tool result for the source."),
		file_uris: z
			.array(z.string())
			.nullish()
			.describe(
				"File source only: the staged `s3://` upload URIs to register — one " +
					"content-keyed source per file. Takes precedence over `prefix`; omit " +
					"for a single connected file or a prefix.",
			),
		prefix: z
			.string()
			.nullish()
			.describe(
				"File source only: an `s3://<bucket>/<prefix>` folder to enumerate into " +
					"file URIs. Omit to register just the single connected file.",
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
	// The lambda is load-bearing: .server() calls its handler as (input, context)
	// — passing `select` bare would shove the SDK's context object into select's
	// injectable `enumerate` test-seam parameter, clobbering its default.
	// ctx.abortSignal deliberately NOT forwarded (DAT-449): the trigger's
	// `workflow.start` is a short, non-blocking gRPC call — an abort mid-start
	// can't un-start the Temporal workflow and would only orphan the seeded
	// investigation_sessions row (the exact failure seam trigger-add-source.ts
	// guards against).
}).server((input) => select(input));
