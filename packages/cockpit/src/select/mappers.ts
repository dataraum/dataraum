// select-stage pure mappers (DAT-398) — turn a `ConnectSchema` + the user's
// subset choice into the exact `sources`-row payload the engine import phase
// consumes. NO I/O here: the tool (`tools/select.ts`) carries the DB write and
// the prefix-enumeration driver; everything that decides the *shape* of the
// persisted Source row lives here so it is unit-testable without a live driver,
// a bucket, or Postgres.
//
// The two source kinds map to two DISTINCT `connection_config` keys — never
// folded into one another (the engine reads `file_uris` for files and `tables`
// for db_recipe, and a collision between them is a loud import failure):
//   - file:     `connection_config.file_uris = ["s3://<bucket>/<key>", …]`
//               + `source_type` derived from the URI suffix (csv|parquet|json),
//                 NOT the literal "file".
//   - database: `connection_config.tables = [{name, sql}, …]` synthesized from
//               the picked `ConnectSchema.tables[]`, + `source_type="db_recipe"`,
//               + the `backend` COLUMN (import fails loud without it).
//
// The recipe synthesis is the subtle part. A `ConnectSchema.tables[].name` is a
// DISPLAY name `groupInformationSchema` qualifies as `<schema>.<table>` only for
// non-default schemas (e.g. `dbo.Invoices`); the default schema stays
// unqualified (`Invoices`). The engine runs each recipe `sql` VERBATIM after
// `USE src.<backend-default-schema>`, so:
//   - the recipe `name` must be a fresh sanitized `[a-z][a-z0-9_]*` identifier
//     (it becomes the DuckDB raw table `raw_<name>` / `<source>__<name>`), and
//   - the `sql` is `SELECT * FROM <quoted schema-qualified ident>` so a
//     non-default-schema table still resolves, with identifier quoting treated
//     as a (low) injection surface and escaped.

import { createHash } from "node:crypto";

import type { ConnectSchema } from "../duckdb/connect";
import { UPLOAD_PREFIX } from "../upload/policy";

// THE source-name rule — this pattern is the authority (DAT-430 deleted the
// engine's legacy `SourceManager` and its `_NAME_PATTERN`; `select` is the only
// writer of source rows): lowercase, starts with a letter, 2–49 chars of
// `[a-z0-9_]`. The engine consumes the persisted name verbatim — the credential
// lookup `DATARAUM_<NAME>_URL` and the raw-table prefix `<name>__` both key off
// it — and it is UNIQUE (`uq_sources_name`). Lives here in the pure-shape module
// so both the db-source name validation (`tools/select.ts`) and the content-keyed
// file-source name derivation below agree on one pattern.
export const SOURCE_NAME_PATTERN = /^[a-z][a-z0-9_]{1,48}$/;

// Reserved family prefixes (DAT-433). The display rules in lib/display-names.ts
// are SOUND only because a user-chosen source name can never start with a
// derived-table family prefix: `src_` (content-keyed upload sources →
// `src_<digest>__<table>` physical names), `enriched_` (enriched views
// `enriched_<source>__<table>`), `slice_` (slice tables
// `slice_<source table>_<col>_<value>`). Without the reservation, a db source
// named `enriched_data` would display differently per tool (callers without
// sourceName context apply the family rule) AND collide with the real enriched
// view of a source named `data`. Only the PREFIXED forms collide — the bare
// words `src`/`enriched`/`slice` are fine as source names.
export const RESERVED_SOURCE_NAME_PREFIXES = [
	"src_",
	"enriched_",
	"slice_",
] as const;

/**
 * The reserved family prefix a candidate source name starts with, or null when
 * the name is safe. The db-source branch of `tools/select.ts` rejects on
 * non-null — `select` is the only writer of source rows, so this IS the
 * reservation. The content-keyed `src_<digest>` names minted by
 * `contentKeyedSourceName` below are exempt by construction: they ARE the
 * family the `src_` prefix is reserved for.
 */
export function reservedSourceNamePrefix(name: string): string | null {
	return RESERVED_SOURCE_NAME_PREFIXES.find((p) => name.startsWith(p)) ?? null;
}

// --- source_type from a file URI suffix -------------------------------------

// `sourceTypeForUri` (suffix → engine `source_type`) moved to the crypto-free
// upload/policy so the CLIENT upload dropzone can import it (via upload/batch)
// without dragging this module's `node:crypto` into the browser bundle. Re-
// exported here so select-stage callers keep importing it from `select/mappers`.
export { sourceTypeForUri } from "../upload/policy";

// --- content-keyed file sources (DAT-422) ------------------------------------

// Each uploaded FILE is its own source, keyed by its content digest (the model:
// one file = one content-keyed source, DD/30900226 v2). A staged upload's object
// key is the locked `<ws>/uploads/<digest>/<filename>` shape (upload/policy.ts
// buildUploadKey, DAT-505) whose digest segment IS the file's content digest, so
// the source name is `src_<digest>`: identical bytes (same digest → same key →
// same name) UPSERT one row (re-upload dedup), and two distinct files never
// collide on a source — even when their basenames match — because the digests
// differ. The name is engine-valid by construction (a 40-char sha-1 hex digest →
// `src_` + 40 = 44 chars, lowercase, letter-led).
const CONTENT_SOURCE_PREFIX = "src_";

/**
 * The content-keyed source NAME (`src_<digest>`) for a staged upload URI.
 *
 * Parses the content digest from the locked `s3://<bucket>/<ws>/uploads/<digest>/
 * <filename>` upload shape (upload/policy.ts, DAT-505). A URI that is NOT
 * upload-shaped — a bare bucket key, or a prefix-enumerated object that was never
 * content-addressed — cannot be content-keyed and is a loud failure here: content
 * identity requires the upload digest, and the bucket/prefix connector is a
 * separate future concern (DAT-390), not a silently path-keyed source. The
 * returned name is asserted against `SOURCE_NAME_PATTERN` so a malformed digest
 * segment fails loud rather than persisting an unusable row.
 */
export function contentKeyedSourceName(uri: string): string {
	const path = uri.replace(/^s3:\/\/[^/]+\//, "");
	const segments = path.split("/").filter(Boolean);
	// Exactly `<ws>/uploads/<digest>/<filename>` — the workspace prefix, then the
	// locked upload triple. Locate the `uploads` marker rather than hard-coding an
	// index so the workspace segment (which can itself be a dashed UUID) is robust.
	const uploadsAt = segments.indexOf(UPLOAD_PREFIX);
	const digest = uploadsAt >= 0 ? segments[uploadsAt + 1] : undefined;
	const filename = uploadsAt >= 0 ? segments[uploadsAt + 2] : undefined;
	if (uploadsAt !== 1 || segments.length !== 4 || !digest || !filename) {
		throw new Error(
			`Cannot content-key '${uri}' — a file source must be a staged upload ` +
				`(s3://<bucket>/<ws>/${UPLOAD_PREFIX}/<digest>/<filename>). A bucket/prefix ` +
				"source is not content-addressed (a future connector).",
		);
	}
	// SHA-1 hex (what digestBytes produces) is already lowercase; toLowerCase() is
	// defensive so a non-canonical digest segment still yields an engine-valid name.
	const name = `${CONTENT_SOURCE_PREFIX}${digest.toLowerCase()}`;
	if (!SOURCE_NAME_PATTERN.test(name)) {
		throw new Error(
			`Content key '${name}' for '${uri}' is not a valid source name ` +
				"(lowercase, letter-led, 2–49 chars of [a-z0-9_]) — the upload digest " +
				"segment is malformed.",
		);
	}
	return name;
}

// --- narrow raw-table name a file upload loads into (DAT-639) ----------------

/**
 * The NARROW, workspace-unique raw table name a staged upload URI loads into —
 * the cockpit mirror of the engine's `raw_table_name_for_uri`
 * (sources/base.py, DAT-639).
 *
 * Post-DAT-639 raw table names are narrow (no `src_<digest>__` source prefix):
 * the per-workspace DuckLake catalog is the namespace, and `(table_name, layer)`
 * is workspace-unique (`uq_table_name_layer`). The engine names a file's raw
 * table after the FILE STEM — the last path segment with its extension stripped,
 * sanitized — NOT the content-keyed `src_<digest>` source name. So a CSV at
 * `…/uploads/<digest>/Orders.CSV` loads into raw table `orders`.
 *
 * This is the cockpit "say no" pre-check input (DAT-639): the import-set guard
 * derives each file's candidate name through here so it can reject a collision
 * (with an existing workspace table, or another file in the same batch) BEFORE
 * any write, in front of the engine's hard `uq_table_name_layer` backstop. Minor
 * lead-digit edge divergence from the engine sanitizer is acceptable — the engine
 * is the authoritative backstop; this stays simple and reuses `sanitizeRecipeName`.
 */
export function uploadTableName(fileUri: string): string {
	const basename = fileUri.split("/").filter(Boolean).at(-1) ?? "";
	const dot = basename.lastIndexOf(".");
	const stem = dot > 0 ? basename.slice(0, dot) : basename;
	return sanitizeRecipeName(stem);
}

// --- recipe synthesis (db_recipe `connection_config.tables`) -----------------

/** One synthesized recipe query the engine materializes into `raw_<name>`. */
export interface RecipeTable {
	name: string;
	sql: string;
}

/**
 * The recipe content hash (`connection_config.recipe_hash`) — sha256 over the
 * canonical `{backend, tables}` JSON (DAT-430).
 *
 * Canonical = `JSON.stringify` of a `{backend, tables}` object: key order is
 * fixed by construction (this literal + the `{name, sql}` entries) and array
 * order follows the connected schema's table order, so a re-select of the SAME
 * pick serializes — and hashes — identically. The backend is PART of the
 * identity: the recipe SQL is interpreted against the connected backend, so the
 * same table names against a DIFFERENT backend are a different recipe — without
 * it, re-selecting a source name against another DBMS with identical table
 * names would match the import witness and silently skip over raw tables
 * extracted from the old backend. The engine treats the value as an OPAQUE
 * token: it never recomputes it, only copies it to `imported_recipe_hash` at
 * import success and compares the two on a later run
 * (`ImportPhase.should_skip`) — so no cross-language canonicalization contract
 * exists beyond this one function. This is what kills the silent-staleness
 * hole for name-keyed db sources: a re-pointed recipe stops matching the
 * import witness and the run fails loud instead of presence-skipping over the
 * old raw tables.
 */
export function recipeContentHash(
	backend: string,
	tables: RecipeTable[],
	credentialSource?: string,
): string {
	// `credentialSource` (DAT-592) is part of the recipe identity when present: a
	// query-source reads through a named connection, so re-pointing it (same SQL,
	// different DB) is a DIFFERENT recipe. Omitted for the table-pick path (a source
	// that is its own credential), keeping that path's hash byte-identical.
	const canonical =
		credentialSource === undefined
			? { backend, tables }
			: { backend, credential_source: credentialSource, tables };
	return createHash("sha256").update(JSON.stringify(canonical)).digest("hex");
}

/** Quote a single SQL identifier segment, doubling embedded double-quotes.
 *
 * The schema-qualified display name is split on the FIRST dot into a
 * schema + table; each segment is quoted independently so a dot inside a
 * (quoted) table name is impossible to confuse with the schema separator, and
 * any `"` in a segment is escaped. This is the (low) injection surface the spec
 * flags: the display name originates from `information_schema`, but we never
 * interpolate it raw. */
function quoteIdent(segment: string): string {
	return `"${segment.replace(/"/g, '""')}"`;
}

/**
 * The `SELECT * FROM …` SQL for a `ConnectSchema.tables[].name` display name.
 *
 * `groupInformationSchema` qualifies the name `<schema>.<table>` ONLY for a
 * non-default schema; a default-schema table is the bare `<table>`. We split on
 * the first dot to recover that structure and quote each part, producing
 * `SELECT * FROM "schema"."table"` (qualified) or `SELECT * FROM "table"`
 * (unqualified). The engine runs this verbatim after `USE src.<default_schema>`,
 * so the unqualified form resolves against the backend default schema and the
 * qualified form against its explicit schema.
 */
export function recipeSqlForDisplayName(displayName: string): string {
	const dot = displayName.indexOf(".");
	if (dot > 0 && dot < displayName.length - 1) {
		const schema = displayName.slice(0, dot);
		const table = displayName.slice(dot + 1);
		return `SELECT * FROM ${quoteIdent(schema)}.${quoteIdent(table)}`;
	}
	return `SELECT * FROM ${quoteIdent(displayName)}`;
}

/** Lowercase + collapse non-identifier runs to `_`, strip edge underscores, and
 * ensure a leading letter — the cockpit mirror of the engine's
 * `sanitize_identifier` (core/duckdb_naming.py), tightened to the recipe
 * `name` pattern `^[a-z][a-z0-9_]*$` (sources/db_recipe/recipe.py). A name that
 * sanitizes to empty, or whose first char isn't a letter, is prefixed `t_` so
 * the result is always a valid recipe identifier. */
export function sanitizeRecipeName(displayName: string): string {
	let s = displayName
		.trim()
		.toLowerCase()
		.replace(/[^a-z0-9]+/g, "_")
		.replace(/_+/g, "_")
		.replace(/^_+|_+$/g, "");
	if (s.length === 0 || !/^[a-z]/.test(s)) {
		s = `t_${s}`.replace(/_+$/g, "");
	}
	return s;
}

/**
 * Synthesize the `connection_config.tables` recipe list from the picked
 * `ConnectSchema.tables[]`.
 *
 * Each picked table becomes one `{name, sql}` recipe entry: `name` is a fresh
 * sanitized identifier (it becomes the DuckDB raw table `raw_<name>`), `sql` is
 * `SELECT * FROM <quoted schema-qualified ident>`. Recipe names must be unique
 * (two display names sanitizing to the same identifier would collide on one raw
 * table); a collision is de-duplicated by appending `_2`, `_3`, … so the
 * persisted recipe is always materializable.
 */
export function connectTablesToRecipeTables(
	tables: ConnectSchema["tables"],
): RecipeTable[] {
	if (tables.length === 0) {
		throw new Error(
			"Database select has no tables — pick at least one table to import.",
		);
	}
	const used = new Map<string, number>();
	return tables.map((t) => {
		const base = sanitizeRecipeName(t.name);
		const seen = used.get(base) ?? 0;
		used.set(base, seen + 1);
		const name = seen === 0 ? base : `${base}_${seen + 1}`;
		return { name, sql: recipeSqlForDisplayName(t.name) };
	});
}
