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
import {
	ALLOWED_EXTENSIONS,
	fileExtension,
	UPLOAD_PREFIX,
} from "../upload/policy";

// THE source-name rule — this pattern is the authority (DAT-430 deleted the
// engine's legacy `SourceManager` and its `_NAME_PATTERN`; `select` is the only
// writer of source rows): lowercase, starts with a letter, 2–49 chars of
// `[a-z0-9_]`. The engine consumes the persisted name verbatim — the credential
// lookup `DATARAUM_<NAME>_URL` and the raw-table prefix `<name>__` both key off
// it — and it is UNIQUE (`uq_sources_name`). Lives here in the pure-shape module
// so both the db-source name validation (`tools/select.ts`) and the content-keyed
// file-source name derivation below agree on one pattern.
export const SOURCE_NAME_PATTERN = /^[a-z][a-z0-9_]{1,48}$/;

// --- source_type from a file URI suffix -------------------------------------

// Suffix → engine `source_type`. MIRRORS the engine's import-dispatch suffix map
// (`pipeline/phases/import_phase.py` `_PARQUET_EXTENSIONS`/`_JSON_EXTENSIONS` +
// CSV default) + the cockpit connect/upload contract: csv/tsv/txt → csv,
// parquet/pq → parquet, json/jsonl/ndjson → json. A file source's `source_type`
// is this derived value, NEVER the literal "file" — the engine import dispatch
// keys off it.
const EXTENSION_TO_SOURCE_TYPE: Record<string, "csv" | "parquet" | "json"> = {
	csv: "csv",
	tsv: "csv",
	txt: "csv",
	parquet: "parquet",
	pq: "parquet",
	json: "json",
	jsonl: "json",
	ndjson: "json",
};

/**
 * The engine `source_type` for a single file URI, derived from its suffix.
 *
 * Throws on an unsupported / extensionless URI — a select that can't name the
 * source_type is a loud error, not a silently-mislabelled row the engine import
 * would reject. Each content-keyed file source carries one file, so this single
 * value is its declared `source_type` (the loader still dispatches per-URI by
 * suffix at import time).
 */
export function sourceTypeForUri(uri: string): "csv" | "parquet" | "json" {
	const ext = fileExtension(uri);
	const type = ext ? EXTENSION_TO_SOURCE_TYPE[ext] : undefined;
	if (!type) {
		throw new Error(
			`Cannot derive source_type for '${uri}' — unsupported or missing extension ` +
				`(supported: ${ALLOWED_EXTENSIONS.join(", ")}).`,
		);
	}
	return type;
}

// --- content-keyed file sources (DAT-422) ------------------------------------

// Each uploaded FILE is its own source, keyed by its content digest (the model:
// one file = one content-keyed source, DD/30900226 v2). A staged upload's object
// key is the locked `uploads/<digest>/<filename>` shape (upload/policy.ts
// buildUploadKey) whose directory segment IS the file's content digest, so the
// source name is `src_<digest>`: identical bytes (same digest → same key → same
// name) UPSERT one row (re-upload dedup), and two distinct files never collide on
// a source — even when their basenames match — because the digests differ. The
// name is engine-valid by construction (a 40-char sha-1 hex digest → `src_` + 40
// = 44 chars, lowercase, letter-led).
const CONTENT_SOURCE_PREFIX = "src_";

/**
 * The content-keyed source NAME (`src_<digest>`) for a staged upload URI.
 *
 * Parses the content digest from the locked `s3://<bucket>/uploads/<digest>/
 * <filename>` upload shape (upload/policy.ts). A URI that is NOT upload-shaped —
 * a bare bucket key, or a prefix-enumerated object that was never
 * content-addressed — cannot be content-keyed and is a loud failure here: content
 * identity requires the upload digest, and the bucket/prefix connector is a
 * separate future concern (DAT-390), not a silently path-keyed source. The
 * returned name is asserted against `SOURCE_NAME_PATTERN` so a malformed digest
 * segment fails loud rather than persisting an unusable row.
 */
export function contentKeyedSourceName(uri: string): string {
	const path = uri.replace(/^s3:\/\/[^/]+\//, "");
	const segments = path.split("/").filter(Boolean);
	// Exactly `uploads/<digest>/<filename>` — no shallower, no nested key.
	if (
		segments.length !== 3 ||
		segments[0] !== UPLOAD_PREFIX ||
		!segments[1] ||
		!segments[2]
	) {
		throw new Error(
			`Cannot content-key '${uri}' — a file source must be a staged upload ` +
				`(s3://<bucket>/${UPLOAD_PREFIX}/<digest>/<filename>). A bucket/prefix ` +
				"source is not content-addressed (a future connector).",
		);
	}
	// SHA-1 hex (what digestBytes produces) is already lowercase; toLowerCase() is
	// defensive so a non-canonical digest segment still yields an engine-valid name.
	const name = `${CONTENT_SOURCE_PREFIX}${segments[1].toLowerCase()}`;
	if (!SOURCE_NAME_PATTERN.test(name)) {
		throw new Error(
			`Content key '${name}' for '${uri}' is not a valid source name ` +
				"(lowercase, letter-led, 2–49 chars of [a-z0-9_]) — the upload digest " +
				"segment is malformed.",
		);
	}
	return name;
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
): string {
	return createHash("sha256")
		.update(JSON.stringify({ backend, tables }))
		.digest("hex");
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
