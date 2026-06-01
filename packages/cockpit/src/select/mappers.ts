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

import type { ConnectSchema } from "../duckdb/connect";
import { ALLOWED_EXTENSIONS, fileExtension } from "../upload/policy";

// --- source_type from a file URI suffix -------------------------------------

// Suffix → engine `source_type`. MIRRORS the engine's `_EXTENSION_MAP`
// (sources/manager.py) + the cockpit connect/upload contract: csv/tsv/txt → csv,
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
 * would reject. A multi-file source is homogeneous by this value (see
 * `sourceTypeForUris`).
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

/**
 * The single `source_type` for a file source's URI list.
 *
 * Every URI must derive the same type — the engine stores ONE `source_type` per
 * Source row, so a mixed selection (a `.csv` next to a `.parquet`) is ambiguous
 * and rejected here rather than persisting a row whose `source_type` describes
 * only some of its files. (Loaders dispatch per-URI by suffix at import time, so
 * this column is the source's declared *kind*, not a per-file switch.)
 */
export function sourceTypeForUris(uris: string[]): "csv" | "parquet" | "json" {
	if (uris.length === 0) {
		throw new Error("Cannot derive source_type from an empty URI list.");
	}
	const types = new Set(uris.map(sourceTypeForUri));
	if (types.size > 1) {
		throw new Error(
			`File selection mixes incompatible source types (${[...types].sort().join(", ")}). ` +
				"Select files of a single type per source.",
		);
	}
	return [...types][0];
}

// --- duplicate-basename rejection (DAT-398 owns this; engine fails loud) ------

/** The basename stem of an `s3://` URI (the leaf segment minus its extension).
 *
 * Mirrors the engine's `uri_stem` (core/uri.py): `s3://b/a/orders.csv` →
 * `orders`. Two URIs sharing this stem map to the SAME raw table
 * `<source>__<stem>`, which the engine refuses to load (import_phase.py:223).
 */
export function uriStem(uri: string): string {
	const path = uri.replace(/^s3:\/\/[^/]+\//, "");
	const leaf = path.replace(/\/+$/, "").split("/").pop() ?? path;
	const dot = leaf.lastIndexOf(".");
	return dot > 0 ? leaf.slice(0, dot) : leaf;
}

/**
 * Mirror of the engine's `sanitize_identifier` (core/duckdb_naming.py) — the
 * actual collision domain for raw table names: lowercase, collapse runs of
 * non-identifier chars to `_`, strip edge underscores, prefix a leading digit
 * with `x_`. DISTINCT from `sanitizeRecipeName` (the recipe-name `t_` rule);
 * the FILE raw table `<source>__<stem>` collides on THIS, so the basename guard
 * must group on it.
 */
export function sanitizedStem(stem: string): string {
	const s = stem
		.trim()
		.toLowerCase()
		.replace(/[^a-z0-9_]+/g, "_")
		.replace(/_+/g, "_")
		.replace(/^_+|_+$/g, "");
	return /^[0-9]/.test(s) ? `x_${s}` : s;
}

/**
 * Display basenames whose files would collide on the same engine raw table,
 * sorted. Empty when every file resolves to a distinct raw table.
 *
 * The select stage owns preventing this collision (DAT-378's fix deferred it
 * here): the engine names each file's raw table `<source>__sanitize(<stem>)`
 * and fails loud on the second `CREATE OR REPLACE` for a name already taken. We
 * group on the SANITIZED stem (`sanitizedStem`, the engine's collision domain),
 * NOT the raw stem — so files differing only by case or punctuation (`Orders`
 * vs `orders`, `q1-data` vs `q1_data`) are caught, not just same-stem/diff-ext —
 * and surface the offending display basenames BEFORE persisting so the user
 * fixes the selection, never the import.
 */
export function duplicateBasenames(uris: string[]): string[] {
	const byKey = new Map<string, string[]>();
	for (const uri of uris) {
		const stem = uriStem(uri);
		const key = sanitizedStem(stem);
		const group = byKey.get(key);
		if (group) group.push(stem);
		else byKey.set(key, [stem]);
	}
	const clashing = new Set<string>();
	for (const stems of byKey.values()) {
		if (stems.length > 1) for (const s of stems) clashing.add(s);
	}
	return [...clashing].sort();
}

// --- recipe synthesis (db_recipe `connection_config.tables`) -----------------

/** One synthesized recipe query the engine materializes into `raw_<name>`. */
export interface RecipeTable {
	name: string;
	sql: string;
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
