// select-stage pure mappers (DAT-398) — turn a staged source + the user's
// subset choice into the exact `sources`-row payload the engine import phase
// consumes. NO I/O here: `select/source-write.ts` carries the DB write and
// `server/import-sources.ts` drives the batch; everything that decides the
// *shape* of the persisted Source row lives here so it is unit-testable without
// a live driver, a bucket, or Postgres.
//
// The two source kinds map to two DISTINCT `connection_config` keys — never
// folded into one another (the engine reads `file_uris` for files and `tables`
// for db_recipe, and a collision between them is a loud import failure):
//   - file:     `connection_config.file_uris = ["s3://<bucket>/<key>", …]`
//               + `source_type` derived from the URI suffix (csv|parquet|json),
//                 NOT the literal "file".
//   - database: `connection_config.tables = [{name, sql}, …]` + the `backend`
//               COLUMN (import fails loud without it). `RecipeTable` is that
//               entry's shape; the recipe rows themselves are written by
//               `select/recipe-source.ts`, ONE per staged query.
//
// A second producer of those rows used to live here — the picked tables of a
// database-wide `ConnectSchema`, whose `<schema>.<table>` display names were
// re-quoted into `SELECT * FROM "schema"."table"`. Its only source was the
// `connect(source_kind=database)` introspection, which had no caller and is
// gone (DAT-671 R6), so the synthesis went with it rather than waiting for a
// producer that was never coming (ADR-0024 decision 3).

// THE source-name rule — this pattern is the authority (DAT-430 deleted the
// engine's legacy `SourceManager` and its `_NAME_PATTERN`; the cockpit's
// `select/` path is the only writer of source rows): lowercase, starts with a
// letter, 2–49 chars of
// `[a-z0-9_]`. The engine consumes the persisted name verbatim — the credential
// lookup `DATARAUM_<NAME>_URL` keys off it — and it is UNIQUE (`uq_sources_name`).
// (Post-DAT-639 there is no `<name>__` raw-table prefix; physical table names are
// narrow.) Lives here in the pure-shape module so both the db-source name
// validation (`select/recipe-source.ts`) and the content-keyed file-source name
// derivation below agree on one pattern.
export const SOURCE_NAME_PATTERN = /^[a-z][a-z0-9_]{1,48}$/;

// Reserved family prefixes (DAT-433). A user-chosen source name can never start
// with a derived-name family prefix: `src_` (the content-keyed upload SOURCE name
// itself is `src_<digest>`) or `enriched_` (enriched views are `enriched_<table>`
// — engine `enriched_views_phase.py`). Without the reservation, a source named
// `enriched_orders` would collide with the real enriched view of a table
// `orders`. (Post-DAT-639 physical TABLE names are narrow — no `src_<digest>__` /
// `<source>__` prefix — and slices were removed, so the `slice_` family is gone.)
// Only the PREFIXED forms collide — the bare words `src`/`enriched` are fine.
export const RESERVED_SOURCE_NAME_PREFIXES = ["src_", "enriched_"] as const;

/**
 * The reserved family prefix a candidate source name starts with, or null when
 * the name is safe. `select/recipe-source.ts` rejects a spec on non-null —
 * `select/source-write.ts` is the only writer of source rows, so this IS the
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

// `contentKeyedSourceName` (`src_<digest>`) and `recipeContentHash` moved to the
// server-only `select/source-content-hash.ts`: they use `node:crypto`, and this
// module must stay crypto-free so it can ride into the CLIENT graph (the connect
// canvas → import flow) without crashing the browser bundle. The naming helpers
// below are pure and stay here.

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

// `recipeContentHash` (sha256 over `{backend, tables}`) moved to the server-only
// `select/source-content-hash.ts` — see the note above.

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
