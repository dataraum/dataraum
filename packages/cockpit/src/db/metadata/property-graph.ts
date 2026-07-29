// GRAPH_TABLE helper (DAT-671 R0) — the operating-model property graph
// (ADR-0021) lives in the engine's `ws_<id>_read` schema as a first-class
// Postgres SQL/PGQ object (`CREATE PROPERTY GRAPH`), distinct from every
// table/view the Drizzle mirror pulls. PGQ EXECUTES IN POSTGRES: `GRAPH_TABLE
// (... MATCH ...)` is ordinary SQL the server plans and runs like any other
// query, so the calling client's language is irrelevant — this was for a long
// time miscast in this package as "TS has no PGQ" (`../../tools/concept-graph.ts`
// carried the same framing; both corrected alongside this helper landing).
// The one REAL constraint is PG19's own, not TypeScript's: `MATCH` is
// FIXED-DEPTH only — no path quantifier — so a transitive closure (e.g.
// `part_of` ancestry) needs a bounded recursive CTE over the edge view
// instead, exactly like the engine's own reads (`graphs/context_reads.py`).
//
// The grant is a SEPARATE privilege object from the table grants
// (`GRANT SELECT ON ALL TABLES` does not cover `GRAPH_TABLE`) — the engine
// wires `GRANT SELECT ON PROPERTY GRAPH` for the reader role into every boot
// (`storage/property_graph.py::grant_reader_on_graph`, called from
// `core/connections.py`, pinned by `test_reader_role_can_query_the_graph`).
// `metadataDb` already connects as that reader role (DAT-816), so no
// additional grant plumbing is needed on this side.
//
// PG19 SQL/PGQ gotchas (house memory, verified against the same
// `postgres:19beta1` image the engine's own integration suite and this
// package's fixture workspace both use):
//   - element KEYs must be `::text` — the engine's element views already cast
//     (e.g. `og_grounded_by.concept_id::text`), so reading an EXISTING element
//     view needs no re-cast; a caller adding a NEW one must cast it there.
//   - only FIXED-DEPTH patterns are supported; no unbounded recursion.
//   - views are fine as graph elements — no primary key required.

import { type SQL, sql } from "drizzle-orm";

import { metadataDb } from "./client";

/**
 * The one property graph the engine's read schema exposes. Matches
 * `property_graph.py`'s `PROPERTY_GRAPH_NAME` — bare and unqualified, like
 * every other name this client emits: the reader role's `search_path` already
 * resolves to `ws_<id>_read` (DAT-816), so this carries zero workspace
 * literal, exactly like the Drizzle-mirrored views.
 */
export const OPERATING_MODEL_GRAPH = "operating_model";

/**
 * Run a fixed-depth SQL/PGQ `MATCH` against the operating-model property
 * graph, straight through the metadata reader connection.
 *
 * `matchAndColumns` is the `MATCH (...) COLUMNS (...)` clause body, built with
 * Drizzle's `sql` template tag exactly like any other raw fragment in this
 * package (see `tools/query-context.ts`) — bind actual VALUES the normal
 * parametrized way inside it; only the pattern/label syntax itself has to be
 * raw text, since there is no query-builder DSL for PGQ in Postgres yet.
 *
 * Returns rows exactly as Postgres names the `COLUMNS` aliases — this bypasses
 * Drizzle's schema-driven camelCase mapping (there is no declared schema for a
 * graph projection), so callers own their own alias spelling.
 *
 * **INJECTION WARNING — read before threading a request-derived value (a
 * snippet id, a concept name) into a MATCH clause.** `sql.raw(str)` and a
 * parametrized `` sql`...` `` template both produce the SAME nominal `SQL`
 * type — this signature cannot tell them apart, and neither can the compiler
 * at a call site. A dynamic value MUST ride inside a NESTED `` sql`...${value}` ``
 * fragment composed into `matchAndColumns` (Drizzle binds it as a real
 * parameter there); it must NEVER be string-concatenated into `sql.raw(...)` —
 * that reintroduces exactly the SQL-injection class parametrization exists to
 * close, and nothing here will flag it. `sql.raw` is for STATIC
 * pattern/label/column syntax only (see the worked example in
 * `property-graph.integration.test.ts` for both idioms side by side — one
 * static `MATCH`, one filtered on a bound value).
 */
export async function queryOperatingModelGraph<
	TRow extends Record<string, unknown> = Record<string, unknown>,
>(matchAndColumns: SQL): Promise<TRow[]> {
	return metadataDb.execute<TRow>(
		sql`SELECT * FROM GRAPH_TABLE (${sql.raw(OPERATING_MODEL_GRAPH)} ${matchAndColumns})`,
	);
}
