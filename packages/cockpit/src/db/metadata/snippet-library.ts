// Read-only TS port of the engine SnippetLibrary CONSUMER surface (DAT-484 P0) —
// vocabulary lookup + key-based graph search over the curated SQL-snippet store.
//
// This is the cockpit half of the query migration's keeps #2 (reuse) + #4
// (vocabulary lookup). The snippet substrate STAYS engine-side: the live
// GraphAgent producer fills `sql_snippets` under `schema_mapping_id =
// workspace_id`; here we only READ it, through the existing read-only
// `sqlSnippets` Drizzle view. Byte-compatible with `snippet_library.py`'s
// get_search_vocabulary / find_graphs_by_keys / find_by_id (conformance-tested
// against test_snippet_search.py + test_snippet_library.py).
//
// KEYING (the fault line, DAT-484): `workspaceId` is the dashed-UUID workspace_id
// VALUE (`config.dataraumWorkspaceId`), NOT the `ws_`-prefixed Postgres schema
// name — filtering on the schema name reads an EMPTY library. Callers pass
// `config.dataraumWorkspaceId`. The `sqlSnippets` view is already hard-scoped to
// the one workspace schema, so this filter is redundant-but-correct today; it
// becomes load-bearing once workspaces multiply (DAT-357).
//
// Producer-side APIs (find_by_key, save_snippet, record_usage) are intentionally
// absent: lookup-only here; the write seam is P2a.

import { and, eq, inArray, isNotNull, like, or, type SQL } from "drizzle-orm";

import { metadataDb } from "./client";
import { sqlSnippets } from "./schema";

// The `graph:%` provenance filter that gates the search VOCABULARY: only the
// authoritative, YAML-derived ontology concepts (graph-agent snippets) seed the
// searchable terms. `query:`/`mcp:` per-execution snippets are excluded so their
// step-ids/UUIDs don't pollute the vocabulary — they remain reachable by
// `findGraphsByKeys`/`findById` (source is NOT filtered on the key match).
const GRAPH_SOURCE_LIKE = "graph:%";

/** One snippet row as the consumer reads it (non-null fields are DB NOT NULL). */
export interface SnippetRow {
	snippetId: string;
	snippetType: string;
	standardField: string | null;
	statement: string | null;
	aggregation: string | null;
	parameterValue: string | null;
	normalizedExpression: string | null;
	inputFields: unknown;
	sql: string;
	description: string;
	// DAT-616: {column_mappings_basis, …} — the agent's own prior value→concept
	// FILTER decisions, fed back so grounding isn't re-invented.
	provenance: unknown;
	source: string;
}

const SNIPPET_COLUMNS = {
	snippetId: sqlSnippets.snippetId,
	snippetType: sqlSnippets.snippetType,
	standardField: sqlSnippets.standardField,
	statement: sqlSnippets.statement,
	aggregation: sqlSnippets.aggregation,
	parameterValue: sqlSnippets.parameterValue,
	normalizedExpression: sqlSnippets.normalizedExpression,
	inputFields: sqlSnippets.inputFields,
	sql: sqlSnippets.sql,
	description: sqlSnippets.description,
	provenance: sqlSnippets.provenance,
	source: sqlSnippets.source,
} as const;

type RawSnippetRow = {
	[K in keyof typeof SNIPPET_COLUMNS]: unknown;
};

// The view is nullable-typed (Drizzle views default every column nullable), but
// snippet_id / snippet_type / sql / source are NOT NULL in the engine table. A
// null here means view/schema drift — fail LOUD rather than propagate a `null`
// typed as `string` into the consumer (the codebase's fail-loud + tool-output-
// narrowing ethos). `description` is NOT NULL with a "" default, so it coerces.
function req(value: unknown, column: string): string {
	if (typeof value !== "string") {
		throw new Error(
			`sql_snippets.${column} is null/non-string — view or schema drift?`,
		);
	}
	return value;
}

function mapRow(r: RawSnippetRow): SnippetRow {
	return {
		snippetId: req(r.snippetId, "snippet_id"),
		snippetType: req(r.snippetType, "snippet_type"),
		standardField: (r.standardField as string | null) ?? null,
		statement: (r.statement as string | null) ?? null,
		aggregation: (r.aggregation as string | null) ?? null,
		parameterValue: (r.parameterValue as string | null) ?? null,
		normalizedExpression: (r.normalizedExpression as string | null) ?? null,
		inputFields: r.inputFields,
		sql: req(r.sql, "sql"),
		description: (r.description as string | null) ?? "",
		provenance: r.provenance,
		source: req(r.source, "source"),
	};
}

/** A group of snippets sharing one `source` — the calc chain (engine SnippetGraph). */
export interface SnippetGraph {
	source: string;
	/** The id after the first `:` (e.g. `graph:dso` → `dso`). */
	graphId: string;
	/** The prefix before the first `:` (e.g. `graph`, `query`), or `unknown`. */
	sourceType: string;
	snippets: SnippetRow[];
}

function splitSource(source: string): { sourceType: string; graphId: string } {
	const idx = source.indexOf(":");
	if (idx === -1) return { sourceType: "unknown", graphId: source };
	return { sourceType: source.slice(0, idx), graphId: source.slice(idx + 1) };
}

/** The curated search terms surfaced from `graph:%` snippets. */
export interface SearchVocabulary {
	standardFields: string[];
	statements: string[];
	aggregations: string[];
	graphIds: string[];
}

/** The keys a caller selects from the vocabulary to discover snippet graphs. */
export interface SearchKeys {
	standardFields?: string[];
	statements?: string[];
	aggregations?: string[];
	graphIds?: string[];
}

/**
 * Distinct vocabulary terms for key-based discovery, curated from `graph:%`
 * snippets ONLY. Returns sorted, de-duplicated lists; `query:`/`mcp:` snippets
 * never contribute (the load-bearing `graph:%` filter).
 */
export async function getSearchVocabulary(
	workspaceId: string,
): Promise<SearchVocabulary> {
	const scope = and(
		eq(sqlSnippets.schemaMappingId, workspaceId),
		like(sqlSnippets.source, GRAPH_SOURCE_LIKE),
	);

	const standardFieldRows = await metadataDb
		.selectDistinct({ v: sqlSnippets.standardField })
		.from(sqlSnippets)
		.where(and(scope, isNotNull(sqlSnippets.standardField)));
	const statementRows = await metadataDb
		.selectDistinct({ v: sqlSnippets.statement })
		.from(sqlSnippets)
		.where(and(scope, isNotNull(sqlSnippets.statement)));
	const aggregationRows = await metadataDb
		.selectDistinct({ v: sqlSnippets.aggregation })
		.from(sqlSnippets)
		.where(and(scope, isNotNull(sqlSnippets.aggregation)));
	const sourceRows = await metadataDb
		.selectDistinct({ source: sqlSnippets.source })
		.from(sqlSnippets)
		.where(scope);

	const sorted = (rows: { v: string | null }[]): string[] =>
		rows
			.map((r) => r.v)
			.filter((v): v is string => v !== null)
			.sort();

	const graphIds = [
		...new Set(
			sourceRows
				.map((r) => r.source)
				.filter((s): s is string => s !== null)
				.filter((s) => s.includes(":"))
				.map((s) => s.slice(s.indexOf(":") + 1)),
		),
	].sort();

	return {
		standardFields: sorted(standardFieldRows),
		statements: sorted(statementRows),
		aggregations: sorted(aggregationRows),
		graphIds,
	};
}

/**
 * Find snippet graphs by direct key lookup. Mirrors the engine exactly:
 * - OR-union across the standardField / statement / aggregation categories, plus
 *   a direct `graph:<id>` source set from `graphIds`;
 * - each matched source expands to its WHOLE snippet group (the calc chain);
 * - graphs are sorted by source string, then sliced to `limit`;
 * - NO source filter on the key match — a `query:`-sourced snippet keyed by a
 *   concept IS returned (the learned-snippet reuse path).
 */
export async function findGraphsByKeys(
	workspaceId: string,
	keys: SearchKeys,
	limit = 50,
): Promise<SnippetGraph[]> {
	const matchedSources = new Set<string>();

	if (keys.graphIds?.length) {
		for (const gid of keys.graphIds) matchedSources.add(`graph:${gid}`);
	}

	const conditions: SQL[] = [];
	if (keys.standardFields?.length)
		conditions.push(inArray(sqlSnippets.standardField, keys.standardFields));
	if (keys.statements?.length)
		conditions.push(inArray(sqlSnippets.statement, keys.statements));
	if (keys.aggregations?.length)
		conditions.push(inArray(sqlSnippets.aggregation, keys.aggregations));

	if (conditions.length > 0) {
		const rows = await metadataDb
			.selectDistinct({ source: sqlSnippets.source })
			.from(sqlSnippets)
			.where(
				and(eq(sqlSnippets.schemaMappingId, workspaceId), or(...conditions)),
			);
		for (const r of rows) if (r.source !== null) matchedSources.add(r.source);
	}

	if (matchedSources.size === 0) return [];
	return expandToGraphs(workspaceId, [...matchedSources], limit);
}

async function expandToGraphs(
	workspaceId: string,
	sources: string[],
	limit: number,
): Promise<SnippetGraph[]> {
	if (sources.length === 0) return [];

	const rows = await metadataDb
		.select(SNIPPET_COLUMNS)
		.from(sqlSnippets)
		.where(
			and(
				eq(sqlSnippets.schemaMappingId, workspaceId),
				inArray(sqlSnippets.source, sources),
			),
		);

	const bySource = new Map<string, SnippetRow[]>();
	for (const raw of rows) {
		const row = mapRow(raw);
		const list = bySource.get(row.source);
		if (list) list.push(row);
		else bySource.set(row.source, [row]);
	}

	const graphs = [...bySource.keys()].sort().map((source): SnippetGraph => {
		const { sourceType, graphId } = splitSource(source);
		return {
			source,
			graphId,
			sourceType,
			snippets: bySource.get(source) ?? [],
		};
	});

	return graphs.slice(0, limit);
}

/**
 * Fetch a single snippet by id, or `null`. Intentionally NOT workspace-scoped:
 * mirrors the engine's `find_by_id` (a primary-key `session.get`), and
 * `snippet_id` is a globally-unique UUID, so a PK lookup returns the one row it
 * names and cannot leak across workspaces. Callers reach a valid id only via
 * `findGraphsByKeys` (which IS workspace-scoped), so the id is already correct.
 */
export async function findById(snippetId: string): Promise<SnippetRow | null> {
	const rows = await metadataDb
		.select(SNIPPET_COLUMNS)
		.from(sqlSnippets)
		.where(eq(sqlSnippets.snippetId, snippetId))
		.limit(1);
	return rows.length > 0 ? mapRow(rows[0]) : null;
}
