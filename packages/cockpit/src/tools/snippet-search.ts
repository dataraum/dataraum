// `snippet_search` — the query sub-agent's discovery tool over the validated
// SQL-snippet Knowledge Base (DAT-485).
//
// The reuse half of the migration: the sub-agent searches the curated snippet
// store by the vertical's vocabulary (concepts / statements / graph ids) to find
// validated calculation graphs to compose its answer from. It reads the engine-
// owned `sql_snippets` substrate through P0's read-only library
// (`findGraphsByKeys` / `getSearchVocabulary`, keyed on `config.dataraumWorkspaceId`
// — the dashed-UUID workspace VALUE, NOT the `ws_` schema name).
//
// The tool returns the concept-key metadata PLUS the snippet's validated `sql`
// body (DAT-494): snippet_id + the concept keys (standard_field / statement /
// aggregation) + the validated `sql` (the canonical, execution-tested computation)
// + the per-concept grounding record (column_mappings_basis, from provenance) +
// dependencies (input_fields). The model REPRODUCES the validated `sql`
// faithfully rather than reconstructing it — that faithful reproduction is what
// lets the reuse classify as `exact_reuse` (snippet bodies are tiny single-SELECTs,
// so this is cheap on context). The reuse is still CLASSIFIED, not SUBSTITUTED, by
// `classifyComponents` (query.ts): the model declares reuse (sets snippet_id) and
// addresses the table as lake.<layer>.<name>, so its executable (qualified) SQL is
// what runs; the stored BARE-name form would not resolve in the cockpit's execution
// context (the reason P1 classifies rather than substitutes), and
// `canonicalizeForReuse` strips the qualifier only for the equality DECISION.
//
// `buildVocabularyBlock` formats the searchable keys (`get_search_vocabulary`,
// `graph:%`-curated only) as a prompt block — the engine's `<available_search_keys>`
// — so the sub-agent knows what to search for before it calls the tool.

import { toolDefinition } from "@tanstack/ai";
import { z } from "zod";

import { config } from "../config";
import {
	findGraphsByKeys,
	getSearchVocabulary,
	type SearchVocabulary,
	type SnippetGraph,
	type SnippetRow,
} from "../db/metadata/snippet-library";
import { withAgentError } from "./agent-error";

// The per-snippet projection the model reuses from: the concept keys + the
// validated `sql` (the AUTHORITATIVE computation to reproduce, DAT-494) + the
// per-concept grounding record (column_mappings_basis; the `sql` is the thing
// to reproduce) + formula dependencies (input_fields). The producer-side
// `normalized_expression` stays internal.
const SnippetMeta = z.object({
	snippet_id: z.string(),
	snippet_type: z.string(),
	standard_field: z.string().nullable(),
	statement: z.string().nullable(),
	aggregation: z.string().nullable(),
	// The validated, execution-tested SQL body — the canonical computation the
	// model reproduces faithfully, re-qualifying the table to lake.<layer>.<name>.
	// NOT NULL in the store.
	sql: z.string(),
	description: z.string(),
	// JSONB blobs — formula dependencies + the constant value. Passed through
	// as-is (validated leniently by the model's use).
	input_fields: z.unknown(),
	parameter_value: z.string().nullable(),
	// DAT-616: the prior value→concept FILTER decisions ({concept:{column,filter,
	// resolution}}) — reuse the same grounding instead of re-inventing it.
	column_mappings_basis: z.unknown(),
});

// One matched calculation graph (a `source` group) and its member snippets.
const SnippetGraphProjection = z.object({
	graph_id: z.string(),
	source: z.string(),
	source_type: z.string(),
	snippets: z.array(SnippetMeta),
});
export type SnippetGraphProjection = z.infer<typeof SnippetGraphProjection>;

const SnippetSearchResult = withAgentError(z.array(SnippetGraphProjection));

export interface SnippetSearchInput {
	concepts?: string[];
	statements?: string[];
	graph_ids?: string[];
}

/** Project one library `SnippetRow` to the reuse shape — concept keys + the
 * validated `sql` to reproduce (DAT-494). Drops only the internal
 * `normalized_expression`. */
function projectSnippet(s: SnippetRow): z.infer<typeof SnippetMeta> {
	return {
		snippet_id: s.snippetId,
		snippet_type: s.snippetType,
		standard_field: s.standardField,
		statement: s.statement,
		aggregation: s.aggregation,
		sql: s.sql,
		description: s.description,
		input_fields: s.inputFields,
		parameter_value: s.parameterValue,
		column_mappings_basis:
			s.provenance && typeof s.provenance === "object"
				? (s.provenance as Record<string, unknown>).column_mappings_basis
				: undefined,
	};
}

/** Project a library `SnippetGraph` to the reuse shape — concept keys, the
 * validated `sql`, and the column expressions (DAT-494). */
export function projectGraph(g: SnippetGraph): SnippetGraphProjection {
	return {
		graph_id: g.graphId,
		source: g.source,
		source_type: g.sourceType,
		snippets: g.snippets.map(projectSnippet),
	};
}

/**
 * Search the snippet KB by vocabulary keys and return matching graphs — concept
 * keys + each snippet's validated `sql` to reproduce (DAT-494). At least one key
 * category is required — an
 * all-empty call is agent-fixable (`{ error }`) so the model picks keys from the
 * advertised vocabulary instead of searching blindly. An empty match set is a
 * legitimate `[]` (nothing curated for those keys yet), not an error.
 */
export async function snippetSearch(
	input: SnippetSearchInput,
): Promise<SnippetGraphProjection[] | { error: string }> {
	const concepts = input.concepts ?? [];
	const statements = input.statements ?? [];
	const graphIds = input.graph_ids ?? [];

	if (
		concepts.length === 0 &&
		statements.length === 0 &&
		graphIds.length === 0
	) {
		return {
			error:
				"Provide at least one search key — concepts, statements, or graph_ids " +
				"drawn from the available vocabulary.",
		};
	}

	// Read-path workspace scoping resolves from the env-designated workspace, not
	// the cockpit_db registry (DAT-505 boundary): identical in single-active-
	// workspace. Per-request registry resolution for reads is the DAT-357 switcher.
	const graphs = await findGraphsByKeys(config.dataraumWorkspaceId, {
		standardFields: concepts.length > 0 ? concepts : undefined,
		statements: statements.length > 0 ? statements : undefined,
		graphIds: graphIds.length > 0 ? graphIds : undefined,
	});

	return graphs.map(projectGraph);
}

export const snippetSearchTool = toolDefinition({
	name: "snippet_search",
	description:
		"Search the validated SQL Knowledge Base for snippet graphs that match the " +
		"concepts a question needs. Pass keys drawn from the available vocabulary: " +
		"`concepts` (business concepts like 'revenue', 'accounts_receivable'), " +
		"`statements` (e.g. 'income_statement'), and/or `graph_ids` (specific " +
		"calculation graphs like 'dso'). Returns matching graphs with each snippet's " +
		"concept keys, its validated `sql` (the canonical computation to reproduce), " +
		"its grounding record (column_mappings_basis), and dependencies. Match by the " +
		"concept keys, then reproduce the chosen snippet's validated SQL — set the " +
		"step's snippet_id to declare reuse. Returns [] when nothing is curated for " +
		"those keys yet.",
	inputSchema: z.object({
		concepts: z
			.array(z.string())
			.optional()
			.describe("Business-concept keys (standard_field) to match."),
		statements: z
			.array(z.string())
			.optional()
			.describe("Statement-type keys to match (e.g. 'income_statement')."),
		graph_ids: z
			.array(z.string())
			.optional()
			.describe("Specific calculation-graph ids to pull whole (e.g. 'dso')."),
	}),
	outputSchema: SnippetSearchResult,
}).server((input) => snippetSearch(input));

/**
 * Format the curated search vocabulary as the sub-agent's `<available_search_keys>`
 * prompt block (pure). Lists each non-empty category; an empty vocabulary (no
 * `graph:%` snippets curated yet) yields a one-line note so the sub-agent composes
 * fresh SQL from the schema directly instead of searching blindly.
 */
export function formatVocabulary(vocab: SearchVocabulary): string {
	const isEmpty =
		vocab.standardFields.length === 0 &&
		vocab.statements.length === 0 &&
		vocab.aggregations.length === 0 &&
		vocab.graphIds.length === 0;

	if (isEmpty) {
		return (
			"<available_search_keys>\n" +
			"(No curated SQL snippets exist yet — there is nothing to reuse. Compose " +
			"fresh SQL from the schema below.)\n" +
			"</available_search_keys>"
		);
	}

	const line = (label: string, values: string[]): string =>
		values.length > 0 ? `${label}: ${values.join(", ")}` : "";

	const body = [
		line("concepts", vocab.standardFields),
		line("statements", vocab.statements),
		line("aggregations", vocab.aggregations),
		line("graph_ids", vocab.graphIds),
	]
		.filter((l) => l !== "")
		.join("\n");

	return (
		"<available_search_keys>\n" +
		"Search the snippet KB with these keys (snippet_search). Match a question's " +
		"concepts to these terms; do NOT invent keys.\n" +
		`${body}\n` +
		"</available_search_keys>"
	);
}

/** Read + format the search vocabulary for the active workspace (the prompt block). */
export async function buildVocabularyBlock(): Promise<string> {
	// Read-path workspace scoping from the env-designated workspace, not the
	// registry (DAT-505 boundary; per-request registry reads = DAT-357 switcher).
	const vocab = await getSearchVocabulary(config.dataraumWorkspaceId);
	return formatVocabulary(vocab);
}
