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
// The tool returns a METADATA PROJECTION, never raw SQL bodies: snippet_id +
// the concept keys (standard_field / statement / aggregation) + the validated
// column expressions (column_mappings) + dependencies (input_fields) — enough to
// MATCH a snippet by metadata and reconstruct its SQL grounded in the validated
// column expressions, while keeping the sub-agent's context lean. The authoritative
// SQL is substituted post-chat by `resolveSnippetReferences` (query.ts) when the
// model declares reuse (sets snippet_id) and its SQL matches the stored one.
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

// The per-snippet metadata projection — every reusable signal EXCEPT the raw SQL
// body (`sql`) and the producer-side `normalized_expression`. `column_mappings`
// carries the validated SQL expression fragments (e.g. {"revenue":"SUM(\"Betrag\")"}),
// which is the lean, reusable essence the model grounds its SQL in.
const SnippetMeta = z.object({
	snippet_id: z.string(),
	snippet_type: z.string(),
	standard_field: z.string().nullable(),
	statement: z.string().nullable(),
	aggregation: z.string().nullable(),
	description: z.string(),
	// JSONB blobs — concrete column expressions + formula dependencies + the
	// constant value. Passed through as-is (validated leniently by the model's use).
	column_mappings: z.unknown(),
	input_fields: z.unknown(),
	parameter_value: z.string().nullable(),
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

/** Project one library `SnippetRow` to the lean metadata shape (drops raw SQL). */
function projectSnippet(s: SnippetRow): z.infer<typeof SnippetMeta> {
	return {
		snippet_id: s.snippetId,
		snippet_type: s.snippetType,
		standard_field: s.standardField,
		statement: s.statement,
		aggregation: s.aggregation,
		description: s.description,
		column_mappings: s.columnMappings,
		input_fields: s.inputFields,
		parameter_value: s.parameterValue,
	};
}

/** Project a library `SnippetGraph` to the tool's metadata projection. */
export function projectGraph(g: SnippetGraph): SnippetGraphProjection {
	return {
		graph_id: g.graphId,
		source: g.source,
		source_type: g.sourceType,
		snippets: g.snippets.map(projectSnippet),
	};
}

/**
 * Search the snippet KB by vocabulary keys and return matching graphs as a
 * metadata projection (no raw SQL). At least one key category is required — an
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
		"metadata — snippet_id, the concept keys, the validated column expressions " +
		"(column_mappings), and dependencies — but NOT the raw SQL. Match by this " +
		"metadata; set a step's snippet_id to the chosen snippet to declare reuse. " +
		"Returns [] when nothing is curated for those keys yet.",
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
	const vocab = await getSearchVocabulary(config.dataraumWorkspaceId);
	return formatVocabulary(vocab);
}
