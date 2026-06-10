// Unit coverage for snippet_search's PURE projection + vocabulary formatting +
// the no-keys guard (DAT-485). The live key-lookup is integration-covered via
// P0's snippet-library.integration.test.ts; here we pin the metadata projection
// (no raw SQL bodies) and the prompt block.

import { describe, expect, it, vi } from "vitest";

// snippet_search → ../config + the metadata client (via snippet-library). The
// pure functions under test touch neither; stub the boundary so the graph loads.
vi.mock("#/config", () => ({ config: { dataraumWorkspaceId: "ws-test" } }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

import type {
	SearchVocabulary,
	SnippetGraph,
} from "../db/metadata/snippet-library";
import {
	formatVocabulary,
	projectGraph,
	snippetSearch,
} from "./snippet-search";

const graph = (): SnippetGraph => ({
	source: "graph:dso",
	graphId: "dso",
	sourceType: "graph",
	snippets: [
		{
			snippetId: "snip-1",
			snippetType: "extract",
			standardField: "revenue",
			statement: "income_statement",
			aggregation: "sum",
			parameterValue: null,
			normalizedExpression: "sum(betrag)",
			inputFields: null,
			sql: 'SELECT SUM("Betrag") AS revenue FROM journal_lines',
			description: "Revenue from the income statement",
			columnMappings: { revenue: 'SUM("Betrag")' },
			source: "graph:dso",
		},
	],
});

describe("projectGraph", () => {
	it("projects the concept keys + the validated sql; DROPS only normalized_expression", () => {
		const p = projectGraph(graph());
		expect(p.graph_id).toBe("dso");
		expect(p.source).toBe("graph:dso");
		expect(p.source_type).toBe("graph");
		expect(p.snippets).toHaveLength(1);
		const s = p.snippets[0];
		expect(s).toEqual({
			snippet_id: "snip-1",
			snippet_type: "extract",
			standard_field: "revenue",
			statement: "income_statement",
			aggregation: "sum",
			// DAT-494: the validated sql IS surfaced — the model reproduces it.
			sql: 'SELECT SUM("Betrag") AS revenue FROM journal_lines',
			description: "Revenue from the income statement",
			column_mappings: { revenue: 'SUM("Betrag")' },
			input_fields: null,
			parameter_value: null,
		});
		// The validated sql is now part of the projection (DAT-494, the thing the
		// model reproduces) — but the producer-internal normalized_expression stays out.
		expect(s.sql).toBe('SELECT SUM("Betrag") AS revenue FROM journal_lines');
		expect("normalized_expression" in s).toBe(false);
	});
});

describe("snippetSearch no-keys guard", () => {
	it("returns { error } when no key category is provided", async () => {
		const result = await snippetSearch({});
		expect("error" in result).toBe(true);
		if (!("error" in result)) throw new Error("expected error");
		expect(result.error).toContain("at least one search key");
	});

	it("also errors on all-empty key arrays", async () => {
		const result = await snippetSearch({
			concepts: [],
			statements: [],
			graph_ids: [],
		});
		expect("error" in result).toBe(true);
	});
});

describe("formatVocabulary", () => {
	it("lists each non-empty category", () => {
		const vocab: SearchVocabulary = {
			standardFields: ["revenue", "accounts_receivable"],
			statements: ["income_statement"],
			aggregations: ["sum"],
			graphIds: ["dso"],
		};
		const block = formatVocabulary(vocab);
		expect(block).toContain("<available_search_keys>");
		expect(block).toContain("concepts: revenue, accounts_receivable");
		expect(block).toContain("statements: income_statement");
		expect(block).toContain("aggregations: sum");
		expect(block).toContain("graph_ids: dso");
	});

	it("omits empty categories", () => {
		const vocab: SearchVocabulary = {
			standardFields: ["revenue"],
			statements: [],
			aggregations: [],
			graphIds: [],
		};
		const block = formatVocabulary(vocab);
		expect(block).toContain("concepts: revenue");
		expect(block).not.toContain("statements:");
		expect(block).not.toContain("aggregations:");
		expect(block).not.toContain("graph_ids:");
	});

	it("notes an empty vocabulary so the model composes fresh SQL", () => {
		const block = formatVocabulary({
			standardFields: [],
			statements: [],
			aggregations: [],
			graphIds: [],
		});
		expect(block).toContain("No curated SQL snippets exist yet");
		expect(block).toContain("<available_search_keys>");
	});
});
