import { describe, expect, it } from "vitest";

import {
	buildConceptGraph,
	type ConceptEdgeRow,
	type ConceptRow,
	formatConceptContext,
	type GroundingRow,
	parseWherePredicates,
} from "./concept-graph";

function concept(
	overrides: Partial<ConceptRow> & { name: string },
): ConceptRow {
	return {
		conceptId: `id:${overrides.name}`,
		kind: null,
		description: null,
		indicators: null,
		excludePatterns: null,
		supersededAt: null,
		...overrides,
	};
}

function edge(
	overrides: Partial<ConceptEdgeRow> & {
		predicate: string;
		fromConcept: string;
		toConcept: string;
	},
): ConceptEdgeRow {
	return {
		edgeId: `${overrides.fromConcept}->${overrides.toConcept}:${overrides.predicate}`,
		tolerance: null,
		supersededAt: null,
		...overrides,
	};
}

function grounding(
	overrides: Partial<GroundingRow> & { concept: string },
): GroundingRow {
	return {
		snippetId: `snippet:${overrides.concept}:${Math.random()}`,
		statement: null,
		relation: null,
		selectExpr: null,
		wherePredicates: null,
		failed: false,
		...overrides,
	};
}

describe("buildConceptGraph", () => {
	it("every active concept is a node, even with zero edges/groundings (no ungrounded-node regressions)", () => {
		const graph = buildConceptGraph({
			concepts: [concept({ name: "revenue" })],
			edges: [],
			groundings: [],
		});
		expect(graph.nodes).toHaveLength(1);
		expect(graph.nodes[0]).toMatchObject({
			id: "concept:revenue",
			name: "revenue",
			groundings: [],
			partOfParents: [],
			disjointWith: [],
			reconcilesWith: [],
		});
	});

	it("drops a superseded concept entirely (not run-versioned — superseded_at is the identity gate)", () => {
		const graph = buildConceptGraph({
			concepts: [
				concept({ name: "old_revenue", supersededAt: new Date() }),
				concept({ name: "revenue" }),
			],
			edges: [],
			groundings: [],
		});
		expect(graph.nodes.map((n) => n.name)).toEqual(["revenue"]);
	});

	it("resolves part_of into parents (1-hop) and children (1-hop), directed", () => {
		const graph = buildConceptGraph({
			concepts: [
				concept({ name: "cash" }),
				concept({ name: "current_assets" }),
			],
			edges: [
				edge({
					predicate: "part_of",
					fromConcept: "cash",
					toConcept: "current_assets",
				}),
			],
			groundings: [],
		});
		const cash = graph.nodes.find((n) => n.name === "cash");
		const currentAssets = graph.nodes.find((n) => n.name === "current_assets");
		expect(cash?.partOfParents).toEqual(["current_assets"]);
		expect(cash?.partOfChildren).toEqual([]);
		expect(currentAssets?.partOfChildren).toEqual(["cash"]);
		expect(currentAssets?.partOfParents).toEqual([]);
	});

	it("walks bounded transitive part_of ancestry beyond the 1-hop parent, nearest-first", () => {
		const graph = buildConceptGraph({
			concepts: [
				concept({ name: "cash" }),
				concept({ name: "current_assets" }),
				concept({ name: "assets" }),
				concept({ name: "balance_sheet" }),
			],
			edges: [
				edge({
					predicate: "part_of",
					fromConcept: "cash",
					toConcept: "current_assets",
				}),
				edge({
					predicate: "part_of",
					fromConcept: "current_assets",
					toConcept: "assets",
				}),
				edge({
					predicate: "part_of",
					fromConcept: "assets",
					toConcept: "balance_sheet",
				}),
			],
			groundings: [],
		});
		const cash = graph.nodes.find((n) => n.name === "cash");
		expect(cash?.partOfParents).toEqual(["current_assets"]);
		expect(cash?.partOfAncestry).toEqual(["assets", "balance_sheet"]);
	});

	it("never hangs on a part_of cycle — the ancestry walk is bounded and cycle-safe", () => {
		const graph = buildConceptGraph({
			concepts: [concept({ name: "a" }), concept({ name: "b" })],
			edges: [
				edge({ predicate: "part_of", fromConcept: "a", toConcept: "b" }),
				edge({ predicate: "part_of", fromConcept: "b", toConcept: "a" }),
			],
			groundings: [],
		});
		const a = graph.nodes.find((n) => n.name === "a");
		// b is the 1-hop parent; the cycle back to "a" must never appear (self-exclusion).
		expect(a?.partOfParents).toEqual(["b"]);
		expect(a?.partOfAncestry).toEqual([]);
	});

	it("reads disjoint_with directionally — no client-side symmetrization (the engine stores both directions itself)", () => {
		const graph = buildConceptGraph({
			concepts: [concept({ name: "asset" }), concept({ name: "liability" })],
			edges: [
				edge({
					predicate: "disjoint_with",
					fromConcept: "asset",
					toConcept: "liability",
				}),
			],
			groundings: [],
		});
		const asset = graph.nodes.find((n) => n.name === "asset");
		const liability = graph.nodes.find((n) => n.name === "liability");
		expect(asset?.disjointWith).toEqual(["liability"]);
		// Only one direction was written in this fixture — the builder must not invent the reverse.
		expect(liability?.disjointWith).toEqual([]);
	});

	it("carries reconciles_with self-loops (multi-grounding tie-out) and cross-concept assertions with tolerance", () => {
		const graph = buildConceptGraph({
			concepts: [
				concept({ name: "account_balance" }),
				concept({ name: "gl_balance" }),
			],
			edges: [
				edge({
					predicate: "reconciles_with",
					fromConcept: "account_balance",
					toConcept: "account_balance",
				}),
				edge({
					predicate: "reconciles_with",
					fromConcept: "account_balance",
					toConcept: "gl_balance",
					tolerance: 0.01,
				}),
			],
			groundings: [],
		});
		const node = graph.nodes.find((n) => n.name === "account_balance");
		expect(node?.reconcilesWith).toEqual([
			{ partner: "account_balance", tolerance: null },
			{ partner: "gl_balance", tolerance: 0.01 },
		]);
	});

	it("drops an edge with a superseded or missing endpoint, never throws (dangling-reference safety)", () => {
		const graph = buildConceptGraph({
			concepts: [concept({ name: "cash" })],
			edges: [
				edge({
					predicate: "part_of",
					fromConcept: "cash",
					toConcept: "current_assets",
				}),
				edge({
					predicate: "part_of",
					fromConcept: "cash",
					toConcept: "retired_concept",
					supersededAt: new Date(),
				}),
			],
			groundings: [],
		});
		expect(graph.edges).toHaveLength(0);
		expect(graph.nodes.find((n) => n.name === "cash")?.partOfParents).toEqual(
			[],
		);
	});

	it("drops an edge with an unrecognized predicate rather than miscategorizing it", () => {
		const graph = buildConceptGraph({
			concepts: [concept({ name: "a" }), concept({ name: "b" })],
			edges: [edge({ predicate: "same_as", fromConcept: "a", toConcept: "b" })],
			groundings: [],
		});
		expect(graph.edges).toHaveLength(0);
	});

	it("carries multiple groundings for one concept (multi-groundings), matched by name", () => {
		const graph = buildConceptGraph({
			concepts: [concept({ name: "account_balance" })],
			edges: [],
			groundings: [
				grounding({ concept: "account_balance", relation: "trial_balance" }),
				grounding({ concept: "account_balance", relation: "balance_sheet" }),
				grounding({ concept: "other_concept", relation: "irrelevant" }),
			],
		});
		const node = graph.nodes.find((n) => n.name === "account_balance");
		expect(node?.groundings).toHaveLength(2);
		expect(node?.groundings.map((g) => g.relation).sort()).toEqual([
			"balance_sheet",
			"trial_balance",
		]);
	});

	it("drops a grounding naming a concept that doesn't exist (or is superseded) — never fabricates a node for it", () => {
		const graph = buildConceptGraph({
			concepts: [],
			edges: [],
			groundings: [grounding({ concept: "ghost_concept" })],
		});
		expect(graph.nodes).toHaveLength(0);
	});

	it("narrows indicators/exclude_patterns json columns defensively (rule 11) — non-array or absent yields []", () => {
		const graph = buildConceptGraph({
			concepts: [
				concept({
					name: "revenue",
					indicators: ["income", "sales"],
					excludePatterns: null,
				}),
				concept({
					name: "cost",
					indicators: "not-an-array",
					excludePatterns: undefined,
				}),
			],
			edges: [],
			groundings: [],
		});
		expect(graph.nodes.find((n) => n.name === "revenue")?.indicators).toEqual([
			"income",
			"sales",
		]);
		expect(graph.nodes.find((n) => n.name === "cost")?.indicators).toEqual([]);
	});
});

describe("parseWherePredicates", () => {
	it("parses a JSON array of declared predicate strings", () => {
		expect(
			parseWherePredicates('["region = \'EU\'", "active = true"]'),
		).toEqual(["region = 'EU'", "active = true"]);
	});

	it("returns [] for null, the literal 'null' string, invalid JSON, or a non-array", () => {
		expect(parseWherePredicates(null)).toEqual([]);
		expect(parseWherePredicates("null")).toEqual([]);
		expect(parseWherePredicates("{not json")).toEqual([]);
		expect(parseWherePredicates('{"not": "an array"}')).toEqual([]);
	});
});

describe("formatConceptContext", () => {
	it("omits the block entirely for an empty graph (never an empty-but-present tag)", () => {
		expect(formatConceptContext({ nodes: [], edges: [] })).toBe("");
	});

	it("renders name/kind/description, sorted by name for a deterministic prompt", () => {
		const text = formatConceptContext({
			nodes: [
				{
					id: "concept:revenue",
					conceptId: "id:revenue",
					name: "revenue",
					kind: "measure",
					description: "Recognized income",
					indicators: [],
					excludePatterns: [],
					partOfParents: [],
					partOfChildren: [],
					partOfAncestry: [],
					disjointWith: [],
					reconcilesWith: [],
					groundings: [],
				},
				{
					id: "concept:assets",
					conceptId: "id:assets",
					name: "assets",
					kind: null,
					description: null,
					indicators: [],
					excludePatterns: [],
					partOfParents: [],
					partOfChildren: [],
					partOfAncestry: [],
					disjointWith: [],
					reconcilesWith: [],
					groundings: [],
				},
			],
			edges: [],
		});
		expect(text).toContain("<business_concepts>");
		expect(text.indexOf("- assets")).toBeLessThan(
			text.indexOf("- revenue (measure)"),
		);
		expect(text).toContain("- revenue (measure): Recognized income");
	});

	it("renders part_of with the ancestry arrow, subconcepts, disjoint_with, and both reconciliation wordings", () => {
		const text = formatConceptContext({
			nodes: [
				{
					id: "concept:cash",
					conceptId: "id:cash",
					name: "cash",
					kind: null,
					description: null,
					indicators: [],
					excludePatterns: [],
					partOfParents: ["current_assets"],
					partOfChildren: ["petty_cash"],
					partOfAncestry: ["assets", "balance_sheet"],
					disjointWith: ["liability"],
					reconcilesWith: [
						{ partner: "cash", tolerance: null },
						{ partner: "gl_cash", tolerance: 0.01 },
					],
					groundings: [],
				},
			],
			edges: [],
		});
		expect(text).toContain(
			"part of: current_assets (→ assets → balance_sheet)",
		);
		expect(text).toContain("subconcepts: petty_cash");
		expect(text).toContain("disjoint with: liability");
		expect(text).toContain(
			"reconciles: across its own groundings — must tie out",
		);
		expect(text).toContain("reconciles with: gl_cash (tolerance 0.01)");
	});

	it("renders healthy groundings with statement/relation/select/where, and counts failed ones without their text", () => {
		const text = formatConceptContext({
			nodes: [
				{
					id: "concept:account_balance",
					conceptId: "id:account_balance",
					name: "account_balance",
					kind: null,
					description: null,
					indicators: [],
					excludePatterns: [],
					partOfParents: [],
					partOfChildren: [],
					partOfAncestry: [],
					disjointWith: [],
					reconcilesWith: [],
					groundings: [
						{
							snippetId: "s1",
							statement: "ending balance",
							relation: "trial_balance",
							selectExpr: "SUM(ending_balance)",
							wherePredicates: ["account_type = 'asset'"],
							failed: false,
						},
						{
							snippetId: "s2",
							statement: null,
							relation: "balance_sheet",
							selectExpr: null,
							wherePredicates: [],
							failed: true,
						},
					],
				},
			],
			edges: [],
		});
		expect(text).toContain(
			"grounded by:\n    - ending balance @ trial_balance: SUM(ending_balance) WHERE account_type = 'asset'",
		);
		expect(text).toContain("1 failed grounding attempt(s) not shown here");
		expect(text).not.toContain("balance_sheet: ");
	});
});
