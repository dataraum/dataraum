import { describe, expect, it } from "vitest";

import {
	buildConceptGraph,
	type ConceptEdgeRow,
	type ConceptGraph,
	type ConceptRow,
	formatConceptContext,
	type GroundingRow,
	isReusableGrounding,
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
		provenance: null,
		...overrides,
	};
}

/** A part_of CHAIN of `length` edges: names[0] part_of names[1] part_of … —
 *  used by the ancestry-depth tests below. */
function chainOf(names: string[]): ConceptEdgeRow[] {
	const edges: ConceptEdgeRow[] = [];
	for (let i = 0; i < names.length - 1; i++) {
		edges.push(
			edge({
				predicate: "part_of",
				fromConcept: names[i],
				toConcept: names[i + 1],
			}),
		);
	}
	return edges;
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

	it("sorts nodes by name — the ONE place this order is decided (no consumer re-sorts)", () => {
		const graph = buildConceptGraph({
			concepts: [
				concept({ name: "revenue" }),
				concept({ name: "assets" }),
				concept({ name: "cogs" }),
			],
			edges: [],
			groundings: [],
		});
		expect(graph.nodes.map((n) => n.name)).toEqual([
			"assets",
			"cogs",
			"revenue",
		]);
	});

	it("resolves part_of into parents (1-hop) and children (1-hop), directed and sorted", () => {
		const graph = buildConceptGraph({
			concepts: [
				concept({ name: "cash" }),
				concept({ name: "current_assets" }),
				concept({ name: "other_assets" }),
			],
			edges: [
				edge({
					predicate: "part_of",
					fromConcept: "cash",
					toConcept: "other_assets",
				}),
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
		// Two parents inserted "other_assets" then "current_assets" — sorted output.
		expect(cash?.partOfParents).toEqual(["current_assets", "other_assets"]);
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
			edges: chainOf(["cash", "current_assets", "assets", "balance_sheet"]),
			groundings: [],
		});
		const cash = graph.nodes.find((n) => n.name === "cash");
		expect(cash?.partOfParents).toEqual(["current_assets"]);
		expect(cash?.partOfAncestry).toEqual(["assets", "balance_sheet"]);
	});

	// CRITICAL: pins the boundary between PART_OF_ANCESTRY_DEPTH (=3, the
	// number of hops WALKED beyond the 1-hop parent) and the engine's
	// _PART_OF_MAX_DEPTH (=4, the TOTAL depth from the origin concept) — the
	// two constants read like an off-by-one but agree on this exact boundary.
	// A 5-deep chain: a→b→c→d→e→f (b=depth1 .. f=depth5). For "a": parents=[b],
	// ancestry must include c,d,e (depths 2-4) and EXCLUDE f (depth 5).
	it("pins the ancestry depth boundary — the 5th ancestor is excluded, matching the engine's _PART_OF_MAX_DEPTH=4", () => {
		const names = ["a", "b", "c", "d", "e", "f"];
		const graph = buildConceptGraph({
			concepts: names.map((n) => concept({ name: n })),
			edges: chainOf(names),
			groundings: [],
		});
		const a = graph.nodes.find((n) => n.name === "a");
		expect(a?.partOfParents).toEqual(["b"]);
		expect(a?.partOfAncestry).toEqual(["c", "d", "e"]);
		expect(a?.partOfAncestry).not.toContain("f");
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

	it("reads disjoint_with directionally and sorted — no client-side symmetrization (the engine stores both directions itself)", () => {
		const graph = buildConceptGraph({
			concepts: [
				concept({ name: "asset" }),
				concept({ name: "liability" }),
				concept({ name: "equity" }),
			],
			edges: [
				edge({
					predicate: "disjoint_with",
					fromConcept: "asset",
					toConcept: "equity",
				}),
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
		expect(asset?.disjointWith).toEqual(["equity", "liability"]);
		// Only one direction was written in this fixture — the builder must not invent the reverse.
		expect(liability?.disjointWith).toEqual([]);
	});

	it("carries reconciles_with self-loops (multi-grounding tie-out) and cross-concept assertions, sorted by partner", () => {
		const graph = buildConceptGraph({
			concepts: [
				concept({ name: "account_balance" }),
				concept({ name: "gl_balance" }),
			],
			edges: [
				edge({
					predicate: "reconciles_with",
					fromConcept: "account_balance",
					toConcept: "gl_balance",
					tolerance: 0.01,
				}),
				edge({
					predicate: "reconciles_with",
					fromConcept: "account_balance",
					toConcept: "account_balance",
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
		const a = graph.nodes.find((n) => n.name === "a");
		expect(a?.partOfParents).toEqual([]);
		expect(a?.disjointWith).toEqual([]);
		expect(a?.reconcilesWith).toEqual([]);
	});

	it("carries multiple groundings for one concept (multi-groundings), matched by name, sorted (failed, relation, snippetId)", () => {
		const graph = buildConceptGraph({
			concepts: [concept({ name: "account_balance" })],
			edges: [],
			groundings: [
				grounding({
					concept: "account_balance",
					relation: "trial_balance",
					snippetId: "s2",
				}),
				grounding({
					concept: "account_balance",
					relation: "balance_sheet",
					snippetId: "s1",
				}),
				grounding({ concept: "other_concept", relation: "irrelevant" }),
			],
		});
		const node = graph.nodes.find((n) => n.name === "account_balance");
		expect(node?.groundings.map((g) => g.relation)).toEqual([
			"balance_sheet",
			"trial_balance",
		]);
	});

	it("sorts groundings healthy-before-failed, then by relation, then snippetId", () => {
		const graph = buildConceptGraph({
			concepts: [concept({ name: "account_balance" })],
			edges: [],
			groundings: [
				grounding({
					concept: "account_balance",
					relation: "trial_balance",
					failed: true,
					snippetId: "s-failed",
				}),
				grounding({
					concept: "account_balance",
					relation: "balance_sheet",
					failed: false,
					snippetId: "s-healthy",
				}),
			],
		});
		const node = graph.nodes.find((n) => n.name === "account_balance");
		expect(node?.groundings.map((g) => g.snippetId)).toEqual([
			"s-healthy",
			"s-failed",
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

	it("parses a failed grounding's provenance into failureMode/failureReason (rule 11 — defensive narrowing)", () => {
		const graph = buildConceptGraph({
			concepts: [concept({ name: "account_balance" })],
			edges: [],
			groundings: [
				grounding({
					concept: "account_balance",
					failed: true,
					provenance: {
						failure_mode: "no_support",
						failure_reason: "0 rows matched",
					},
				}),
				grounding({
					concept: "account_balance",
					failed: true,
					snippetId: "no-provenance",
					provenance: null,
				}),
				grounding({
					concept: "account_balance",
					failed: true,
					snippetId: "malformed-provenance",
					provenance: "not an object",
				}),
			],
		});
		const node = graph.nodes.find((n) => n.name === "account_balance");
		const withDetail = node?.groundings.find(
			(g) => g.failureMode === "no_support",
		);
		expect(withDetail?.failureReason).toBe("0 rows matched");
		const noProvenance = node?.groundings.find(
			(g) => g.snippetId === "no-provenance",
		);
		expect(noProvenance).toMatchObject({
			failureMode: null,
			failureReason: null,
		});
		const malformed = node?.groundings.find(
			(g) => g.snippetId === "malformed-provenance",
		);
		expect(malformed).toMatchObject({ failureMode: null, failureReason: null });
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

describe("isReusableGrounding", () => {
	it("is false for a healthy grounding with no relation (a pre-parts row the engine also skips)", () => {
		expect(
			isReusableGrounding({
				snippetId: "s1",
				statement: null,
				relation: null,
				selectExpr: null,
				wherePredicates: [],
				failed: false,
				failureMode: null,
				failureReason: null,
			}),
		).toBe(false);
	});

	it("is true for a healthy grounding with a relation, false for any failed grounding", () => {
		const base = {
			snippetId: "s1",
			statement: null,
			selectExpr: null,
			wherePredicates: [],
			failureMode: null,
			failureReason: null,
		};
		expect(
			isReusableGrounding({ ...base, relation: "orders", failed: false }),
		).toBe(true);
		expect(
			isReusableGrounding({ ...base, relation: "orders", failed: true }),
		).toBe(false);
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
		expect(formatConceptContext({ nodes: [] })).toBe("");
	});

	it("renders name/kind/description in the given (builder-sorted) node order", () => {
		const graph: ConceptGraph = {
			nodes: [
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
			],
		};
		const text = formatConceptContext(graph);
		expect(text).toContain("<business_concepts>");
		expect(text.indexOf("- assets")).toBeLessThan(
			text.indexOf("- revenue (measure)"),
		);
		expect(text).toContain("- revenue (measure): Recognized income");
	});

	it("renders part_of with the ancestry arrow, subconcepts, disjoint_with, and both reconciliation wordings", () => {
		const graph: ConceptGraph = {
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
		};
		const text = formatConceptContext(graph);
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

	it("renders reusable healthy groundings under 'grounded by:' and each failed grounding discriminated by mode/reason", () => {
		const graph: ConceptGraph = {
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
							failureMode: null,
							failureReason: null,
						},
						{
							snippetId: "s2",
							statement: null,
							relation: "balance_sheet",
							selectExpr: null,
							wherePredicates: [],
							failed: true,
							failureMode: "no_support",
							failureReason: "0 rows matched the filter",
						},
					],
				},
			],
		};
		const text = formatConceptContext(graph);
		expect(text).toContain(
			"grounded by:\n    - ending balance @ trial_balance: SUM(ending_balance) WHERE account_type = 'asset'",
		);
		expect(text).toContain(
			"failed attempt [no_support]: 0 rows matched the filter",
		);
		expect(text).not.toContain("balance_sheet: ");
	});

	it("skips a healthy-but-relation-less grounding from 'grounded by:' and discloses the skip (matching the engine's own skip)", () => {
		const graph: ConceptGraph = {
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
							snippetId: "s-no-relation",
							statement: "ending balance",
							relation: null,
							selectExpr: "SUM(ending_balance)",
							wherePredicates: [],
							failed: false,
							failureMode: null,
							failureReason: null,
						},
					],
				},
			],
		};
		const text = formatConceptContext(graph);
		expect(text).not.toContain("grounded by:");
		expect(text).toContain(
			"1 grounding(s) recorded with no relation — not reusable, omitted",
		);
	});

	it("renders a failure with no recorded detail using the honest fallback wording", () => {
		const graph: ConceptGraph = {
			nodes: [
				{
					id: "concept:x",
					conceptId: "id:x",
					name: "x",
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
							statement: null,
							relation: null,
							selectExpr: null,
							wherePredicates: [],
							failed: true,
							failureMode: null,
							failureReason: null,
						},
					],
				},
			],
		};
		const text = formatConceptContext(graph);
		expect(text).toContain("failed attempt [failed]: (no reason recorded)");
	});
});
