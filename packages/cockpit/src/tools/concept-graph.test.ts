import { describe, expect, it } from "vitest";

import {
	buildConceptGraph,
	type ConceptEdgeRow,
	type ConceptGraph,
	type ConceptGraphInput,
	type ConceptReconciliation,
	type ConceptRow,
	formatConceptContext,
	type GroundingRow,
	isReusableGrounding,
	parseWherePredicates,
	pyG,
	type ReconciliationRow,
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

/** `buildConceptGraph` with the DAT-739 reconciliation rows defaulted empty —
 *  most cases here predate the evaluated state and assert structure, not the
 *  tie-out fold (which has its own suite below). */
function build(
	input: Omit<ConceptGraphInput, "reconciliations"> &
		Partial<Pick<ConceptGraphInput, "reconciliations">>,
): ConceptGraph {
	return buildConceptGraph({ reconciliations: [], ...input });
}

/** A never-evaluated assertion, as the builder emits it when no promoted run
 *  carries a row for the pair. */
function unevaluated(
	partner: string,
	tolerance: number | null = null,
): ConceptReconciliation {
	return {
		partner,
		tolerance,
		status: null,
		verdict: null,
		abstainReason: null,
		observedDelta: null,
		relativeDelta: null,
		pairs: 0,
		evaluatedPairs: 0,
	};
}

/** One raw per-pair row with sane defaults (an exact-tie evaluated pair). */
function recRow(
	overrides: Partial<ReconciliationRow> & {
		fromConcept: string;
		toConcept: string;
	},
): ReconciliationRow {
	return {
		pairKey: "a|b",
		status: "evaluated",
		verdict: "no_tolerance_declared",
		abstainReason: null,
		delta: 0,
		relativeDelta: 0,
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
		const graph = build({
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
		const graph = build({
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
		const graph = build({
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
		const graph = build({
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
		const graph = build({
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
		const graph = build({
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
		const graph = build({
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
		const graph = build({
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
		const graph = build({
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
			unevaluated("account_balance"),
			unevaluated("gl_balance", 0.01),
		]);
	});

	it("drops an edge with a superseded or missing endpoint, never throws (dangling-reference safety)", () => {
		const graph = build({
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
		const graph = build({
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
		const graph = build({
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
		const graph = build({
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
		const graph = build({
			concepts: [],
			edges: [],
			groundings: [grounding({ concept: "ghost_concept" })],
		});
		expect(graph.nodes).toHaveLength(0);
	});

	it("parses a failed grounding's provenance into failureMode/failureReason (rule 11 — defensive narrowing)", () => {
		const graph = build({
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
		const graph = build({
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
					reconcilesWith: [unevaluated("cash"), unevaluated("gl_cash", 0.01)],
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

// --- DAT-739: the tie-out fold + evaluated-state render ---------------------
// Mirrors the engine's `_read_reconciliation_rows` fold and
// `_reconciliation_state` render. The mirror-key case is the one that bit the
// engine (one endpoint rendering "not yet evaluated" for an evaluated
// assertion) — dropping the TS mirror-key line must fail here.

describe("reconciliation fold", () => {
	const selfLoop = (name: string) =>
		edge({ predicate: "reconciles_with", fromConcept: name, toConcept: name });
	const pair = (frm: string, to: string) =>
		edge({ predicate: "reconciles_with", fromConcept: frm, toConcept: to });

	it("serves ONE evaluation to BOTH endpoints of a partner assertion (mirror key)", () => {
		const graph = build({
			concepts: [concept({ name: "ap" }), concept({ name: "purchases" })],
			// Both directions stored (concept_edges contract) — one evaluation.
			edges: [pair("ap", "purchases"), pair("purchases", "ap")],
			groundings: [],
			reconciliations: [
				recRow({
					fromConcept: "ap",
					toConcept: "purchases",
					delta: "500",
					relativeDelta: "0.05",
				}),
			],
		});
		const [ap, purchases] = graph.nodes;
		expect(ap.reconcilesWith[0].status).toBe("evaluated");
		expect(purchases.reconcilesWith[0].status).toBe("evaluated");
		expect(purchases.reconcilesWith[0].observedDelta).toBe(500);
		expect(purchases.reconcilesWith[0].relativeDelta).toBe(0.05);
	});

	it("reports the WIDEST relative divergence across a multi-pair assertion", () => {
		const graph = build({
			concepts: [concept({ name: "cash" })],
			edges: [selfLoop("cash")],
			groundings: [],
			reconciliations: [
				// Widest deliberately neither first nor last by pair_key.
				recRow({
					fromConcept: "cash",
					toConcept: "cash",
					pairKey: "a|b",
					delta: "10",
					relativeDelta: "0.1",
				}),
				recRow({
					fromConcept: "cash",
					toConcept: "cash",
					pairKey: "a|c",
					delta: "50",
					relativeDelta: "0.5",
				}),
				recRow({
					fromConcept: "cash",
					toConcept: "cash",
					pairKey: "b|c",
					delta: "20",
					relativeDelta: "0.2",
				}),
			],
		});
		const rec = graph.nodes[0].reconcilesWith[0];
		expect(rec.relativeDelta).toBe(0.5);
		expect(rec.observedDelta).toBe(50);
		expect(rec.pairs).toBe(3);
		expect(rec.evaluatedPairs).toBe(3);
	});

	it("folds a mixed evaluated/abstained assertion to evaluated, counting both", () => {
		const graph = build({
			concepts: [concept({ name: "cash" })],
			edges: [selfLoop("cash")],
			groundings: [],
			reconciliations: [
				recRow({
					fromConcept: "cash",
					toConcept: "cash",
					pairKey: "a|b",
					delta: "0",
					relativeDelta: "0",
				}),
				recRow({
					fromConcept: "cash",
					toConcept: "cash",
					pairKey: "a|c",
					status: "abstained",
					verdict: null,
					abstainReason: "different_reporting_instants",
					delta: null,
					relativeDelta: null,
				}),
			],
		});
		const rec = graph.nodes[0].reconcilesWith[0];
		expect(rec.status).toBe("evaluated");
		expect(rec.pairs).toBe(2);
		expect(rec.evaluatedPairs).toBe(1);
		expect(rec.abstainReason).toBeNull();
	});

	it("carries the abstain reason only when every pair agreed on it", () => {
		const allAgree = build({
			concepts: [concept({ name: "cash" })],
			edges: [selfLoop("cash")],
			groundings: [],
			reconciliations: [
				recRow({
					fromConcept: "cash",
					toConcept: "cash",
					pairKey: "a|b",
					status: "abstained",
					verdict: null,
					abstainReason: "different_reporting_instants",
					delta: null,
					relativeDelta: null,
				}),
			],
		});
		expect(allAgree.nodes[0].reconcilesWith[0].status).toBe("abstained");
		expect(allAgree.nodes[0].reconcilesWith[0].abstainReason).toBe(
			"different_reporting_instants",
		);

		const disagree = build({
			concepts: [concept({ name: "cash" })],
			edges: [selfLoop("cash")],
			groundings: [],
			reconciliations: [
				recRow({
					fromConcept: "cash",
					toConcept: "cash",
					pairKey: "a|b",
					status: "abstained",
					verdict: null,
					abstainReason: "different_reporting_instants",
					delta: null,
					relativeDelta: null,
				}),
				recRow({
					fromConcept: "cash",
					toConcept: "cash",
					pairKey: "a|c",
					status: "abstained",
					verdict: null,
					abstainReason: "no_value",
					delta: null,
					relativeDelta: null,
				}),
			],
		});
		expect(disagree.nodes[0].reconcilesWith[0].status).toBe("abstained");
		expect(disagree.nodes[0].reconcilesWith[0].abstainReason).toBeNull();
	});
});

describe("reconciliation state render", () => {
	const nodeWith = (rec: ConceptReconciliation): ConceptGraph => ({
		nodes: [
			{
				id: "concept:cash",
				conceptId: "id:cash",
				name: "cash",
				kind: null,
				description: null,
				indicators: [],
				excludePatterns: [],
				partOfParents: [],
				partOfChildren: [],
				partOfAncestry: [],
				disjointWith: [],
				reconcilesWith: [rec],
				groundings: [],
			},
		],
	});

	it("states the preamble's promise: entries carry the last run's observation", () => {
		const text = formatConceptContext(nodeWith(unevaluated("cash")));
		expect(text).toContain(
			"each entry states whether the last completed run actually checked that",
		);
	});

	it("renders never-evaluated as unchecked, NEVER as agreement", () => {
		const text = formatConceptContext(nodeWith(unevaluated("cash")));
		expect(text).toContain(
			"- reconciles: across its own groundings — must tie out (not yet evaluated)",
		);
	});

	it("renders an abstention with the engine's exact phrasing", () => {
		const text = formatConceptContext(
			nodeWith({
				...unevaluated("cash"),
				status: "abstained",
				abstainReason: "no_evaluable_pair",
				pairs: 1,
			}),
		);
		expect(text).toContain(
			"must tie out; not compared because only one grounding exists to measure",
		);
	});

	it("renders an ungraded delta as a measurement, not a failure", () => {
		const text = formatConceptContext(
			nodeWith({
				...unevaluated("cash"),
				status: "evaluated",
				verdict: "no_tolerance_declared",
				observedDelta: 500,
				relativeDelta: 0.05,
				pairs: 1,
				evaluatedPairs: 1,
			}),
		);
		expect(text).toContain(
			"evaluated: observed delta 500 (0.05 relative) — no tolerance is " +
				"declared, so this is a measurement, not a failure",
		);
	});

	it("never lets a partial evaluation read as a whole one", () => {
		const text = formatConceptContext(
			nodeWith({
				...unevaluated("cash"),
				status: "evaluated",
				verdict: "no_tolerance_declared",
				observedDelta: 0,
				relativeDelta: 0,
				pairs: 5,
				evaluatedPairs: 2,
			}),
		);
		expect(text).toContain(
			"evaluated: the groundings tie out exactly (widest of 2 pairs); " +
				"3 of 5 pairs not comparable",
		);
	});

	it("renders graded verdicts with the tolerance band", () => {
		const beyond = formatConceptContext(
			nodeWith({
				...unevaluated("cash", 0.01),
				status: "evaluated",
				verdict: "beyond_tolerance",
				observedDelta: 500,
				relativeDelta: 0.05,
				pairs: 1,
				evaluatedPairs: 1,
			}),
		);
		expect(beyond).toContain(
			"- reconciles: across its own groundings (tolerance 0.01) — " +
				"evaluated: 0.05 relative divergence exceeds the tolerance",
		);
		const within = formatConceptContext(
			nodeWith({
				...unevaluated("cash", 0.1),
				status: "evaluated",
				verdict: "within_tolerance",
				observedDelta: 5,
				relativeDelta: 0.005,
				pairs: 1,
				evaluatedPairs: 1,
			}),
		);
		expect(within).toContain(
			"evaluated: ties out within tolerance, 0.005 relative",
		);
	});
});

describe("pyG (python %g mirror)", () => {
	it("matches python's %g on the realistic range", () => {
		// Each pinned against CPython: f"{x:g}" / f"{x:.3g}".
		expect(pyG(0)).toBe("0");
		expect(pyG(500)).toBe("500");
		expect(pyG(0.01)).toBe("0.01");
		expect(pyG(0.05, 3)).toBe("0.05");
		expect(pyG(1234.5, 3)).toBe("1.23e+03");
		expect(pyG(51766199.72)).toBe("5.17662e+07");
		expect(pyG(0.00001)).toBe("1e-05");
		expect(pyG(-0.25, 3)).toBe("-0.25");
		expect(pyG(1000000)).toBe("1e+06");
	});
});
