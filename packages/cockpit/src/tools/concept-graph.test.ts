// Unit coverage for the concept graph's PURE half — the model's folds and the
// `<business_concepts>` render (DAT-737/739, extended with the verdict/DAG
// lines in DAT-671 R3).
//
// The graph is now READ, not rebuilt: what used to be `buildConceptGraph`'s
// assembly (active-row filtering, edge-endpoint resolution, the
// grounding↔concept join, the `part_of` closure) is served by the engine's own
// element views, so its behaviour is pinned against the REAL Postgres 19
// property graph in `concept-graph-load.integration.test.ts` instead of against
// an in-memory fixture. What remains here is what stays pure: the
// reconciliation fold, and every wording the block commits to.

import { describe, expect, it } from "vitest";

import {
	type ConceptAdditivity,
	type ConceptGraph,
	type ConceptGraphNode,
	type ConceptReconciliation,
	type DerivedMetric,
	foldReconciliations,
	formatAdditivity,
	formatConceptContext,
	isReusableGrounding,
	parseWherePredicates,
	pyG,
	type ReconciliationRow,
	reconciliationFor,
} from "./concept-graph";

/** One node with every list empty — the render cases below each fill in only
 *  the field they are about, so a wording assertion can never accidentally
 *  depend on a neighbouring section. */
function node(
	overrides: Partial<ConceptGraphNode> & { name: string },
): ConceptGraphNode {
	return {
		conceptId: `cpt_${overrides.name}`,
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
		additivity: [],
		derivedMetrics: [],
		...overrides,
	};
}

const graphOf = (...nodes: ConceptGraphNode[]): ConceptGraph => ({ nodes });

/** A never-evaluated assertion, as the loader emits it when no promoted run
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

/** One class-level verdict with the engine's own defaults. */
function verdict(
	overrides: Partial<ConceptAdditivity> & { axisKind: string },
): ConceptAdditivity {
	return {
		axisKey: "*",
		status: "classified",
		verdict: "additive",
		reason: null,
		abstainReason: null,
		bucketGrain: null,
		...overrides,
	};
}

function metric(overrides: Partial<DerivedMetric> & { graphId: string }) {
	return {
		name: overrides.graphId,
		category: null,
		unit: null,
		outputType: null,
		...overrides,
	};
}

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

	it("renders name/kind/description in the given (loader-sorted) node order", () => {
		const text = formatConceptContext(
			graphOf(
				node({ name: "assets" }),
				node({
					name: "revenue",
					kind: "measure",
					description: "Recognized income",
				}),
			),
		);
		expect(text).toContain("<business_concepts>");
		expect(text.indexOf("- assets")).toBeLessThan(
			text.indexOf("- revenue (measure)"),
		);
		expect(text).toContain("- revenue (measure): Recognized income");
	});

	it("renders part_of with the ancestry arrow, subconcepts, disjoint_with, and both reconciliation wordings", () => {
		const text = formatConceptContext(
			graphOf(
				node({
					name: "cash",
					partOfParents: ["current_assets"],
					partOfChildren: ["petty_cash"],
					partOfAncestry: ["assets", "balance_sheet"],
					disjointWith: ["liability"],
					reconcilesWith: [unevaluated("cash"), unevaluated("gl_cash", 0.01)],
				}),
			),
		);
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
		const text = formatConceptContext(
			graphOf(
				node({
					name: "account_balance",
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
				}),
			),
		);
		expect(text).toContain(
			"grounded by:\n    - ending balance @ trial_balance: SUM(ending_balance) WHERE account_type = 'asset'",
		);
		expect(text).toContain(
			"failed attempt [no_support]: 0 rows matched the filter",
		);
		expect(text).not.toContain("balance_sheet: ");
	});

	it("skips a healthy-but-relation-less grounding from 'grounded by:' and discloses the skip (matching the engine's own skip)", () => {
		const text = formatConceptContext(
			graphOf(
				node({
					name: "account_balance",
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
				}),
			),
		);
		expect(text).not.toContain("grounded by:");
		expect(text).toContain(
			"1 grounding(s) recorded with no relation — not reusable, omitted",
		);
	});

	it("renders a failure with no recorded detail using the honest fallback wording", () => {
		const text = formatConceptContext(
			graphOf(
				node({
					name: "x",
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
				}),
			),
		);
		expect(text).toContain("failed attempt [failed]: (no reason recorded)");
	});
});

// --- DAT-671 R3: the verdict + metric-DAG lines ------------------------------
// The answer agent used to compose SQL blind to the additivity verdict the
// drill would then gate it by. These are the lines that close that gap, so the
// wording has to survive: an ABSENT verdict must read as "no verdict", never as
// permission, and an ABSTAINED one must never read as a "no".

describe("formatAdditivity (DAT-671 R3)", () => {
	it("names the axis CLASS for a '*' row and the COLUMN for a concrete one", () => {
		expect(formatAdditivity(verdict({ axisKind: "time" }))).toContain(
			"any time axis",
		);
		expect(
			formatAdditivity(verdict({ axisKind: "categorical", axisKey: "region" })),
		).toContain('categorical axis "region"');
	});

	it("carries the doctrine reason and the bucket cadence for a non-additive verdict", () => {
		expect(
			formatAdditivity(
				verdict({
					axisKind: "time",
					verdict: "semi_additive",
					reason: "stock",
					bucketGrain: "month",
				}),
			),
		).toBe(
			"any time axis — semi_additive (stock), bucketable no finer than month",
		);
	});

	it("says an abstention was NOT JUDGED, never that the answer is no", () => {
		const text = formatAdditivity(
			verdict({
				axisKind: "categorical",
				status: "abstained",
				verdict: null,
				abstainReason: "unknown_aggregate",
			}),
		);
		expect(text).toContain("NOT CLASSIFIED (unknown_aggregate)");
		expect(text).toContain("not the same as a no");
	});

	it("names the abstention honestly when the engine recorded no reason", () => {
		expect(
			formatAdditivity(
				verdict({ axisKind: "time", status: "abstained", verdict: null }),
			),
		).toContain("NOT CLASSIFIED (no reason recorded)");
	});
});

describe("verdict + derives_from in the block (DAT-671 R3)", () => {
	it("renders one additivity line per verdict and tells the model what it means", () => {
		const text = formatConceptContext(
			graphOf(
				node({
					name: "revenue",
					additivity: [
						verdict({ axisKind: "categorical" }),
						verdict({ axisKind: "time", bucketGrain: "month" }),
					],
				}),
			),
		);
		expect(text).toContain("- additivity: any categorical axis — additive");
		expect(text).toContain(
			"- additivity: any time axis — additive, bucketable no finer than month",
		);
		// The preamble has to state the CONSEQUENCE, not just the vocabulary:
		// this block is what the drill enforces against.
		expect(text).toContain("it is what the drill ENFORCES on a result");
		expect(text).toContain("never SUM across periods");
	});

	it("says NOTHING for a concept the engine classified nothing for — absence is not a verdict", () => {
		const text = formatConceptContext(graphOf(node({ name: "revenue" })));
		expect(text).not.toContain("- additivity:");
		// …but the preamble still tells the model how to read that silence.
		expect(text).toContain("carries no verdict at all");
	});

	it("names each metric this concept feeds, id first, with its category/output/unit", () => {
		const text = formatConceptContext(
			graphOf(
				node({
					name: "revenue",
					derivedMetrics: [
						metric({
							graphId: "gross_margin",
							name: "Gross Margin",
							category: "profitability",
							unit: "percent",
							outputType: "ratio",
						}),
						metric({ graphId: "burn_rate" }),
					],
				}),
			),
		);
		expect(text).toContain(
			"- feeds metric: gross_margin (profitability, ratio in percent)",
		);
		// A metric with no category/unit/output renders bare rather than with an
		// empty parenthetical.
		expect(text).toContain("- feeds metric: burn_rate\n");
	});
});

// --- DAT-739: the tie-out fold + evaluated-state render ---------------------
// Mirrors the engine's `_read_reconciliation_rows` fold and
// `_reconciliation_state` render. The mirror-key case is the one that bit the
// engine (one endpoint rendering "not yet evaluated" for an evaluated
// assertion) — dropping the TS mirror-key line must fail here.

describe("reconciliation fold", () => {
	it("serves ONE evaluation to BOTH endpoints of a partner assertion (mirror key)", () => {
		// Only ONE direction is evaluated and stored, but concept_edges holds both
		// — so the loader must find the observation from either side.
		const folds = foldReconciliations([
			recRow({
				fromConcept: "ap",
				toConcept: "purchases",
				delta: "500",
				relativeDelta: "0.05",
			}),
		]);
		const forward = reconciliationFor(folds, "ap", "purchases");
		const mirrored = reconciliationFor(folds, "purchases", "ap");
		expect(forward?.status).toBe("evaluated");
		expect(mirrored?.status).toBe("evaluated");
		expect(mirrored?.observedDelta).toBe(500);
		expect(mirrored?.relativeDelta).toBe(0.05);
	});

	it("reports the WIDEST relative divergence across a multi-pair assertion", () => {
		const folds = foldReconciliations([
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
		]);
		const rec = reconciliationFor(folds, "cash", "cash");
		expect(rec?.relativeDelta).toBe(0.5);
		expect(rec?.observedDelta).toBe(50);
		expect(rec?.pairs).toBe(3);
		expect(rec?.evaluatedPairs).toBe(3);
	});

	it("folds a mixed evaluated/abstained assertion to evaluated, counting both", () => {
		const folds = foldReconciliations([
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
		]);
		const rec = reconciliationFor(folds, "cash", "cash");
		expect(rec?.status).toBe("evaluated");
		expect(rec?.pairs).toBe(2);
		expect(rec?.evaluatedPairs).toBe(1);
		expect(rec?.abstainReason).toBeNull();
	});

	it("carries the abstain reason only when every pair agreed on it", () => {
		const agreed = reconciliationFor(
			foldReconciliations([
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
			]),
			"cash",
			"cash",
		);
		expect(agreed?.status).toBe("abstained");
		expect(agreed?.abstainReason).toBe("different_reporting_instants");

		const disagreed = reconciliationFor(
			foldReconciliations([
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
			]),
			"cash",
			"cash",
		);
		expect(disagreed?.status).toBe("abstained");
		expect(disagreed?.abstainReason).toBeNull();
	});

	it("keys on the pair, so a concept name containing the separator cannot collide", () => {
		// The fold key joins the endpoints with NUL — the one byte Postgres text
		// cannot hold — so no name can forge another pair's key.
		const folds = foldReconciliations([
			recRow({ fromConcept: "a b", toConcept: "c", relativeDelta: "0.1" }),
			recRow({ fromConcept: "a", toConcept: "b c", relativeDelta: "0.2" }),
		]);
		expect(reconciliationFor(folds, "a b", "c")?.relativeDelta).toBe(0.1);
		expect(reconciliationFor(folds, "a", "b c")?.relativeDelta).toBe(0.2);
	});
});

describe("reconciliation state render", () => {
	const nodeWith = (rec: ConceptReconciliation): ConceptGraph =>
		graphOf(node({ name: "cash", reconcilesWith: [rec] }));

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
