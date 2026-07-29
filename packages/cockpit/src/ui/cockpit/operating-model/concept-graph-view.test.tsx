// @vitest-environment jsdom
//
// Render tests for ConceptGraphView (DAT-737): every concept renders as a row
// regardless of grounding status ("no ungrounded-node regressions"), the
// grounding-count badge is honest (ungrounded / singular / plural), a healthy-
// but-relation-less grounding gets a DISTINCT badge (not "grounded" — it isn't
// reusable), the neighbourhood detail (part_of/disjoint/reconciles/groundings)
// is present, and the empty-workspace state is distinguishable from "has
// concepts."

import { MantineProvider } from "@mantine/core";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import type { ConceptGraph, ConceptGraphNode } from "#/tools/concept-graph";
import { theme } from "#/ui/theme";
import { ConceptGraphView } from "./concept-graph-view";

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
		// Served but not rendered here: the verdict/DAG edges DAT-671 R3 added
		// reach the ANSWER AGENT's block, not this panel — the concept view's
		// job is the vocabulary a practitioner browses, and adding a verdict
		// badge is a design decision, not a side effect of the read moving.
		additivity: [],
		derivedMetrics: [],
		...overrides,
	};
}

function renderView(graph: ConceptGraph) {
	render(
		<MantineProvider theme={theme} env="test">
			<ConceptGraphView graph={graph} />
		</MantineProvider>,
	);
}

afterEach(cleanup);

describe("ConceptGraphView (DAT-737)", () => {
	it("shows the empty state when the workspace has no concepts yet", () => {
		renderView({ nodes: [] });
		expect(screen.getByTestId("concept-graph-empty")).toBeTruthy();
	});

	it("renders every concept, including an ungrounded one — never dropped", () => {
		renderView({
			nodes: [
				node({ name: "revenue", kind: "measure" }),
				node({ name: "unmeasured_concept" }),
			],
		});
		expect(screen.getByTestId("concept-graph-view")).toBeTruthy();
		expect(screen.getByText("revenue")).toBeTruthy();
		expect(screen.getByText("unmeasured_concept")).toBeTruthy();
		expect(
			screen.getByTestId("concept-grounding-count-unmeasured_concept")
				.textContent,
		).toBe("ungrounded");
	});

	it("labels the grounding count honestly — singular vs plural", () => {
		renderView({
			nodes: [
				node({
					name: "one_grounding",
					groundings: [
						{
							snippetId: "s1",
							statement: "x",
							relation: "orders",
							selectExpr: "SUM(x)",
							wherePredicates: [],
							failed: false,
							failureMode: null,
							failureReason: null,
						},
					],
				}),
				node({
					name: "two_groundings",
					groundings: [
						{
							snippetId: "s2",
							statement: "y",
							relation: "trial_balance",
							selectExpr: "SUM(y)",
							wherePredicates: [],
							failed: false,
							failureMode: null,
							failureReason: null,
						},
						{
							snippetId: "s3",
							statement: "y",
							relation: "balance_sheet",
							selectExpr: "SUM(y)",
							wherePredicates: [],
							failed: false,
							failureMode: null,
							failureReason: null,
						},
					],
				}),
			],
		});
		expect(
			screen.getByTestId("concept-grounding-count-one_grounding").textContent,
		).toBe("1 grounding");
		expect(
			screen.getByTestId("concept-grounding-count-two_groundings").textContent,
		).toBe("2 groundings");
	});

	// Mantine's Accordion.Panel uses Collapse with `keepMounted: true` by
	// default (panel content stays in the DOM — React 19 Activity preserves
	// it, visibility toggles via CSS) — so the neighbourhood detail below is
	// ALREADY queryable without ever clicking the row. No `fireEvent.click`
	// here: it would be theater, asserting on content the click didn't cause
	// to appear.
	it("renders part_of ancestry, disjoint_with, reconciles, and groundings in the (always-mounted) panel content", () => {
		renderView({
			nodes: [
				node({
					name: "cash",
					partOfParents: ["current_assets"],
					partOfAncestry: ["assets"],
					disjointWith: ["liability"],
					reconcilesWith: [
						{
							partner: "cash",
							tolerance: null,
							status: null,
							verdict: null,
							abstainReason: null,
							observedDelta: null,
							relativeDelta: null,
							pairs: 0,
							evaluatedPairs: 0,
						},
					],
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
					],
				}),
			],
		});
		expect(screen.getByText(/current_assets/)).toBeTruthy();
		expect(screen.getByText(/→ assets/)).toBeTruthy();
		expect(screen.getByText(/liability/)).toBeTruthy();
		expect(screen.getByText(/across its own groundings/)).toBeTruthy();
		// The evaluated state rides the panel line too (DAT-739): a fixture with
		// status null must say UNCHECKED, never render as bare "must tie out".
		expect(screen.getByText(/must tie out \(not yet evaluated\)/)).toBeTruthy();
		expect(screen.getByText(/ending balance @ trial_balance/)).toBeTruthy();
	});

	it("renders an abstained tie-out with its typed reason in the panel (DAT-739)", () => {
		renderView({
			nodes: [
				node({
					name: "transaction_amount",
					reconcilesWith: [
						{
							partner: "transaction_amount",
							tolerance: null,
							status: "abstained",
							verdict: null,
							abstainReason: "different_aggregations",
							observedDelta: null,
							relativeDelta: null,
							pairs: 1,
							evaluatedPairs: 0,
						},
					],
				}),
			],
		});
		expect(
			screen.getByText(
				/not compared because the groundings aggregate differently/,
			),
		).toBeTruthy();
	});

	it("shows the honest not-grounded note when a concept has zero groundings", () => {
		renderView({ nodes: [node({ name: "aspirational_concept" })] });
		expect(screen.getByTestId("concept-ungrounded")).toBeTruthy();
	});

	it("gives a healthy-but-relation-less grounding a DISTINCT badge, never the plain 'grounded' one", () => {
		renderView({
			nodes: [
				node({
					name: "unresolved_grounding",
					groundings: [
						{
							snippetId: "s1",
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
			],
		});
		const badge = screen.getByTestId("concept-grounding-badge");
		expect(badge.textContent).toBe("not reusable — no relation");
		expect(badge.textContent).not.toBe("grounded");
	});

	it("shows a failed grounding's mode/reason, and the honest fallback when neither is recorded", () => {
		renderView({
			nodes: [
				node({
					name: "failed_concept",
					groundings: [
						{
							snippetId: "s1",
							statement: null,
							relation: null,
							selectExpr: null,
							wherePredicates: [],
							failed: true,
							failureMode: "no_support",
							failureReason: "0 rows matched",
						},
					],
				}),
			],
		});
		expect(screen.getByText("[no_support] 0 rows matched")).toBeTruthy();
	});

	it("caps groundings shown per concept and reports the overflow", () => {
		const groundings = Array.from({ length: 25 }, (_, i) => ({
			snippetId: `s${i}`,
			statement: null,
			relation: `relation_${i}`,
			selectExpr: null,
			wherePredicates: [],
			failed: false,
			failureMode: null,
			failureReason: null,
		}));
		renderView({ nodes: [node({ name: "many_groundings", groundings })] });
		expect(screen.getByText("…and 5 more groundings not shown.")).toBeTruthy();
	});

	it("caps rendering and shows an overflow note past the visible concept limit", () => {
		const nodes = Array.from({ length: 205 }, (_, i) =>
			node({ name: `concept_${String(i).padStart(3, "0")}` }),
		);
		renderView({ nodes });
		expect(screen.getByTestId("concept-graph-overflow").textContent).toContain(
			"5 more",
		);
	});
});
