// @vitest-environment jsdom
//
// Render tests for ConceptGraphView (DAT-737): every concept renders as a row
// regardless of grounding status ("no ungrounded-node regressions"), the
// grounding-count badge is honest (ungrounded / singular / plural), the
// neighbourhood detail (part_of/disjoint/reconciles/groundings) expands, and
// the empty-workspace state is distinguishable from "has concepts."

import { MantineProvider } from "@mantine/core";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import type { ConceptGraph, ConceptGraphNode } from "#/tools/concept-graph";
import { theme } from "#/ui/theme";
import { ConceptGraphView } from "./concept-graph-view";

function node(
	overrides: Partial<ConceptGraphNode> & { name: string },
): ConceptGraphNode {
	return {
		id: `concept:${overrides.name}`,
		conceptId: `id:${overrides.name}`,
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
		renderView({ nodes: [], edges: [] });
		expect(screen.getByTestId("concept-graph-empty")).toBeTruthy();
	});

	it("renders every concept, including an ungrounded one — never dropped", () => {
		renderView({
			nodes: [
				node({ name: "revenue", kind: "measure" }),
				node({ name: "unmeasured_concept" }),
			],
			edges: [],
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
						},
						{
							snippetId: "s3",
							statement: "y",
							relation: "balance_sheet",
							selectExpr: "SUM(y)",
							wherePredicates: [],
							failed: false,
						},
					],
				}),
			],
			edges: [],
		});
		expect(
			screen.getByTestId("concept-grounding-count-one_grounding").textContent,
		).toBe("1 grounding");
		expect(
			screen.getByTestId("concept-grounding-count-two_groundings").textContent,
		).toBe("2 groundings");
	});

	it("expands to show part_of ancestry, disjoint_with, reconciles, and groundings", () => {
		renderView({
			nodes: [
				node({
					name: "cash",
					partOfParents: ["current_assets"],
					partOfAncestry: ["assets"],
					disjointWith: ["liability"],
					reconcilesWith: [{ partner: "cash", tolerance: null }],
					groundings: [
						{
							snippetId: "s1",
							statement: "ending balance",
							relation: "trial_balance",
							selectExpr: "SUM(ending_balance)",
							wherePredicates: ["account_type = 'asset'"],
							failed: false,
						},
					],
				}),
			],
			edges: [],
		});
		fireEvent.click(screen.getByText("cash"));
		expect(screen.getByText(/current_assets/)).toBeTruthy();
		expect(screen.getByText(/→ assets/)).toBeTruthy();
		expect(screen.getByText(/liability/)).toBeTruthy();
		expect(screen.getByText(/across its own groundings/)).toBeTruthy();
		expect(screen.getByText(/ending balance @ trial_balance/)).toBeTruthy();
	});

	it("shows the honest not-grounded note when a concept has zero groundings", () => {
		renderView({ nodes: [node({ name: "aspirational_concept" })], edges: [] });
		fireEvent.click(screen.getByText("aspirational_concept"));
		expect(screen.getByTestId("concept-ungrounded")).toBeTruthy();
	});

	it("caps rendering and shows an overflow note past the visible limit", () => {
		const nodes = Array.from({ length: 205 }, (_, i) =>
			node({ name: `concept_${String(i).padStart(3, "0")}` }),
		);
		renderView({ nodes, edges: [] });
		expect(screen.getByTestId("concept-graph-overflow").textContent).toContain(
			"5 more",
		);
	});
});
