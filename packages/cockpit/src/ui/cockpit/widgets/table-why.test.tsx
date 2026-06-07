// @vitest-environment jsdom
//
// Render tests for the TableWhyWidget (DAT-434) — mirrors column-why.test.tsx:
// the narrative, per-intent drivers, signal caption, evidence table, and the
// not-found / not-analyzed states. The live read + LLM synthesis are
// smoke-covered.

import { MantineProvider } from "@mantine/core";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import type { WhyTableResult } from "#/tools/why-table";
import { TableWhyWidget } from "#/ui/cockpit/widgets/table-why";
import { theme } from "#/ui/theme";

function renderWidget(why: WhyTableResult) {
	render(
		<MantineProvider theme={theme} env="test">
			<TableWhyWidget state={{ kind: "table-why", why }} />
		</MantineProvider>,
	);
}

const analyzed: WhyTableResult = {
	table_id: "t_orders",
	table_name: "orders",
	found: true,
	band: "investigate",
	worst_intent_risk: 0.42,
	analyzed: true,
	intents: [
		{
			intent: "aggregation_intent",
			band: "investigate",
			risk: 0.42,
			drivers: [
				{
					node: "dimension_coverage",
					dimension_path: "structural.coverage.dimension_coverage",
					label: "Dimension Coverage",
					state: "high",
					impact_delta: 0.3,
				},
			],
		},
	],
	evidence: [
		{
			dimension_path: "structural.coverage.dimension_coverage",
			detector_id: "dimension_coverage",
			score: 0.7,
			detail: '[{"metric":"covered_ratio","value":0.55}]',
		},
	],
	signal_count: 1,
	analysis: "orders lacks dimensional coverage for reliable aggregation.",
	pending_teaches: 0,
};

afterEach(cleanup);

describe("TableWhyWidget (DAT-434)", () => {
	it("renders the narrative, band, drivers, and evidence for an analyzed table", () => {
		renderWidget(analyzed);
		expect(screen.getByTestId("canvas-table-why")).toBeTruthy();
		expect(screen.getByText("orders")).toBeTruthy();
		// Shared BandBadge humanizes the band (DAT-451).
		expect(screen.getAllByText("Investigate").length).toBeGreaterThan(0);
		expect(screen.getByTestId("canvas-table-why-analysis").textContent).toBe(
			analyzed.analysis,
		);
		expect(screen.getByText("Aggregation")).toBeTruthy();
		expect(screen.getByText("Dimension Coverage (high)")).toBeTruthy();
		expect(screen.getByTestId("canvas-table-why-evidence")).toBeTruthy();
		expect(
			screen.getByTestId("canvas-table-why-signals").textContent,
		).toContain("Based on 1 signal");
	});

	it("renders the not-found state — no id leaks", () => {
		renderWidget({
			...analyzed,
			found: false,
			table_name: null,
			analyzed: false,
		});
		expect(screen.getByTestId("canvas-table-why-notfound")).toBeTruthy();
		expect(document.body.textContent).not.toContain("t_orders");
	});

	it("renders the not-analyzed state pointing at begin_session", () => {
		renderWidget({
			...analyzed,
			analyzed: false,
			band: null,
			intents: [],
			evidence: [],
			signal_count: 0,
			analysis: "",
		});
		expect(screen.getByTestId("canvas-table-why-unanalyzed")).toBeTruthy();
	});

	it("a null table_name renders a placeholder, never the table_id", () => {
		renderWidget({ ...analyzed, table_name: null });
		expect(screen.getByText(/unknown table/)).toBeTruthy();
		expect(document.body.textContent).not.toContain("t_orders");
	});

	it("surfaces the pending-teach note", () => {
		renderWidget({ ...analyzed, pending_teaches: 2 });
		expect(
			screen.getByTestId("canvas-table-why-pending").textContent,
		).toContain("2 pending teaches");
	});
});
