// @vitest-environment jsdom
//
// Render tests for the MetricWhyWidget (DAT-466): the not-found state, the
// ungroundable reason as a first-class alert ("visibly impossible"), and the
// executed drill-down (step count, the per-step SQL composition, grounded
// detail). All values are the engine's persisted strings — the widget only
// formats; the numeric value is deliberately absent.

import { MantineProvider } from "@mantine/core";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import type { WhyMetricResult } from "#/tools/why-metric";
import { MetricWhyWidget } from "#/ui/cockpit/widgets/metric-why";
import { theme } from "#/ui/theme";

function renderWidget(why: WhyMetricResult) {
	render(
		<MantineProvider theme={theme} env="test">
			<MetricWhyWidget state={{ kind: "metric-why", why }} />
		</MantineProvider>,
	);
}

const EXECUTED: WhyMetricResult = {
	graph_id: "gross_margin",
	found: true,
	state: "executed",
	state_reason: null,
	strictness: 0.8,
	grounded_against: JSON.stringify({ detect: "run-7" }),
	snippet_count: 2,
	steps: [
		{
			snippet_id: "s1",
			type: "extract",
			label: "revenue",
			sql: "SELECT sum(amount) FROM lake.typed.income",
			description: "Total revenue",
			execution_count: 4,
			failure_count: 0,
		},
		{
			snippet_id: "s2",
			type: "formula",
			label: "revenue - cost_of_goods_sold",
			sql: "SELECT revenue - cogs AS result",
			description: null,
			execution_count: 4,
			failure_count: 1,
		},
	],
	pending_teaches: 0,
};

const UNGROUNDABLE: WhyMetricResult = {
	...EXECUTED,
	state: "declared",
	state_reason: "ungroundable: required field mappings missing",
	strictness: null,
	grounded_against: "",
	snippet_count: 0,
	steps: [],
};

afterEach(cleanup);

describe("MetricWhyWidget (DAT-466)", () => {
	it("renders the not-found state", () => {
		renderWidget({ ...EXECUTED, found: false });
		expect(screen.getByTestId("canvas-metric-why-notfound")).toBeTruthy();
	});

	it("renders the ungroundable reason as a first-class alert (visibly impossible)", () => {
		renderWidget(UNGROUNDABLE);
		expect(
			screen.getByTestId("canvas-metric-why-reason").textContent,
		).toContain("ungroundable: required field mappings missing");
		expect(screen.getByText("Gross margin")).toBeTruthy();
		expect(screen.getByText("Declared")).toBeTruthy();
		// No composition block when nothing composed.
		expect(screen.queryByTestId("canvas-metric-why-steps")).toBeNull();
	});

	it("renders the executed drill-down: step count + the per-step SQL composition", () => {
		renderWidget(EXECUTED);
		expect(screen.getByText("Gross margin")).toBeTruthy();
		expect(screen.getByText("Executed")).toBeTruthy();
		expect(
			screen.getByTestId("canvas-metric-why-stepcount").textContent,
		).toContain("2 SQL steps");
		// The composition block renders each step's label + SQL.
		expect(screen.getByTestId("canvas-metric-why-steps")).toBeTruthy();
		expect(screen.getByText("revenue")).toBeTruthy();
		expect(screen.getByText("revenue - cost_of_goods_sold")).toBeTruthy();
		expect(
			screen.getByText(/SELECT sum\(amount\) FROM lake\.typed\.income/),
		).toBeTruthy();
		// No reason alert when the metric executed.
		expect(screen.queryByTestId("canvas-metric-why-reason")).toBeNull();
	});

	it("surfaces the pending-teach note", () => {
		renderWidget({ ...EXECUTED, pending_teaches: 2 });
		expect(
			screen.getByTestId("canvas-metric-why-pending").textContent,
		).toContain("2 pending teaches");
	});
});
