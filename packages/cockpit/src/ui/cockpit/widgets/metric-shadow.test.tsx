// @vitest-environment jsdom

// Unit tests for the MetricShadowWidget (DAT-482). Mocks `useQuery` at the
// TanStack Query boundary (the test controls the fetched DAG) — the widget only
// `import type`s ShippedMetricDag (erased) and reaches the server over
// `fetch("/api/shipped-metric-dag")`, so no config/server mock is needed (the
// mocked useQuery never runs the queryFn). Asserts the loading / error / empty /
// rendered-DAG states. The real fetch + route are exercised by the browser smoke.

import { MantineProvider } from "@mantine/core";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const h = vi.hoisted(() => ({
	queryResult: {
		data: undefined as unknown,
		error: undefined as unknown,
		isLoading: false,
	},
}));

vi.mock("@tanstack/react-query", () => ({
	useQuery: () => h.queryResult,
}));

import { MetricShadowWidget } from "#/ui/cockpit/widgets/metric-shadow";

const STATE = {
	kind: "metric-shadow" as const,
	vertical: "finance",
	graphId: "ebitda",
};

const EBITDA_DAG = {
	graph_id: "ebitda",
	name: "EBITDA",
	category: "profitability",
	output: { type: "scalar", metricId: "ebitda", unit: "currency" },
	steps: [
		{
			id: "revenue",
			type: "extract",
			level: 1,
			standardField: "revenue",
			statement: "income_statement",
			aggregation: "sum",
			expression: null,
			dependsOn: [],
			outputStep: false,
		},
		{
			id: "ebitda",
			type: "formula",
			level: 3,
			standardField: null,
			statement: null,
			aggregation: null,
			expression: "operating_income + depreciation",
			dependsOn: ["operating_income", "depreciation"],
			outputStep: true,
		},
	],
};

function renderWidget() {
	render(
		<MantineProvider env="test">
			<MetricShadowWidget state={STATE} />
		</MantineProvider>,
	);
}

describe("MetricShadowWidget", () => {
	beforeEach(() => {
		h.queryResult = { data: undefined, error: undefined, isLoading: false };
	});
	afterEach(cleanup);

	it("shows a loading state while fetching the shipped DAG", () => {
		h.queryResult = { data: undefined, error: undefined, isLoading: true };
		renderWidget();
		expect(screen.getByTestId("canvas-metric-shadow-loading")).toBeTruthy();
	});

	it("shows an error state when the read fails", () => {
		h.queryResult = {
			data: undefined,
			error: new Error("config tree unreadable"),
			isLoading: false,
		};
		renderWidget();
		const alert = screen.getByTestId("canvas-metric-shadow-error");
		expect(alert.textContent).toContain("config tree unreadable");
	});

	it("shows an empty state when no shipped metric matches (defensive)", () => {
		h.queryResult = { data: null, error: undefined, isLoading: false };
		renderWidget();
		expect(screen.getByTestId("canvas-metric-shadow-empty")).toBeTruthy();
	});

	it("renders the shipped DAG being replaced: header, output, and each step", () => {
		h.queryResult = { data: EBITDA_DAG, error: undefined, isLoading: false };
		renderWidget();
		// Header names the metric being replaced.
		expect(screen.getByText("Replacing: EBITDA")).toBeTruthy();
		// The output node + each dependency step render (not just the summary).
		expect(screen.getByTestId("metric-dag")).toBeTruthy();
		const revenue = screen.getByTestId("metric-dag-step-revenue");
		expect(revenue.textContent).toContain("revenue");
		expect(revenue.textContent).toContain("sum");
		const ebitda = screen.getByTestId("metric-dag-step-ebitda");
		expect(ebitda.textContent).toContain("operating_income + depreciation");
	});
});
