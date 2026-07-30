// @vitest-environment jsdom
import { MantineProvider } from "@mantine/core";
import { cleanup, render, screen, within } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import { buildCoverageMap, type CoverageMapInput } from "#/tools/coverage-map";
import { theme } from "#/ui/theme";
import { CoverageMapView } from "./coverage-map-view";

// The view is rendered from the REAL builder rather than hand-written view models:
// state/reason/ordering are the builder's decisions, and asserting the rendered
// result of a fabricated map would prove nothing about what a practitioner sees.

const EMPTY: CoverageMapInput = {
	metrics: [],
	concepts: [],
	lifecycle: [],
	groundings: [],
	reconciliation: [],
};

function renderMap(input: CoverageMapInput) {
	render(
		<MantineProvider theme={theme} env="test">
			<CoverageMapView map={buildCoverageMap(input)} />
		</MantineProvider>,
	);
}

afterEach(cleanup);

describe("CoverageMapView (DAT-855 B2)", () => {
	it("always renders all six dimension rows, even with nothing declared", () => {
		renderMap(EMPTY);
		const grid = screen.getByTestId("coverage-map-grid");
		for (const dim of [
			"demand",
			"offer",
			"supply",
			"capacity",
			"throughput",
			"capital",
		]) {
			expect(within(grid).getByTestId(`coverage-row-${dim}`)).toBeTruthy();
		}
	});

	it("renders a dark row's reason as text, not blankness", () => {
		renderMap(EMPTY);
		const row = screen.getByTestId("coverage-row-demand");
		expect(
			within(row).getByTestId("coverage-reason-demand").textContent,
		).toContain("no demand unit metric declared");
		expect(within(row).getByText("—")).toBeTruthy();
	});

	it("marks a lit dimension and lists the metric that grounds it", () => {
		renderMap({
			...EMPTY,
			metrics: [
				{
					graphId: "dso",
					name: "Days Sales Outstanding",
					dimensionFacet: "capital",
				},
			],
			lifecycle: [{ graphId: "dso", state: "executed", stateReason: null }],
			groundings: [
				{
					graphId: "dso",
					snippetType: "formula",
					failed: false,
					provenance: null,
					resolvedPeriod: null,
					calendarSource: null,
				},
			],
		});
		const row = screen.getByTestId("coverage-row-capital");
		expect(within(row).getByTestId("coverage-state-badge-lit")).toBeTruthy();
		expect(
			within(row).getByTestId("coverage-metric-dso").textContent,
		).toContain("Days Sales Outstanding");
		expect(within(row).queryByTestId("coverage-reason-capital")).toBeNull();
	});

	it("marks a partial dimension with its badge and reason, grounded metric still listed", () => {
		renderMap({
			...EMPTY,
			metrics: [
				{
					graphId: "dso",
					name: "Days Sales Outstanding",
					dimensionFacet: "capital",
				},
			],
			lifecycle: [{ graphId: "dso", state: "grounded", stateReason: null }],
			groundings: [
				{
					graphId: "dso",
					snippetType: "formula",
					failed: false,
					provenance: null,
					resolvedPeriod: null,
					calendarSource: null,
				},
			],
		});
		const row = screen.getByTestId("coverage-row-capital");
		expect(
			within(row).getByTestId("coverage-state-badge-partial"),
		).toBeTruthy();
		expect(
			within(row).getByTestId("coverage-reason-capital").textContent,
		).toContain("grounded but not yet executed");
		expect(within(row).getByTestId("coverage-metric-dso")).toBeTruthy();
	});

	it("distinguishes 'no metric declared' from 'declared but never grounded'", () => {
		renderMap({
			...EMPTY,
			metrics: [{ graphId: "dso", name: "DSO", dimensionFacet: "capital" }],
			lifecycle: [{ graphId: "dso", state: "declared", stateReason: null }],
		});
		expect(
			within(screen.getByTestId("coverage-row-capital")).getByTestId(
				"coverage-reason-capital",
			).textContent,
		).toContain("declared but never grounded");
		expect(
			within(screen.getByTestId("coverage-row-offer")).getByTestId(
				"coverage-reason-offer",
			).textContent,
		).toContain("no offer unit metric declared");
	});

	it("summarises how many dimensions are lit/partial/dark", () => {
		renderMap({
			...EMPTY,
			metrics: [{ graphId: "dso", name: "DSO", dimensionFacet: "capital" }],
			lifecycle: [{ graphId: "dso", state: "executed", stateReason: null }],
			groundings: [
				{
					graphId: "dso",
					snippetType: "formula",
					failed: false,
					provenance: null,
					resolvedPeriod: null,
					calendarSource: null,
				},
			],
		});
		const summary = screen.getByTestId("coverage-map-summary");
		expect(summary.textContent).toContain("1 of 6 dimensions lit");
		expect(summary.textContent).toContain("5 dark");
	});

	it("shows an unclassified badge line for NULL-facet metrics/concepts, not a row", () => {
		renderMap({
			...EMPTY,
			metrics: [
				{ graphId: "mystery", name: "Mystery Metric", dimensionFacet: null },
			],
			concepts: [{ name: "revenue", kind: "measure", dimensionFacet: null }],
		});
		const note = screen.getByTestId("coverage-unclassified");
		expect(note.textContent).toContain("1 metric");
		expect(note.textContent).toContain("1 concept");
		for (const dim of [
			"demand",
			"offer",
			"supply",
			"capacity",
			"throughput",
			"capital",
		]) {
			expect(
				within(screen.getByTestId(`coverage-row-${dim}`)).queryByText(
					"Mystery Metric",
				),
			).toBeNull();
		}
	});

	it("omits the unclassified line entirely when nothing is unclassified", () => {
		renderMap(EMPTY);
		expect(screen.queryByTestId("coverage-unclassified")).toBeNull();
	});

	it("renders the failed-grounding reason verbatim for a partial cell", () => {
		renderMap({
			...EMPTY,
			metrics: [{ graphId: "dso", name: "DSO", dimensionFacet: "capital" }],
			lifecycle: [{ graphId: "dso", state: "executed", stateReason: null }],
			groundings: [
				{
					graphId: "dso",
					snippetType: "extract",
					failed: true,
					provenance: {
						failure_mode: "verifier_rejected",
						failure_reason: "Current assets cannot be negative (value=-1.00)",
					},
					resolvedPeriod: null,
					calendarSource: null,
				},
			],
		});
		expect(document.body.textContent).toContain(
			"Current assets cannot be negative (value=-1.00)",
		);
	});
});
