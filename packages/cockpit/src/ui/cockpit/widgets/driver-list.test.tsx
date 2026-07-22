// @vitest-environment jsdom
//
// Render tests for the DriverListWidget (DAT-579 follow-up): one row per ranked
// measure with humanized measure + target badge + grain + sample + the top driver
// dimensions, the "+N more" tail, the honest no-significant-driver case, the
// not-run / empty states, and the overflow cap (rule 15). Read-only — no click
// drill (there is no why_drivers tool).

import { MantineProvider } from "@mantine/core";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import type { DriverRanking, LookDriversResult } from "#/tools/look-drivers";
import { DriverListWidget } from "#/ui/cockpit/widgets/driver-list";
import { theme } from "#/ui/theme";

function renderWidget(look: LookDriversResult) {
	render(
		<MantineProvider theme={theme} env="test">
			<DriverListWidget state={{ kind: "driver-list", look }} />
		</MantineProvider>,
	);
}

const ranking = (over: Partial<DriverRanking> = {}): DriverRanking => ({
	measure: "net_revenue",
	target_type: "flow",
	grain: "row",
	entity: null,
	n_rows: 50000,
	status: "measured",
	abstain_reason: null,
	ranked_dimensions: [
		{ dimension: "supplier_region", gain: 0.4 },
		{ dimension: "product_category", gain: 0.2 },
	],
	driver_paths: [],
	interesting_slices: [],
	secondary_dimensions: [],
	...over,
});

const analyzed: LookDriversResult = {
	analyzed: true,
	rankings: [ranking()],
};

afterEach(cleanup);

describe("DriverListWidget (DAT-579 follow-up)", () => {
	it("renders a row per measure: humanized measure, type, sample, top drivers", () => {
		renderWidget(analyzed);
		expect(screen.getByTestId("canvas-driver-list")).toBeTruthy();
		expect(screen.getByText("Net revenue")).toBeTruthy();
		expect(screen.getByText("flow")).toBeTruthy();
		// Effective sample is shown. toLocaleString's separator is locale-specific
		// ("50,000" en-US, "50'000" de-CH), so strip every non-digit from the table
		// text and assert the bare sample — separator- and locale-agnostic.
		expect(
			screen.getByTestId("driver-rows").textContent?.replace(/\D/g, ""),
		).toContain("50000");
		// The ranked dimensions are humanized + comma-joined.
		expect(screen.getByText(/Supplier region, Product category/)).toBeTruthy();
	});

	it("labels an entity-grain measure as 'per <entity>'", () => {
		renderWidget({
			analyzed: true,
			rankings: [ranking({ grain: "entity", entity: "customer_id" })],
		});
		expect(screen.getByText("per Customer id")).toBeTruthy();
	});

	it("caps the named drivers at three and shows a '+N more' tail", () => {
		renderWidget({
			analyzed: true,
			rankings: [
				ranking({
					ranked_dimensions: [
						{ dimension: "a", gain: 0.5 },
						{ dimension: "b", gain: 0.4 },
						{ dimension: "c", gain: 0.3 },
						{ dimension: "d", gain: 0.2 },
						{ dimension: "e", gain: 0.1 },
					],
				}),
			],
		});
		expect(screen.getByText(/\+2 more/)).toBeTruthy();
	});

	it("shows the honest no-significant-driver case (empty ranking)", () => {
		renderWidget({
			analyzed: true,
			rankings: [ranking({ ranked_dimensions: [] })],
		});
		expect(screen.getByText("no significant driver")).toBeTruthy();
	});

	// DAT-859: an abstained measure gets its own distinct badge — never the
	// measured-empty "no significant driver" text, which would misreport an
	// abstention as "we tried and found nothing".
	it("renders an abstained ranking with a distinct badge, not 'no significant driver'", () => {
		renderWidget({
			analyzed: true,
			rankings: [
				ranking({
					measure: "unclassified_amount",
					target_type: "",
					status: "abstained",
					abstain_reason: "missing_inputs",
					ranked_dimensions: [],
				}),
			],
		});
		expect(screen.getByText("Abstained (Missing inputs)")).toBeTruthy();
		expect(screen.queryByText("no significant driver")).toBeNull();
	});

	it("falls back to a bare 'Abstained' badge when no reason is present", () => {
		renderWidget({
			analyzed: true,
			rankings: [
				ranking({
					status: "abstained",
					abstain_reason: null,
					ranked_dimensions: [],
				}),
			],
		});
		expect(screen.getByText("Abstained")).toBeTruthy();
	});

	// DAT-859: an abstained measure was never ranked, so it must not inflate
	// "N ranked measures" — it gets its own, separately-labeled count instead.
	it("excludes abstained measures from the ranked-measures count", () => {
		renderWidget({
			analyzed: true,
			rankings: [
				ranking({ measure: "revenue" }),
				ranking({
					measure: "unclassified_amount",
					status: "abstained",
					abstain_reason: "missing_inputs",
					ranked_dimensions: [],
				}),
			],
		});
		expect(screen.getByText(/1 ranked measure/)).toBeTruthy();
		expect(screen.getByText(/1 abstained measure/)).toBeTruthy();
	});

	it("renders the not-run state pointing at begin_session", () => {
		renderWidget({ analyzed: false, rankings: [] });
		expect(screen.getByTestId("canvas-driver-list-unanalyzed")).toBeTruthy();
	});

	it("renders the empty state for a run that ranked no measures", () => {
		renderWidget({ analyzed: true, rankings: [] });
		expect(screen.getByTestId("canvas-driver-list-empty")).toBeTruthy();
	});

	it("caps rendered rows and shows the overflow tail (rule 15)", () => {
		const many = Array.from({ length: 120 }, (_, i) =>
			ranking({ measure: `measure_${i}` }),
		);
		renderWidget({ analyzed: true, rankings: many });
		expect(screen.getAllByText("flow")).toHaveLength(100);
		expect(screen.getByTestId("driver-list-overflow").textContent).toContain(
			"…and 20 more",
		);
	});
});
